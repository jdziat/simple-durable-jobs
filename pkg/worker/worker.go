package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/fanout"
	intctx "github.com/jdziat/simple-durable-jobs/pkg/internal/context"
	"github.com/jdziat/simple-durable-jobs/pkg/internal/handler"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
)

// Worker processes jobs from the queue.
type Worker struct {
	queue  *queue.Queue
	config WorkerConfig
	logger *slog.Logger
	wg     sync.WaitGroup
}

// NewWorker creates a new worker for the given queue.
func NewWorker(q *queue.Queue, opts ...WorkerOption) *Worker {
	config := WorkerConfig{
		Queues:       nil, // Will be set to default if no queue options provided
		PollInterval: 100 * time.Millisecond,
		WorkerID:     uuid.New().String(),
	}

	for _, opt := range opts {
		opt.ApplyWorker(&config)
	}

	// If no queues configured, use default
	if config.Queues == nil {
		config.Queues = map[string]int{"default": 10}
	}

	// Set default retry configs if not specified
	if config.StorageRetry == nil {
		defaultCfg := DefaultRetryConfig()
		config.StorageRetry = &defaultCfg
	}
	if config.DequeueRetry == nil {
		// Use longer backoff for dequeue to avoid hammering DB during outages
		dequeueCfg := RetryConfig{
			MaxAttempts:       3,
			InitialBackoff:    500 * time.Millisecond,
			MaxBackoff:        10 * time.Second,
			BackoffMultiplier: 2.0,
			JitterFraction:    0.2,
		}
		config.DequeueRetry = &dequeueCfg
	}

	return &Worker{
		queue:  q,
		config: config,
		logger: slog.Default(),
	}
}

// Start begins processing jobs. Blocks until context is cancelled.
func (w *Worker) Start(ctx context.Context) error {
	queues := make([]string, 0, len(w.config.Queues))
	for q := range w.config.Queues {
		queues = append(queues, q)
	}

	totalConcurrency := 0
	for _, c := range w.config.Queues {
		totalConcurrency += c
	}

	jobsChan := make(chan *core.Job, totalConcurrency)

	// Start scheduler if enabled
	if w.config.EnableScheduler {
		go w.runScheduler(ctx)
	}

	// Start polling for waiting jobs (fan-out fallback)
	go w.pollWaitingJobs(ctx)

	for i := 0; i < totalConcurrency; i++ {
		w.wg.Add(1)
		go w.processLoop(ctx, jobsChan)
	}

	ticker := time.NewTicker(w.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(jobsChan)
			w.wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			job, err := w.dequeueWithRetry(ctx, queues)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					w.logger.Error("failed to dequeue after retries", "error", err)
				}
				continue
			}
			if job != nil {
				select {
				case jobsChan <- job:
				case <-ctx.Done():
				}
			}
		}
	}
}

// dequeueWithRetry attempts to dequeue a job with exponential backoff on failure.
func (w *Worker) dequeueWithRetry(ctx context.Context, queues []string) (*core.Job, error) {
	var job *core.Job
	err := retryWithBackoff(ctx, *w.config.DequeueRetry, func() error {
		var dequeueErr error
		job, dequeueErr = w.queue.Storage().Dequeue(ctx, queues, w.config.WorkerID)
		return dequeueErr
	})
	return job, err
}

func (w *Worker) processLoop(ctx context.Context, jobs <-chan *core.Job) {
	defer w.wg.Done()

	for job := range jobs {
		w.processJob(ctx, job)
	}
}

func (w *Worker) processJob(ctx context.Context, job *core.Job) {
	startTime := time.Now()

	h, ok := w.queue.GetHandler(job.Type)
	if !ok {
		w.logger.Error("no handler for job", "type", job.Type)
		w.failWithRetry(ctx, job.ID, fmt.Sprintf("no handler for %s", job.Type), nil)
		return
	}

	// Call start hooks
	w.queue.CallStartHooks(ctx, job)

	// Emit start event
	w.queue.Emit(&core.JobStarted{Job: job, Timestamp: startTime})

	// Create a cancellable context for the heartbeat goroutine
	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()

	// Start heartbeat goroutine to extend lock during long-running jobs
	go w.runHeartbeat(heartbeatCtx, job)

	err := w.executeHandler(ctx, job, h)

	// Stop heartbeat before completing/failing the job
	cancelHeartbeat()

	if err != nil {
		// Check for SuspendError - job is waiting for sub-jobs
		if fanout.IsSuspendError(err) {
			w.logger.Info("job suspended waiting for sub-jobs", "job_id", job.ID)
			// Job is already suspended in storage, just return
			return
		}
		w.handleError(ctx, job, err)
	} else {
		completeErr := w.completeWithRetry(ctx, job.ID)
		if completeErr != nil {
			w.logger.Error("failed to complete job after retries", "job_id", job.ID, "error", completeErr)
			return
		}
		w.queue.CallCompleteHooks(ctx, job)
		// Emit completion event
		w.queue.Emit(&core.JobCompleted{Job: job, Duration: time.Since(startTime), Timestamp: time.Now()})

		// Handle sub-job completion (resume parent if needed)
		if err := w.handleSubJobCompletion(ctx, job, true); err != nil {
			w.logger.Error("failed to handle sub-job completion", "job_id", job.ID, "error", err)
		}
	}
}

// completeWithRetry marks a job complete with retry on transient failures.
func (w *Worker) completeWithRetry(ctx context.Context, jobID string) error {
	return retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		return w.queue.Storage().Complete(ctx, jobID, w.config.WorkerID)
	})
}

// runHeartbeat periodically extends the job lock during execution.
// This prevents long-running jobs from being reclaimed as stale.
func (w *Worker) runHeartbeat(ctx context.Context, job *core.Job) {
	// Heartbeat every 2 minutes (lock is 45 minutes, so plenty of buffer)
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
				return w.queue.Storage().Heartbeat(ctx, job.ID, w.config.WorkerID)
			})
			if err != nil {
				w.logger.Warn("heartbeat failed after retries", "job_id", job.ID, "error", err)
			} else {
				w.logger.Debug("heartbeat sent", "job_id", job.ID)
			}
		}
	}
}

func (w *Worker) executeHandler(ctx context.Context, job *core.Job, h *handler.Handler) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()

	// Load checkpoints for replay
	checkpoints, err := w.queue.Storage().GetCheckpoints(ctx, job.ID)
	if err != nil {
		return fmt.Errorf("failed to load checkpoints: %w", err)
	}

	// Create job context with all necessary references
	jc := &intctx.JobContext{
		Job:      job,
		Storage:  w.queue.Storage(),
		WorkerID: w.config.WorkerID,
		HandlerLookup: func(name string) (any, bool) {
			return w.queue.GetHandler(name)
		},
		SaveCheckpoint: func(ctx context.Context, cp *core.Checkpoint) error {
			return w.queue.Storage().SaveCheckpoint(ctx, cp)
		},
	}
	jobCtx := intctx.WithJobContext(ctx, jc)
	jobCtx = intctx.WithCallState(jobCtx, checkpoints)

	return h.Execute(jobCtx, job.Args)
}

func (w *Worker) handleError(ctx context.Context, job *core.Job, err error) {
	// Check for NoRetry
	var noRetry *core.NoRetryError
	if errors.As(err, &noRetry) {
		w.failWithRetry(ctx, job.ID, err.Error(), nil)
		w.queue.CallFailHooks(ctx, job, err)
		// Emit failure event
		w.queue.Emit(&core.JobFailed{Job: job, Error: err, Timestamp: time.Now()})
		return
	}

	// Check for RetryAfter
	var retryAfter *core.RetryAfterError
	if errors.As(err, &retryAfter) {
		if job.Attempt < job.MaxRetries {
			retryAt := time.Now().Add(retryAfter.Delay)
			w.failWithRetry(ctx, job.ID, err.Error(), &retryAt)
			w.queue.CallRetryHooks(ctx, job, job.Attempt, err)
			// Emit retry event
			w.queue.Emit(&core.JobRetrying{Job: job, Attempt: job.Attempt, Error: err, NextRunAt: retryAt, Timestamp: time.Now()})
			return
		}
	}

	// Normal retry logic
	if job.Attempt < job.MaxRetries {
		backoff := w.calculateBackoff(job.Attempt)
		retryAt := time.Now().Add(backoff)
		w.failWithRetry(ctx, job.ID, err.Error(), &retryAt)
		w.queue.CallRetryHooks(ctx, job, job.Attempt, err)
		// Emit retry event
		w.queue.Emit(&core.JobRetrying{Job: job, Attempt: job.Attempt, Error: err, NextRunAt: retryAt, Timestamp: time.Now()})
	} else {
		w.failWithRetry(ctx, job.ID, err.Error(), nil)
		w.queue.CallFailHooks(ctx, job, err)
		// Emit failure event
		w.queue.Emit(&core.JobFailed{Job: job, Error: err, Timestamp: time.Now()})

		// Handle sub-job failure (resume parent if needed)
		if handleErr := w.handleSubJobCompletion(ctx, job, false); handleErr != nil {
			w.logger.Error("failed to handle sub-job failure", "job_id", job.ID, "error", handleErr)
		}
	}
}

// failWithRetry marks a job as failed with retry on transient storage failures.
func (w *Worker) failWithRetry(ctx context.Context, jobID string, errMsg string, retryAt *time.Time) {
	err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		return w.queue.Storage().Fail(ctx, jobID, w.config.WorkerID, errMsg, retryAt)
	})
	if err != nil {
		w.logger.Error("failed to mark job as failed after retries", "job_id", jobID, "error", err)
	}
}

// handleSubJobCompletion updates fan-out counters and resumes parent if needed.
func (w *Worker) handleSubJobCompletion(ctx context.Context, job *core.Job, succeeded bool) error {
	if job.FanOutID == nil {
		return nil // Not a sub-job
	}

	var fo *core.FanOut
	var err error

	if succeeded {
		fo, err = w.queue.Storage().IncrementFanOutCompleted(ctx, *job.FanOutID)
	} else {
		fo, err = w.queue.Storage().IncrementFanOutFailed(ctx, *job.FanOutID)
	}
	if err != nil {
		return fmt.Errorf("failed to update fan-out: %w", err)
	}
	if fo == nil {
		return nil
	}

	// Check if fan-out is complete
	return w.checkFanOutCompletion(ctx, fo)
}

// checkFanOutCompletion checks if a fan-out is complete and resumes parent.
func (w *Worker) checkFanOutCompletion(ctx context.Context, fo *core.FanOut) error {
	total := fo.CompletedCount + fo.FailedCount
	if total < fo.TotalCount {
		// Not all sub-jobs done yet

		// Check fail-fast
		if fo.Strategy == core.StrategyFailFast && fo.FailedCount > 0 {
			return w.completeFanOut(ctx, fo, core.FanOutFailed)
		}

		// Check threshold
		if fo.Strategy == core.StrategyThreshold {
			maxFailures := int(float64(fo.TotalCount) * (1 - fo.Threshold))
			if fo.FailedCount > maxFailures {
				return w.completeFanOut(ctx, fo, core.FanOutFailed)
			}
		}

		return nil
	}

	// All sub-jobs done
	status := core.FanOutCompleted
	if fo.Strategy == core.StrategyFailFast && fo.FailedCount > 0 {
		status = core.FanOutFailed
	} else if fo.Strategy == core.StrategyThreshold {
		successRate := float64(fo.CompletedCount) / float64(fo.TotalCount)
		if successRate < fo.Threshold {
			status = core.FanOutFailed
		}
	}
	// CollectAll always marks as completed - error is returned to parent

	return w.completeFanOut(ctx, fo, status)
}

// completeFanOut marks a fan-out as complete and resumes the parent job.
func (w *Worker) completeFanOut(ctx context.Context, fo *core.FanOut, status core.FanOutStatus) error {
	if err := w.queue.Storage().UpdateFanOutStatus(ctx, fo.ID, status); err != nil {
		return err
	}

	// Cancel remaining sub-jobs if needed
	if status == core.FanOutFailed && fo.CancelOnFail {
		if _, err := w.queue.Storage().CancelSubJobs(ctx, fo.ID); err != nil {
			w.logger.Error("failed to cancel sub-jobs", "fan_out_id", fo.ID, "error", err)
		}
	}

	// Resume parent job
	if err := w.queue.Storage().ResumeJob(ctx, fo.ParentJobID); err != nil {
		return fmt.Errorf("failed to resume parent job: %w", err)
	}

	w.logger.Info("resumed parent job after fan-out completion",
		"parent_job_id", fo.ParentJobID,
		"fan_out_id", fo.ID,
		"status", status)

	return nil
}

func (w *Worker) calculateBackoff(attempt int) time.Duration {
	base := time.Second
	backoff := base * (1 << attempt)
	if backoff > time.Minute {
		backoff = time.Minute
	}
	return backoff
}

func (w *Worker) runScheduler(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	lastRun := make(map[string]time.Time)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			scheduled := w.queue.GetScheduledJobs()

			if scheduled == nil {
				continue
			}

			now := time.Now()
			for name, sj := range scheduled {
				nextRun := sj.Schedule.Next(lastRun[name])
				if now.After(nextRun) || now.Equal(nextRun) {
					_, err := w.queue.Enqueue(ctx, sj.Name, sj.Args,
						queue.QueueOpt(sj.Options.Queue),
						queue.Priority(sj.Options.Priority),
					)
					if err != nil {
						w.logger.Error("failed to enqueue scheduled job", "name", name, "error", err)
					} else {
						lastRun[name] = now
					}
				}
			}
		}
	}
}

// pollWaitingJobs periodically checks for waiting jobs that should be resumed.
// This is a fallback mechanism in case event-driven resume fails.
func (w *Worker) pollWaitingJobs(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			jobs, err := w.queue.Storage().GetWaitingJobsToResume(ctx)
			if err != nil {
				w.logger.Error("failed to get waiting jobs", "error", err)
				continue
			}
			for _, job := range jobs {
				if err := w.queue.Storage().ResumeJob(ctx, job.ID); err != nil {
					w.logger.Error("failed to resume waiting job", "job_id", job.ID, "error", err)
				} else {
					w.logger.Info("resumed waiting job via polling fallback", "job_id", job.ID)
				}
			}
		}
	}
}
