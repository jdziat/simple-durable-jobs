package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
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

	// Pause state
	paused    atomic.Bool
	pauseMode atomic.Value // stores core.PauseMode

	// Running job cancellation (for aggressive pause)
	runningJobs   map[string]context.CancelFunc
	runningJobsMu sync.Mutex

	// Per-queue concurrency tracking
	queueRunning   map[string]*atomic.Int32 // queue name -> active count
	queueJobID     map[string]string        // job ID -> queue name (for decrement on completion)
	queueJobIDMu   sync.Mutex
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

	// Initialize per-queue concurrency counters
	queueRunning := make(map[string]*atomic.Int32, len(config.Queues))
	for name := range config.Queues {
		queueRunning[name] = &atomic.Int32{}
	}

	return &Worker{
		queue:        q,
		config:       config,
		logger:       slog.Default(),
		runningJobs:  make(map[string]context.CancelFunc),
		queueRunning: queueRunning,
		queueJobID:   make(map[string]string),
	}
}

// Start begins processing jobs. Blocks until context is cancelled.
// Per-queue concurrency is enforced: each queue only dequeues up to its
// configured concurrency limit.
func (w *Worker) Start(ctx context.Context) error {
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
			// Skip dequeue if paused
			if w.IsPaused() {
				continue
			}

			// Build list of queues that have available capacity
			availableQueues := w.queuesWithCapacity()
			if len(availableQueues) == 0 {
				continue
			}

			job, err := w.dequeueWithRetry(ctx, availableQueues)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					w.logger.Error("failed to dequeue after retries", "error", err)
				}
				continue
			}
			if job != nil {
				// Track this job against its queue's concurrency
				w.trackQueueJob(job.ID, job.Queue)

				select {
				case jobsChan <- job:
				case <-ctx.Done():
				}
			}
		}
	}
}

// queuesWithCapacity returns queue names that haven't reached their concurrency limit.
func (w *Worker) queuesWithCapacity() []string {
	available := make([]string, 0, len(w.config.Queues))
	for name, maxConcurrency := range w.config.Queues {
		counter, ok := w.queueRunning[name]
		if !ok {
			available = append(available, name)
			continue
		}
		if int(counter.Load()) < maxConcurrency {
			available = append(available, name)
		}
	}
	return available
}

// trackQueueJob increments the running counter for a queue and records the job→queue mapping.
func (w *Worker) trackQueueJob(jobID, queueName string) {
	if counter, ok := w.queueRunning[queueName]; ok {
		counter.Add(1)
	}
	w.queueJobIDMu.Lock()
	w.queueJobID[jobID] = queueName
	w.queueJobIDMu.Unlock()
}

// untrackQueueJob decrements the running counter for a job's queue.
func (w *Worker) untrackQueueJob(jobID string) {
	w.queueJobIDMu.Lock()
	queueName, ok := w.queueJobID[jobID]
	if ok {
		delete(w.queueJobID, jobID)
	}
	w.queueJobIDMu.Unlock()

	if ok {
		if counter, exists := w.queueRunning[queueName]; exists {
			counter.Add(-1)
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
	// Ensure per-queue concurrency counter is decremented when job finishes
	defer w.untrackQueueJob(job.ID)

	startTime := time.Now()

	h, ok := w.queue.GetHandler(job.Type)
	if !ok {
		w.logger.Error("no handler for job", "type", job.Type)
		w.failWithRetry(ctx, job.ID, fmt.Sprintf("no handler for %s", job.Type), nil)
		return
	}

	// Create cancellable context for this job
	jobCtx, cancelJob := context.WithCancel(ctx)
	defer cancelJob()

	// Track this running job for aggressive pause
	w.runningJobsMu.Lock()
	w.runningJobs[job.ID] = cancelJob
	w.runningJobsMu.Unlock()
	defer func() {
		w.runningJobsMu.Lock()
		delete(w.runningJobs, job.ID)
		w.runningJobsMu.Unlock()
	}()

	// Call start hooks
	w.queue.CallStartHooks(jobCtx, job)

	// Emit start event
	w.queue.Emit(&core.JobStarted{Job: job, Timestamp: startTime})

	// Create a cancellable context for the heartbeat goroutine
	heartbeatCtx, cancelHeartbeat := context.WithCancel(jobCtx)
	defer cancelHeartbeat()

	// Start heartbeat goroutine to extend lock during long-running jobs
	go w.runHeartbeat(heartbeatCtx, job)

	err := w.executeHandler(jobCtx, job, h)

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
			// Stop heartbeat if aggressively paused
			if w.IsPaused() && w.PauseMode() == core.PauseModeAggressive {
				return
			}

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
			// Check if the panicked value is an error - preserve type for special errors
			// like SuspendError that need type-based detection
			if e, ok := r.(error); ok {
				// Check if this is a suspend error (expected behavior from fan-out)
				if fanout.IsSuspendError(e) {
					// Don't log as panic - this is expected behavior
					w.logger.Debug("job handler signaled suspend via panic",
						"job_id", job.ID,
						"job_type", job.Type)
					err = e
					return
				}
				// Capture stack trace for debugging - critical for production troubleshooting
				stack := debug.Stack()
				w.logger.Error("job handler panicked with error",
					"job_id", job.ID,
					"job_type", job.Type,
					"error", e,
					"stack", string(stack))
				err = e
			} else {
				// Capture stack trace for debugging - critical for production troubleshooting
				stack := debug.Stack()
				w.logger.Error("job handler panicked",
					"job_id", job.ID,
					"job_type", job.Type,
					"panic", r,
					"stack", string(stack))
				err = fmt.Errorf("panic: %v", r)
			}
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
		// Handle sub-job failure (resume parent if needed)
		if handleErr := w.handleSubJobCompletion(ctx, job, false); handleErr != nil {
			w.logger.Error("failed to handle sub-job failure", "job_id", job.ID, "error", handleErr)
		}
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
// Uses retry to prevent lost increments that would leave parent jobs stuck forever.
func (w *Worker) handleSubJobCompletion(ctx context.Context, job *core.Job, succeeded bool) error {
	if job.FanOutID == nil {
		return nil // Not a sub-job
	}

	var fo *core.FanOut
	var err error

	// Retry the increment to prevent lost counts (which cause stuck parents).
	err = retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		var incrementErr error
		if succeeded {
			fo, incrementErr = w.queue.Storage().IncrementFanOutCompleted(ctx, *job.FanOutID)
		} else {
			fo, incrementErr = w.queue.Storage().IncrementFanOutFailed(ctx, *job.FanOutID)
		}
		return incrementErr
	})
	if err != nil {
		return fmt.Errorf("failed to update fan-out after retries: %w", err)
	}
	if fo == nil {
		return nil
	}

	// Check if fan-out is complete
	return w.checkFanOutCompletion(ctx, fo)
}

// checkFanOutCompletion checks if a fan-out is complete and resumes parent.
func (w *Worker) checkFanOutCompletion(ctx context.Context, fo *core.FanOut) error {
	total := fo.CompletedCount + fo.FailedCount + fo.CancelledCount
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

	// All sub-jobs done (completed + failed + cancelled = total)
	status := core.FanOutCompleted
	if fo.Strategy == core.StrategyFailFast && (fo.FailedCount > 0 || fo.CancelledCount > 0) {
		status = core.FanOutFailed
	} else if fo.Strategy == core.StrategyThreshold {
		activeTotal := fo.CompletedCount + fo.FailedCount
		if activeTotal > 0 {
			successRate := float64(fo.CompletedCount) / float64(activeTotal)
			if successRate < fo.Threshold {
				status = core.FanOutFailed
			}
		} else {
			// All jobs cancelled, no successes
			status = core.FanOutFailed
		}
	} else if fo.CancelledCount > 0 && fo.CompletedCount == 0 {
		// All jobs cancelled with no completions
		status = core.FanOutFailed
	}
	// CollectAll always marks as completed - error is returned to parent

	return w.completeFanOut(ctx, fo, status)
}

// completeFanOut marks a fan-out as complete and resumes the parent job.
// Uses atomic status update to prevent race conditions when multiple workers
// complete the last sub-jobs simultaneously.
func (w *Worker) completeFanOut(ctx context.Context, fo *core.FanOut, status core.FanOutStatus) error {
	// Atomic update: only succeeds if status is still 'pending'
	// This prevents race where two workers both try to complete
	updated, err := w.queue.Storage().UpdateFanOutStatus(ctx, fo.ID, status)
	if err != nil {
		return err
	}
	if !updated {
		// Another worker already completed this fan-out
		w.logger.Debug("fan-out already completed by another worker", "fan_out_id", fo.ID)
		return nil
	}

	// Cancel remaining sub-jobs if needed
	if status == core.FanOutFailed && fo.CancelOnFail {
		if _, err := w.queue.Storage().CancelSubJobs(ctx, fo.ID); err != nil {
			w.logger.Error("failed to cancel sub-jobs", "fan_out_id", fo.ID, "error", err)
		}
	}

	// Resume parent job
	resumed, err := w.queue.Storage().ResumeJob(ctx, fo.ParentJobID)
	if err != nil {
		return fmt.Errorf("failed to resume parent job: %w", err)
	}
	if !resumed {
		w.logger.Warn("parent job was not in waiting status", "parent_job_id", fo.ParentJobID)
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
	return min(backoff, time.Minute)
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
			var jobs []*core.Job
			err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
				var queryErr error
				jobs, queryErr = w.queue.Storage().GetWaitingJobsToResume(ctx)
				return queryErr
			})
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					w.logger.Error("failed to get waiting jobs after retries", "error", err)
				}
				continue
			}
			for _, job := range jobs {
				resumeErr := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
					_, err := w.queue.Storage().ResumeJob(ctx, job.ID)
					return err
				})
				if resumeErr != nil {
					w.logger.Error("failed to resume waiting job after retries", "job_id", job.ID, "error", resumeErr)
				} else {
					w.logger.Info("resumed waiting job via polling fallback", "job_id", job.ID)
				}
			}
		}
	}
}

// Pause pauses the worker.
func (w *Worker) Pause(mode core.PauseMode) {
	w.pauseMode.Store(mode)
	w.paused.Store(true)

	if mode == core.PauseModeAggressive {
		// Cancel all running jobs
		w.runningJobsMu.Lock()
		for _, cancel := range w.runningJobs {
			cancel()
		}
		w.runningJobsMu.Unlock()
	}

	// Emit event
	w.queue.Emit(&core.WorkerPaused{
		WorkerID:  w.config.WorkerID,
		Mode:      mode,
		Timestamp: time.Now(),
	})
}

// CancelJob cancels a specific running job's context.
// Returns true if the job was found and cancelled.
func (w *Worker) CancelJob(jobID string) bool {
	w.runningJobsMu.Lock()
	cancel, ok := w.runningJobs[jobID]
	w.runningJobsMu.Unlock()
	if ok {
		cancel()
	}
	return ok
}

// Resume resumes the worker.
func (w *Worker) Resume() {
	w.paused.Store(false)

	// Emit event
	w.queue.Emit(&core.WorkerResumed{
		WorkerID:  w.config.WorkerID,
		Timestamp: time.Now(),
	})
}

// IsPaused returns true if the worker is paused.
func (w *Worker) IsPaused() bool {
	return w.paused.Load()
}

// PauseMode returns the current pause mode.
func (w *Worker) PauseMode() core.PauseMode {
	mode := w.pauseMode.Load()
	if mode == nil {
		return core.PauseModeGraceful
	}
	return mode.(core.PauseMode)
}

// RunningJobCount returns the number of currently running jobs.
func (w *Worker) RunningJobCount() int {
	w.runningJobsMu.Lock()
	defer w.runningJobsMu.Unlock()
	return len(w.runningJobs)
}

// WaitForPause blocks until all running jobs complete or the timeout expires.
// Returns nil if all jobs completed, or an error if timeout was reached.
// The worker must be paused before calling this method.
func (w *Worker) WaitForPause(timeout time.Duration) error {
	if !w.IsPaused() {
		return errors.New("worker is not paused")
	}

	deadline := time.Now().Add(timeout)
	pollInterval := 50 * time.Millisecond

	for {
		w.runningJobsMu.Lock()
		count := len(w.runningJobs)
		w.runningJobsMu.Unlock()

		if count == 0 {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timeout waiting for %d running jobs to complete", count)
		}

		time.Sleep(pollInterval)
	}
}
