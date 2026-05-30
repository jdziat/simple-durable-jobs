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
	queueRunning map[string]*atomic.Int32 // queue name -> active count
	queueJobID   map[string]string        // job ID -> queue name (for decrement on completion)
	queueJobIDMu sync.Mutex

	// heartbeatInterval is the tick rate for runHeartbeat. Defaults to
	// 2 minutes; tests override with a sub-second value. Not exposed via
	// WorkerConfig because changing it in production would change lock
	// contention semantics — the 2-minute default is paired with the
	// 45-minute lock expiry assumed elsewhere.
	heartbeatInterval time.Duration
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

	// Set default stale lock reaper cadence. The reaper always runs (it
	// recovers jobs from crashed workers and cannot be disabled), so a
	// non-positive interval simply falls back to the 5m default.
	if config.StaleLockInterval <= 0 {
		config.StaleLockInterval = 5 * time.Minute
	}
	if config.StaleLockAge == 0 {
		config.StaleLockAge = 45 * time.Minute
	}
	if config.FanOutRecoveryStaleAge <= 0 {
		config.FanOutRecoveryStaleAge = 2 * time.Minute
	}

	// Propagate lock duration to the storage backend if supported.
	// The storage must implement SetLockDuration(time.Duration) for this to take effect.
	if config.LockDuration > 0 {
		type lockDurationSetter interface {
			SetLockDuration(time.Duration)
		}
		if setter, ok := q.Storage().(lockDurationSetter); ok {
			setter.SetLockDuration(config.LockDuration)
		}
	}

	// Initialize per-queue concurrency counters
	queueRunning := make(map[string]*atomic.Int32, len(config.Queues))
	for name := range config.Queues {
		queueRunning[name] = &atomic.Int32{}
	}

	// Default OwnershipAuditInterval to 5s only when it was never set. An
	// explicit WithOwnershipAuditInterval(0) is honored as "disable" (the
	// Start guard below skips the goroutine); without the ownershipAuditSet
	// flag we couldn't tell that apart from "unset".
	if !config.ownershipAuditSet && config.OwnershipAuditInterval == 0 {
		config.OwnershipAuditInterval = 5 * time.Second
	}

	return &Worker{
		queue:             q,
		config:            config,
		logger:            slog.Default(),
		runningJobs:       make(map[string]context.CancelFunc),
		queueRunning:      queueRunning,
		queueJobID:        make(map[string]string),
		heartbeatInterval: 2 * time.Minute,
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

	// Start the stale-lock reaper to reclaim jobs whose owning worker died.
	// This always runs — it's the only recovery path for crashed workers, so
	// it cannot be disabled (NewWorker guarantees a positive interval).
	go w.reapStaleLocks(ctx)

	// Start ownership audit to cancel local handlers for jobs cancelled
	// or reclaimed by other workers. Same-worker cancellation is handled
	// directly by completeFanOut/reapStaleLocks; this is the cross-worker
	// counterpart.
	if w.config.OwnershipAuditInterval > 0 {
		go w.runOwnershipAudit(ctx)
	}

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
		// No fan-out side effects here, so the lost-ownership return value is
		// not actionable — failWithRetry already logs any real failure.
		_ = w.failWithRetry(ctx, job.ID, fmt.Sprintf("no handler for %s", job.Type), nil)
		return
	}

	// Create context for this job — with timeout if handler specifies one
	var jobCtx context.Context
	var cancelJob context.CancelFunc
	if h.Timeout > 0 {
		jobCtx, cancelJob = context.WithTimeout(ctx, h.Timeout)
	} else {
		jobCtx, cancelJob = context.WithCancel(ctx)
	}
	defer cancelJob()

	// Track this running job for aggressive pause (worker-local + queue-level registry)
	w.runningJobsMu.Lock()
	w.runningJobs[job.ID] = cancelJob
	w.runningJobsMu.Unlock()
	w.queue.RegisterRunningJob(job.ID, cancelJob)
	defer func() {
		w.runningJobsMu.Lock()
		delete(w.runningJobs, job.ID)
		w.runningJobsMu.Unlock()
		w.queue.UnregisterRunningJob(job.ID)
	}()

	// Call start hooks
	w.queue.CallStartHooks(jobCtx, job)

	// Call context-modifying start hooks (e.g. OTel span injection)
	jobCtx = w.queue.CallStartCtxHooks(jobCtx, job)

	// Emit start event
	w.queue.Emit(&core.JobStarted{Job: job, Timestamp: startTime})

	// Create a cancellable context for the heartbeat goroutine
	heartbeatCtx, cancelHeartbeat := context.WithCancel(jobCtx)
	defer cancelHeartbeat()

	// Start heartbeat goroutine to extend lock during long-running jobs
	go w.runHeartbeat(heartbeatCtx, job)

	resultBytes, err := w.executeHandler(jobCtx, job, h)

	// Stop heartbeat before completing/failing the job
	cancelHeartbeat()

	if err != nil {
		// Check for WaitingError - job is waiting for sub-jobs
		if fanout.IsWaitingError(err) {
			w.logger.Info("job waiting for sub-jobs", "job_id", job.ID)
			// Job is already in StatusWaiting; just return
			return
		}
		w.handleError(ctx, job, err)
	} else {
		if resultBytes != nil {
			if saveErr := w.queue.Storage().SaveJobResult(ctx, job.ID, w.config.WorkerID, resultBytes); saveErr != nil {
				w.logger.Error("failed to persist job result", "job_id", job.ID, "error", saveErr)
				// fall through — completion still proceeds
			}
		}
		completeErr := w.completeWithRetry(ctx, job.ID)
		if errors.Is(completeErr, core.ErrJobNotOwned) {
			// The job was reclaimed, cancelled, or already completed by
			// another path while this handler was running. The worker that
			// now owns it is responsible for fan-out accounting; doing it
			// here would double-count the fan-out and race the new owner.
			w.logger.Warn("job no longer owned at completion; skipping completion handling",
				"job_id", job.ID)
			return
		}
		if completeErr != nil {
			w.logger.Error("failed to complete job after retries", "job_id", job.ID, "error", completeErr)
			// Still handle sub-job completion — the work is done even if
			// we couldn't mark it in the DB. Without this, the fan-out
			// counter never increments and the parent stays in 'waiting'.
		} else {
			w.queue.CallCompleteHooks(ctx, job)
			// Emit completion event
			w.queue.Emit(&core.JobCompleted{Job: job, Duration: time.Since(startTime), Timestamp: time.Now()})
		}

		// Handle sub-job completion (resume parent if needed) — always run
		// regardless of whether Complete() succeeded, to prevent orphaned
		// parent jobs stuck in 'waiting' with all sub-jobs actually done.
		// (The lost-ownership case returned above.)
		if err := w.handleSubJobCompletion(ctx, job, completeErr == nil); err != nil {
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

// orphanHeartbeatThreshold is the number of consecutive Heartbeat calls
// returning ErrJobNotOwned that runHeartbeat will tolerate before
// concluding the job has been reclaimed by another worker (via stale-lock
// recovery) and cancelling the in-flight handler.
//
// Set to 3 so a transient ownership blip — e.g. a clock skew between the
// worker and the DB at the moment of a lock-renewal race — doesn't kill
// a legitimate run. With a 2-minute tick, 3 consecutive failures = 6
// minutes of confirmed orphaning, which is well past any normal lock
// contention window.
const orphanHeartbeatThreshold = 3

// runHeartbeat periodically extends the job lock during execution.
// This prevents long-running jobs from being reclaimed as stale.
//
// If the heartbeat repeatedly receives core.ErrJobNotOwned, the handler
// is presumed orphaned (the stale-lock reaper at line 708 has released
// the lock and another worker has picked the job up). In that case
// runHeartbeat cancels the handler's context via CancelJob and returns,
// so:
//  1. The handler stops doing wasted work against a job it doesn't own.
//  2. The "heartbeat failed after retries / jobs: job not owned by this
//     worker" log line stops repeating forever — observed in production
//     on 2026-05-19 firing every ~2 minutes for HOURS after the job
//     was reclaimed.
//  3. Activities the orphaned handler had spawned in goroutines (e.g.
//     FireAndForgetNotification) stop racing the new handler's state
//     transitions.
//
// Non-ownership errors (DB unreachable, retry exhaustion on a transient
// error) are logged but don't trip the counter — those are operational
// issues to fix elsewhere, not orphaning.
func (w *Worker) runHeartbeat(ctx context.Context, job *core.Job) {
	// Heartbeat every 2 minutes (lock is 45 minutes, so plenty of buffer).
	// Tests override w.heartbeatInterval directly to drive the loop at
	// sub-second speed.
	interval := w.heartbeatInterval
	if interval <= 0 {
		interval = 2 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var consecutiveOrphanErrs int

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
			switch {
			case err == nil:
				consecutiveOrphanErrs = 0
				w.logger.Debug("heartbeat sent", "job_id", job.ID)
			case errors.Is(err, core.ErrJobNotOwned):
				consecutiveOrphanErrs++
				w.logger.Warn("heartbeat failed: job not owned by this worker",
					"job_id", job.ID,
					"consecutive_orphan_errs", consecutiveOrphanErrs,
					"threshold", orphanHeartbeatThreshold)
				if consecutiveOrphanErrs >= orphanHeartbeatThreshold {
					w.logger.Error("heartbeat abandoning orphaned job — cancelling handler",
						"job_id", job.ID,
						"consecutive_orphan_errs", consecutiveOrphanErrs)
					w.CancelJob(job.ID)
					return
				}
			default:
				// Some other error (DB down, retry exhaustion on a transient
				// failure, etc.). Log but don't trip the orphan counter — these
				// are operational concerns, not ownership transfer.
				consecutiveOrphanErrs = 0
				w.logger.Warn("heartbeat failed after retries", "job_id", job.ID, "error", err)
			}
		}
	}
}

func (w *Worker) executeHandler(ctx context.Context, job *core.Job, h *handler.Handler) (resultBytes []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			// Check if the panicked value is an error - preserve type for special errors
			// like WaitingError that need type-based detection
			if e, ok := r.(error); ok {
				// Check if this is a waiting signal (expected behavior from fan-out)
				if fanout.IsWaitingError(e) {
					// Don't log as panic - this is expected behavior
					w.logger.Debug("job handler signaled waiting via panic",
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
		return nil, fmt.Errorf("failed to load checkpoints: %w", err)
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
	// Decide the disposition: a scheduled retry (retryAt != nil) or a terminal
	// failure (retryAt == nil). NoRetry always wins; otherwise we retry while
	// attempts remain. This mirrors the original branch-by-branch logic.
	var retryAt *time.Time
	var noRetry *core.NoRetryError
	var retryAfter *core.RetryAfterError
	switch {
	case errors.As(err, &noRetry):
		// terminal — NoRetry overrides any remaining attempts.
	case errors.As(err, &retryAfter) && job.Attempt < job.MaxRetries:
		t := time.Now().Add(retryAfter.Delay)
		retryAt = &t
	case job.Attempt < job.MaxRetries:
		t := time.Now().Add(w.calculateBackoff(job.Attempt))
		retryAt = &t
	default:
		// terminal — attempts exhausted.
	}

	// Persist the outcome first. If storage reports the job is no longer owned
	// by this worker, it was reclaimed or cancelled by another path (the
	// stale-lock reaper, a fan-out cancel, or another worker in the fleet —
	// often the very reason this handler's context was cancelled). The owner
	// is now responsible for hooks, events, and fan-out accounting; running
	// them here would race the new owner's state writes and double-count the
	// fan-out. So skip all side effects when ownership is lost.
	if failErr := w.failWithRetry(ctx, job.ID, err.Error(), retryAt); errors.Is(failErr, core.ErrJobNotOwned) {
		w.logger.Warn("job no longer owned by this worker; skipping failure handling",
			"job_id", job.ID, "error", err)
		return
	}

	if retryAt != nil {
		w.queue.CallRetryHooks(ctx, job, job.Attempt, err)
		w.queue.Emit(&core.JobRetrying{Job: job, Attempt: job.Attempt, Error: err, NextRunAt: *retryAt, Timestamp: time.Now()})
		return
	}

	// Terminal failure.
	w.queue.CallFailHooks(ctx, job, err)
	w.queue.Emit(&core.JobFailed{Job: job, Error: err, Timestamp: time.Now()})
	// Handle sub-job failure (resume parent if needed).
	if handleErr := w.handleSubJobCompletion(ctx, job, false); handleErr != nil {
		w.logger.Error("failed to handle sub-job failure", "job_id", job.ID, "error", handleErr)
	}
}

// failWithRetry marks a job as failed with retry on transient storage failures.
// It returns the final storage error so callers can detect a lost-ownership
// outcome (core.ErrJobNotOwned) and skip downstream side effects.
func (w *Worker) failWithRetry(ctx context.Context, jobID string, errMsg string, retryAt *time.Time) error {
	err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		return w.queue.Storage().Fail(ctx, jobID, w.config.WorkerID, errMsg, retryAt)
	})
	// ErrJobNotOwned is an expected, caller-handled outcome — don't log it as
	// an error here (the caller decides what to do about lost ownership).
	if err != nil && !errors.Is(err, core.ErrJobNotOwned) {
		w.logger.Error("failed to mark job as failed after retries", "job_id", jobID, "error", err)
	}
	return err
}

// handleSubJobCompletion updates fan-out counters and resumes parent if needed.
// Uses retry to prevent lost increments that would leave parent jobs stuck forever.
func (w *Worker) handleSubJobCompletion(ctx context.Context, job *core.Job, succeeded bool) error {
	if job.FanOutID == nil {
		return nil // Not a sub-job
	}

	var fo *core.FanOut

	// Retry the increment to prevent lost counts (which cause stuck parents).
	err := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
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

	// Cancel remaining sub-jobs if needed. CancelSubJobs only updates the
	// DB rows — to actually stop the in-flight handlers we have to cancel
	// their contexts via w.CancelJob (one entry per local sub-job in the
	// runningJobs map). Sub-jobs running on OTHER workers in the fleet
	// won't see this signal directly; they'll notice via their heartbeat
	// returning ErrJobNotOwned and abandon after the configured threshold
	// (see runHeartbeat).
	if status == core.FanOutFailed && fo.CancelOnFail {
		cancelledIDs, err := w.queue.Storage().CancelSubJobs(ctx, fo.ID)
		if err != nil {
			w.logger.Error("failed to cancel sub-jobs", "fan_out_id", fo.ID, "error", err)
		} else {
			cancelledLocally := 0
			for _, jobID := range cancelledIDs {
				if w.CancelJob(jobID) {
					cancelledLocally++
				}
			}
			if cancelledLocally > 0 {
				w.logger.Info("cancelled in-flight sub-job handlers on this worker",
					"fan_out_id", fo.ID,
					"cancelled_locally", cancelledLocally,
					"cancelled_total", len(cancelledIDs))
			}
		}
	}

	// Resume parent job — retry a few times because the parent might still be
	// transitioning from running → waiting (SuspendJob hasn't completed yet).
	var resumed bool
	for attempt := 0; attempt < 5; attempt++ {
		resumed, err = w.queue.Storage().ResumeJob(ctx, fo.ParentJobID)
		if err != nil {
			return fmt.Errorf("failed to resume parent job: %w", err)
		}
		if resumed {
			break
		}
		// Parent not in waiting/paused status yet — wait briefly and retry
		if attempt < 4 {
			w.logger.Debug("parent job not yet in resumable status, retrying",
				"parent_job_id", fo.ParentJobID,
				"attempt", attempt+1)
			time.Sleep(time.Duration(100*(1<<attempt)) * time.Millisecond) // 100ms, 200ms, 400ms, 800ms
		}
	}
	if !resumed {
		w.logger.Error("CRITICAL: parent job could not be resumed after retries — may be stuck",
			"parent_job_id", fo.ParentJobID,
			"fan_out_id", fo.ID)
	}

	w.logger.Info("resumed parent job after fan-out completion",
		"parent_job_id", fo.ParentJobID,
		"fan_out_id", fo.ID,
		"status", status,
		"resumed", resumed)

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
				if _, ok := lastRun[name]; !ok {
					lastRun[name] = now
				}
				nextRun := sj.Schedule.Next(lastRun[name])
				if now.After(nextRun) || now.Equal(nextRun) {
					claimed, err := w.queue.Storage().ClaimScheduledFire(ctx, name, nextRun)
					if err != nil {
						w.logger.Error("failed to claim scheduled fire", "name", name, "fire_time", nextRun, "error", err)
						continue
					}
					if !claimed {
						lastRun[name] = nextRun
						continue
					}
					lastRun[name] = nextRun
					opts := []queue.Option{
						queue.QueueOpt(sj.Options.Queue),
						queue.Priority(sj.Options.Priority),
						queue.Retries(sj.Options.MaxRetries),
					}
					if sj.Options.UniqueKey != "" {
						opts = append(opts, queue.Unique(sj.Options.UniqueKey))
					}
					if sj.Options.Delay > 0 {
						opts = append(opts, queue.Delay(sj.Options.Delay))
					}
					if sj.Options.RunAt != nil {
						opts = append(opts, queue.At(*sj.Options.RunAt))
					}
					if sj.Options.Timeout > 0 {
						opts = append(opts, queue.Timeout(sj.Options.Timeout))
					}
					_, err = w.queue.Enqueue(ctx, sj.Name, sj.Args,
						opts...,
					)
					if err != nil {
						w.logger.Error("failed to enqueue scheduled job", "name", name, "error", err)
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
			w.pollWaitingJobsOnce(ctx)
		}
	}
}

func (w *Worker) pollWaitingJobsOnce(ctx context.Context) {
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
		return
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

	stalledCutoff := time.Now().Add(-w.config.FanOutRecoveryStaleAge)
	var stalled []*core.Job
	err = retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
		var queryErr error
		stalled, queryErr = w.queue.Storage().GetStalledFanOutParents(ctx, stalledCutoff)
		return queryErr
	})
	if err != nil {
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			w.logger.Error("failed to get stalled fan-out parents after retries", "error", err)
		}
		return
	}
	for _, job := range stalled {
		resumeErr := retryWithBackoff(ctx, *w.config.StorageRetry, func() error {
			_, err := w.queue.Storage().ResumeJob(ctx, job.ID)
			return err
		})
		if resumeErr != nil {
			w.logger.Error("failed to resume stalled fan-out parent after retries", "job_id", job.ID, "error", resumeErr)
		} else {
			w.logger.Info("resumed stalled fan-out parent via polling fallback", "job_id", job.ID)
		}
	}
}

// reapStaleLocks periodically releases locks on jobs that are stuck in running
// status with expired locks. This handles cases where:
// - A worker crashed without properly completing/failing the job
// - Complete/Fail failed due to ErrJobNotOwned (lock expired during processing)
// - A handler hung and the heartbeat eventually stopped
func (w *Worker) reapStaleLocks(ctx context.Context) {
	// Defensive: NewWorker guarantees a positive interval, but guard against a
	// zero value (which would panic time.NewTicker) in case the config field
	// is set directly.
	interval := w.config.StaleLockInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			released, err := w.queue.Storage().ReleaseStaleLocks(ctx, w.config.StaleLockAge)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					w.logger.Error("failed to release stale locks", "error", err)
				}
				continue
			}
			if len(released) == 0 {
				continue
			}

			// Cancel any local in-flight handlers for jobs whose locks
			// were just released. The DB-level release already reverted
			// the lock fields; without this loop the original handler
			// would keep running until its own heartbeat-abandon timer
			// fires (~6 minutes by default). This brings the local
			// cancel latency down to "next heartbeat tick."
			cancelledLocally := 0
			for _, jobID := range released {
				if w.CancelJob(jobID) {
					cancelledLocally++
				}
			}
			w.logger.Info("released stale running jobs",
				"count", len(released),
				"cancelled_locally", cancelledLocally)
		}
	}
}

// runOwnershipAudit periodically checks whether any of this worker's
// running jobs have been cancelled or reclaimed by another worker, and
// cancels the corresponding local handler context. This is the
// cross-worker counterpart of the cancellation logic in completeFanOut
// and reapStaleLocks (which only see local sub-jobs).
//
// The query cost is one row per running job per tick — bounded by THIS
// worker's concurrency, not by the size of the fleet.
//
// No grace window is needed for newly-acquired jobs: a job only enters
// runningJobs (in processJob) after Dequeue has returned, and Dequeue
// commits locked_by=this-worker before returning. So any ID in the
// snapshot already has its ownership row persisted, and a freshly
// dequeued job can't be mis-flagged as orphaned.
func (w *Worker) runOwnershipAudit(ctx context.Context) {
	ticker := time.NewTicker(w.config.OwnershipAuditInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Snapshot the IDs we think we own. Holding the mutex during
			// the DB call would block dequeue/complete; copy and release.
			w.runningJobsMu.Lock()
			ids := make([]string, 0, len(w.runningJobs))
			for id := range w.runningJobs {
				ids = append(ids, id)
			}
			w.runningJobsMu.Unlock()
			if len(ids) == 0 {
				continue
			}

			orphaned, err := w.queue.Storage().FindOrphanedJobs(ctx, ids, w.config.WorkerID)
			if err != nil {
				if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
					w.logger.Warn("ownership audit query failed", "error", err)
				}
				continue
			}
			if len(orphaned) == 0 {
				continue
			}

			cancelled := 0
			for _, id := range orphaned {
				if w.CancelJob(id) {
					cancelled++
				}
			}
			if cancelled > 0 {
				w.logger.Warn("ownership audit cancelled orphaned local handlers",
					"orphaned_count", len(orphaned),
					"cancelled_count", cancelled,
					"audit_interval", w.config.OwnershipAuditInterval)
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
