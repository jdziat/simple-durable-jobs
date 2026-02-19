package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/internal/handler"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
	"github.com/jdziat/simple-durable-jobs/pkg/schedule"
)

// Queue manages job registration, enqueueing, and processing.
type Queue struct {
	storage       core.Storage
	handlers      map[string]*handler.Handler
	scheduledJobs map[string]*ScheduledJob
	mu            sync.RWMutex

	// Hooks
	onStart    []func(context.Context, *core.Job)
	onComplete []func(context.Context, *core.Job)
	onFail     []func(context.Context, *core.Job, error)
	onRetry    []func(context.Context, *core.Job, int, error)

	// Event stream
	events    chan core.Event
	eventSubs []chan core.Event

	// Running job cancellation registry (used by workers to register cancel funcs)
	runningJobs   map[string]context.CancelFunc
	runningJobsMu sync.Mutex

	// Config
	determinism DeterminismMode
}

// ScheduledJob holds configuration for a recurring job.
type ScheduledJob struct {
	Name     string
	Schedule schedule.Schedule
	Args     any
	Options  *Options
}

// New creates a new Queue with the given storage backend.
func New(s core.Storage) *Queue {
	return &Queue{
		storage:     s,
		handlers:    make(map[string]*handler.Handler),
		determinism: ExplicitCheckpoints,
		events:      make(chan core.Event, 1000),
		runningJobs: make(map[string]context.CancelFunc),
	}
}

// Register registers a job handler function.
// The function must have signature: func(ctx context.Context, args T) error
// Job type names must be alphanumeric (starting with a letter), max 255 chars.
func (q *Queue) Register(name string, fn any, opts ...Option) {
	// Validate job type name
	if err := security.ValidateJobTypeName(name); err != nil {
		panic(fmt.Sprintf("jobs: invalid handler name %q: %v", name, err))
	}

	h, err := handler.NewHandler(fn)
	if err != nil {
		panic(fmt.Sprintf("jobs: handler for %q: %v", name, err))
	}

	// Apply registration options (e.g. Timeout)
	if len(opts) > 0 {
		o := NewOptions()
		for _, opt := range opts {
			opt.Apply(o)
		}
		h.Timeout = o.Timeout
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[name] = h
}

// HasHandler checks if a handler is registered.
func (q *Queue) HasHandler(name string) bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	_, ok := q.handlers[name]
	return ok
}

// GetHandler returns a handler by name.
func (q *Queue) GetHandler(name string) (*handler.Handler, bool) {
	q.mu.RLock()
	defer q.mu.RUnlock()
	h, ok := q.handlers[name]
	return h, ok
}

// Enqueue adds a job to the queue.
func (q *Queue) Enqueue(ctx context.Context, name string, args any, opts ...Option) (string, error) {
	q.mu.RLock()
	_, ok := q.handlers[name]
	q.mu.RUnlock()

	if !ok {
		return "", fmt.Errorf("jobs: no handler registered for %q", name)
	}

	options := NewOptions()
	for _, opt := range opts {
		opt.Apply(options)
	}

	// Validate queue name
	if err := security.ValidateQueueName(options.Queue); err != nil {
		return "", err
	}

	argsBytes, err := json.Marshal(args)
	if err != nil {
		return "", fmt.Errorf("jobs: failed to marshal args: %w", err)
	}

	// Enforce size limit on arguments
	if len(argsBytes) > security.MaxJobArgsSize {
		return "", core.ErrJobArgsTooLarge
	}

	// Clamp retries to maximum
	maxRetries := security.ClampRetries(options.MaxRetries)

	job := &core.Job{
		ID:         uuid.New().String(),
		Type:       name,
		Args:       argsBytes,
		Queue:      options.Queue,
		Priority:   options.Priority,
		MaxRetries: maxRetries,
		Status:     core.StatusPending,
	}

	if options.Delay > 0 {
		runAt := time.Now().Add(options.Delay)
		job.RunAt = &runAt
	}
	if options.RunAt != nil {
		job.RunAt = options.RunAt
	}

	// Use unique enqueue if a unique key is specified
	if options.UniqueKey != "" {
		// Validate unique key length to prevent database errors
		if err := security.ValidateUniqueKey(options.UniqueKey); err != nil {
			return "", err
		}
		if err := q.storage.EnqueueUnique(ctx, job, options.UniqueKey); err != nil {
			if errors.Is(err, core.ErrDuplicateJob) {
				return "", err
			}
			return "", fmt.Errorf("jobs: failed to enqueue: %w", err)
		}
		return job.ID, nil
	}

	if err := q.storage.Enqueue(ctx, job); err != nil {
		return "", fmt.Errorf("jobs: failed to enqueue: %w", err)
	}

	return job.ID, nil
}

// Schedule registers a recurring job.
func (q *Queue) Schedule(name string, sched schedule.Schedule, opts ...Option) {
	options := NewOptions()
	for _, opt := range opts {
		opt.Apply(options)
	}

	q.mu.Lock()
	if q.scheduledJobs == nil {
		q.scheduledJobs = make(map[string]*ScheduledJob)
	}
	q.scheduledJobs[name] = &ScheduledJob{
		Name:     name,
		Schedule: sched,
		Options:  options,
	}
	q.mu.Unlock()
}

// GetScheduledJobs returns the scheduled jobs map (for worker scheduler).
func (q *Queue) GetScheduledJobs() map[string]*ScheduledJob {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.scheduledJobs
}

// Storage returns the underlying storage.
func (q *Queue) Storage() core.Storage {
	return q.storage
}

// SetDeterminism sets the default determinism mode.
func (q *Queue) SetDeterminism(mode DeterminismMode) {
	q.determinism = mode
}

// OnJobStart registers a callback for when a job starts.
func (q *Queue) OnJobStart(fn func(context.Context, *core.Job)) {
	q.mu.Lock()
	q.onStart = append(q.onStart, fn)
	q.mu.Unlock()
}

// OnJobComplete registers a callback for when a job completes successfully.
func (q *Queue) OnJobComplete(fn func(context.Context, *core.Job)) {
	q.mu.Lock()
	q.onComplete = append(q.onComplete, fn)
	q.mu.Unlock()
}

// OnJobFail registers a callback for when a job fails permanently.
func (q *Queue) OnJobFail(fn func(context.Context, *core.Job, error)) {
	q.mu.Lock()
	q.onFail = append(q.onFail, fn)
	q.mu.Unlock()
}

// OnRetry registers a callback for when a job is retried.
func (q *Queue) OnRetry(fn func(context.Context, *core.Job, int, error)) {
	q.mu.Lock()
	q.onRetry = append(q.onRetry, fn)
	q.mu.Unlock()
}

// Events returns a channel for receiving queue events.
// The caller must call Unsubscribe when done to prevent resource leaks.
func (q *Queue) Events() <-chan core.Event {
	ch := make(chan core.Event, 100)
	q.mu.Lock()
	q.eventSubs = append(q.eventSubs, ch)
	q.mu.Unlock()
	return ch
}

// Unsubscribe removes a subscriber channel created by Events().
// The channel is not closed — callers must stop reading before calling Unsubscribe.
// After Unsubscribe returns, no further events will be sent to the channel.
func (q *Queue) Unsubscribe(ch <-chan core.Event) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for i, sub := range q.eventSubs {
		if sub == ch {
			q.eventSubs = append(q.eventSubs[:i], q.eventSubs[i+1:]...)
			return
		}
	}
}

// Emit emits an event to all subscribers.
func (q *Queue) Emit(e core.Event) {
	q.mu.RLock()
	// Make a copy of the slice to avoid race conditions
	// if Events() is called while we're iterating
	subs := make([]chan core.Event, len(q.eventSubs))
	copy(subs, q.eventSubs)
	q.mu.RUnlock()

	for _, ch := range subs {
		select {
		case ch <- e:
		default:
			// Drop if full - this prevents blocking on slow consumers
		}
	}
}

// EmitCustomEvent emits a CustomEvent for a specific job with arbitrary data.
// Custom events are ephemeral — they are broadcast to Events() subscribers
// but not persisted. Callers should persist to their own storage if needed.
func (q *Queue) EmitCustomEvent(jobID, kind string, data map[string]any) {
	q.Emit(&core.CustomEvent{
		JobID:     jobID,
		Kind:      kind,
		Data:      data,
		Timestamp: time.Now(),
	})
}

// CallStartHooks calls all registered start hooks.
func (q *Queue) CallStartHooks(ctx context.Context, job *core.Job) {
	q.mu.RLock()
	hooks := make([]func(context.Context, *core.Job), len(q.onStart))
	copy(hooks, q.onStart)
	q.mu.RUnlock()

	for _, fn := range hooks {
		fn(ctx, job)
	}
}

// CallCompleteHooks calls all registered complete hooks.
func (q *Queue) CallCompleteHooks(ctx context.Context, job *core.Job) {
	q.mu.RLock()
	hooks := make([]func(context.Context, *core.Job), len(q.onComplete))
	copy(hooks, q.onComplete)
	q.mu.RUnlock()

	for _, fn := range hooks {
		fn(ctx, job)
	}
}

// CallFailHooks calls all registered fail hooks.
func (q *Queue) CallFailHooks(ctx context.Context, job *core.Job, err error) {
	q.mu.RLock()
	hooks := make([]func(context.Context, *core.Job, error), len(q.onFail))
	copy(hooks, q.onFail)
	q.mu.RUnlock()

	for _, fn := range hooks {
		fn(ctx, job, err)
	}
}

// CallRetryHooks calls all registered retry hooks.
func (q *Queue) CallRetryHooks(ctx context.Context, job *core.Job, attempt int, err error) {
	q.mu.RLock()
	hooks := make([]func(context.Context, *core.Job, int, error), len(q.onRetry))
	copy(hooks, q.onRetry)
	q.mu.RUnlock()

	for _, fn := range hooks {
		fn(ctx, job, attempt, err)
	}
}

// WorkerFactory is set by the root package to create workers.
// This avoids import cycles between queue and worker packages.
var WorkerFactory func(q *Queue, opts ...any) core.Starter

// NewWorker creates a new worker for this queue.
// Options should be worker.WorkerOption values.
func (q *Queue) NewWorker(opts ...any) core.Starter {
	if WorkerFactory == nil {
		panic("jobs: WorkerFactory not initialized - import github.com/jdziat/simple-durable-jobs to initialize")
	}
	return WorkerFactory(q, opts...)
}

// --- Pause Options ---

// PauseOptions configures pause behavior.
type PauseOptions struct {
	Mode core.PauseMode
}

// PauseOption configures pause operations.
type PauseOption interface {
	ApplyPause(*PauseOptions)
}

type pauseModeOption struct {
	mode core.PauseMode
}

func (o pauseModeOption) ApplyPause(opts *PauseOptions) {
	opts.Mode = o.mode
}

// WithPauseMode sets the pause mode.
func WithPauseMode(mode core.PauseMode) PauseOption {
	return pauseModeOption{mode: mode}
}

// --- Running Job Registry ---

// RegisterRunningJob registers a cancel function for a running job.
// Workers call this when they start executing a job so that PauseJob
// can cancel running jobs via context cancellation.
func (q *Queue) RegisterRunningJob(jobID string, cancel context.CancelFunc) {
	q.runningJobsMu.Lock()
	q.runningJobs[jobID] = cancel
	q.runningJobsMu.Unlock()
}

// UnregisterRunningJob removes a job from the running registry.
// Workers call this when a job finishes executing.
func (q *Queue) UnregisterRunningJob(jobID string) {
	q.runningJobsMu.Lock()
	delete(q.runningJobs, jobID)
	q.runningJobsMu.Unlock()
}

// --- Job Pause Operations ---

// PauseJob pauses a specific job. For pending/waiting jobs, the status is set
// to paused in storage. For running jobs in aggressive mode, the job's context
// is cancelled via the running job registry. In graceful mode for running jobs,
// the pause flag is set so the job stops after the current phase completes.
func (q *Queue) PauseJob(ctx context.Context, jobID string, opts ...PauseOption) error {
	po := &PauseOptions{Mode: core.PauseModeGraceful}
	for _, opt := range opts {
		opt.ApplyPause(po)
	}

	err := q.storage.PauseJob(ctx, jobID)
	if err == nil {
		// Pending/waiting job paused successfully in storage
		job, getErr := q.storage.GetJob(ctx, jobID)
		if getErr == nil && job != nil {
			q.Emit(&core.JobPaused{Job: job, Mode: po.Mode, Timestamp: time.Now()})
		}
		return nil
	}

	// If the error is ErrCannotPauseStatus, the job might be running.
	// Try to cancel it via the running job registry.
	if !errors.Is(err, core.ErrCannotPauseStatus) {
		return err
	}

	// Check if the job is actually running and we have a cancel for it
	q.runningJobsMu.Lock()
	cancel, found := q.runningJobs[jobID]
	q.runningJobsMu.Unlock()

	if !found {
		// Job is in a status we can't pause and it's not in the running registry
		return err
	}

	// Cancel the running job's context
	cancel()

	// Emit event
	job, getErr := q.storage.GetJob(ctx, jobID)
	if getErr == nil && job != nil {
		q.Emit(&core.JobPaused{Job: job, Mode: po.Mode, Timestamp: time.Now()})
	}
	return nil
}

// ResumeJob resumes a paused job.
func (q *Queue) ResumeJob(ctx context.Context, jobID string) error {
	if err := q.storage.UnpauseJob(ctx, jobID); err != nil {
		return err
	}

	// Emit event
	job, err := q.storage.GetJob(ctx, jobID)
	if err == nil && job != nil {
		q.Emit(&core.JobResumed{Job: job, Timestamp: time.Now()})
	}
	return nil
}

// IsJobPaused checks if a job is paused.
func (q *Queue) IsJobPaused(ctx context.Context, jobID string) (bool, error) {
	return q.storage.IsJobPaused(ctx, jobID)
}

// GetPausedJobs returns all paused jobs in a queue.
func (q *Queue) GetPausedJobs(ctx context.Context, queueName string) ([]*core.Job, error) {
	if err := security.ValidateQueueName(queueName); err != nil {
		return nil, err
	}
	return q.storage.GetPausedJobs(ctx, queueName)
}

// --- Queue Pause Operations ---

// PauseQueue pauses an entire queue.
func (q *Queue) PauseQueue(ctx context.Context, queueName string) error {
	if err := security.ValidateQueueName(queueName); err != nil {
		return err
	}
	if err := q.storage.PauseQueue(ctx, queueName); err != nil {
		return err
	}
	q.Emit(&core.QueuePaused{Queue: queueName, Timestamp: time.Now()})
	return nil
}

// ResumeQueue resumes a paused queue.
func (q *Queue) ResumeQueue(ctx context.Context, queueName string) error {
	if err := security.ValidateQueueName(queueName); err != nil {
		return err
	}
	if err := q.storage.UnpauseQueue(ctx, queueName); err != nil {
		return err
	}
	q.Emit(&core.QueueResumed{Queue: queueName, Timestamp: time.Now()})
	return nil
}

// IsQueuePaused checks if a queue is paused.
func (q *Queue) IsQueuePaused(ctx context.Context, queueName string) (bool, error) {
	if err := security.ValidateQueueName(queueName); err != nil {
		return false, err
	}
	return q.storage.IsQueuePaused(ctx, queueName)
}

// GetPausedQueues returns all paused queue names.
func (q *Queue) GetPausedQueues(ctx context.Context) ([]string, error) {
	return q.storage.GetPausedQueues(ctx)
}

// CancelSubJob cancels a single sub-job and checks if its fan-out is now complete.
// If all sub-jobs are accounted for (completed + failed + cancelled = total), the parent
// job is automatically resumed. Returns the updated FanOut, or nil if the job is not a sub-job.
func (q *Queue) CancelSubJob(ctx context.Context, jobID string) (*core.FanOut, error) {
	fo, err := q.storage.CancelSubJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if fo == nil {
		return nil, nil // Not a sub-job
	}

	// Check if all sub-jobs are now accounted for
	total := fo.CompletedCount + fo.FailedCount + fo.CancelledCount
	if total >= fo.TotalCount && fo.Status == core.FanOutPending {
		// Mark fan-out as completed/failed
		status := core.FanOutCompleted
		if fo.FailedCount > 0 || (fo.CancelledCount > 0 && fo.CompletedCount == 0) {
			status = core.FanOutFailed
		}

		updated, err := q.storage.UpdateFanOutStatus(ctx, fo.ID, status)
		if err != nil {
			return fo, fmt.Errorf("update fan-out status: %w", err)
		}
		if updated {
			// Resume the parent job
			if _, err := q.storage.ResumeJob(ctx, fo.ParentJobID); err != nil {
				return fo, fmt.Errorf("resume parent job: %w", err)
			}
		}
	}

	return fo, nil
}
