package queue

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v3/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/internal/handler"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/schedule"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/security"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/signal"
	"github.com/jdziat/simple-durable-jobs/v3/pkg/storage"
)

// EnqueueMiddleware wraps the enqueue operation.
// next persists the job; middleware can modify the job or context before calling next.
// Middleware must mutate the provided *core.Job in place; it must not replace it with a
// different pointer, because enqueue (and especially EnqueueBatch) reports the persisted
// ID back through that same pointer after dedup resolution.
type EnqueueMiddleware func(ctx context.Context, job *core.Job, next func(context.Context, *core.Job) error) error

// Queue manages job registration, enqueueing, and processing.
type Queue struct {
	storage       core.Storage
	handlers      map[string]*handler.Handler
	scheduledJobs map[string]*ScheduledJob
	mu            sync.RWMutex

	// Hooks
	onStart    []func(context.Context, *core.Job)
	onStartCtx []func(context.Context, *core.Job) context.Context
	onComplete []func(context.Context, *core.Job)
	onFail     []func(context.Context, *core.Job, error)
	onRetry    []func(context.Context, *core.Job, int, error)
	onReclaim  []func(context.Context, core.UUID, string)

	// Enqueue middleware chain
	enqueueMiddleware []EnqueueMiddleware

	// Execution middleware and policies
	executionMiddleware []ExecutionMiddleware
	errorHandler        func(context.Context, *core.Job, error)
	isFailure           func(*core.Job, error) bool

	// Event stream
	eventSubs     []chan core.Event
	droppedEvents atomic.Uint64

	// Running job cancellation registry (used by workers to register cancel funcs)
	runningJobs   map[core.UUID]context.CancelFunc
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
		runningJobs: make(map[core.UUID]context.CancelFunc),
	}
}

// Register registers a job handler function.
// The function must have signature: func(ctx context.Context, args T) error
// Job type names must be alphanumeric (starting with a letter), max 255 chars.
// Register panics on invalid input; use RegisterE for configuration-driven
// registration that should return validation errors instead.
func (q *Queue) Register(name string, fn any, opts ...Option) {
	if err := q.RegisterE(name, fn, opts...); err != nil {
		panic(err)
	}
}

// RegisterE registers a job handler function and returns validation errors
// instead of panicking.
//
// The function must have signature: func(ctx context.Context, args T) error.
// Job type names must be alphanumeric (starting with a letter), max 255 chars.
func (q *Queue) RegisterE(name string, fn any, opts ...Option) error {
	// Validate job type name
	if err := security.ValidateJobTypeName(name); err != nil {
		return fmt.Errorf("jobs: invalid handler name %q: %w", name, err)
	}

	h, err := handler.NewHandler(fn)
	if err != nil {
		return fmt.Errorf("jobs: handler for %q: %w", name, err)
	}

	// Apply registration options (e.g. Timeout)
	if len(opts) > 0 {
		o := NewOptions()
		for _, opt := range opts {
			opt.Apply(o)
		}
		h.Timeout = o.Timeout
		h.Backoff = o.Backoff
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[name] = h
	return nil
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

// CallDirect looks up a registered handler by name and invokes it directly
// with the given context and serialized args. Unlike Call/Enqueue, this does
// not create a job record or checkpoint — it's a synchronous in-process call.
// Use this when a workflow needs to invoke an activity handler inline.
func (q *Queue) CallDirect(ctx context.Context, name string, argsJSON []byte) error {
	q.mu.RLock()
	h, ok := q.handlers[name]
	q.mu.RUnlock()
	if !ok {
		return fmt.Errorf("jobs: no handler registered for %q", name)
	}
	_, err := h.Execute(ctx, argsJSON)
	return err
}

// Enqueue adds a job to the queue. The job type must have a registered handler.
// A Timeout option bounds this job's handler execution and overrides the
// handler's registration-time timeout for this job.
//
// args is validated against the registered handler's argument type: passing a
// different concrete struct, or a payload that cannot decode into it, returns
// ErrJobArgsMismatch at the call instead of failing far later at dispatch.
// Producers that intentionally send a dynamic or differently-shaped payload
// (no local handler) should use EnqueueRemote/EnqueueTx, which are unchecked.
func (q *Queue) Enqueue(ctx context.Context, name string, args any, opts ...Option) (core.UUID, error) {
	q.mu.RLock()
	h, ok := q.handlers[name]
	q.mu.RUnlock()

	if !ok {
		return core.NilUUID, fmt.Errorf("jobs: no handler registered for %q", name)
	}
	if err := h.ValidateArgs(name, args); err != nil {
		return core.NilUUID, err
	}

	return q.enqueue(ctx, name, args, opts...)
}

// EnqueueRemote adds a job to the queue without requiring a local handler registration.
// Use this for producer-only clients that enqueue jobs for workers in a separate process.
func (q *Queue) EnqueueRemote(ctx context.Context, name string, args any, opts ...Option) (core.UUID, error) {
	if err := security.ValidateJobTypeName(name); err != nil {
		return core.NilUUID, err
	}
	return q.enqueue(ctx, name, args, opts...)
}

// EnqueueTx adds a job through a caller-owned GORM transaction without requiring
// a local handler registration. The caller is responsible for committing or
// rolling back tx.
func (q *Queue) EnqueueTx(ctx context.Context, tx *gorm.DB, name string, args any, opts ...Option) (core.UUID, error) {
	options := NewOptions()
	for _, opt := range opts {
		opt.Apply(options)
	}
	return q.enqueueTxWithOptions(ctx, tx, name, args, options)
}

func (q *Queue) enqueue(ctx context.Context, name string, args any, opts ...Option) (core.UUID, error) {
	options := NewOptions()
	for _, opt := range opts {
		opt.Apply(options)
	}
	return q.enqueueWithOptions(ctx, name, args, options)
}

func (q *Queue) enqueueTxWithOptions(ctx context.Context, tx *gorm.DB, name string, args any, options *Options) (core.UUID, error) {
	txEnqueuer, ok := q.storage.(storage.TxEnqueuer)
	if !ok {
		return core.NilUUID, core.ErrStorageNoTxEnqueue
	}

	job, err := q.buildJob(name, args, options)
	if err != nil {
		return core.NilUUID, err
	}

	persist := func(ctx context.Context, j *core.Job) error {
		if scopeHash, ttl := uniqueLockScope(j, options); scopeHash != "" {
			txEnqueuer, ok := q.storage.(storage.TxUniqueLockEnqueuer)
			if !ok {
				return core.ErrStorageNoUniqueLocks
			}
			jobID, err := txEnqueuer.EnqueueWithUniqueLockTx(ctx, tx, j, scopeHash, ttl)
			if err != nil {
				return fmt.Errorf("jobs: failed to enqueue: %w", err)
			}
			j.ID = jobID
			return nil
		}
		if options.UniqueKey != "" {
			err := txEnqueuer.EnqueueUniqueTx(ctx, tx, j, options.UniqueKey)
			if err != nil {
				if errors.Is(err, core.ErrDuplicateJob) {
					return err
				}
				return fmt.Errorf("jobs: failed to enqueue: %w", err)
			}
			return nil
		}
		if err := txEnqueuer.EnqueueTx(ctx, tx, j); err != nil {
			return fmt.Errorf("jobs: failed to enqueue: %w", err)
		}
		return nil
	}

	if err := q.runEnqueueMiddleware(ctx, job, persist); err != nil {
		return core.NilUUID, err
	}
	return job.ID, nil
}

// enqueueWithOptions adds a job using a pre-built Options value.
func (q *Queue) enqueueWithOptions(ctx context.Context, name string, args any, options *Options) (core.UUID, error) {
	job, err := q.buildJob(name, args, options)
	if err != nil {
		return core.NilUUID, err
	}

	// Build the persist function that the middleware chain will eventually call
	persist := func(ctx context.Context, j *core.Job) error {
		if scopeHash, ttl := uniqueLockScope(j, options); scopeHash != "" {
			enqueuer, ok := q.storage.(core.UniqueLockEnqueuer)
			if !ok {
				return core.ErrStorageNoUniqueLocks
			}
			jobID, err := enqueuer.EnqueueWithUniqueLock(ctx, j, scopeHash, ttl)
			if err != nil {
				return fmt.Errorf("jobs: failed to enqueue: %w", err)
			}
			j.ID = jobID
			return nil
		}
		if options.UniqueKey != "" {
			if err := q.storage.EnqueueUnique(ctx, j, options.UniqueKey); err != nil {
				if errors.Is(err, core.ErrDuplicateJob) {
					return err
				}
				return fmt.Errorf("jobs: failed to enqueue: %w", err)
			}
			return nil
		}
		if err := q.storage.Enqueue(ctx, j); err != nil {
			return fmt.Errorf("jobs: failed to enqueue: %w", err)
		}
		return nil
	}

	// Run enqueue middleware chain
	if err := q.runEnqueueMiddleware(ctx, job, persist); err != nil {
		return core.NilUUID, err
	}

	return job.ID, nil
}

func (q *Queue) buildJob(name string, args any, options *Options) (*core.Job, error) {
	// Validate job type name
	if err := security.ValidateJobTypeName(name); err != nil {
		return nil, err
	}

	// Validate queue name
	if err := security.ValidateQueueName(options.Queue); err != nil {
		return nil, err
	}
	if options.UniqueKey != "" {
		if err := security.ValidateUniqueKey(options.UniqueKey); err != nil {
			return nil, err
		}
	}
	if options.IdempotencyKey != "" {
		if err := security.ValidateUniqueKey(options.IdempotencyKey); err != nil {
			return nil, err
		}
	}
	switch options.windowedDedup {
	case windowedDedupIdempotencyKey:
		if options.IdempotencyKey == "" {
			return nil, fmt.Errorf("%w: IdempotencyKey requires a non-empty key", core.ErrInvalidWindowedDedup)
		}
		if options.UniqueLockTTL <= 0 {
			return nil, fmt.Errorf("%w: IdempotencyKey requires a positive TTL", core.ErrInvalidWindowedDedup)
		}
	case windowedDedupUniqueFor:
		if options.UniqueLockTTL <= 0 || options.UniqueForTTL <= 0 {
			return nil, fmt.Errorf("%w: UniqueFor requires a positive TTL", core.ErrInvalidWindowedDedup)
		}
	}
	if len(options.Tenant) > security.MaxQueueNameLength {
		return nil, fmt.Errorf("jobs: tenant exceeds maximum length")
	}

	argsBytes, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("jobs: failed to marshal args: %w", err)
	}

	// Enforce size limit on arguments
	if len(argsBytes) > security.MaxJobArgsSize {
		return nil, core.ErrJobArgsTooLarge
	}
	if options.Metadata != nil && options.MaxMetadataSize > 0 {
		metadataBytes, err := json.Marshal(options.Metadata)
		if err != nil {
			return nil, fmt.Errorf("jobs: failed to marshal metadata: %w", err)
		}
		if len(metadataBytes) > options.MaxMetadataSize {
			return nil, ErrJobMetadataTooLarge
		}
	}

	// Clamp retries to maximum
	maxRetries := security.ClampRetries(options.MaxRetries)
	effDet := options.Determinism
	if !options.determinismSet {
		q.mu.RLock()
		effDet = q.determinism
		q.mu.RUnlock()
	}

	job := &core.Job{
		ID:          core.NewID(),
		Type:        name,
		Args:        argsBytes,
		Queue:       options.Queue,
		Tenant:      options.Tenant,
		Metadata:    cloneOptionsMetadata(options.Metadata),
		Priority:    options.Priority,
		MaxRetries:  maxRetries,
		UniqueKey:   options.UniqueKey,
		Determinism: int(effDet),
		Status:      core.StatusPending,
	}

	if options.Timeout > 0 {
		job.Timeout = options.Timeout
	}
	if options.Delay > 0 {
		runAt := time.Now().Add(options.Delay)
		job.RunAt = &runAt
	}
	if options.RunAt != nil {
		job.RunAt = options.RunAt
	}

	return job, nil
}

func uniqueLockScope(job *core.Job, options *Options) (string, time.Duration) {
	if job == nil || options == nil || options.UniqueLockTTL <= 0 {
		return "", 0
	}
	if options.IdempotencyKey != "" {
		return uniqueScopeHash([]byte(job.Queue), []byte(job.Type), []byte("key"), []byte(options.IdempotencyKey)), options.UniqueLockTTL
	}
	if options.UniqueForTTL > 0 {
		return uniqueScopeHash([]byte(job.Queue), []byte(job.Type), []byte("args"), job.Args), options.UniqueForTTL
	}
	return "", 0
}

func uniqueScopeHash(parts ...[]byte) string {
	h := sha256.New()
	for _, part := range parts {
		writeHashPart(h, part)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func writeHashPart(h hash.Hash, part []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(part)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(part)
}

func cloneOptionsMetadata(m *core.MetadataMap) map[string]string {
	if m == nil {
		return nil
	}
	return cloneMetadata(map[string]string(*m))
}

// EnqueueBatch adds multiple jobs to the queue in one storage operation.
//
// The entries are validated and marshaled with the same rules as EnqueueRemote,
// and enqueue middleware runs once per entry before persistence. Returned IDs
// match input order. When Unique keys collide, storage deduplicates silently;
// a returned ID for a deduped entry refers to the existing job.
func (q *Queue) EnqueueBatch(ctx context.Context, entries []BatchEntry) ([]core.UUID, error) {
	return q.enqueueBatch(ctx, entries, q.storage.EnqueueBatch)
}

// EnqueueBatchTx adds multiple jobs through a caller-owned GORM transaction.
// The caller is responsible for committing or rolling back tx.
func (q *Queue) EnqueueBatchTx(ctx context.Context, tx *gorm.DB, entries []BatchEntry) ([]core.UUID, error) {
	txEnqueuer, ok := q.storage.(storage.TxEnqueuer)
	if !ok {
		return nil, core.ErrStorageNoTxEnqueue
	}
	return q.enqueueBatch(ctx, entries, func(ctx context.Context, jobs []*core.Job) error {
		return txEnqueuer.EnqueueBatchTx(ctx, tx, jobs)
	})
}

func (q *Queue) enqueueBatch(ctx context.Context, entries []BatchEntry, enqueueBatch func(context.Context, []*core.Job) error) ([]core.UUID, error) {
	if len(entries) == 0 {
		return []core.UUID{}, nil
	}

	jobs := make([]*core.Job, len(entries))
	ids := make([]core.UUID, len(entries))
	for i, entry := range entries {
		options := NewOptions()
		for _, opt := range entry.Options {
			opt.Apply(options)
		}
		// IdempotencyKey/UniqueFor are backed by the per-row windowed unique-lock
		// path, which has no batch variant — honoring only UniqueKey here would
		// silently drop the requested dedup. Reject explicitly so the caller learns
		// the limitation instead of getting un-deduplicated jobs.
		if options.windowedDedup != windowedDedupNone {
			return nil, fmt.Errorf("%w (entry %d)", core.ErrBatchWindowedDedup, i)
		}
		job, err := q.buildJob(entry.Name, entry.Args, options)
		if err != nil {
			return nil, err
		}
		jobs[i] = job
		ids[i] = job.ID
	}

	seenUnique := make(map[string]core.UUID, len(jobs))
	for i, job := range jobs {
		if job.UniqueKey == "" {
			continue
		}
		if existingID, ok := seenUnique[job.UniqueKey]; ok {
			job.ID = existingID
			ids[i] = existingID
			continue
		}
		seenUnique[job.UniqueKey] = job.ID
	}

	toPersist := make([]*core.Job, 0, len(jobs))
	for _, job := range jobs {
		persist := func(ctx context.Context, j *core.Job) error {
			toPersist = append(toPersist, j)
			return nil
		}
		if err := q.runEnqueueMiddleware(ctx, job, persist); err != nil {
			return nil, err
		}
	}

	if err := enqueueBatch(ctx, toPersist); err != nil {
		return nil, fmt.Errorf("jobs: failed to enqueue batch: %w", err)
	}
	for i, job := range jobs {
		ids[i] = job.ID
	}
	return ids, nil
}

// runEnqueueMiddleware executes the enqueue middleware chain, ending with persist.
func (q *Queue) runEnqueueMiddleware(ctx context.Context, job *core.Job, persist func(context.Context, *core.Job) error) error {
	q.mu.RLock()
	mws := make([]EnqueueMiddleware, len(q.enqueueMiddleware))
	copy(mws, q.enqueueMiddleware)
	q.mu.RUnlock()

	// Build the chain from inside out: persist is the innermost, each middleware wraps the next
	next := persist
	for i := len(mws) - 1; i >= 0; i-- {
		mw := mws[i]
		inner := next
		next = func(ctx context.Context, j *core.Job) error {
			return mw(ctx, j, inner)
		}
	}
	return next(ctx, job)
}

// Schedule registers a recurring job.
func (q *Queue) Schedule(name string, args any, sched schedule.Schedule, opts ...Option) error {
	options := NewOptions()
	for _, opt := range opts {
		opt.Apply(options)
	}

	if sched == nil {
		return fmt.Errorf("jobs: Schedule: schedule must not be nil for %q", name)
	}

	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.handlers[name]; !ok {
		return fmt.Errorf("jobs: Schedule: no handler registered for %q", name)
	}
	if q.scheduledJobs == nil {
		q.scheduledJobs = make(map[string]*ScheduledJob)
	}
	if _, exists := q.scheduledJobs[name]; exists {
		return fmt.Errorf("jobs: Schedule: schedule already registered for %q", name)
	}
	q.scheduledJobs[name] = &ScheduledJob{
		Name:     name,
		Schedule: sched,
		Args:     args,
		Options:  options,
	}
	return nil
}

// GetScheduledJobs returns the scheduled jobs map (for worker scheduler).
func (q *Queue) GetScheduledJobs() map[string]*ScheduledJob {
	q.mu.RLock()
	defer q.mu.RUnlock()

	if q.scheduledJobs == nil {
		return nil
	}
	scheduled := make(map[string]*ScheduledJob, len(q.scheduledJobs))
	for name, job := range q.scheduledJobs {
		scheduled[name] = job
	}
	return scheduled
}

// Storage returns the underlying storage.
func (q *Queue) Storage() core.Storage {
	return q.storage
}

// Signal delivers a named signal carrying payload to a job (workflow). Signal
// names starting with "_" are reserved for library-internal primitives such as
// durable timers and are rejected with ErrSignalNameReserved. The signal is
// buffered durably, so it is not lost if sent before the handler waits for it.
// Once Signal returns nil, the signal is durably delivered in FIFO order per
// (job, name). If the target job is currently waiting on a signal, Signal wakes
// it immediately when possible; otherwise the resume poll wakes it. A failed
// immediate wake after delivery is not returned as an error.
//
// The handler receives signals with WaitForSignal / WaitForSignalTimeout /
// CheckSignal / DrainSignals. Returns ErrJobNotFound if the job does not exist,
// ErrStorageNoSignals if the backend lacks signal support,
// ErrSignalNameReserved, or ErrSignalNameTooLong.
func (q *Queue) Signal(ctx context.Context, jobID core.UUID, name string, payload any) error {
	if name == "" {
		return fmt.Errorf("jobs: signal name must not be empty")
	}
	if strings.HasPrefix(name, "_") {
		return signal.ErrSignalNameReserved
	}
	if len(name) > security.MaxSignalNameLength {
		return core.ErrSignalNameTooLong
	}
	type signalSender interface {
		SendSignal(ctx context.Context, jobID core.UUID, name string, payload []byte) error
	}
	var _ signalSender = (*storage.GormStorage)(nil)
	sender, ok := q.Storage().(signalSender)
	if !ok {
		return core.ErrStorageNoSignals
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("jobs: marshal signal payload: %w", err)
	}
	if len(data) > security.MaxResultSize {
		return fmt.Errorf("jobs: signal %q payload is %d bytes, limit is %d", name, len(data), security.MaxResultSize)
	}

	job, err := q.Storage().GetJob(ctx, jobID)
	if err != nil {
		return err
	}
	if job == nil {
		return fmt.Errorf("%w: %s", core.ErrJobNotFound, jobID)
	}

	if err := sender.SendSignal(ctx, jobID, name, data); err != nil {
		return err
	}
	q.Emit(&core.SignalDelivered{JobID: jobID, Name: name, Timestamp: time.Now()})

	// Fast path: wake a job that's currently waiting. ResumeSignalWaitingJob
	// matches StatusWaiting only (its WHERE guard closes the TOCTOU where the job
	// is paused between the GetJob read above and the resume), so a producer can
	// never un-pause an operator-paused job. The signal-resume poll backstops the
	// deliver-vs-suspend race for anything this fast path misses.
	if job.Status == core.StatusWaiting {
		if signal.WaitingOnFutureSleep(ctx, q.Storage(), job, slog.Default()) {
			return nil
		}
		type signalResumer interface {
			ResumeSignalWaitingJob(ctx context.Context, jobID core.UUID) (bool, error)
		}
		var _ signalResumer = (*storage.GormStorage)(nil)
		if r, ok := q.Storage().(signalResumer); ok {
			resumed, err := r.ResumeSignalWaitingJob(ctx, jobID)
			if err != nil {
				slog.Default().Warn("signal delivered but immediate resume failed; the resume poll will wake the job", "job_id", jobID, "name", name, "error", err)
			} else if resumed {
				q.Emit(&core.JobResumedBySignal{JobID: jobID, SignalName: name, Timestamp: time.Now()})
			}
		}
	}
	return nil
}

// Requeue resets a terminally failed or cancelled job back to pending so it
// runs again from scratch. Exhausted failed jobs are the dead-letter set; query
// them with ListDeadLettered/CountDeadLettered and replay one with Requeue.
// DLQ metadata and checkpoints are cleared so a workflow replays from the
// beginning (handlers must be idempotent regardless), which is the safe behavior
// when the usual reason to requeue is a code or dependency fix that changes the
// workflow's steps. Requeuing a fan-out parent also clears its entire fan-out
// subtree (descendant fan-outs and sub-jobs at every depth, including nested
// workflows) so the replay re-dispatches cleanly.
//
// Returns true if the job was requeued, false if it was not found or not in a
// requeuable (failed/cancelled) state. Returns ErrCannotRequeueSubJob for a
// fan-out sub-job (requeue its parent instead), or an error if the storage
// backend does not support requeueing.
func (q *Queue) Requeue(ctx context.Context, jobID core.UUID) (bool, error) {
	type requeuer interface {
		Requeue(ctx context.Context, jobID core.UUID) (bool, error)
	}
	var _ requeuer = (*storage.GormStorage)(nil)
	r, ok := q.Storage().(requeuer)
	if !ok {
		return false, fmt.Errorf("jobs: storage backend does not support Requeue")
	}
	return r.Requeue(ctx, jobID)
}

// DeadLetterOption configures dead-letter triage queries.
type DeadLetterOption func(*core.DeadLetterFilter)

// ListDeadLettered returns jobs with explicit DLQ metadata, ordered by
// dead_lettered_at descending. Replay a returned job with Requeue.
func (q *Queue) ListDeadLettered(ctx context.Context, opts ...DeadLetterOption) ([]*core.Job, error) {
	type deadLetterLister interface {
		ListDeadLettered(ctx context.Context, filter core.DeadLetterFilter) ([]*core.Job, error)
	}
	var _ deadLetterLister = (*storage.GormStorage)(nil)
	l, ok := q.Storage().(deadLetterLister)
	if !ok {
		return nil, fmt.Errorf("jobs: storage backend does not support dead-letter triage")
	}
	filter := newDeadLetterFilter(opts...)
	return l.ListDeadLettered(ctx, filter)
}

// CountDeadLettered returns the number of jobs with explicit DLQ metadata.
func (q *Queue) CountDeadLettered(ctx context.Context, opts ...DeadLetterOption) (int64, error) {
	type deadLetterCounter interface {
		CountDeadLettered(ctx context.Context, filter core.DeadLetterFilter) (int64, error)
	}
	var _ deadLetterCounter = (*storage.GormStorage)(nil)
	c, ok := q.Storage().(deadLetterCounter)
	if !ok {
		return 0, fmt.Errorf("jobs: storage backend does not support dead-letter triage")
	}
	filter := newDeadLetterFilter(opts...)
	return c.CountDeadLettered(ctx, filter)
}

// DeadLetterQueue filters dead-letter queries to one queue.
func DeadLetterQueue(queue string) DeadLetterOption {
	return func(f *core.DeadLetterFilter) {
		f.Queue = queue
	}
}

// DeadLetterType filters dead-letter queries to one job type.
func DeadLetterType(jobType string) DeadLetterOption {
	return func(f *core.DeadLetterFilter) {
		f.Type = jobType
	}
}

// DeadLetterLimit sets the maximum number of dead-lettered jobs returned.
func DeadLetterLimit(limit int) DeadLetterOption {
	return func(f *core.DeadLetterFilter) {
		f.Limit = limit
	}
}

// DeadLetterOffset sets the pagination offset for dead-lettered jobs.
func DeadLetterOffset(offset int) DeadLetterOption {
	return func(f *core.DeadLetterFilter) {
		f.Offset = offset
	}
}

func newDeadLetterFilter(opts ...DeadLetterOption) core.DeadLetterFilter {
	var filter core.DeadLetterFilter
	for _, opt := range opts {
		opt(&filter)
	}
	return filter
}

// LoadStatus returns the current status of a job by ID.
//
// Returns:
//   - The job's current status if found.
//   - An error formatted as "jobs: job not found: <id>" if no row matches the ID.
//   - The underlying storage error (unwrapped) if GetJob itself fails.
func (q *Queue) LoadStatus(ctx context.Context, jobID core.UUID) (core.JobStatus, error) {
	job, err := q.storage.GetJob(ctx, jobID)
	if err != nil {
		return "", err
	}
	if job == nil {
		return "", fmt.Errorf("%w: %s", core.ErrJobNotFound, jobID)
	}
	return job.Status, nil
}

// SetDeterminism sets the default determinism mode.
func (q *Queue) SetDeterminism(mode DeterminismMode) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.determinism = mode
}

// UseEnqueueMiddleware registers middleware that wraps the enqueue operation.
// Middleware is called in registration order, wrapping the storage persist call.
func (q *Queue) UseEnqueueMiddleware(mw EnqueueMiddleware) {
	q.mu.Lock()
	q.enqueueMiddleware = append(q.enqueueMiddleware, mw)
	q.mu.Unlock()
}

// OnJobStart registers a callback for when a job starts.
func (q *Queue) OnJobStart(fn func(context.Context, *core.Job)) {
	q.mu.Lock()
	q.onStart = append(q.onStart, fn)
	q.mu.Unlock()
}

// OnJobStartCtx registers a callback that can modify the job's context.
// The returned context replaces the job context for handler execution.
// Use this to inject trace spans or other context values before the handler runs.
func (q *Queue) OnJobStartCtx(fn func(context.Context, *core.Job) context.Context) {
	q.mu.Lock()
	q.onStartCtx = append(q.onStartCtx, fn)
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

// OnJobReclaimed registers a callback for when a job lease is reclaimed from a
// presumed-dead owner (stale-lock reaper) or observed reclaimed by a peer
// (ownership audit). The callback receives the job ID and the reclaim reason
// (core.ReclaimReasonStaleLock or core.ReclaimReasonOwnershipAudit).
func (q *Queue) OnJobReclaimed(fn func(ctx context.Context, jobID core.UUID, reason string)) {
	q.mu.Lock()
	q.onReclaim = append(q.onReclaim, fn)
	q.mu.Unlock()
}

// Events returns a best-effort channel for receiving queue events.
// Events may be dropped when a subscriber is slow and its buffer fills.
// Clients that need a complete view should periodically resync from storage.
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

// DroppedEventCount returns the number of subscriber event deliveries dropped
// because a subscriber buffer was full.
func (q *Queue) DroppedEventCount() uint64 {
	return q.droppedEvents.Load()
}

// Emit emits an event to all subscribers on a best-effort basis.
// If a subscriber buffer is full, that delivery is dropped and counted.
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
			// Drop if full - this prevents blocking on slow consumers.
			q.droppedEvents.Add(1)
		}
	}
}

// EmitCustomEvent emits a CustomEvent for a specific job with arbitrary data.
// Custom events are ephemeral — they are broadcast to Events() subscribers
// but not persisted. Callers should persist to their own storage if needed.
func (q *Queue) EmitCustomEvent(jobID core.UUID, kind string, data map[string]any) {
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

// CallStartCtxHooks calls all registered context-modifying start hooks.
// Returns the (possibly modified) context.
func (q *Queue) CallStartCtxHooks(ctx context.Context, job *core.Job) context.Context {
	q.mu.RLock()
	hooks := make([]func(context.Context, *core.Job) context.Context, len(q.onStartCtx))
	copy(hooks, q.onStartCtx)
	q.mu.RUnlock()

	for _, fn := range hooks {
		ctx = fn(ctx, job)
	}
	return ctx
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

// CallJobReclaimedHooks calls all registered job-reclaimed hooks.
func (q *Queue) CallJobReclaimedHooks(ctx context.Context, jobID core.UUID, reason string) {
	q.mu.RLock()
	hooks := make([]func(context.Context, core.UUID, string), len(q.onReclaim))
	copy(hooks, q.onReclaim)
	q.mu.RUnlock()

	for _, fn := range hooks {
		fn(ctx, jobID, reason)
	}
}

// WorkerFactory is set by the root package to create workers.
// This avoids import cycles between queue and worker packages.
var WorkerFactory func(q *Queue, opts ...any) core.Starter

// NewWorker creates a new worker for this queue.
// Options should be worker.WorkerOption values.
func (q *Queue) NewWorker(opts ...any) core.Starter {
	if WorkerFactory == nil {
		panic("jobs: WorkerFactory not initialized - import github.com/jdziat/simple-durable-jobs/v3 to initialize")
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
func (q *Queue) RegisterRunningJob(jobID core.UUID, cancel context.CancelFunc) {
	q.runningJobsMu.Lock()
	q.runningJobs[jobID] = cancel
	q.runningJobsMu.Unlock()
}

// UnregisterRunningJob removes a job from the running registry.
// Workers call this when a job finishes executing.
func (q *Queue) UnregisterRunningJob(jobID core.UUID) {
	q.runningJobsMu.Lock()
	delete(q.runningJobs, jobID)
	q.runningJobsMu.Unlock()
}

// --- Job Pause Operations ---

// PauseJob pauses a specific job. For pending/waiting jobs, the status is set
// to paused in storage. Running jobs require aggressive mode, which cancels
// the local job context when the job is running in this process. Graceful mode
// returns ErrCannotPauseStatus for running jobs.
func (q *Queue) PauseJob(ctx context.Context, jobID core.UUID, opts ...PauseOption) error {
	po := &PauseOptions{Mode: core.PauseModeGraceful}
	for _, opt := range opts {
		opt.ApplyPause(po)
	}

	// Check if the job is running locally before touching storage,
	// so we can cancel its context after the DB update.
	q.runningJobsMu.Lock()
	cancel, runningLocally := q.runningJobs[jobID]
	q.runningJobsMu.Unlock()

	type pauseModeStorage interface {
		PauseJobWithMode(ctx context.Context, jobID core.UUID, mode core.PauseMode) error
	}
	var _ pauseModeStorage = (*storage.GormStorage)(nil)
	var err error
	if pm, ok := q.storage.(pauseModeStorage); ok {
		err = pm.PauseJobWithMode(ctx, jobID, po.Mode)
	} else {
		err = q.storage.PauseJob(ctx, jobID)
	}
	if err != nil {
		return err
	}

	// If the job was running locally, cancel its context so the handler stops.
	if runningLocally && po.Mode == core.PauseModeAggressive {
		cancel()
	}

	// Emit event
	job, getErr := q.storage.GetJob(ctx, jobID)
	if getErr == nil && job != nil {
		q.Emit(&core.JobPaused{Job: job, Mode: po.Mode, Timestamp: time.Now()})
	}
	return nil
}

// CancelJob cooperatively cancels a running job by aliasing aggressive pause.
// It durably records cancellation through the storage pause path and cancels a
// locally-running handler's context; handlers that ignore ctx are not force-killed.
// Pending or waiting jobs are paused by the underlying pause operation. Already
// paused jobs, terminal jobs, and missing jobs return the same sentinel errors
// as PauseJob, such as ErrJobAlreadyPaused, ErrCannotPauseStatus, or ErrJobNotFound.
func (q *Queue) CancelJob(ctx context.Context, jobID core.UUID) error {
	return q.PauseJob(ctx, jobID, WithPauseMode(core.PauseModeAggressive))
}

// ResumeJob resumes a paused job.
func (q *Queue) ResumeJob(ctx context.Context, jobID core.UUID) error {
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
func (q *Queue) IsJobPaused(ctx context.Context, jobID core.UUID) (bool, error) {
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
func (q *Queue) CancelSubJob(ctx context.Context, jobID core.UUID) (*core.FanOut, error) {
	fo, err := q.storage.CancelSubJob(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if fo == nil {
		return nil, nil // Not a sub-job
	}

	q.runningJobsMu.Lock()
	cancel, runningLocally := q.runningJobs[jobID]
	q.runningJobsMu.Unlock()
	if runningLocally {
		cancel()
	}

	done, status := fo.TerminalStatus()
	if done && fo.Status == core.FanOutPending {
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
