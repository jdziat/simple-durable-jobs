// Package jobs provides a durable job queue with checkpointed nested calls.
//
// This is the main package users should import. It re-exports all public
// types from the internal pkg/ packages for a clean API surface.
//
// Basic usage:
//
//	// Create storage and queue. The SQLite DSN parameters are required for safe
//	// concurrent workers (WAL + busy_timeout + immediate transactions).
//	db, _ := gorm.Open(sqlite.Open("jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"), &gorm.Config{})
//	store := jobs.NewGormStorage(db)
//	store.Migrate(context.Background())
//	queue := jobs.New(store)
//
//	// Register handler
//	queue.Register("send-email", func(ctx context.Context, email string) error {
//	    return sendEmail(email)
//	})
//
//	// Enqueue job
//	queue.Enqueue(ctx, "send-email", "user@example.com")
//
//	// Start worker
//	worker := queue.NewWorker()
//	worker.Start(ctx)
package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/pkg/call"
	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/fanout"
	"github.com/jdziat/simple-durable-jobs/pkg/jobctx"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/schedule"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
	"github.com/jdziat/simple-durable-jobs/pkg/signal"
	"github.com/jdziat/simple-durable-jobs/pkg/storage"
	"github.com/jdziat/simple-durable-jobs/pkg/worker"
)

func init() {
	// Register the worker factory to enable queue.NewWorker()
	queue.WorkerFactory = func(q *queue.Queue, opts ...any) core.Starter {
		workerOpts := make([]worker.WorkerOption, 0, len(opts))
		for _, opt := range opts {
			if wo, ok := opt.(worker.WorkerOption); ok {
				workerOpts = append(workerOpts, wo)
			}
		}
		return worker.NewWorker(q, workerOpts...)
	}
}

// Type aliases for backward compatibility
type (
	// Job represents a unit of work to be processed.
	Job = core.Job

	// Checkpoint stores the result of a durable Call() for replay.
	Checkpoint = core.Checkpoint

	// JobStatus represents the current state of a job.
	JobStatus = core.JobStatus

	// Storage defines the persistence layer for jobs.
	Storage = core.Storage

	// Starter is an interface for types that can be started with a context.
	Starter = core.Starter

	// Event is the interface for all queue events.
	Event = core.Event

	// JobStarted is emitted when a job starts processing.
	JobStarted = core.JobStarted

	// JobCompleted is emitted when a job completes successfully.
	JobCompleted = core.JobCompleted

	// JobFailed is emitted when a job fails permanently.
	JobFailed = core.JobFailed

	// JobRetrying is emitted when a job is retried.
	JobRetrying = core.JobRetrying

	// CheckpointSaved is emitted when a checkpoint is saved.
	CheckpointSaved = core.CheckpointSaved

	// SignalDelivered is emitted when a signal is persisted for a job.
	SignalDelivered = core.SignalDelivered

	// NoRetryError indicates an error that should not be retried.
	NoRetryError = core.NoRetryError

	// RetryAfterError indicates an error that should be retried after a delay.
	RetryAfterError = core.RetryAfterError

	// Queue manages job registration, enqueueing, and processing.
	Queue = queue.Queue

	// Option modifies Options.
	Option = queue.Option

	// BatchEntry describes one job in a batch enqueue request.
	BatchEntry = queue.BatchEntry

	// EnqueueMiddleware wraps the enqueue operation for cross-cutting concerns.
	EnqueueMiddleware = queue.EnqueueMiddleware

	// Options holds configuration for job enqueueing and registration.
	Options = queue.Options

	// DeterminismMode controls Call replay strictness.
	DeterminismMode = queue.DeterminismMode

	// ScheduledJob holds configuration for a recurring job.
	ScheduledJob = queue.ScheduledJob

	// Worker processes jobs from the queue.
	Worker = worker.Worker

	// WorkerOption configures a Worker.
	WorkerOption = worker.WorkerOption

	// WorkerConfig holds worker configuration.
	WorkerConfig = worker.WorkerConfig

	// RetryConfig holds configuration for retry with backoff.
	RetryConfig = worker.RetryConfig

	// BackoffPolicy computes the delay before a failed job is retried.
	BackoffPolicy = worker.BackoffPolicy

	// BackoffFunc adapts a function to BackoffPolicy.
	BackoffFunc = worker.BackoffFunc

	// ExponentialBackoff computes exponential job retry delays with optional jitter.
	ExponentialBackoff = worker.ExponentialBackoff

	// Schedule defines when a job should run next.
	Schedule = schedule.Schedule

	// GormStorage implements Storage using GORM.
	GormStorage = storage.GormStorage

	// TxEnqueuer is the optional storage capability for caller-supplied transactions.
	TxEnqueuer = storage.TxEnqueuer

	// PoolConfig holds connection pool configuration.
	PoolConfig = storage.PoolConfig

	// PoolOption configures connection pool settings.
	PoolOption = storage.PoolOption

	// Fan-out types

	// SubJob represents a sub-job to be spawned in a fan-out.
	SubJob = fanout.SubJob

	// FanOutOption configures fan-out behavior.
	FanOutOption = fanout.Option

	// FanOutError contains details about fan-out failures.
	FanOutError = fanout.Error

	// SubJobFailure contains details about a single sub-job failure.
	SubJobFailure = fanout.SubJobFailure

	// FanOut tracking types

	// FanOut tracks a batch of sub-jobs spawned by a parent job.
	FanOutRecord = core.FanOut

	// FanOutStrategy defines how sub-job failures affect the parent.
	FanOutStrategy = core.FanOutStrategy

	// FanOutStatus represents the state of a fan-out batch.
	FanOutStatus = core.FanOutStatus
)

// Status constants
const (
	StatusPending   = core.StatusPending
	StatusRunning   = core.StatusRunning
	StatusCompleted = core.StatusCompleted
	StatusFailed    = core.StatusFailed
	StatusRetrying  = core.StatusRetrying
	StatusWaiting   = core.StatusWaiting
	StatusCancelled = core.StatusCancelled
	StatusPaused    = core.StatusPaused
)

// Fan-out strategy constants
const (
	StrategyFailFast   = core.StrategyFailFast
	StrategyCollectAll = core.StrategyCollectAll
	StrategyThreshold  = core.StrategyThreshold
)

// Fan-out status constants
const (
	FanOutPending   = core.FanOutPending
	FanOutCompleted = core.FanOutCompleted
	FanOutFailed    = core.FanOutFailed
)

// Determinism mode constants, in increasing order of strictness:
//   - ExplicitCheckpoints (default): error on a replay checkpoint type mismatch.
//   - Strict: also fail terminally if any recorded Call checkpoint is not
//     replayed (the handler's Call sequence changed).
//   - BestEffort: log type mismatches and re-execute the call.
const (
	ExplicitCheckpoints = queue.ExplicitCheckpoints
	Strict              = queue.Strict
	BestEffort          = queue.BestEffort
)

// Security limits
const (
	MaxJobTypeNameLength  = security.MaxJobTypeNameLength
	MaxJobArgsSize        = security.MaxJobArgsSize
	MaxResultSize         = security.MaxResultSize
	MaxRetries            = security.MaxRetries
	MaxConcurrency        = security.MaxConcurrency
	MaxErrorMessageLength = security.MaxErrorMessageLength
	MaxQueueNameLength    = security.MaxQueueNameLength
	MaxUniqueKeyLength    = security.MaxUniqueKeyLength
	MaxSignalNameLength   = security.MaxSignalNameLength
)

// Error variables
var (
	ErrInvalidJobTypeName  = core.ErrInvalidJobTypeName
	ErrJobTypeNameTooLong  = core.ErrJobTypeNameTooLong
	ErrInvalidQueueName    = core.ErrInvalidQueueName
	ErrQueueNameTooLong    = core.ErrQueueNameTooLong
	ErrJobArgsTooLarge     = core.ErrJobArgsTooLarge
	ErrJobNotCompleted     = core.ErrJobNotCompleted
	ErrJobNotOwned         = core.ErrJobNotOwned
	ErrDuplicateJob        = core.ErrDuplicateJob
	ErrUniqueKeyTooLong    = core.ErrUniqueKeyTooLong
	ErrJobAlreadyPaused    = core.ErrJobAlreadyPaused
	ErrJobNotPaused        = core.ErrJobNotPaused
	ErrQueueAlreadyPaused  = core.ErrQueueAlreadyPaused
	ErrQueueNotPaused      = core.ErrQueueNotPaused
	ErrCannotPauseStatus   = core.ErrCannotPauseStatus
	ErrJobNotFound         = core.ErrJobNotFound
	ErrNoResult            = core.ErrNoResult
	ErrCannotRequeueSubJob = core.ErrCannotRequeueSubJob
	ErrSignalNameTooLong   = core.ErrSignalNameTooLong
	ErrStorageNoSignals    = core.ErrStorageNoSignals
	ErrStorageNoTxEnqueue  = core.ErrStorageNoTxEnqueue
)

// Default values
var (
	DefaultJobRetries  = queue.DefaultJobRetries
	DefaultCallRetries = queue.DefaultCallRetries
)

// New creates a new Queue with the given storage backend.
func New(s Storage) *Queue {
	return queue.New(s)
}

// NewGormStorage creates a new GORM-backed storage.
func NewGormStorage(db *gorm.DB) *GormStorage {
	return storage.NewGormStorage(db)
}

// NewGormStorageWithPool creates storage with connection pool configuration.
// Uses sensible defaults that can be overridden with PoolOption arguments.
func NewGormStorageWithPool(db *gorm.DB, opts ...PoolOption) (*GormStorage, error) {
	return storage.NewGormStorageWithPool(db, opts...)
}

// ConfigurePool applies pool configuration to a GORM database connection.
func ConfigurePool(db *gorm.DB, opts ...PoolOption) error {
	return storage.ConfigurePool(db, opts...)
}

// DefaultPoolConfig returns sensible defaults for connection pooling.
func DefaultPoolConfig() PoolConfig {
	return storage.DefaultPoolConfig()
}

// HighConcurrencyPoolConfig returns pool settings for high-concurrency workloads.
func HighConcurrencyPoolConfig() PoolConfig {
	return storage.HighConcurrencyPoolConfig()
}

// LowLatencyPoolConfig returns pool settings optimized for low-latency processing.
func LowLatencyPoolConfig() PoolConfig {
	return storage.LowLatencyPoolConfig()
}

// ResourceConstrainedPoolConfig returns pool settings for limited database resources.
func ResourceConstrainedPoolConfig() PoolConfig {
	return storage.ResourceConstrainedPoolConfig()
}

// MaxOpenConns sets the maximum number of open database connections.
func MaxOpenConns(n int) PoolOption {
	return storage.MaxOpenConns(n)
}

// MaxIdleConns sets the maximum number of idle connections in the pool.
func MaxIdleConns(n int) PoolOption {
	return storage.MaxIdleConns(n)
}

// ConnMaxLifetime sets the maximum lifetime of database connections.
func ConnMaxLifetime(d time.Duration) PoolOption {
	return storage.ConnMaxLifetime(d)
}

// ConnMaxIdleTime sets the maximum idle time for connections.
func ConnMaxIdleTime(d time.Duration) PoolOption {
	return storage.ConnMaxIdleTime(d)
}

// NewOptions creates Options with defaults.
func NewOptions() *Options {
	return queue.NewOptions()
}

// Batch creates a batch enqueue entry.
func Batch(name string, args any, opts ...Option) BatchEntry {
	return queue.Batch(name, args, opts...)
}

// NewWorker creates a new worker for the given queue.
func NewWorker(q *Queue, opts ...WorkerOption) *Worker {
	return worker.NewWorker(q, opts...)
}

// Call executes a durable nested job call.
// Results are checkpointed; on replay, cached results are returned without re-execution.
func Call[T any](ctx context.Context, name string, args any) (T, error) {
	return call.Call[T](ctx, name, args)
}

// CallWithCheckpointCtx is like Call but uses execCtx for handler execution
// and checkpointCtx for the checkpoint write. Use this when the activity has
// a per-call deadline (execCtx = WithTimeout(root, deadline)) but the checkpoint
// write must survive past that deadline (checkpointCtx = workflow root context).
func CallWithCheckpointCtx[T any](execCtx, checkpointCtx context.Context, name string, args any) (T, error) {
	return call.CallWithCheckpointCtx[T](execCtx, checkpointCtx, name, args)
}

// SavePhaseCheckpoint saves a phase result to the checkpoint store.
// Use this to checkpoint expensive operations so they can be skipped on job retry.
func SavePhaseCheckpoint(ctx context.Context, phaseName string, result any) error {
	return jobctx.SavePhaseCheckpoint(ctx, phaseName, result)
}

// LoadPhaseCheckpoint loads a previously saved phase result from the checkpoint store.
// Returns (result, true) if found, (zero, false) if not found or not in job context.
func LoadPhaseCheckpoint[T any](ctx context.Context, phaseName string) (T, bool) {
	return jobctx.LoadPhaseCheckpoint[T](ctx, phaseName)
}

// NoRetry wraps an error to indicate it should not be retried.
func NoRetry(err error) error {
	return core.NoRetry(err)
}

// RetryAfter wraps an error to indicate it should be retried after a delay.
func RetryAfter(d time.Duration, err error) error {
	return core.RetryAfter(d, err)
}

// ValidateJobTypeName validates a job type name.
func ValidateJobTypeName(name string) error {
	return security.ValidateJobTypeName(name)
}

// ValidateQueueName validates a queue name.
func ValidateQueueName(name string) error {
	return security.ValidateQueueName(name)
}

// SanitizeErrorMessage truncates and sanitizes error messages for storage.
func SanitizeErrorMessage(msg string) string {
	return security.SanitizeErrorMessage(msg)
}

// ClampRetries ensures retry count is within limits.
func ClampRetries(n int) int {
	return security.ClampRetries(n)
}

// ClampConcurrency ensures concurrency is within limits.
func ClampConcurrency(n int) int {
	return security.ClampConcurrency(n)
}

// Job option functions

// QueueOpt sets the queue name.
func QueueOpt(name string) Option {
	return queue.QueueOpt(name)
}

// Priority sets the job priority (higher = runs first).
func Priority(p int) Option {
	return queue.Priority(p)
}

// Retries sets the maximum retry count.
func Retries(n int) Option {
	return queue.Retries(n)
}

// Delay schedules the job to run after a duration.
func Delay(d time.Duration) Option {
	return queue.Delay(d)
}

// At schedules the job to run at a specific time.
func At(t time.Time) Option {
	return queue.At(t)
}

// Timeout sets the maximum wall time for handler execution.
// When the deadline expires, the handler's context is cancelled.
// 0 means no limit (default).
func Timeout(d time.Duration) Option {
	return queue.Timeout(d)
}

// WithHandlerBackoff sets the retry backoff policy for this handler.
func WithHandlerBackoff(p BackoffPolicy) Option {
	return queue.WithHandlerBackoff(p)
}

// Unique ensures only one job with this key exists.
func Unique(key string) Option {
	return queue.Unique(key)
}

// Determinism sets the Call replay mode.
func Determinism(mode DeterminismMode) Option {
	return queue.Determinism(mode)
}

// Worker option functions

// Concurrency sets the concurrency for a queue.
func Concurrency(n int) WorkerOption {
	return worker.Concurrency(n)
}

// WithScheduler enables the scheduler in the worker.
func WithScheduler(enabled bool) WorkerOption {
	return worker.WithScheduler(enabled)
}

// WorkerQueue adds a queue to process with optional concurrency.
func WorkerQueue(name string, opts ...WorkerOption) WorkerOption {
	return worker.WorkerQueue(name, opts...)
}

// WithStorageRetry configures retry behavior for storage operations.
func WithStorageRetry(config RetryConfig) WorkerOption {
	return worker.WithStorageRetry(config)
}

// WithDequeueRetry configures retry behavior for dequeue operations.
func WithDequeueRetry(config RetryConfig) WorkerOption {
	return worker.WithDequeueRetry(config)
}

// WithRetryAttempts sets the max retry attempts for storage operations.
func WithRetryAttempts(attempts int) WorkerOption {
	return worker.WithRetryAttempts(attempts)
}

// DisableRetry disables retry for storage operations.
func DisableRetry() WorkerOption {
	return worker.DisableRetry()
}

// WithPollInterval sets the interval between job polling attempts.
// Lower values increase throughput but also database load.
// Default is 100ms. Minimum is 50ms to prevent database overload.
func WithPollInterval(d time.Duration) WorkerOption {
	return worker.WithPollInterval(d)
}

// WithDrainTimeout sets how long Start waits for in-flight handlers to finish
// after its context is cancelled. A non-positive duration aborts immediately.
func WithDrainTimeout(d time.Duration) WorkerOption {
	return worker.WithDrainTimeout(d)
}

// WithBackoff configures the worker-default retry backoff policy for failed
// job re-execution. Per-handler policies and RetryAfter override it.
func WithBackoff(p BackoffPolicy) WorkerOption {
	return worker.WithBackoff(p)
}

// WithStaleLockInterval sets how often the worker checks for stale running jobs.
func WithStaleLockInterval(d time.Duration) WorkerOption {
	return worker.WithStaleLockInterval(d)
}

// WithStaleLockAge sets how long a lock must be expired before reclaim.
func WithStaleLockAge(d time.Duration) WorkerOption {
	return worker.WithStaleLockAge(d)
}

// WithLockDuration configures how long jobs are locked via the storage
// backend's SetLockDuration. GormStorage implements SetLockDuration; custom
// Storage backends must implement SetLockDuration(time.Duration) to honor it.
func WithLockDuration(d time.Duration) WorkerOption {
	return worker.WithLockDuration(d)
}

// WithFanOutRecoveryStaleAge sets how old a pending fan-out must be before the
// worker resumes a parent that crashed mid-enqueue (recovery for an
// incompletely-enqueued fan-out). Default is 2 minutes; non-positive keeps the
// default (recovery cannot be disabled).
func WithFanOutRecoveryStaleAge(d time.Duration) WorkerOption {
	return worker.WithFanOutRecoveryStaleAge(d)
}

// DefaultRetryConfig returns the default retry configuration.
func DefaultRetryConfig() RetryConfig {
	return worker.DefaultRetryConfig()
}

// DefaultBackoffPolicy returns the default job retry backoff policy.
func DefaultBackoffPolicy() BackoffPolicy {
	return worker.DefaultBackoffPolicy()
}

// Schedule functions

// Every creates a schedule that runs at fixed intervals.
func Every(d time.Duration) Schedule {
	return schedule.Every(d)
}

// Daily creates a schedule that runs at a specific time each day.
func Daily(hour, minute int) Schedule {
	return schedule.Daily(hour, minute)
}

// Weekly creates a schedule that runs at a specific day and time each week.
func Weekly(day time.Weekday, hour, minute int) Schedule {
	return schedule.Weekly(day, hour, minute)
}

// Cron creates a schedule from a cron expression.
func Cron(expr string) (Schedule, error) {
	return schedule.Cron(expr)
}

// MustCron creates a schedule from a cron expression and panics if invalid.
func MustCron(expr string) Schedule {
	return schedule.MustCron(expr)
}

// JobFromContext returns the current Job from context, or nil if not in a job handler.
// Use this to get the job ID for logging or progress tracking.
func JobFromContext(ctx context.Context) *Job {
	return jobctx.JobFromContext(ctx)
}

// JobIDFromContext returns the current job ID from context, or empty string if not in a job handler.
func JobIDFromContext(ctx context.Context) string {
	return jobctx.JobIDFromContext(ctx)
}

// Fan-out functions

// Sub creates a sub-job definition for use with FanOut.
func Sub(jobType string, args any, opts ...Option) SubJob {
	return fanout.Sub(jobType, args, opts...)
}

// FanOut spawns sub-jobs in parallel and waits for all results.
// Checkpoints progress - safe to retry if parent crashes.
// Returns a slice of Result[T] with success/failure for each sub-job.
func FanOut[T any](ctx context.Context, subJobs []SubJob, opts ...FanOutOption) ([]fanout.Result[T], error) {
	return fanout.FanOut[T](ctx, subJobs, opts...)
}

// FanOutResult wraps a sub-job result with its index and potential error.
type FanOutResult[T any] = fanout.Result[T]

// Values extracts values from successful fan-out results.
func Values[T any](results []fanout.Result[T]) []T {
	return fanout.Values(results)
}

// Partition splits fan-out results into successes and failures.
func Partition[T any](results []fanout.Result[T]) ([]T, []error) {
	return fanout.Partition(results)
}

// AllSucceeded checks if all fan-out results succeeded.
func AllSucceeded[T any](results []fanout.Result[T]) bool {
	return fanout.AllSucceeded(results)
}

// SuccessCount returns the number of successful fan-out results.
func SuccessCount[T any](results []fanout.Result[T]) int {
	return fanout.SuccessCount(results)
}

// Fan-out option functions

// FailFast fails the parent on first sub-job failure.
func FailFast() FanOutOption {
	return fanout.FailFast()
}

// CollectAll waits for all sub-jobs, returns partial results.
func CollectAll() FanOutOption {
	return fanout.CollectAll()
}

// Threshold succeeds if at least pct% of sub-jobs succeed.
func Threshold(pct float64) FanOutOption {
	return fanout.Threshold(pct)
}

// WithFanOutQueue sets the queue for sub-jobs.
func WithFanOutQueue(q string) FanOutOption {
	return fanout.WithQueue(q)
}

// WithFanOutPriority sets the priority for sub-jobs.
func WithFanOutPriority(p int) FanOutOption {
	return fanout.WithPriority(p)
}

// WithFanOutRetries sets the retry count for sub-jobs.
func WithFanOutRetries(n int) FanOutOption {
	return fanout.WithRetries(n)
}

// WithFanOutTimeout sets a deadline for the entire fan-out operation.
// The deadline is stored on the fan-out record (TimeoutAt field) but is not
// automatically enforced. Applications can query fan-outs by TimeoutAt to
// implement custom timeout handling.
func WithFanOutTimeout(d time.Duration) FanOutOption {
	return fanout.WithTimeout(d)
}

// CancelOnParentFailure cancels sub-jobs if parent fails.
func CancelOnParentFailure() FanOutOption {
	return fanout.CancelOnParentFailure()
}

// IsWaitingError reports whether err is the signal FanOut returns to
// tell the worker a job has moved into the waiting status pending its
// sub-jobs.
func IsWaitingError(err error) bool {
	return fanout.IsWaitingError(err)
}

// IsSuspendError reports whether err is a waiting-for-fan-out signal.
//
// Deprecated: Use IsWaitingError. The naming was revised to match
// core.StatusWaiting, which is the actual status the parent job carries.
func IsSuspendError(err error) bool {
	return fanout.IsWaitingError(err)
}

// Requeue resets a terminally failed or cancelled job back to pending so it
// runs again from scratch. Failed jobs are the dead-letter set — there is no
// separate DLQ table; query them with q.Storage().GetJobsByStatus(StatusFailed)
// and replay one with Requeue. The job's checkpoints are cleared so a workflow
// replays from the beginning (handlers must be idempotent regardless), which is
// the safe behavior when the usual reason to requeue is a code or dependency
// fix that changes the workflow's steps. Requeuing a fan-out parent also clears
// its entire fan-out subtree (descendant fan-outs and sub-jobs at every depth,
// including nested workflows) so the replay re-dispatches cleanly.
//
// Returns true if the job was requeued, false if it was not found or not in a
// requeuable (failed/cancelled) state. Returns ErrCannotRequeueSubJob for a
// fan-out sub-job (requeue its parent instead), or an error if the storage
// backend does not support requeueing.
func Requeue(ctx context.Context, q *Queue, jobID string) (bool, error) {
	type requeuer interface {
		Requeue(ctx context.Context, jobID string) (bool, error)
	}
	r, ok := q.Storage().(requeuer)
	if !ok {
		return false, fmt.Errorf("jobs: storage backend does not support Requeue")
	}
	return r.Requeue(ctx, jobID)
}

// Signal delivers a named signal carrying payload to a job (workflow). The
// signal is buffered durably, so it is not lost if sent before the handler
// waits for it; and if the target job is currently waiting on a signal, this
// resumes it promptly (a recovery poll is the backstop). Delivery is FIFO per
// (job, name).
//
// The handler receives signals with WaitForSignal / WaitForSignalTimeout /
// CheckSignal / DrainSignals. Returns ErrJobNotFound if the job does not exist,
// ErrStorageNoSignals if the backend lacks signal support, or ErrSignalNameTooLong.
func Signal(ctx context.Context, q *Queue, jobID, name string, payload any) error {
	if name == "" {
		return fmt.Errorf("jobs: signal name must not be empty")
	}
	if len(name) > MaxSignalNameLength {
		return core.ErrSignalNameTooLong
	}
	type signalSender interface {
		SendSignal(ctx context.Context, jobID, name string, payload []byte) error
	}
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
		type signalResumer interface {
			ResumeSignalWaitingJob(ctx context.Context, jobID string) (bool, error)
		}
		if r, ok := q.Storage().(signalResumer); ok {
			if _, err := r.ResumeSignalWaitingJob(ctx, jobID); err != nil {
				return fmt.Errorf("jobs: signal sent but resume failed (poll will recover): %w", err)
			}
		}
	}
	return nil
}

// WaitForSignal consumes the oldest pending signal of name from within a job
// handler, suspending the job until one arrives. See pkg/signal.
func WaitForSignal[T any](ctx context.Context, name string) (T, error) {
	return signal.WaitForSignal[T](ctx, name)
}

// WaitForSignalTimeout is WaitForSignal with a deadline: returns (zero, false)
// if no signal arrives within d.
func WaitForSignalTimeout[T any](ctx context.Context, name string, d time.Duration) (T, bool, error) {
	return signal.WaitForSignalTimeout[T](ctx, name, d)
}

// CheckSignal reports the oldest pending signal of name without consuming it
// (non-blocking). Returns (zero, false) when none is pending.
func CheckSignal[T any](ctx context.Context, name string) (T, bool, error) {
	return signal.CheckSignal[T](ctx, name)
}

// DrainSignals consumes and returns all currently-pending signals of name
// (non-blocking), in FIFO order.
func DrainSignals[T any](ctx context.Context, name string) ([]T, error) {
	return signal.DrainSignals[T](ctx, name)
}

// LoadResult decodes the persisted return value of a completed job into T.
//
// Returns:
//   - ErrJobNotCompleted if the job has not reached a terminal state.
//   - An error containing job.LastError if the job failed.
//   - ErrNoResult if the job completed but has no persisted result value.
//   - An error wrapping the JSON decode failure if Result cannot be unmarshaled into T.
func LoadResult[T any](ctx context.Context, q *Queue, jobID string) (T, error) {
	var zero T
	job, err := q.Storage().GetJob(ctx, jobID)
	if err != nil {
		return zero, err
	}
	if job == nil {
		return zero, fmt.Errorf("%w: %s", core.ErrJobNotFound, jobID)
	}
	switch job.Status {
	case core.StatusCompleted:
		if job.Result == nil {
			return zero, core.ErrNoResult
		}
		var out T
		if err := json.Unmarshal(job.Result, &out); err != nil {
			return zero, fmt.Errorf("jobs: failed to decode result: %w", err)
		}
		return out, nil
	case core.StatusFailed:
		return zero, fmt.Errorf("jobs: job %s failed: %s", jobID, job.LastError)
	default:
		return zero, core.ErrJobNotCompleted
	}
}
