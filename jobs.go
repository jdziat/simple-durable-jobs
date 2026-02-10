// Package jobs provides a durable job queue with checkpointed nested calls.
//
// This is the main package users should import. It re-exports all public
// types from the internal pkg/ packages for a clean API surface.
//
// Basic usage:
//
//	// Create storage and queue
//	db, _ := gorm.Open(sqlite.Open("jobs.db"), &gorm.Config{})
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
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/pkg/call"
	"github.com/jdziat/simple-durable-jobs/pkg/core"
	"github.com/jdziat/simple-durable-jobs/pkg/fanout"
	"github.com/jdziat/simple-durable-jobs/pkg/jobctx"
	"github.com/jdziat/simple-durable-jobs/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/pkg/schedule"
	"github.com/jdziat/simple-durable-jobs/pkg/security"
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

	// NoRetryError indicates an error that should not be retried.
	NoRetryError = core.NoRetryError

	// RetryAfterError indicates an error that should be retried after a delay.
	RetryAfterError = core.RetryAfterError

	// Queue manages job registration, enqueueing, and processing.
	Queue = queue.Queue

	// Option modifies Options.
	Option = queue.Option

	// Options holds configuration for job enqueueing and registration.
	Options = queue.Options

	// DeterminismMode controls replay strictness.
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

	// Schedule defines when a job should run next.
	Schedule = schedule.Schedule

	// GormStorage implements Storage using GORM.
	GormStorage = storage.GormStorage

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

// Determinism mode constants
const (
	ExplicitCheckpoints = queue.ExplicitCheckpoints
	Strict              = queue.Strict
	BestEffort          = queue.BestEffort
)

// Security limits
const (
	MaxJobTypeNameLength  = security.MaxJobTypeNameLength
	MaxJobArgsSize        = security.MaxJobArgsSize
	MaxRetries            = security.MaxRetries
	MaxConcurrency        = security.MaxConcurrency
	MaxErrorMessageLength = security.MaxErrorMessageLength
	MaxQueueNameLength    = security.MaxQueueNameLength
	MaxUniqueKeyLength    = security.MaxUniqueKeyLength
)

// Error variables
var (
	ErrInvalidJobTypeName = core.ErrInvalidJobTypeName
	ErrJobTypeNameTooLong = core.ErrJobTypeNameTooLong
	ErrInvalidQueueName   = core.ErrInvalidQueueName
	ErrQueueNameTooLong   = core.ErrQueueNameTooLong
	ErrJobArgsTooLarge    = core.ErrJobArgsTooLarge
	ErrJobNotOwned        = core.ErrJobNotOwned
	ErrDuplicateJob       = core.ErrDuplicateJob
	ErrUniqueKeyTooLong   = core.ErrUniqueKeyTooLong
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

// NewWorker creates a new worker for the given queue.
func NewWorker(q *Queue, opts ...WorkerOption) *Worker {
	return worker.NewWorker(q, opts...)
}

// Call executes a durable nested job call.
// Results are checkpointed; on replay, cached results are returned without re-execution.
func Call[T any](ctx context.Context, name string, args any) (T, error) {
	return call.Call[T](ctx, name, args)
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

// Unique ensures only one job with this key exists.
func Unique(key string) Option {
	return queue.Unique(key)
}

// Determinism sets the replay mode.
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

// DefaultRetryConfig returns the default retry configuration.
func DefaultRetryConfig() RetryConfig {
	return worker.DefaultRetryConfig()
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
func Cron(expr string) Schedule {
	return schedule.Cron(expr)
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
type FanOutResult[T any] struct {
	Index int
	Value T
	Err   error
}

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

// WithSubJobTimeout sets timeout for each sub-job.
func WithSubJobTimeout(d time.Duration) FanOutOption {
	return fanout.WithSubJobTimeout(d)
}

// WithFanOutTimeout sets timeout for entire fan-out.
func WithFanOutTimeout(d time.Duration) FanOutOption {
	return fanout.WithTimeout(d)
}

// CancelOnParentFailure cancels sub-jobs if parent fails.
func CancelOnParentFailure() FanOutOption {
	return fanout.CancelOnParentFailure()
}

// IsSuspendError checks if an error indicates a fan-out suspend.
func IsSuspendError(err error) bool {
	return fanout.IsSuspendError(err)
}
