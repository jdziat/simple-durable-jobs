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

	// Schedule defines when a job should run next.
	Schedule = schedule.Schedule

	// GormStorage implements Storage using GORM.
	GormStorage = storage.GormStorage
)

// Status constants
const (
	StatusPending   = core.StatusPending
	StatusRunning   = core.StatusRunning
	StatusCompleted = core.StatusCompleted
	StatusFailed    = core.StatusFailed
	StatusRetrying  = core.StatusRetrying
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
