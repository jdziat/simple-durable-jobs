// Package jobs provides a durable job queue with checkpointed nested calls.
//
// This is the main package users should import. It re-exports all public
// types from the internal pkg/ packages for a clean API surface.
//
// Basic usage:
//
//	// Create storage and queue. SafeSQLiteDSN applies the recommended SQLite
//	// settings for file-based databases used by this library.
//	db, _ := gorm.Open(sqlite.Open(jobs.SafeSQLiteDSN("jobs.db")), &gorm.Config{})
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
	"log/slog"
	"strings"
	"time"

	"gorm.io/gorm"

	"github.com/jdziat/simple-durable-jobs/v2/pkg/call"
	payloadcodec "github.com/jdziat/simple-durable-jobs/v2/pkg/codec"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/core"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/fanout"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/jobctx"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/queue"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/schedule"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/security"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/signal"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/storage"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/typed"
	"github.com/jdziat/simple-durable-jobs/v2/pkg/worker"
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

	// PayloadCodec transforms serialized payload bytes at the storage boundary.
	PayloadCodec = core.PayloadCodec

	// MetadataMap stores queryable string metadata for jobs and job filters.
	MetadataMap = core.MetadataMap

	// DeadLetterFilter scopes dead-letter triage queries.
	DeadLetterFilter = core.DeadLetterFilter

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

	// JobResumedBySignal is emitted when a signal wakes a waiting job.
	JobResumedBySignal = core.JobResumedBySignal

	// JobReclaimed is emitted when a worker reclaims a job from a presumed-dead owner.
	JobReclaimed = core.JobReclaimed

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

	// ExecutionMiddleware wraps each job handler attempt for cross-cutting concerns.
	ExecutionMiddleware = queue.ExecutionMiddleware

	// Options holds configuration for job enqueueing and registration.
	Options = queue.Options

	// DeterminismMode controls Call replay strictness.
	DeterminismMode = queue.DeterminismMode

	// Def is a typed handle to a registered job definition.
	Def[A any, R any] = typed.Def[A, R]

	// ScheduledJob holds configuration for a recurring job.
	ScheduledJob = queue.ScheduledJob

	// Worker processes jobs from the queue.
	Worker = worker.Worker

	// WorkerOption configures a Worker.
	WorkerOption = worker.WorkerOption

	// WorkerConfig holds worker configuration.
	WorkerConfig = worker.WorkerConfig

	// ConcurrencyCapConfig describes a DB-backed fleet-wide or per-key cap.
	ConcurrencyCapConfig = worker.ConcurrencyCapConfig

	// CapOption configures a ConcurrencyCap.
	CapOption = worker.CapOption

	// RateLimitConfig describes a DB-backed fleet-wide or per-key rate limit.
	RateLimitConfig = worker.RateLimitConfig

	// RateLimitOption configures a RateLimit.
	RateLimitOption = worker.RateLimitOption

	// RetentionConfig controls automatic retention.
	RetentionConfig = worker.RetentionConfig

	// RetentionOption configures automatic retention.
	RetentionOption = worker.RetentionOption

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

	// GormStorageOption configures GormStorage.
	GormStorageOption = storage.GormStorageOption

	// TxEnqueuer is the optional storage capability for caller-supplied transactions.
	TxEnqueuer = storage.TxEnqueuer

	// TxCheckpointer is the optional storage capability for caller-supplied transaction checkpoints.
	TxCheckpointer = storage.TxCheckpointer

	// Secretbox is the built-in NaCl Secretbox payload codec.
	Secretbox = payloadcodec.Secretbox

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

// DeadLetterOption configures dead-letter triage queries.
type DeadLetterOption func(*DeadLetterFilter)

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

// Reclaim reason constants for the JobReclaimed event. ReclaimReasonStaleLock
// marks a reclaim by this worker's stale-lock reaper (the crash leading
// indicator); ReclaimReasonOwnershipAudit marks a peer reclaiming one of this
// worker's in-flight jobs.
const (
	ReclaimReasonStaleLock      = core.ReclaimReasonStaleLock
	ReclaimReasonOwnershipAudit = core.ReclaimReasonOwnershipAudit
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

// Workflow version constants
const (
	DefaultVersion = jobctx.DefaultVersion
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
	ErrInvalidJobTypeName    = core.ErrInvalidJobTypeName
	ErrJobTypeNameTooLong    = core.ErrJobTypeNameTooLong
	ErrInvalidQueueName      = core.ErrInvalidQueueName
	ErrQueueNameTooLong      = core.ErrQueueNameTooLong
	ErrJobArgsTooLarge       = core.ErrJobArgsTooLarge
	ErrJobNotCompleted       = core.ErrJobNotCompleted
	ErrJobFailed             = core.ErrJobFailed
	ErrJobCancelled          = core.ErrJobCancelled
	ErrJobNotOwned           = core.ErrJobNotOwned
	ErrDuplicateJob          = core.ErrDuplicateJob
	ErrUniqueKeyTooLong      = core.ErrUniqueKeyTooLong
	ErrJobAlreadyPaused      = core.ErrJobAlreadyPaused
	ErrJobNotPaused          = core.ErrJobNotPaused
	ErrQueueAlreadyPaused    = core.ErrQueueAlreadyPaused
	ErrQueueNotPaused        = core.ErrQueueNotPaused
	ErrCannotPauseStatus     = core.ErrCannotPauseStatus
	ErrJobNotFound           = core.ErrJobNotFound
	ErrNoResult              = core.ErrNoResult
	ErrCannotRequeueSubJob   = core.ErrCannotRequeueSubJob
	ErrSignalNameTooLong     = core.ErrSignalNameTooLong
	ErrStorageNoSignals      = core.ErrStorageNoSignals
	ErrStorageNoTxEnqueue    = core.ErrStorageNoTxEnqueue
	ErrStorageNoTxCheckpoint = core.ErrStorageNoTxCheckpoint
	ErrStorageNoBatchDequeue = core.ErrStorageNoBatchDequeue
	ErrPayloadDecode         = core.ErrPayloadDecode

	ErrUnsupportedWorkflowVersion = jobctx.ErrUnsupportedWorkflowVersion
	ErrSignalNameReserved         = signal.ErrSignalNameReserved

	ErrSecretboxAuthentication = payloadcodec.ErrSecretboxAuthentication
)

// Default values
var (
	DefaultJobRetries  = queue.DefaultJobRetries
	DefaultCallRetries = queue.DefaultCallRetries

	IdentityCodec = core.IdentityCodec
	NopCodec      = core.NopCodec
)

const safeSQLiteDSNQuery = "_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate"

// New creates a new Queue with the given storage backend.
func New(s Storage) *Queue {
	return queue.New(s)
}

// Define registers a typed job handler and returns its typed definition.
//
// Define panics on invalid registration, matching Queue.Register. Use DefineE
// when handler names or functions are configuration-driven and should return
// errors instead.
func Define[A any, R any](
	q *Queue,
	name string,
	fn func(context.Context, A) (R, error),
	opts ...Option,
) *Def[A, R] {
	return typed.Define(q, name, fn, opts...)
}

// DefineE registers a typed job handler and returns validation errors.
//
// The handler is registered through Queue.RegisterE, so routing remains
// string-keyed and execution uses the same handler path as the untyped API.
func DefineE[A any, R any](
	q *Queue,
	name string,
	fn func(context.Context, A) (R, error),
	opts ...Option,
) (*Def[A, R], error) {
	return typed.DefineE(q, name, fn, opts...)
}

// Declare returns a typed definition handle without registering a handler.
//
// Use Declare in producer-only processes that enqueue work for workers running
// elsewhere. The returned handle remains string-routed by name and does not
// create a type-keyed registry.
//
// Warning: A and R are unchecked on this producer-only EnqueueRemote path.
// There is no registered handler for Declare to validate them against, so they
// must be hand-synchronized with the remote worker's handler signature or the
// worker will fail to decode the payload.
func Declare[A any, R any](q *Queue, name string) *Def[A, R] {
	return typed.Declare[A, R](q, name)
}

// DefineVoid registers an error-only typed job handler.
//
// DefineVoid adapts fn to a handler returning struct{} so a completed job has a
// typed result value that can be used with Def.Call and Def.Load.
func DefineVoid[A any](
	q *Queue,
	name string,
	fn func(context.Context, A) error,
	opts ...Option,
) *Def[A, struct{}] {
	return typed.DefineVoid(q, name, fn, opts...)
}

// SafeSQLiteDSN returns path with the recommended SQLite DSN parameters for
// any file-based SQLite use of this library. It is not applicable to :memory:
// databases.
//
// WAL lets readers proceed without blocking the single writer. busy_timeout=5000
// makes SQLite wait up to 5 seconds for a lock instead of returning SQLITE_BUSY
// immediately. _txlock=immediate takes the write lock at BEGIN, avoiding
// deferred-transaction lock upgrade deadlocks under concurrent workers.
func SafeSQLiteDSN(path string) string {
	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}
	// A path already ending in "?" or "&" needs no separator at all.
	if strings.HasSuffix(path, "?") || strings.HasSuffix(path, "&") {
		sep = ""
	}
	return path + sep + safeSQLiteDSNQuery
}

// NewGormStorage creates a new GORM-backed storage. Unless the caller has
// already sized the pool (or opts out via NewGormStorageWithPool), it installs a
// bounded default connection pool: DefaultPoolConfig (25 open / 10 idle / 5m
// lifetime / 1m idle) for PostgreSQL and MySQL, and a small SQLite-safe pool
// (4 open / 2 idle, no connection expiry) for SQLite.
func NewGormStorage(db *gorm.DB, opts ...GormStorageOption) *GormStorage {
	return storage.NewGormStorage(db, opts...)
}

// WithCodec configures a payload codec for GormStorage.
func WithCodec(c PayloadCodec) GormStorageOption {
	return storage.WithCodec(c)
}

// NewSecretbox creates the built-in NaCl Secretbox payload codec.
func NewSecretbox(primaryKey [32]byte, fallbackKeys ...[32]byte) (*Secretbox, error) {
	return payloadcodec.NewSecretbox(primaryKey, fallbackKeys...)
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

// SavePhaseCheckpointTx saves a phase result through a caller-owned GORM
// transaction. Use this when the phase's business side effect and checkpoint
// must commit or roll back together.
func SavePhaseCheckpointTx(ctx context.Context, tx *gorm.DB, phaseName string, result any) error {
	return jobctx.SavePhaseCheckpointTx(ctx, tx, phaseName, result)
}

// LoadPhaseCheckpoint loads a previously saved phase result from the checkpoint store.
// Returns (result, true) if found, (zero, false) if not found or not in job context.
func LoadPhaseCheckpoint[T any](ctx context.Context, phaseName string) (T, bool) {
	return jobctx.LoadPhaseCheckpoint[T](ctx, phaseName)
}

// GetVersion records or replays a workflow-code version marker for changeID.
// Use the returned version to branch around changes to Call, fan-out, and signal
// wait sequences so in-flight runs keep their originally recorded path.
func GetVersion(ctx context.Context, changeID string, minSupported, maxSupported int) (int, error) {
	return jobctx.GetVersion(ctx, changeID, minSupported, maxSupported)
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

// WithTenant sets the tenant that owns the job.
func WithTenant(t string) Option {
	return queue.WithTenant(t)
}

// WithMetadata replaces the job metadata with a defensive copy of m.
// It replaces any metadata set by earlier WithMetadata or WithMeta options.
func WithMetadata(m map[string]string) Option {
	return queue.WithMetadata(m)
}

// WithMeta adds or replaces one metadata key/value pair.
func WithMeta(key, value string) Option {
	return queue.WithMeta(key, value)
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

// ConcurrencyCap limits concurrent jobs across the fleet when the storage
// backend implements DB-backed concurrency slots.
func ConcurrencyCap(name string, limit int, opts ...CapOption) WorkerOption {
	return worker.ConcurrencyCap(name, limit, opts...)
}

// CapKey derives the per-key partition for a ConcurrencyCap.
func CapKey(fn func(*Job) string) CapOption {
	return worker.CapKey(fn)
}

// RateLimit limits admitted jobs per second across the fleet when the storage
// backend implements DB-backed rate windows.
func RateLimit(name string, perSecond float64, opts ...RateLimitOption) WorkerOption {
	return worker.RateLimit(name, perSecond, opts...)
}

// RateLimitKey derives the per-key partition for a RateLimit.
func RateLimitKey(fn func(*Job) string) RateLimitOption {
	return worker.RateLimitKey(fn)
}

// WithRetention enables optional automatic garbage collection of terminal jobs.
func WithRetention(opts ...RetentionOption) WorkerOption {
	return worker.WithRetention(opts...)
}

// DefaultRetention is an explicit opt-in conservative retention preset (NOT a
// silent default): completed jobs 7 days, terminal failed/cancelled jobs 30
// days, consumed signals 7 days. Tune individual windows by composing the
// Retention* options under WithRetention instead.
func DefaultRetention() WorkerOption {
	return worker.DefaultRetention()
}

// RetentionCompletedAfter deletes completed jobs older than d. A non-positive
// duration keeps completed jobs forever.
func RetentionCompletedAfter(d time.Duration) RetentionOption {
	return worker.RetentionCompletedAfter(d)
}

// RetentionFailedAfter deletes terminal failed and cancelled jobs older than d.
// A non-positive duration keeps failed/cancelled jobs forever.
func RetentionFailedAfter(d time.Duration) RetentionOption {
	return worker.RetentionFailedAfter(d)
}

// RetentionConsumedSignalsAfter deletes consumed signal rows older than d.
// Pending/unconsumed signals are durable workflow state and are never pruned.
// A non-positive duration keeps consumed signal rows forever.
func RetentionConsumedSignalsAfter(d time.Duration) RetentionOption {
	return worker.RetentionConsumedSignalsAfter(d)
}

// RetentionInterval sets the retention scan cadence.
func RetentionInterval(d time.Duration) RetentionOption {
	return worker.RetentionInterval(d)
}

// RetentionBatchSize sets the maximum rows deleted in one pass.
func RetentionBatchSize(n int) RetentionOption {
	return worker.RetentionBatchSize(n)
}

// RetentionDeleteCheckpointsOnComplete opts in to deleting a successful job's
// checkpoints transactionally with its completion, bounding the checkpoints
// table without a background sweep. Off by default because the dashboard reads
// completed jobs' checkpoints for finished-workflow phase results; the failure
// path is never affected (retry replay keeps checkpoints).
func RetentionDeleteCheckpointsOnComplete() RetentionOption {
	return worker.RetentionDeleteCheckpointsOnComplete()
}

// WithQueueRateLimit applies a per-worker token bucket before dequeueing from
// the named queue.
func WithQueueRateLimit(queue string, perSecond float64, burst int) WorkerOption {
	return worker.WithQueueRateLimit(queue, perSecond, burst)
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

// WithDequeueBatchSize sets the per-poll cap for optional batch dequeue.
func WithDequeueBatchSize(n int) WorkerOption {
	return worker.WithDequeueBatchSize(n)
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
//
// The default is 100ms and the floor is 50ms (to prevent database overload). A
// positive duration below 50ms is clamped up to 50ms (it is not discarded). A
// non-positive duration is ignored and the existing value is kept.
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

// WithStaleLockAge sets how long the owning worker must have made no contact
// before a running job is reclaimed. Reclaim anchors on the last contact —
// COALESCE(last_heartbeat_at, started_at, locked_until) older than StaleLockAge —
// not on lease (LockedUntil) expiry, so a job whose lease has been pushed into
// the future is still reclaimed once its last contact is stale. The default is
// 45 minutes.
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
// worker's polling backstop heals it. This single cutoff gates two recovery
// paths: resuming a parent that crashed mid-enqueue (an incompletely-enqueued
// fan-out), and the terminal-strand sweep (GetCompletablePendingFanOuts) that
// completes a fan-out left status=pending with terminal counts and a waiting
// parent after a crash between the last sub-job's terminal write and the parent
// resume. Default is 2 minutes; non-positive keeps the default (recovery cannot
// be disabled).
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
// Fan-out children inherit the parent job's Tenant and Metadata.
func Sub(jobType string, args any, opts ...Option) SubJob {
	return fanout.Sub(jobType, args, opts...)
}

// FanOut spawns sub-jobs in parallel and waits for all results.
// Checkpoints progress - safe to retry if parent crashes.
// Fan-out children inherit the parent job's Tenant and Metadata.
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

// ListDeadLettered returns jobs with explicit DLQ metadata, ordered by
// dead_lettered_at descending. Replay a returned job with Requeue.
func ListDeadLettered(ctx context.Context, q *Queue, opts ...DeadLetterOption) ([]*Job, error) {
	type deadLetterLister interface {
		ListDeadLettered(ctx context.Context, filter core.DeadLetterFilter) ([]*core.Job, error)
	}
	l, ok := q.Storage().(deadLetterLister)
	if !ok {
		return nil, fmt.Errorf("jobs: storage backend does not support dead-letter triage")
	}
	filter := newDeadLetterFilter(opts...)
	return l.ListDeadLettered(ctx, filter)
}

// CountDeadLettered returns the number of jobs with explicit DLQ metadata.
func CountDeadLettered(ctx context.Context, q *Queue, opts ...DeadLetterOption) (int64, error) {
	type deadLetterCounter interface {
		CountDeadLettered(ctx context.Context, filter core.DeadLetterFilter) (int64, error)
	}
	c, ok := q.Storage().(deadLetterCounter)
	if !ok {
		return 0, fmt.Errorf("jobs: storage backend does not support dead-letter triage")
	}
	filter := newDeadLetterFilter(opts...)
	return c.CountDeadLettered(ctx, filter)
}

// DeadLetterQueue filters dead-letter queries to one queue.
func DeadLetterQueue(queue string) DeadLetterOption {
	return func(f *DeadLetterFilter) {
		f.Queue = queue
	}
}

// DeadLetterType filters dead-letter queries to one job type.
func DeadLetterType(jobType string) DeadLetterOption {
	return func(f *DeadLetterFilter) {
		f.Type = jobType
	}
}

// DeadLetterLimit sets the maximum number of dead-lettered jobs returned.
func DeadLetterLimit(limit int) DeadLetterOption {
	return func(f *DeadLetterFilter) {
		f.Limit = limit
	}
}

// DeadLetterOffset sets the pagination offset for dead-lettered jobs.
func DeadLetterOffset(offset int) DeadLetterOption {
	return func(f *DeadLetterFilter) {
		f.Offset = offset
	}
}

func newDeadLetterFilter(opts ...DeadLetterOption) DeadLetterFilter {
	var filter DeadLetterFilter
	for _, opt := range opts {
		opt(&filter)
	}
	return filter
}

// Signal delivers a named signal carrying payload to a job (workflow). Signal
// names starting with "_" are reserved for library-internal primitives such as
// durable timers and are rejected with ErrSignalNameReserved. The signal is
// buffered durably, so it is not lost if sent before the handler waits for it;
// and if the target job is currently waiting on a signal, this resumes it
// promptly (a recovery poll is the backstop). Delivery is FIFO per (job, name).
//
// The handler receives signals with WaitForSignal / WaitForSignalTimeout /
// CheckSignal / DrainSignals. Returns ErrJobNotFound if the job does not exist,
// ErrStorageNoSignals if the backend lacks signal support,
// ErrSignalNameReserved, or ErrSignalNameTooLong.
func Signal(ctx context.Context, q *Queue, jobID, name string, payload any) error {
	if name == "" {
		return fmt.Errorf("jobs: signal name must not be empty")
	}
	if strings.HasPrefix(name, "_") {
		return signal.ErrSignalNameReserved
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
		if signal.WaitingOnFutureSleep(ctx, q.Storage(), job, slog.Default()) {
			return nil
		}
		type signalResumer interface {
			ResumeSignalWaitingJob(ctx context.Context, jobID string) (bool, error)
		}
		if r, ok := q.Storage().(signalResumer); ok {
			resumed, err := r.ResumeSignalWaitingJob(ctx, jobID)
			if err != nil {
				return fmt.Errorf("jobs: signal sent but resume failed (poll will recover): %w", err)
			}
			if resumed {
				q.Emit(&core.JobResumedBySignal{JobID: jobID, SignalName: name, Timestamp: time.Now()})
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

// Sleep durably pauses the current job until at least d has elapsed. It may
// only be called from inside a registered job handler, like WaitForSignal. While
// sleeping, the job is suspended as StatusWaiting with the deadline stored in
// run_at and does not occupy a worker slot; after the deadline, any worker can
// resume it. The deadline is checkpointed on first execution, so replay after a
// crash uses the original deadline rather than restarting the duration.
//
// Non-positive durations return nil immediately without suspending, but still
// checkpoint the resolved sleep for deterministic replay. Wakeups are coarse:
// elapsed sleeps are detected by the worker's waiting-job polling backstop
// (about every 5 seconds) plus normal dispatch polling. Sleep is for backoff and
// scheduling gaps, not sub-second timing.
//
// Durable timers require storage that implements the optional signal capability
// (GormStorage does). A storage backend without it returns ErrStorageNoSignals.
func Sleep(ctx context.Context, d time.Duration) error {
	return signal.Sleep(ctx, d)
}

// SleepUntil durably pauses the current job until t. It has the same
// checkpointing, worker-slot, crash-recovery, and coarse wake-granularity
// semantics as Sleep. Times in the past return nil immediately without
// suspending, while still recording a resolved checkpoint for deterministic
// replay. Durable timers require storage that implements the optional signal
// capability (GormStorage does). A storage backend without it returns
// ErrStorageNoSignals.
func SleepUntil(ctx context.Context, t time.Time) error {
	return signal.SleepUntil(ctx, t)
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
//   - ErrJobNotCompleted only when the job is in a genuinely non-terminal state
//     (pending, running, retrying, waiting, or paused) — a poller should keep polling.
//   - An error wrapping ErrJobFailed (whose message embeds job.LastError) if the
//     job failed — errors.Is(err, ErrJobFailed) lets a poller stop.
//   - An error wrapping ErrJobCancelled if the job was cancelled.
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
		return zero, fmt.Errorf("%w: %s", core.ErrJobFailed, job.LastError)
	case core.StatusCancelled:
		return zero, fmt.Errorf("%w: %s", core.ErrJobCancelled, jobID)
	default:
		return zero, core.ErrJobNotCompleted
	}
}
