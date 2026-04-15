---
title: "API Reference"
weight: 2
---

Complete API documentation for Simple Durable Jobs.

## Package `jobs`

```go
import jobs "github.com/jdziat/simple-durable-jobs"
```

---

## Queue

### `New(storage Storage) *Queue`

Creates a new job queue with the given storage backend.

```go
queue := jobs.New(storage)
```

### `(*Queue) Register(name string, fn any, opts ...Option)`

Registers a job handler function. The function must have one of these signatures:

```go
func(ctx context.Context, args T) error
func(ctx context.Context, args T) (R, error)
func(args T) error
func(args T) (R, error)
```

Example:
```go
queue.Register("send-email", func(ctx context.Context, args EmailArgs) error {
    return sendEmail(args.To, args.Subject)
})
```

### `(*Queue) Enqueue(ctx context.Context, name string, args any, opts ...Option) (string, error)`

Adds a job to the queue. Returns the job ID.

```go
jobID, err := queue.Enqueue(ctx, "send-email", EmailArgs{
    To: "user@example.com",
})
```

### `(*Queue) Schedule(name string, schedule Schedule, opts ...Option)`

Registers a recurring job with the given schedule.

```go
queue.Schedule("cleanup", jobs.Every(5 * time.Minute))
```

### `(*Queue) NewWorker(opts ...WorkerOption) *Worker`

Creates a new worker for processing jobs.

```go
worker := queue.NewWorker(
    jobs.WorkerQueue("default", jobs.Concurrency(10)),
)
```

### `(*Queue) Storage() Storage`

Returns the underlying storage implementation.

---

## Worker

### `(*Worker) Start(ctx context.Context) error`

Starts the worker. Blocks until the context is cancelled. Returns the context error after shutdown.

```go
ctx, cancel := context.WithCancel(context.Background())
go worker.Start(ctx)
// Later...
cancel() // Graceful shutdown
```

### `(*Worker) Pause(mode PauseMode)`

Pauses the worker. In graceful mode, running jobs complete but no new jobs are picked up. In aggressive mode, running jobs are cancelled via context cancellation.

```go
worker.Pause(jobs.PauseModeGraceful)
worker.Pause(jobs.PauseModeAggressive)
```

### `(*Worker) Resume()`

Resumes a paused worker.

### `(*Worker) IsPaused() bool`

Returns whether the worker is currently paused.

### `(*Worker) PauseMode() PauseMode`

Returns the current pause mode (graceful or aggressive).

### `(*Worker) WaitForPause(timeout time.Duration) error`

Blocks until all running jobs complete (after a graceful pause) or the timeout expires. Returns nil if all jobs completed, or an error if the timeout was reached or the worker is not paused.

### `(*Worker) RunningJobCount() int`

Returns the number of currently executing jobs.

### `(*Worker) CancelJob(jobID string) bool`

Cancels a specific running job's context. Returns true if the job was found and cancelled, false if the job was not running on this worker.

---

## Context Helpers

### `JobFromContext(ctx context.Context) *Job`

Returns the current Job from context, or nil if not in a job handler. Use this to get the job ID for logging or progress tracking.

```go
queue.Register("my-job", func(ctx context.Context, args MyArgs) error {
    job := jobs.JobFromContext(ctx)
    log.Printf("Processing job %s", job.ID)
    return nil
})
```

### `JobIDFromContext(ctx context.Context) string`

Returns the current job ID from context, or empty string if not in a job handler.

---

## Durable Calls

### `Call[T any](ctx context.Context, name string, args any) (T, error)`

Executes a nested job call with checkpointing. Must be called from within a job handler.

```go
queue.Register("workflow", func(ctx context.Context, input Input) error {
    // This call is checkpointed
    result, err := jobs.Call[string](ctx, "step1", input)
    if err != nil {
        return err
    }

    // If this fails, step1 won't re-execute on retry
    _, err = jobs.Call[any](ctx, "step2", result)
    return err
})
```

### `SavePhaseCheckpoint(ctx context.Context, phaseName string, result any) error`

Manually saves a checkpoint for a named phase within a job handler.

### `LoadPhaseCheckpoint[T any](ctx context.Context, phaseName string) (T, bool)`

Retrieves a previously saved phase checkpoint. Returns the result and true if found, or the zero value and false if not found.

---

## Fan-Out/Fan-In

### `Sub(jobType string, args any, opts ...Option) SubJob`

Creates a sub-job definition for use with FanOut.

```go
subJobs := []jobs.SubJob{
    jobs.Sub("process-item", item1),
    jobs.Sub("process-item", item2, jobs.Priority(10)),
}
```

### `FanOut[T any](ctx context.Context, subJobs []SubJob, opts ...FanOutOption) ([]Result[T], error)`

Spawns sub-jobs in parallel and waits for all results. Must be called from within a job handler. On first execution FanOut marks the parent job `waiting`, enqueues the sub-jobs, and returns `(nil, *WaitingError)` — the worker treats that signal as "suspend, not fail". When the sub-jobs complete the worker resumes the parent, FanOut replays, detects the completed fan-out via a checkpoint, and returns the collected results.

Sub-jobs receive a deterministic `UniqueKey` of the form `fanout-<fanOutID>-<index>` so a crashed parent can replay without inserting duplicate children.

```go
results, err := jobs.FanOut[ProcessedItem](ctx, subJobs, jobs.FailFast())
if err != nil {
    return err
}
```

#### `Result[T]` struct

```go
type Result[T any] struct {
    Index int   // Position in the original subJobs slice
    Value T     // Decoded return value; zero value if Err != nil
    Err   error // Non-nil when the sub-job failed
}
```

### `Values[T any](results []Result[T]) []T`

Extracts successful values from fan-out results.

### `Partition[T any](results []Result[T]) ([]T, []error)`

Splits results into successes and failures.

### `AllSucceeded[T any](results []Result[T]) bool`

Returns true if all results succeeded.

### `SuccessCount[T any](results []Result[T]) int`

Returns the number of successful results.

### `IsWaitingError(err error) bool`

Reports whether `err` is the signal `FanOut` returns to the worker when a parent job has moved into `StatusWaiting` for its sub-jobs. Handlers generally do not need to inspect this — the worker treats it as "suspend, do not fail" automatically.

```go
if jobs.IsWaitingError(err) {
    // expected control-flow signal; worker has already suspended the parent
}
```

`IsSuspendError` is the prior name and is still exported as a deprecated alias.

---

## Fan-Out Options

### `FailFast() FanOutOption`

Fails the parent job on first sub-job failure.

### `CollectAll() FanOutOption`

Waits for all sub-jobs and returns partial results.

### `Threshold(pct float64) FanOutOption`

Succeeds if at least pct% of sub-jobs complete successfully.

### `WithFanOutQueue(name string) FanOutOption`

Sets the queue for sub-jobs.

### `WithFanOutPriority(p int) FanOutOption`

Sets the default priority for sub-jobs that do not pin their own via `Sub(...)`. Higher values run first.

### `WithFanOutRetries(n int) FanOutOption`

Sets the retry count for sub-jobs.

### `WithFanOutTimeout(d time.Duration) FanOutOption`

Records a deadline on the fan-out record (`TimeoutAt` field on the `FanOut` row) for bookkeeping and observability. **Not automatically enforced** — applications should react to the timeout via their own monitoring or by checking `FanOut.TimeoutAt` before acting on partial results.

### `CancelOnParentFailure() FanOutOption`

Marks the fan-out so that if the parent job itself enters `failed` before collecting results, the worker cancels any still-pending sub-jobs instead of leaving them to run.

---

## Pause/Resume

### Job-Level Pause

```go
// Via queue (emits events, handles running jobs)
queue.PauseJob(ctx, jobID)
queue.PauseJob(ctx, jobID, jobs.WithPauseMode(jobs.PauseModeAggressive))
queue.ResumeJob(ctx, jobID)
paused, err := queue.IsJobPaused(ctx, jobID)
pausedJobs, err := queue.GetPausedJobs(ctx, "emails")
```

### Queue-Level Pause

```go
// Via queue (emits events)
queue.PauseQueue(ctx, "emails")
queue.ResumeQueue(ctx, "emails")
paused, err := queue.IsQueuePaused(ctx, "emails")
queues, err := queue.GetPausedQueues(ctx)
```

### Standalone Functions

For direct storage operations without a queue instance:

```go
jobs.PauseJob(ctx, storage, jobID)
jobs.ResumeJob(ctx, storage, jobID)
jobs.IsJobPaused(ctx, storage, jobID)
jobs.GetPausedJobs(ctx, storage, "emails")
jobs.PauseQueue(ctx, storage, "emails")
jobs.ResumeQueue(ctx, storage, "emails")
jobs.IsQueuePaused(ctx, storage, "emails")
jobs.GetPausedQueues(ctx, storage)
```

### Pause Modes

```go
const (
    PauseModeGraceful  // Let running jobs finish, stop picking new ones
    PauseModeAggressive // Cancel running jobs immediately via context
)
```

---

## Job Options

### `Priority(p int) Option`

Sets job priority. Higher values run first. Default is 0.

### `Retries(n int) Option`

Sets maximum retry attempts. Default is 3.

### `Delay(d time.Duration) Option`

Delays job execution by the specified duration.

### `At(t time.Time) Option`

Schedules the job to run at a specific time.

### `QueueOpt(name string) Option`

Assigns the job to a specific queue.

### `Unique(key string) Option`

Ensures only one pending-or-running job with this `key` exists. If a matching job already exists, `Queue.Enqueue` returns `ErrDuplicateJob`. The uniqueness check runs inside a transaction with row-level locking on Postgres/MySQL and relies on SQLite's writer serialization. The key has no TTL — the guard releases as soon as the existing job reaches `completed`, `failed`, or `cancelled`.

### `Timeout(d time.Duration) Option`

Records a per-job timeout on the job record. The value is surfaced on the job metadata and in events — applications should enforce it via the handler's `context.Context` or external monitoring; the queue does not cancel handlers automatically.

### `Determinism(mode DeterminismMode) Option`

Controls how strictly a handler's non-deterministic actions are policed on replay of a checkpointed workflow. Exported modes:

| Mode | Behavior |
|---|---|
| `ExplicitCheckpoints` *(default)* | Only values wrapped in `Call()` / `SavePhaseCheckpoint()` are persisted; direct side effects are the handler's responsibility. |
| `Strict` | Replay panics if a new `Call()` invocation appears that was not present in the checkpoint history — useful for catching accidental non-determinism. |
| `BestEffort` | Extra calls on replay are tolerated; the engine re-executes them and captures new checkpoints. |

---

## Worker Options

### `WorkerQueue(name string, opts ...WorkerOption) WorkerOption`

Configures the worker to process a specific queue.

### `Concurrency(n int) WorkerOption`

Sets the number of concurrent job processors. Default is 10.

### `WithScheduler(enabled bool) WorkerOption`

Enables the scheduler for recurring jobs.

### `WithPollInterval(d time.Duration) WorkerOption`

Sets how often the worker polls for new jobs.

### `WithStorageRetry(config RetryConfig) WorkerOption`

Configures retry behavior for storage operations. See [Storage Retry]({{< relref "advanced/storage-retry" >}}) for when and why.

### `WithDequeueRetry(config RetryConfig) WorkerOption`

Configures retry behavior specifically for dequeue operations. When the database is temporarily unavailable, this prevents tight-loop polling. Kept separate from `WithStorageRetry` because dequeue failures are the hottest path and benefit from more aggressive backoff than one-off writes; see [Storage Retry]({{< relref "advanced/storage-retry" >}}).

### `WithRetryAttempts(attempts int) WorkerOption`

Sets the max retry attempts for storage operations. Convenience wrapper; use `WithStorageRetry` for full control.

### `DisableRetry() WorkerOption`

Disables retry for both storage and dequeue operations.

### `WithStaleLockInterval(d time.Duration) WorkerOption`

Sets how often the worker checks for stale running jobs. Default is 5 minutes. Set to 0 to disable. See [Stale Lock Reaper]({{< relref "advanced/stale-lock-reaper" >}}) for the full story.

### `WithStaleLockAge(d time.Duration) WorkerOption`

Sets how long a lock must be expired before the job is reclaimed. Default is 45 minutes.

### `worker.WithLockDuration(d time.Duration) WorkerOption`

Overrides the per-dequeue lock duration (default: 45 minutes). Useful when your jobs routinely run longer than the default lock window and the built-in heartbeat is not enough. Currently exposed via the `pkg/worker` subpackage rather than the root facade:

```go
import "github.com/jdziat/simple-durable-jobs/pkg/worker"

w := queue.NewWorker(worker.WithLockDuration(2 * time.Hour))
```

---

## Types

### `RetryConfig`

```go
type RetryConfig struct {
    MaxAttempts       int           // Default: 5
    InitialBackoff    time.Duration // Default: 100ms
    MaxBackoff        time.Duration // Default: 5s
    BackoffMultiplier float64       // Default: 2.0
    JitterFraction    float64       // Default: 0.1 (10%)
}
```

### `DefaultRetryConfig() RetryConfig`

Returns the default retry configuration.

### `Job`

```go
type Job struct {
    ID              string
    Type            string
    Args            []byte      // JSON-encoded arguments
    Queue           string
    Priority        int
    Status          JobStatus
    Attempt         int
    MaxRetries      int
    LastError       string
    RunAt           *time.Time
    StartedAt       *time.Time
    CompletedAt     *time.Time
    CreatedAt       time.Time
    UpdatedAt       time.Time
    LastHeartbeatAt *time.Time  // Updated by worker heartbeats
}
```

### `JobStatus`

```go
const (
    StatusPending   JobStatus = "pending"
    StatusRunning   JobStatus = "running"
    StatusCompleted JobStatus = "completed"
    StatusFailed    JobStatus = "failed"
    StatusRetrying  JobStatus = "retrying"
    StatusWaiting   JobStatus = "waiting"    // Parent waiting for fan-out sub-jobs
    StatusCancelled JobStatus = "cancelled"  // Terminated before completion
    StatusPaused    JobStatus = "paused"     // Paused by user
)
```

### `PauseMode`

```go
const (
    PauseModeGraceful   // Let running jobs finish
    PauseModeAggressive // Cancel running jobs immediately
)
```

### `SubJobFailure`

Populated on `FanOutError.Failures` when a fan-out returns with at least one failed sub-job.

```go
type SubJobFailure struct {
    Index   int    // Position in the original subJobs slice
    JobID   string // Sub-job ID; useful for cross-referencing logs and events
    Error   string // Last error message reported by the sub-job
    Attempt int    // Attempt number that produced Error
}
```

### Error Variables

Sentinel errors exported from the `jobs` package. Compare with `errors.Is`:

```go
var (
    ErrInvalidJobTypeName error // Job type name failed validation
    ErrJobTypeNameTooLong error
    ErrInvalidQueueName   error
    ErrQueueNameTooLong   error
    ErrJobArgsTooLarge    error // Marshalled args exceed MaxJobArgsSize
    ErrJobNotOwned        error // Complete/Fail called by a worker that does not hold the lock
    ErrDuplicateJob       error // Unique()-keyed job already pending/running
    ErrUniqueKeyTooLong   error
    ErrJobAlreadyPaused   error
    ErrJobNotPaused       error
    ErrQueueAlreadyPaused error
    ErrQueueNotPaused     error
    ErrCannotPauseStatus  error // Attempted to pause a job in an incompatible status
)
```

---

## Schedules

### `Every(d time.Duration) Schedule`

Creates a schedule that runs at fixed intervals.

### `Daily(hour, minute int) Schedule`

Creates a schedule that runs daily at the specified time (UTC).

### `Weekly(day time.Weekday, hour, minute int) Schedule`

Creates a schedule that runs weekly on the specified day and time (UTC).

### `Cron(expr string) Schedule`

Creates a schedule from a cron expression.

---

## Events

### `(*Queue) Events() <-chan Event`

Returns a channel that receives job events. Caller must call `Unsubscribe` when done.

### `(*Queue) Unsubscribe(ch <-chan Event)`

Removes a subscriber channel. The channel is not closed; callers must stop reading before calling this.

### `(*Queue) Emit(event Event)`

Emits an event to all subscribers. Non-blocking; drops events if a subscriber's buffer is full.

### `(*Queue) EmitCustomEvent(jobID, kind string, data map[string]any)`

Emits a custom ephemeral event (not persisted).

### Event Types

Every event implements the `Event` interface. Type-switch on the pointer type in your subscriber loop; each payload's fields are listed below.

```go
// Lifecycle
type JobStarted struct {
    Job       *Job
    Timestamp time.Time
}

type JobCompleted struct {
    Job       *Job
    Duration  time.Duration
    Timestamp time.Time
}

type JobFailed struct {
    Job       *Job
    Error     error
    Timestamp time.Time
}

type JobRetrying struct {
    Job       *Job
    Attempt   int
    Error     error
    NextRunAt time.Time
    Timestamp time.Time
}

type CheckpointSaved struct {
    JobID     string
    CallIndex int
    CallType  string  // e.g. "call", "fanout", "phase"
    Timestamp time.Time
}

// Pause / resume
type JobPaused struct {
    Job       *Job
    Mode      PauseMode
    Timestamp time.Time
}

type JobResumed struct {
    Job       *Job
    Timestamp time.Time
}

type QueuePaused struct {
    Queue     string
    Timestamp time.Time
}

type QueueResumed struct {
    Queue     string
    Timestamp time.Time
}

type WorkerPaused struct {
    WorkerID  string
    Mode      PauseMode
    Timestamp time.Time
}

type WorkerResumed struct {
    WorkerID  string
    Timestamp time.Time
}

// Ephemeral / custom
type CustomEvent struct {
    JobID     string
    Kind      string         // "progress", "phase_change", "log", …
    Data      map[string]any
    Timestamp time.Time
}
```

---

## Hooks

### `(*Queue) OnJobStart(fn func(context.Context, *Job))`

Registers a callback for when a job starts processing.

### `(*Queue) OnJobStartCtx(fn func(context.Context, *Job) context.Context)`

Registers a context-transforming callback that runs when a job starts. The returned `context.Context` is threaded into the handler, so hooks can attach values (OTel spans, tenant IDs, correlation IDs, …) that downstream code reads out of the context. This is the hook the built-in OTel instrumentation (`pkg/otel.Instrument`) uses to re-attach the enqueue-time trace span to the worker-side handler.

### `(*Queue) OnJobComplete(fn func(context.Context, *Job))`

Registers a callback for when a job completes successfully.

### `(*Queue) OnJobFail(fn func(context.Context, *Job, error))`

Registers a callback for when a job fails permanently.

### `(*Queue) OnRetry(fn func(context.Context, *Job, int, error))`

Registers a callback for when a job is being retried.

---

## Error Handling

### `NoRetry(err error) error`

Wraps an error to indicate the job should not be retried.

### `RetryAfter(d time.Duration, err error) error`

Wraps an error to indicate the job should be retried after a specific duration.

---

## OpenTelemetry Tracing

### `otel.Instrument(q *Queue, opts ...otel.Option)`

Imports:

```go
import jobsotel "github.com/jdziat/simple-durable-jobs/pkg/otel"
```

Attaches tracing to a queue with no hand-rolled hook wiring. By default the instrumentation uses the global `TracerProvider`; pass `jobsotel.WithTracerProvider(tp)` to override.

```go
jobsotel.Instrument(queue) // uses otel.GetTracerProvider()
// or
jobsotel.Instrument(queue, jobsotel.WithTracerProvider(tp))
```

Spans emitted:

| Span | When |
|---|---|
| `job.enqueue` | `Queue.Enqueue` — producer span; trace context is persisted on the job so the consumer picks it up. |
| `job.process {type}` | Worker handler execution — child of `job.enqueue`. |
| `job.retry` | Span event added to `job.process` when a job is retried. |

Every span carries `job.id`, `job.type`, `job.queue`, `job.attempt`, and `job.priority` attributes.

---

## Embedded UI

### `ui.Handler(storage core.Storage, opts ...Option) http.Handler`

Creates an HTTP handler serving the dashboard and Connect-RPC API.

```go
import "github.com/jdziat/simple-durable-jobs/ui"

mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage,
    ui.WithQueue(queue),
    ui.WithContext(ctx),
    ui.WithStatsRetention(7 * 24 * time.Hour),
    ui.WithMiddleware(authMiddleware),
)))
```

### UI Options

| Option | Description |
|--------|-------------|
| `WithQueue(q)` | Provides queue for event streaming and scheduled jobs view |
| `WithContext(ctx)` | Lifecycle context for background goroutines; when cancelled, workers flush and exit |
| `WithStatsRetention(d)` | How long stats rows are kept (default: 7 days) |
| `WithMiddleware(mw)` | Wraps handler with middleware (auth, logging, etc.) |

---

## Storage

### `NewGormStorage(db *gorm.DB) *GormStorage`

Creates a new GORM-based storage implementation.

### `NewGormStorageWithPool(db *gorm.DB, opts ...PoolOption) (*GormStorage, error)`

Creates storage with connection pool configuration.

### Pool Presets

```go
jobs.DefaultPoolConfig()              // Sensible defaults
jobs.HighConcurrencyPoolConfig()      // High-concurrency workloads
jobs.LowLatencyPoolConfig()          // Low-latency optimized
jobs.ResourceConstrainedPoolConfig() // Limited resources
```

| Preset | MaxOpen | MaxIdle | MaxLifetime | MaxIdleTime |
|--------|---------|---------|-------------|-------------|
| `DefaultPoolConfig()` | 25 | 10 | 5min | 1min |
| `HighConcurrencyPoolConfig()` | 100 | 25 | 10min | 2min |
| `LowLatencyPoolConfig()` | 50 | 40 | 15min | 5min |
| `ResourceConstrainedPoolConfig()` | 10 | 5 | 3min | 30s |

### Pool Option Functions

### `MaxOpenConns(n int) PoolOption`

Sets the maximum number of open connections.

### `MaxIdleConns(n int) PoolOption`

Sets the maximum number of idle connections.

### `ConnMaxLifetime(d time.Duration) PoolOption`

Sets the maximum connection lifetime.

### `ConnMaxIdleTime(d time.Duration) PoolOption`

Sets the maximum idle time for connections.

### `ConfigurePool(db *gorm.DB, opts ...PoolOption) error`

Applies pool configuration to a GORM database connection.
