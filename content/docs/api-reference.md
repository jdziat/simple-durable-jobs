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

Spawns sub-jobs in parallel and waits for all results. Must be called from within a job handler.

```go
results, err := jobs.FanOut[ProcessedItem](ctx, subJobs, jobs.FailFast())
if err != nil {
    return err
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

### `WithFanOutRetries(n int) FanOutOption`

Sets the retry count for sub-jobs.

### `WithFanOutTimeout(d time.Duration) FanOutOption`

Sets timeout for entire fan-out operation.

### `CancelOnParentFailure() FanOutOption`

Cancels remaining sub-jobs if parent fails.

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

Ensures only one job with this key exists.

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

Configures retry behavior for storage operations.

### `WithDequeueRetry(config RetryConfig) WorkerOption`

Configures retry behavior specifically for dequeue operations. When the database is temporarily unavailable, this prevents tight-loop polling.

### `WithRetryAttempts(attempts int) WorkerOption`

Sets the max retry attempts for storage operations. Convenience wrapper; use `WithStorageRetry` for full control.

### `DisableRetry() WorkerOption`

Disables retry for both storage and dequeue operations.

### `WithStaleLockInterval(d time.Duration) WorkerOption`

Sets how often the worker checks for stale running jobs. Default is 5 minutes. Set to 0 to disable.

### `WithStaleLockAge(d time.Duration) WorkerOption`

Sets how long a lock must be expired before the job is reclaimed. Default is 45 minutes.

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
    StatusWaiting   JobStatus = "waiting"    // Suspended waiting for sub-jobs
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

```go
*JobStarted   // Job began processing
*JobCompleted // Job finished successfully
*JobFailed    // Job failed permanently
*JobRetrying  // Job being retried
*JobPaused    // Job was paused
*JobResumed   // Job was resumed
*QueuePaused  // Queue was paused
*QueueResumed // Queue was resumed
*WorkerPaused // Worker was paused
*WorkerResumed // Worker was resumed
*CheckpointSaved // Checkpoint was saved
*CustomEvent  // Custom ephemeral event
```

---

## Hooks

### `(*Queue) OnJobStart(fn func(context.Context, *Job))`

Registers a callback for when a job starts processing.

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
