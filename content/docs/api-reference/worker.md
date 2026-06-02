---
title: "Worker"
weight: 2
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

Configures retry behavior for storage operations. See [Storage Retry]({{< relref "/docs/advanced/storage-retry" >}}) for when and why.

### `WithDequeueRetry(config RetryConfig) WorkerOption`

Configures retry behavior specifically for dequeue operations. When the database is temporarily unavailable, this prevents tight-loop polling. Kept separate from `WithStorageRetry` because dequeue failures are the hottest path and benefit from more aggressive backoff than one-off writes; see [Storage Retry]({{< relref "/docs/advanced/storage-retry" >}}).

### `WithRetryAttempts(attempts int) WorkerOption`

Sets the max retry attempts for storage operations. Convenience wrapper; use `WithStorageRetry` for full control.

### `DisableRetry() WorkerOption`

Disables retry for both storage and dequeue operations.

### `WithStaleLockInterval(d time.Duration) WorkerOption`

Sets how often the worker checks for stale running jobs. Default is 5 minutes. Set to 0 to disable. See [Stale Lock Reaper]({{< relref "/docs/advanced/stale-lock-reaper" >}}) for the full story.

### `WithStaleLockAge(d time.Duration) WorkerOption`

Sets how long a lock must be expired before the job is reclaimed. Default is 45 minutes.

### `worker.WithLockDuration(d time.Duration) WorkerOption`

Overrides the per-dequeue lock duration (default: 45 minutes). Useful when your jobs routinely run longer than the default lock window and the built-in heartbeat is not enough. Currently exposed via the `pkg/worker` subpackage rather than the root facade:

```go
import "github.com/jdziat/simple-durable-jobs/pkg/worker"

w := queue.NewWorker(worker.WithLockDuration(2 * time.Hour))
```

---

## Pause/Resume

### Job-Level Pause

```go
// Via queue (emits events)
// Default graceful mode pauses pending or waiting jobs.
queue.PauseJob(ctx, jobID)
// Aggressive mode cancels running jobs.
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
