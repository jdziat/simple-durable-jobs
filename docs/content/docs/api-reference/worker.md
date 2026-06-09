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

### `(*Worker) Health() WorkerHealth`

Returns a local point-in-time health snapshot with `RunningCount`, `Paused`, and
`Started`.

### `(*Worker) HealthHandler() http.Handler`

Returns a standalone probe handler for headless workers. It registers
`/healthz` and `/readyz`: `/healthz` always returns `200 OK` without touching
storage, while `/readyz` calls storage `Ping(ctx)` when the backend implements
`storage.Healther` and returns `503 Service Unavailable` on ping failure.
Operator pause does not make `/readyz` fail. See [Production Operations]({{< relref "/docs/production-ops" >}}).

### `(*Worker) CancelJob(jobID string) bool`

Cancels a specific running job's context. Returns true if the job was found and cancelled, false if the job was not running on this worker.

---

## Worker Options

### `WorkerQueue(name string, opts ...WorkerOption) WorkerOption`

Configures the worker to process a specific queue.

### `Concurrency(n int) WorkerOption`

Sets the number of concurrent job processors per worker process. Default is 10.

### `ConcurrencyCap(name string, limit int, opts ...CapOption) WorkerOption`

Adds an optional database-backed fleet-wide cap. Without options, `name` is the
slot name and the cap applies across the fleet. With `CapKey(func(*Job) string)`,
the effective slot name is `name + ":" + key`, so the cap applies independently
per key. See [Concurrency Caps]({{< relref "/docs/advanced/concurrency-caps" >}}).

### `WithScheduler(enabled bool) WorkerOption`

Enables the scheduler for recurring jobs.

### `WithPollInterval(d time.Duration) WorkerOption`

Sets how often the worker polls for new jobs. The default is 100ms and the floor is 50ms (to prevent database overload). A positive value below 50ms is clamped up to 50ms (it is not discarded); a non-positive value is ignored and the previous/default interval is kept.

### `WithDequeueBatchSize(n int) WorkerOption`

Sets the per-poll cap for optional batch dequeue. The default is `10`; values
are clamped to `[1, 1000]`. Set `WithDequeueBatchSize(1)` to force single-row
claims.

### `WithDrainTimeout(d time.Duration) WorkerOption`

Sets how long `Start` waits for in-flight handlers to finish after its context
is cancelled. Default is 30 seconds. A non-positive duration aborts
immediately.

### `WithStorageRetry(config RetryConfig) WorkerOption`

Configures retry behavior for storage operations. See [Storage Retry]({{< relref "/docs/advanced/storage-retry" >}}) for when and why.

### `WithDequeueRetry(config RetryConfig) WorkerOption`

Configures retry behavior specifically for dequeue operations. When the database is temporarily unavailable, this prevents tight-loop polling. Kept separate from `WithStorageRetry` because dequeue failures are the hottest path and benefit from more aggressive backoff than one-off writes; see [Storage Retry]({{< relref "/docs/advanced/storage-retry" >}}).

### `WithRetryAttempts(attempts int) WorkerOption`

Sets the max retry attempts for storage operations. Convenience wrapper; use `WithStorageRetry` for full control.

### `DisableRetry() WorkerOption`

Disables retry for both storage and dequeue operations.

### `WithStaleLockInterval(d time.Duration) WorkerOption`

Sets how often the worker checks for stale running jobs. Default is 5 minutes.
The stale-lock reaper cannot be disabled: non-positive values keep the default,
and positive values below the 1s floor are clamped up. See [Stale Lock
Reaper]({{< relref "/docs/advanced/stale-lock-reaper" >}}) for the full story.

### `WithStaleLockAge(d time.Duration) WorkerOption`

Sets how long the owning worker must have made no contact before its job is reclaimed (reset to pending). The reaper anchors on last contact — it reclaims `status=running` jobs where `COALESCE(last_heartbeat_at, started_at, locked_until)` is older than `StaleLockAge` — not on lease (`LockedUntil`) expiry, so a still-running worker that keeps heartbeating is never reclaimed even if its stacked lease has lapsed. Default is 45 minutes. See [Stale Lock Reaper]({{< relref "/docs/advanced/stale-lock-reaper" >}}).

### `worker.WithLockDuration(d time.Duration) WorkerOption`

Overrides the per-dequeue lock duration (default: 45 minutes). Useful when your jobs routinely run longer than the default lock window and the built-in heartbeat is not enough. Currently exposed via the `pkg/worker` subpackage rather than the root facade:

```go
import "github.com/jdziat/simple-durable-jobs/v2/pkg/worker"

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
