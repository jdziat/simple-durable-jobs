---
title: "Durable Workflows & Fan-Out"
weight: 3
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

Use `SavePhaseCheckpointTx(ctx, tx, phaseName, result)` when the phase's business write and checkpoint must commit atomically in the same GORM transaction; see [Transactional Checkpoints]({{< relref "/docs/advanced/transactional-checkpoints" >}}).

### `LoadPhaseCheckpoint[T any](ctx context.Context, phaseName string) (T, bool)`

Retrieves a previously saved phase checkpoint. Returns the result and true if found, or the zero value and false if not found.

---

## Durable Timers

### `Sleep(ctx context.Context, d time.Duration) error`

### `SleepUntil(ctx context.Context, t time.Time) error`

Durable timers suspend the current workflow until the duration has elapsed or
the target time is reached. The job moves to `waiting`, stores its wake deadline
in `run_at`, frees the worker slot, and can be resumed by any worker after a
crash or deploy. Wakeups are coarse: the worker polling backstop detects elapsed
timers at about 5-second granularity, then normal dispatch picks the job up.

See [Durable Timers]({{< relref "/docs/advanced/durable-timers" >}}) for replay
rules, signal isolation, and examples.

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
