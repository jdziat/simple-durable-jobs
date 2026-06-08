---
title: "Job Options"
weight: 5
---

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

Even when a handler's own deadline or cancellation fires the moment a `Call()` step (or `SavePhaseCheckpoint()` phase) completes, that step's checkpoint is still persisted: the engine writes it on a detached context (cancellation/deadline stripped, with an independent ~5s budget), so a completed step is never lost and re-run on replay because the deadline expired microseconds after the handler returned.

### `WithHandlerBackoff(p BackoffPolicy) Option`

Sets a registration-time retry backoff policy for the handler. This overrides
the worker default set with `WithBackoff`, while `RetryAfter(d, err)` still wins
as an explicit handler-provided delay. See [Job Retry Backoff]({{< relref
"/docs/advanced/retry-backoff" >}}).

### `Determinism(mode DeterminismMode) Option`

Controls how strictly a handler's non-deterministic actions are policed on replay of a checkpointed workflow. Exported modes:

| Mode | Behavior |
|---|---|
| `ExplicitCheckpoints` *(default)* | Only values wrapped in `Call()` / `SavePhaseCheckpoint()` are persisted; direct side effects are the handler's responsibility. |
| `Strict` | Replay panics if a new `Call()` invocation appears that was not present in the checkpoint history — useful for catching accidental non-determinism. |
| `BestEffort` | Extra calls on replay are tolerated; the engine re-executes them and captures new checkpoints. |
