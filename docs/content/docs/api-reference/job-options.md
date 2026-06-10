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

### `WithTenant(tenant string) Option`

Sets the tenant that owns the job. The value is persisted on `Job.Tenant` and
can be filtered programmatically with `SearchJobs` and `JobFilter.Tenant`. The
embedded dashboard's Jobs view also exposes a tenant filter, and job detail
pages display the tenant value.

```go
jobID, err := queue.Enqueue(ctx, "sync-account", args,
    jobs.WithTenant("tenant-a"),
)
```

### `WithMetadata(metadata map[string]string) Option`

Replaces the job metadata map with a defensive copy. Metadata is persisted on
`Job.Metadata` as string key/value tags for filtering and operational display.
The embedded dashboard displays these tags on the job detail page.

```go
jobID, err := queue.Enqueue(ctx, "sync-account", args,
    jobs.WithMetadata(map[string]string{
        "region": "us",
        "plan":   "pro",
    }),
)
```

### `WithMeta(key, value string) Option`

Adds or replaces one metadata key/value pair. If you combine it with
`WithMetadata`, option order matters: `WithMetadata` replaces metadata set by
earlier metadata options, while later `WithMeta` calls update the map.

```go
jobID, err := queue.Enqueue(ctx, "sync-account", args,
    jobs.WithMetadata(map[string]string{"region": "us"}),
    jobs.WithMeta("plan", "pro"),
)
```

Storage UI filters expose `JobFilter.MetaContains` for metadata search.
`GormStorage` implements it as portable substring matching over serialized JSON
metadata, not exact structured key/value matching, so values containing the same
serialized fragment can over-match.

### `Unique(key string) Option`

Ensures only one pending-or-running job with this `key` exists. If a matching job already exists, `Queue.Enqueue` returns `ErrDuplicateJob`. The uniqueness check runs inside a transaction with row-level locking on Postgres/MySQL and relies on SQLite's writer serialization. The key has no TTL — the guard releases as soon as the existing job reaches `completed`, `failed`, or `cancelled`.

### `IdempotencyKey(key string, ttl time.Duration) Option`

Deduplicates enqueue attempts with the same caller-supplied key for `ttl`.
The scope is the queue, job name, and key. If a live idempotency window already
exists, `Queue.Enqueue` returns the original job ID and does not create another
job row.

Use this for API request idempotency, such as honoring an HTTP
`Idempotency-Key` header for 24 hours.

```go
jobID, err := queue.Enqueue(ctx, "charge-card", paymentID,
    jobs.IdempotencyKey(requestID, 24*time.Hour),
)
```

### `UniqueFor(ttl time.Duration) Option`

Deduplicates enqueue attempts with the same queue, job name, and canonical
plaintext arguments for `ttl`. Here "canonical" means the `json.Marshal` output
of your normalized arguments; map keys are sorted by the encoder, but slice
order and numeric representation are still your responsibility. If a matching
live window already exists,
`Queue.Enqueue` returns the original job ID and creates no second job row.

Use this when the job arguments themselves identify the work, such as "sync
this account at most once per hour".

```go
jobID, err := queue.Enqueue(ctx, "sync-account", args,
    jobs.UniqueFor(time.Hour),
)
```

`Unique` and the windowed options solve different problems. `Unique` is an
active-job guard: it blocks another pending or running job with the same key,
then releases when that job becomes terminal. `IdempotencyKey` and `UniqueFor`
are time-window guards: they keep deduplicating until the TTL expires, even if
the original job completed quickly.

If you set `Unique` together with `IdempotencyKey` or `UniqueFor` on one
enqueue, the windowed unique-lock path takes precedence. The duplicate returns
the original job ID rather than `ErrDuplicateJob`.

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
