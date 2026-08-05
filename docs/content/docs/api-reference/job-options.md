---
title: "Job Options"
weight: 5
---

### `Priority(p int) Option`

Sets job priority. Higher values run first. Default is 0.

### `Retries(n int) Option`

Sets maximum retry attempts. Default is 2. `Retries(0)` means run once and do not retry.

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

Ensures only one *unfinished* job with this `key` exists. If a matching job already exists, `Queue.Enqueue` returns `ErrDuplicateJob`. The uniqueness check runs inside a transaction with row-level locking on Postgres/MySQL and relies on SQLite's writer serialization. The key has no TTL — the guard releases as soon as the existing job reaches `completed`, `failed`, or `cancelled`, and is held in every other status: `pending`, `running`, `retrying`, and also `waiting` (parked on a signal or fan-out) and `paused`.

This page previously said "pending-or-running" in the first sentence and "releases on completed/failed/cancelled" in the last, which are not the same rule. The implementation followed the first and a `waiting` holder stopped deduplicating; it now follows the second, matching `IdempotencyKey`/`UniqueFor`, which have always treated a parked job as still in progress.

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
active-job guard: it blocks another job with the same key while the holder is in
ANY non-terminal status — pending, running, retrying, waiting or paused — and
releases only when the holder becomes terminal. `IdempotencyKey` and `UniqueFor`
are time-window guards: they keep deduplicating until the TTL expires, even if
the original job completed quickly.

That holds even when retention is more aggressive than the window. Retention
never deletes a job row that a still-live `unique_locks` row references, so a 90
day `IdempotencyKey` keeps deduplicating for the full 90 days under a 30 day (or
`jobs.DefaultRetention()`'s 7 day) completed-job window. The trade is that such a
job row is retained until its window lapses, after which the ordinary sweep
collects the job and its lock together — so pick a TTL you actually want to keep
rows for. See [Retention & GC](/docs/advanced/retention-gc/).

If you set `Unique` together with `IdempotencyKey` or `UniqueFor` on one
enqueue, the windowed unique-lock path takes precedence. The duplicate returns
the original job ID rather than `ErrDuplicateJob`.

### `Timeout(d time.Duration) Option`

Sets the maximum wall time for this job's handler execution. **The queue enforces
it.** When the deadline expires the worker cancels the handler's
`context.Context` (`pkg/worker`: the per-job timeout overrides the handler's
registration-time `Timeout`, and the handler runs under
`context.WithTimeout`). `0` means no limit, which is the default.

Cancelling a context does not kill a goroutine, so what the deadline actually
does depends on the handler:

- **A handler that propagates the cancellation** — the usual case, because any
  ctx-aware work inside it (database queries, HTTP requests, `Call()` steps)
  starts returning `context deadline exceeded` — fails the attempt with that
  error, burns a retry, and eventually dead-letters the job.
- **A handler that never checks `ctx` and returns `nil` anyway** runs to
  completion past the deadline and is recorded **completed**. The worker waits
  for it; the timeout does not abandon it or mark it failed on its own.

Either way the deadline is live, and it is not a label you can attach for
documentation purposes.

{{< callout type="warning" >}}
**Corrected.** Every release through v4.7.0 shipped this page — and the options
snippet in `README.md` — describing `Timeout` as an advisory label that
applications had to police themselves. That was never true — the worker has
always wrapped the handler in `context.WithTimeout`, and the godoc on
`jobs.Timeout` and `Queue.Enqueue` described the real behaviour all along. If you
built on the old wording, check any handler that ignores `ctx` during a long
phase: its deadline is live.
{{< /callout >}}

Even when a handler's own deadline or cancellation fires the moment a `Call()` step (or `SavePhaseCheckpoint()` phase) completes, that step's checkpoint is still persisted: the engine writes it on a detached context (cancellation/deadline stripped, with an independent ~5s budget), so a completed step is never lost and re-run on replay because the deadline expired microseconds after the handler returned.

### `WithHandlerBackoff(p BackoffPolicy) Option`

Sets a registration-time retry backoff policy for the handler. This overrides
the worker default set with `WithBackoff`, while `RetryAfter(d, err)` still wins
as an explicit handler-provided delay. See [Job Retry Backoff]({{< relref
"/docs/advanced/retry-backoff" >}}).

### `Determinism(mode DeterminismMode) Option`

Controls how strictly a handler's non-deterministic actions are policed on replay of a checkpointed workflow. Exported modes:

All three modes persist only what is wrapped in `Call()` /
`SavePhaseCheckpoint()`; direct side effects are always the handler's
responsibility. All three also let a replay issue an **extra** `Call()` that was
not in the checkpoint history: it simply executes fresh and records its own
checkpoint. What the modes actually differ on is how a replayed `Call` that
*conflicts* with the recorded history is handled.

| Mode | On a type mismatch at a call index | On recorded checkpoints the replay never reaches |
|---|---|---|
| `ExplicitCheckpoints` *(default)* | Returns a determinism-violation error from `Call()`, so the attempt fails and retries. | Tolerated. |
| `Strict` | Same as the default. | Fails the job **terminally** (non-retryable) after the handler returns: `jobs: strict determinism violation: N recorded Call checkpoint(s) were not replayed`. This is the case where the handler issued **fewer or reordered** `Call`s than the original run. |
| `BestEffort` | Logs a warning and **re-executes** the call instead of erroring. | Tolerated. |

Note what `Strict` does *not* do. It never panics — every outcome above is a
returned error — and it does not fire when a replay adds a call that was not
there before. Its extra guard is the opposite trigger: a replay that drops or
reorders calls it previously made. If you are trying to catch a handler whose
`Call` sequence *grows* nondeterministically, no mode reports it; assert on the
sequence in a test instead.
