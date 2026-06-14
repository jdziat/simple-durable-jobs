---
title: "Workflow Signals"
weight: 4
---

Signals let an **external producer** influence a **running or waiting** workflow
mid-flight — update an AI agent's context, inject new instructions, or unblock a
human-in-the-loop step — without restarting the job. They are durable, buffered,
and delivered **FIFO per `(job, name)`**.

Signals are built on the same **suspend → resume → checkpoint-replay** machinery
as [fan-out]({{< relref "/docs/api-reference/workflows#fan-outfan-in" >}}), so they inherit the same correctness
properties: a wait survives a process crash, and every signal *observation* is
checkpointed so handler replay is deterministic.

{{< callout type="info" >}}
Requires a storage backend that implements the optional signal capability. The
bundled `GormStorage` (SQLite/Postgres/MySQL) does. A backend without it returns
`ErrStorageNoSignals`. No schema migration is required beyond the additive
`signals` table created on `Migrate`.
{{< /callout >}}

---

## Producer side (outside the workflow)

### `(*Queue) Signal(ctx context.Context, jobID, name string, payload any) error`

Delivers a named signal carrying `payload` to a job. The signal is persisted
durably (so it is **not lost** if sent before the handler waits for it), and if
the target job is currently `waiting` on a signal, the call resumes it promptly.

```go
// hivemind wants to update a running agent's context
err := queue.Signal(ctx, agentJobID, "ctx", map[string]any{
    "instruction": "user changed their mind: optimize for latency, not cost",
})
```

- Payload is JSON-marshalled (subject to the same size limit as job results).
- Returns `ErrJobNotFound` if the job does not exist, `ErrStorageNoSignals` if
  the backend lacks signal support, `ErrSignalNameReserved` if `name` starts
  with `_`, or `ErrSignalNameTooLong` if `name` exceeds the limit.
- Sending to a **terminal** job (completed/failed/cancelled) stores the signal
  but it is never consumed; it is cleaned up when the job is deleted.

A recovery poll backstops the rare deliver-vs-suspend race (a signal delivered in
the instant a handler is transitioning to `waiting`), so a job that holds a
pending signal is always eventually resumed even if the fast-path resume misses.

---

## Consumer side (inside a job handler)

Mirror `Call[T]`/`FanOut[T]`: call them from within a registered handler.

### `WaitForSignal[T any](ctx context.Context, name string) (T, error)`

Consumes the oldest pending signal of `name`. If none is pending the job
**suspends** to `waiting` until one arrives, then resumes and returns it.
Consuming a signal is exactly-once across all wait/drain calls.

```go
queue.Register("agent", func(ctx context.Context, in AgentInput) error {
    // ... work, checkpointed via Call/SavePhaseCheckpoint ...
    update, err := jobs.WaitForSignal[ContextUpdate](ctx, "ctx")
    if err != nil {
        return err // includes the internal "waiting" sentinel — return it as-is
    }
    // resumes here with `update` once queue.Signal(..., "ctx", ...) is delivered
    return nil
})
```

### `WaitForSignalTimeout[T any](ctx context.Context, name string, d time.Duration) (T, bool, error)`

Like `WaitForSignal`, but returns `(zero, false, nil)` if no signal arrives
within `d`. The deadline is fixed on first execution and checkpointed, so
replays agree on it.

```go
update, ok, err := jobs.WaitForSignalTimeout[ContextUpdate](ctx, "ctx", 30*time.Second)
if err != nil {
    return err
}
if !ok {
    // no update within 30s — proceed with the plan as-is
}
```

### `CheckSignal[T any](ctx context.Context, name string) (T, bool, error)`

**Non-blocking, non-consuming** peek at the oldest pending signal. Returns
`(zero, false, nil)` when none is pending; the signal stays queued for a later
`WaitForSignal`/`DrainSignals`. Because it never suspends, it is the tool for an
agent that wants to *poll* for new context between steps without unwinding.

```go
if update, ok, _ := jobs.CheckSignal[ContextUpdate](ctx, "ctx"); ok {
    // there is new context waiting — decide whether to consume it now
}
```

### `DrainSignals[T any](ctx context.Context, name string) ([]T, error)`

Non-blocking; consumes **all** currently-pending signals of `name`, in FIFO
order, and returns their decoded values (empty slice if none).

```go
updates, err := jobs.DrainSignals[ContextUpdate](ctx, "ctx") // every queued update at once
```

---

## Semantics at a glance

| Function                | Consumes? | Suspends when none? | Returns |
|-------------------------|-----------|---------------------|---------|
| `WaitForSignal`         | Yes (one) | Yes (indefinitely)  | `(T, error)` |
| `WaitForSignalTimeout`  | Yes (one) | Yes (until deadline) | `(T, ok, error)` |
| `CheckSignal`           | **No**    | No                  | `(T, ok, error)` |
| `DrainSignals`          | Yes (all) | No                  | `([]T, error)` |

---

## Replay & determinism (read this)

`WaitForSignal`/`WaitForSignalTimeout` **suspend the handler**: on resume the
handler is *replayed from the start*, with prior `Call`/phase checkpoints
short-circuiting the already-done work. The same rule as fan-out applies:

- **In-flight in-memory state must be checkpointed** (via `Call` or
  `SavePhaseCheckpoint`) to survive a wait — anything you computed but did not
  checkpoint before the wait is recomputed on resume.
- **Every signal observation is itself a checkpoint** keyed by call index, so a
  wait that consumed payload `X`, a peek that found nothing, or a timeout that
  fired all replay to the same outcome — a signal arriving *after* a recorded
  peek cannot retroactively change it.
- Under **strict determinism**, signal calls count as Call-index checkpoints, so
  a *conditional* signal call must keep a deterministic call sequence (or use
  explicit/best-effort checkpoint strategies). See
  [Job Options → Determinism]({{< relref "/docs/api-reference/job-options#determinismmode-determinismmode-option" >}}).

`CheckSignal` and `DrainSignals` never suspend, so they don't unwind the handler.

---

## Observability

A successful `Signal` emits a `SignalDelivered{JobID, Name, Timestamp}` event on
`q.Events()`, so an orchestrator can observe that a signal landed (the handler
consumes it later). When a delivered signal wakes a waiting job, the queue also
emits `JobResumedBySignal{JobID, SignalName, Timestamp}`. `SignalName` is best
effort for recovery/backstop wakes and may be empty for custom storage backends
that can confirm a signal wake without cheaply reporting which signal name did
it. Durable timer wakes and `WaitForSignalTimeout` deadline expirations without a
pending signal do not emit `JobResumedBySignal`.

After any resume, the job re-runs and emits the usual `JobStarted` event.

## Consumed-signal retention

Consumed signal rows are kept by default so existing installations see no
behavior change. For high-volume workflows, opt into pruning only consumed rows
with worker retention:

```go
w := jobs.NewWorker(q,
    jobs.WithRetention(
        jobs.RetentionConsumedSignalsAfter(7*24*time.Hour),
    ),
)
```

Pending/unconsumed signals are durable workflow state and are never pruned by
this option. `DeleteJob` still removes all signal rows for the deleted job.

---

## Errors & limits

| Error | When |
|-------|------|
| `ErrJobNotFound` | `Signal` targets a job that doesn't exist |
| `ErrStorageNoSignals` | backend doesn't implement the signal capability |
| `ErrSignalNameReserved` | signal `name` starts with `_`; this namespace is reserved for library internals such as durable timers, and applies on both send and consume sides |
| `ErrSignalNameTooLong` | signal `name` exceeds `MaxSignalNameLength` (255) |

Payloads are bounded by the same maximum-result-size limit as job results.
