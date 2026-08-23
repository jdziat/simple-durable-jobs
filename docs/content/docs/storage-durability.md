---
title: "Storage Durability & Capabilities"
weight: 7
toc: true
---

Simple Durable Jobs targets **at-least-once** execution. Three of its guarantees
depend on the storage backend implementing an *atomic* capability. `GormStorage`
(the built-in Postgres / MySQL / SQLite backend) implements all of them, so the
default setup is fully durable. A **custom** `core.Storage` that omits one still
runs — every path has a `core.Storage`-only fallback — but with a reduced
crash-durability guarantee. The worker says so at startup, loudly for the two
that risk data loss or a wide crash window, and in the
`storage backend missing optional capabilities` line for the rest, instead of
degrading silently.

## Atomic scheduled fires (`ScheduledFireTxClaimer` + `TxEnqueuer`)

A scheduled fire must *claim the fire boundary* and *enqueue the job* as one unit.
When the storage supports the atomic path, a crash can never record a fire as
"claimed" without also enqueuing its job.

Without it, `EnqueueScheduledFire` falls back to a non-atomic
claim-then-enqueue: if the process crashes **between** the two steps, the fire is
recorded as done but the job was never enqueued — **the fire is lost** (a missed
scheduled run). Because this is silent data loss, the worker logs a `DEGRADED
DURABILITY` warning at startup **when schedules are configured** on a storage that
lacks the atomic path:

```
DEGRADED DURABILITY: scheduled jobs are configured but storage lacks atomic
scheduled-fire enqueue (ScheduledFireTxClaimer + TxEnqueuer); a fire can be LOST
if this worker crashes between claiming the fire boundary and enqueuing the job.
```

Use `GormStorage` (or a `storage.TxEnqueuer` + `storage.ScheduledFireTxClaimer`
storage exposing `DB() *gorm.DB`) for at-least-once scheduled fires.

## Atomic fan-out suspend (`SuspendForFanOut`)

`FanOut()` must atomically create the fan-out, checkpoint the parent, mark it
waiting, and enqueue the children. With the atomic path, a crash either lands all
four or none — a waiting parent is never stranded with missing children.

Without it, `FanOut()` uses the legacy four-write fallback. A crash mid-sequence
leaves the parent **running + locked** until the stale-lock reaper reclaims it —
**recoverable**, but a wider crash window than the atomic path. The worker warns
at startup whenever the capability is absent (any handler may call `FanOut`):

```
DEGRADED DURABILITY: storage lacks atomic fan-out suspend (SuspendForFanOut);
FanOut() uses the legacy non-atomic fallback with a wider crash window ...
```

## Atomic completion (`CompleteWithResult`)

Completing a job means three writes: store the handler's result, flip the row to
`completed`, and — for a fan-out sub-job — increment its fan-out's completed
count. With `CompleteWithResult` all three land in ONE transaction.

Without it, the worker falls back to the plain `core.Storage` sequence:
`SaveJobResult`, then `Complete`, then `IncrementFanOutCompleted`.

A crash between the first two replays the job (the row is still `running`, so the
stale-lock reaper reclaims it — at-least-once, as designed). That window is
**recoverable on any backend**.

A crash between `Complete` and the fan-out increment is the one to understand,
because what recovers it depends on the backend:

- **`GormStorage`**: nothing is lost, because nothing was counted. Its fan-out
  counts are *derived*, not accumulated — `GetFanOut` and
  `IncrementFanOutCompleted` both overlay live `COUNT(*)` of the child jobs by
  status, so the child's already-committed `completed` row is the count. The
  parent is then resumed by `GetCompletablePendingFanOuts`
  (`FanOutRecoveryStaleAge`), which finds pending fan-outs whose live child counts
  already satisfy a terminal condition while the parent sits `waiting`.
- **A custom backend that stores `completed_count` as a real column and loses the
  increment**: the completion **is** lost. Nothing in the required `core.Storage`
  interface recovers it. `GetStalledFanOutParents` cannot: its contract is
  "a pending fan-out with FEWER CHILD ROWS than `total_count`" — a fan-out whose
  creation never finished — so a fan-out with all its children present and a
  counter one short matches nothing. `GetWaitingJobsToResume` excludes any parent
  with a still-`pending` fan-out, which this one is. The parent waits forever, or,
  on a backend whose scan is more permissive, is resumed every recovery tick,
  replays the whole handler, finds the fan-out still non-terminal and marks itself
  `waiting` again — a workflow that never finishes even though every sub-job
  completed.

So if you write a custom backend, implement **either** `CompleteWithResult`
(closing the window outright) **or** both of the mechanisms `GormStorage` relies
on: derive fan-out counts live from child-job statuses in `GetFanOut`, and
implement the optional `GetCompletablePendingFanOuts`. Implementing only the
required interface and a stored counter leaves a fan-out parent that can wedge.

The capability line logged at startup names this one as
`atomic-complete-with-result`.

## Summary

| Capability | Interface | Missing → |
|---|---|---|
| Ownership-fenced checkpoints | `SaveCheckpointOwned` | a stale double-run execution can overwrite the current owner's durable `Call` or signal verdict (**silent replay corruption**) |
| Atomic scheduled fire | `ScheduledFireTxClaimer` + `TxEnqueuer` | a fire can be **lost** on a crash (data loss) |
| Atomic fan-out suspend | `SuspendForFanOut` | wider, **recoverable** crash window |
| Atomic completion | `CompleteWithResult` | split completion writes. **Recoverable** on `GormStorage` (a replayed job, or a parent resumed by `GetCompletablePendingFanOuts`). On a backend with a stored `completed_count` and no `GetCompletablePendingFanOuts`, a lost fan-out increment is **not** recoverable and the parent wedges — see above. |

`GormStorage` implements all four. If you write a custom backend and see a
`DEGRADED DURABILITY` warning, implement the named capability to restore the full
guarantee.
