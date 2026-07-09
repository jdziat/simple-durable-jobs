---
title: "Storage Durability & Capabilities"
weight: 7
toc: true
---

Simple Durable Jobs targets **at-least-once** execution. Two of its guarantees
depend on the storage backend implementing an *atomic* capability. `GormStorage`
(the built-in Postgres / MySQL / SQLite backend) implements both, so the default
setup is fully durable. A **custom** `core.Storage` that omits one of them still
runs, but with a reduced crash-durability guarantee — and the worker now says so
**loudly at startup** instead of degrading silently.

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

## Summary

| Capability | Interface | Missing → |
|---|---|---|
| Atomic scheduled fire | `ScheduledFireTxClaimer` + `TxEnqueuer` | a fire can be **lost** on a crash (data loss) |
| Atomic fan-out suspend | `SuspendForFanOut` | wider, **recoverable** crash window |

`GormStorage` implements both. If you write a custom backend and see a `DEGRADED
DURABILITY` warning, implement the named capability to restore the full guarantee.
