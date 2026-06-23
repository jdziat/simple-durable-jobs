---
title: "Cancel Job"
weight: 12
---

## Named cancellation

`CancelJob` is the first-class operator verb for **terminally** stopping a job:

```go
err := jobs.CancelJob(ctx, q, jobID)
```

A pending, waiting, or running job moves to a terminal `cancelled` state. Unlike
a paused job, a cancelled job is **not resumable** — `ResumeJob` will not bring
it back, and `UnpauseJob` returns `ErrJobNotPaused`. To replay a cancelled job
from scratch, use `Requeue`.

Cancellation is a dedicated storage operation (`CancelJobTerminal`), not an
alias for aggressive pause. It durably records the terminal status, clears the
job's lock, releases any fleet concurrency slot the job held, sets
`last_error` to `cancelled by user`, stamps `completed_at`, and emits a
`JobCancelled` event.

## Fan-out subtrees

When the target is a fan-out parent, its **entire descendant fan-out subtree**
is terminally cancelled in the same storage transaction:

- every direct and nested sub-job in a non-terminal state (`pending`,
  `waiting`, `running`) becomes `cancelled`;
- each affected `fan_outs` row is reconciled so its persisted counts satisfy
  `completed + failed + cancelled == total` and its status is set to the
  terminal `cancelled` state, so the completion reaper never tries to finish it;
- concurrency slots held by cancelled sub-jobs are released atomically with the
  cancel write.

Because the whole subtree is cancelled in one transaction, you never observe a
half-cancelled tree where a parent is cancelled but children keep running in the
database.

> **Operational note:** cancelling a fan-out parent is an `O(subtree)`
> single-transaction operation — every descendant sub-job is locked and updated
> in one transaction to guarantee atomicity. Cancelling a *very wide* fan-out
> parent therefore holds that many row locks for the transaction's duration and
> can briefly stall workers completing those children. The cost is bounded by
> the real subtree size.

## Cooperative handler semantics

Cancellation interrupts a locally-running handler by cancelling its
`context.Context` **after** the durable terminal write succeeds. Handlers must
observe `ctx.Done()`, pass the context into blocking calls, or otherwise check
cancellation to stop promptly.

`CancelJob` does not force-kill a handler that ignores its context. The database
state is already terminal and cannot resume; "terminal" means the durable state
won't run again, **not** that remote CPU is synchronously killed.

## Fleet-wide behavior

The cancellation is written to shared database state. If the job is running in
this process, the queue cancels its registered handler context immediately. If
the job (or a cancelled sub-job) is running on another worker, that worker stops
on the eventually-consistent ownership/heartbeat path: it observes that it no
longer owns a live, non-terminal row and cancels its local handler context.

This makes cancellation fleet-wide without requiring operators to know which
worker owns a job.

## Cancel vs graceful pause

Graceful pause stops future work without interrupting a running handler, and is
**reversible** with `ResumeJob`:

```go
err := q.PauseJob(ctx, jobID)   // recoverable
err = q.ResumeJob(ctx, jobID)
```

Cancel is **terminal** and not recoverable:

```go
err := q.CancelJob(ctx, jobID)  // terminal; use Requeue to replay
```

## Errors

| Situation | Result |
|-----------|--------|
| Pending / waiting / running job | cancelled (terminal), returns `nil` |
| Already cancelled | no-op, returns `nil` (idempotent) |
| Completed, failed, or other non-cancellable terminal state | `ErrJobNotCancellable` |
| Unknown job ID | `ErrJobNotFound` |
