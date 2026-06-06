---
title: "Cancel Job"
weight: 12
---

## Named cancellation

`CancelJob` is the first-class operator verb for stopping a running job:

```go
err := jobs.CancelJob(ctx, q, jobID)
```

It is intentionally a thin alias for aggressive pause:

```go
err := q.PauseJob(ctx, jobID, jobs.WithPauseMode(jobs.PauseModeAggressive))
```

No extra storage method, table, or migration is involved.

## Cooperative semantics

Cancellation interrupts the running handler by cancelling its `context.Context`.
Handlers must observe `ctx.Done()`, pass the context into blocking calls, or
otherwise check cancellation to stop promptly.

`CancelJob` does not force-kill a handler that ignores its context. In that
case, the job row is still durably marked cancelled, and the worker ownership
audit cancels the local handler context when it detects the shared DB state, but
application code that never checks the context can keep running until it returns.

## Fleet-wide behavior

The cancellation is written to shared database state. If the job is running in
the same process, the queue cancels its registered handler context immediately.
If the job is running on another worker, that worker's ownership audit observes
the cancelled row and cancels its local handler context.

This makes cancellation fleet-wide without requiring operators to know which
worker owns a job.

## Cancel vs graceful pause

Graceful pause is for stopping future work without interrupting a running
handler:

```go
err := q.PauseJob(ctx, jobID)
```

Cancel is for interrupting a running handler cooperatively:

```go
err := q.CancelJob(ctx, jobID)
```

Pending or waiting jobs follow the same underlying pause behavior. Already
paused jobs, terminal jobs, and missing jobs return the same sentinel errors as
the pause path, including `ErrJobAlreadyPaused`, `ErrCannotPauseStatus`, and
`ErrJobNotFound`.
