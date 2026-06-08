---
title: "Retention GC"
weight: 10
---

## Disabled by default

Completed, failed, and cancelled jobs, plus consumed signal rows, are kept
forever unless you configure retention on a worker. With no retention options,
no background retention goroutine starts and rows are never deleted
automatically.

```go
w := jobs.NewWorker(q)
```

### Startup warning

A worker started with no retention window logs one warning per process so the
opt-in footgun is loud rather than silent:

```
WARN retention is not configured; completed/failed/cancelled job rows and
consumed signals accumulate forever — enable jobs.WithRetention(...) or
jobs.DefaultRetention() to bound table growth (add
jobs.RetentionDeleteCheckpointsOnComplete() to also prune successful jobs'
checkpoints)
```

The warning fires once per worker process (deduplicated even across repeated
`Start` calls). A fleet of N workers logs it N times — one per node — which is
the intended "every node tells you once" behavior. Configuring any retention
window silences it and starts the background sweep instead.

### A conservative opt-in preset

`jobs.DefaultRetention()` is an explicit opt-in preset (not a silent default)
with conservative windows: completed jobs 7 days, terminal failed/cancelled jobs
30 days, consumed signals 7 days. Tune individual windows by composing the
`Retention*` options under `WithRetention` instead.

```go
w := jobs.NewWorker(q, jobs.DefaultRetention())
```

## Per-status windows

Use `WithRetention` with one or both per-status windows. Completed jobs use the
completed window. Failed retention covers both terminal failed jobs and
cancelled jobs.

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionCompletedAfter(7*24*time.Hour),
		jobs.RetentionFailedAfter(30*24*time.Hour),
	),
)
```

A window of `0` means keep that status forever:

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionCompletedAfter(24*time.Hour),
		jobs.RetentionFailedAfter(0),
	),
)
```

## Consumed-signal window

Signals are durable: pending/unconsumed signal rows are workflow state and are
never pruned by retention. If your workflows consume many signals and keep job
rows for a long time, add a consumed-signal window:

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionConsumedSignalsAfter(7*24*time.Hour),
	),
)
```

Only rows with `consumed_at` set and older than the configured window are
deleted. A window of `0` keeps consumed signal rows forever.

## Batch and interval tuning

Retention runs as a worker background loop. Each tick deletes matching terminal
job rows and consumed signal rows in bounded batches, then keeps looping until a
pass deletes fewer rows than the batch size.

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionCompletedAfter(14*24*time.Hour),
		jobs.RetentionFailedAfter(90*24*time.Hour),
		jobs.RetentionConsumedSignalsAfter(14*24*time.Hour),
		jobs.RetentionInterval(time.Hour),
		jobs.RetentionBatchSize(1000),
	),
)
```

Larger batches clear backlog faster but hold database write locks longer. Shorter
intervals reduce how long old rows remain visible after crossing the retention
window, at the cost of more frequent scans.

## Checkpoints

Checkpoints are the per-call replay markers that make a job exactly-once. They
are read only while a job is still being attempted (a pending/running dequeue
replays from them); a successful job is terminal and never re-dequeued, so its
checkpoints are dead weight after completion.

### Bounding the checkpoints table

By default, completed jobs keep their checkpoints so the dashboard can show a
finished workflow's phase results. If you do not need that and want a bounded
checkpoints table, opt in:

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionDeleteCheckpointsOnComplete(),
	),
)
```

With this option, a successful job's checkpoints are deleted **in the same
transaction** as its completion write. The delete commits or rolls back together
with the status flip, so a crash can never leave a completed job with orphaned
checkpoints, and a lost-ownership completion deletes nothing. The trade-off is
that completed jobs then show an empty checkpoints panel in the dashboard.

This option requires no background sweep and works independently of the
per-status windows — you can enable it with or without `WithRetention` windows.
(It does not by itself start the retention goroutine.)

### Checkpoints on failure are always kept

Checkpoint GC-on-complete only ever fires on **success**. Retryable failures
move the job back to pending and the next attempt replays from its checkpoints,
so deleting them there would break exactly-once replay. Terminally failed
(dead-lettered) jobs also keep their checkpoints for debugging; they are removed
only when a `RetentionFailedAfter` window deletes the terminal job row and its
checkpoints together.

## Storage support

Retention is an optional storage capability. The built-in `GormStorage`
implements it. Custom storage backends that do not implement the optional
retention capability keep running normally; the worker logs one warning and
disables retention.

Deletes are permanent. Terminal-job retention removes terminal job rows and
their stored checkpoints/signals, so configure windows long enough for
dashboards, audits, debugging, and any manual requeue workflow you rely on.
Consumed-signal retention removes only consumed signal rows; pending signals are
left intact.
