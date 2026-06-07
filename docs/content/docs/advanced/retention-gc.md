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
