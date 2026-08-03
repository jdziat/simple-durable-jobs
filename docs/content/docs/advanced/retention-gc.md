---
title: "Retention GC"
weight: 10
---

## Enabled by default

Workers delete terminal job rows and consumed signal rows on their own. A worker
built with no retention options starts a background retention loop using the
stock windows below — there is nothing to switch on, and rows older than a window
are deleted permanently.

```go
// Prunes terminal rows with the stock windows below.
w := jobs.NewWorker(q)
```

### Default windows

<!--
The three window values in this table are pinned to the code constants by
TestRetentionDocMatchesStockWindows (pkg/worker/retention_docs_test.go). Change a
number here and in pkg/worker/options.go together or CI fails, on purpose: this
page told operators retention was DISABLED by default while every worker was
already deleting completed jobs at 30 days and dead-letter rows at 90 (teardown
2026-07-24, PKT-02a). A manual that inverts the code costs someone their job
history.
-->

| Rows deleted | Default window | Option that changes it |
| --- | --- | --- |
| Completed jobs | 30 days | `RetentionCompletedAfter` |
| Terminal failed and cancelled jobs | 90 days | `RetentionFailedAfter` |
| Consumed signal rows | 7 days | `RetentionConsumedSignalsAfter` |

Pending (unconsumed) signal rows are workflow state and are never pruned. If an
audit, compliance, or archival policy needs terminal job history for longer than
these windows, widen them with `WithRetention` or turn retention off and manage
deletion yourself — the sweep does not ask, and the deletes are not recoverable.

### Startup log

Retention announces itself once per worker at `Start`, so the effective policy is
visible in every node's log:

```text
INFO retention GC enabled completed_after=720h0m0s failed_after=2160h0m0s consumed_signals_after=168h0m0s disable_hint="disable with jobs.RetentionDisabled()"
```

A worker whose retention is turned off logs one warning instead:

```text
WARN retention is disabled; completed/failed/cancelled job rows and consumed signals accumulate forever
```

Either line is emitted at most once per worker, deduplicated across repeated
`Start` calls. A fleet of N workers logs it N times — one per node — which is the
intended "every node tells you once" behavior.

## Turning retention off

`jobs.RetentionDisabled()` stops all automatic deletion of terminal jobs and
consumed signals. Use it when retention is managed outside the worker (partition
drop, an external archiver) or when rows must be kept indefinitely.

```go
w := jobs.NewWorker(q, jobs.RetentionDisabled())
```

## Per-status windows

Use `WithRetention` to set the windows yourself. Completed jobs use the completed
window; the failed window covers both terminal failed jobs and cancelled jobs.

`WithRetention` **replaces** the stock windows rather than merging with them. Any
window you leave out of the call is `0`, and a `0` window keeps that status
forever, so list every window you want enforced.

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionCompletedAfter(7*24*time.Hour),
		jobs.RetentionFailedAfter(30*24*time.Hour),
		// Consumed signal rows are now kept forever: this call REPLACES the
		// stock windows, so the one it omits is 0.
	),
)
```

Set a window to `0` deliberately to keep that status forever:

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionCompletedAfter(24*time.Hour),
		jobs.RetentionFailedAfter(0),
	),
)
```

## The DefaultRetention preset

`jobs.DefaultRetention()` is **not** what a default worker uses. It is an
explicit preset whose windows are *tighter* than the stock ones — it deletes
completed jobs after a week rather than a month:

<!--
Pinned to the option itself by TestRetentionDocMatchesDefaultRetentionPreset
(pkg/worker/retention_docs_test.go).
-->

| Rows deleted | `DefaultRetention()` window |
| --- | --- |
| Completed jobs | 7 days |
| Terminal failed and cancelled jobs | 30 days |
| Consumed signal rows | 7 days |

```go
w := jobs.NewWorker(q, jobs.DefaultRetention())
```

It is an ordinary `WithRetention` preset, so the replace-not-merge rule above
applies to it too. Compose the `Retention*` options under `WithRetention` when
you want different windows.

## Consumed-signal window

Signals are durable: pending/unconsumed signal rows are workflow state and are
never pruned by retention. Only rows with `consumed_at` set and older than the
configured window are deleted.

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionCompletedAfter(30*24*time.Hour),
		jobs.RetentionFailedAfter(90*24*time.Hour),
		jobs.RetentionConsumedSignalsAfter(14*24*time.Hour),
	),
)
```

A window of `0` keeps consumed signal rows forever.

## Batch and interval tuning

Retention runs as a worker background loop. By default it wakes every hour and
deletes up to 1000 rows per statement, and within a tick it keeps looping until a
pass deletes fewer rows than the batch size, so a large backlog drains in one
tick instead of one batch per hour.

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

`RetentionBatchSize` accepts 1..10000 and clamps anything larger to 10000. The
loop already drains a backlog within one tick, so a bigger batch buys fewer round
trips rather than more throughput, and the ceiling bounds how long one pass holds
write locks.

## Live idempotency windows pin their job rows

Retention never deletes a terminal job that a still-live `unique_locks` row
references. `jobs.IdempotencyKey` and `jobs.UniqueFor` promise to keep
deduplicating until their own TTL expires, and an operator who writes
`jobs.IdempotencyKey("invoice-42", 90*24*time.Hour)` means it — if the sweep
removed the job at the 30 day (or `jobs.DefaultRetention()`'s 7 day) horizon, the
replayed request on day 31 would enqueue a second job and do the guarded work
twice.

So a job row guarded by a window is retained for `max(retention window, window
TTL)`. Once the window lapses, the next pass collects the job row and the lock
row together, so growth stays bounded by the TTL you chose — but a long TTL is a
deliberate decision to keep those rows that long. To remove such a job sooner,
delete it explicitly: `DeleteJob`, `DeleteWorkflowSubtree` and the dashboard's
purge all release the job's window along with the row.

## Windowed unique-lock GC

`jobs.IdempotencyKey` and `jobs.UniqueFor` use a separate `unique_locks` table
to remember time-windowed enqueue deduplication keys. Each row stores the
deduplication scope, the original job ID returned to duplicate enqueue callers,
and the window expiry time. This table is separate from the active-job
`Unique` guard, so it can keep deduplicating after the original job has already
completed.

Expired `unique_locks` rows are swept by their own worker background loop with
their own options. Like retention, this sweep is on by default; unlike retention,
it stays on even when terminal-job retention is disabled, so windowed enqueue
deduplication always bounds its own table. By default, the sweep runs every hour
and deletes up to 1000 expired locks per pass.

Tune the cadence and batch size with `WithUniqueLockSweep`:

```go
w := jobs.NewWorker(q,
	jobs.WithUniqueLockSweep(
		jobs.UniqueLockSweepInterval(time.Hour),
		jobs.UniqueLockSweepBatchSize(1000),
	),
)
```

`UniqueLockSweepBatchSize` accepts 1..10000 and clamps larger values, like
`RetentionBatchSize`.

You can disable the sweep if another process owns unique-lock cleanup:

```go
w := jobs.NewWorker(q,
	jobs.WithUniqueLockSweep(jobs.UniqueLockSweepDisabled()),
)
```

Disabling this sweep means expired `unique_locks` rows remain until your own
cleanup deletes them. Live deduplication still treats expired rows as
replaceable at enqueue time, but the table will grow without a sweeper.

## Checkpoints

Checkpoints are the per-call replay markers that let a job resume without
re-running already-completed steps (the basis for exactly-once *effects* when
your handlers are idempotent; execution itself is at-least-once). They are read
only while a job is still being attempted (a pending/running dequeue replays
from them); after a successful job reaches its terminal state, it is not
designed to be re-dequeued, so its checkpoints are dead weight after completion.

### Bounding the checkpoints table

By default, completed jobs keep their checkpoints so the dashboard can show a
finished workflow's phase results. If you do not need that and want a bounded
checkpoints table, opt in:

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionCompletedAfter(30*24*time.Hour),
		jobs.RetentionFailedAfter(90*24*time.Hour),
		jobs.RetentionConsumedSignalsAfter(7*24*time.Hour),
		jobs.RetentionDeleteCheckpointsOnComplete(),
	),
)
```

With this option, a successful job's checkpoints are deleted **in the same
transaction** as its completion write. The delete commits or rolls back together
with the status flip; the library is designed so that a crash does not leave a
completed job with orphaned checkpoints, and a lost-ownership completion deletes
nothing. The trade-off is that completed jobs then show an empty checkpoints
panel in the dashboard.

The option needs no background sweep and is independent of the per-status
windows — but it is a `Retention*` option, so it has to be passed inside
`WithRetention`, and by the replace-not-merge rule that call drops the stock
windows. Passing it *alone* therefore turns the terminal-row sweep off entirely
and the worker logs the "retention is disabled" warning. Restate the windows you
want alongside it, as the example above does.

### Checkpoints on failure are always kept

Checkpoint GC-on-complete only ever fires on **success**. Retryable failures
move the job back to pending and the next attempt replays from its checkpoints,
so deleting them there would break replay from already-completed steps (the
basis for exactly-once *effects* when handlers are idempotent; execution itself
is at-least-once). Terminally failed (dead-lettered) jobs also keep their
checkpoints for debugging; they are removed only when a `RetentionFailedAfter`
window deletes the terminal job row and its checkpoints together.

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
