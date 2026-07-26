# Upgrading

## v4.5.x → v4.6.x

Two releases land in this line. **v4.6.0** fixed defects without changing any
behaviour you could have been relying on. **v4.6.1** (this document's subject)
changes runtime behaviour in five places, all of them cases where the old
behaviour was wrong — but wrong in ways a deployment may have been tuned around.

Nothing here is an API break. `gorelease` reports every change compatible; the
only additions are new options, new event aliases and new storage methods.

---

### Before you upgrade: drain nested durable workflows

**This applies to v4.6.0 and later, and it cannot be fixed by upgrading.**

Before v4.6.0, a `Call()` nested inside another `Call()`'d handler corrupted
replay: the outer call's checkpoint did not record the indices its nested
operations consumed, so on an ordinary retry a later call read the wrong
checkpoint. A workflow could **complete successfully carrying another call's
result**, with no error and no log line.

v4.6.0 records the consumed index span and replay skips it. But a checkpoint
already written to your database has no span recorded, and deliberately keeps the
old behaviour — changing it mid-replay would be worse than leaving it. So:

- **Drain nested workflows before upgrading** wherever you can.
- For work you cannot drain, find it:

  ```go
  suspect, err := storage.FindLegacyCallSpanJobs(ctx, 100)
  ```

  or in SQL:

  ```sql
  SELECT j.id, j.type, count(c.id) AS call_checkpoints
  FROM jobs j
  INNER JOIN checkpoints c ON c.job_id = j.id
  WHERE c.call_index >= 0 AND c.span_end = 0
    AND j.status NOT IN ('completed', 'cancelled', 'failed')
  GROUP BY j.id, j.type
  HAVING count(c.id) > 1;
  ```

  `Requeue` is the only operation that clears checkpoints, so requeueing a listed
  job is the repair. A worker replaying such checkpoints also logs one `WARN` per
  run naming the job.

The listing is a deliberate over-approximation — nothing recorded tells us whether
a legacy call actually nested, so flat workflows with two or more calls appear
too. Requeue anything you cannot rule out.

### Security: upgrade if you expose the dashboard

v4.6.0 fixed an authentication bypass. `ui.WithMiddleware` — the mechanism
`SECURITY.md` tells you to authenticate with — ran only on the initial request of
an h2c-upgraded connection. An attacker who could reach any path your middleware
permits (the SPA shell, typically) could upgrade and then issue **every**
subsequent HTTP/2 stream without your middleware ever being consulted.

If you mount the dashboard, upgrade. If you cannot yet, `ui.WithoutH2C()`
disables the upgrade handler entirely.

v4.6.0 also cleared two reachable advisories: **GO-2026-5970** (`golang.org/x/text`,
reachable in every default configuration) and **GO-2026-5506** (`go.opentelemetry.io/otel`).

---

## Behaviour changes in v4.6.1

Each of these was a defect. Each may still surprise a deployment that adapted to
the old behaviour.

### 1. Cancelling a workflow now cancels paused and waiting descendants

**Before:** `CancelJob` on a fan-out parent skipped descendants in `paused` or
`waiting`. A paused child survived its parent's terminal cancellation and stayed
resumable — the dashboard's Resume button would run work you had explicitly
cancelled — and the fan-out row permanently violated the documented
`completed + failed + cancelled == total` invariant.

**After:** cancellation reaches the whole subtree.

**What you may notice:** jobs that previously lingered after a cancel now end
`cancelled`. If you relied on pausing a child to *protect* it from a parent
cancel, that never worked the way it appeared to — the child was only reachable
by manual resume.

`ResumeJob` is correspondingly narrowed: a fan-out completion no longer silently
un-pauses a parent an operator deliberately paused.

### 2. Aggressive pause no longer burns a retry or dead-letters

**Before:** `Worker.Pause(PauseModeAggressive)` cancelled in-flight handler
contexts, and the resulting `context.Canceled` fell through the normal failure
path — burning an attempt and, at the default `MaxRetries`, permanently
dead-lettering a job that was merely *paused*. The heartbeat was also stopped for
a job that was still running.

**After:** a pause-cancelled job is released to `pending` with its attempt
intact, and a still-running job keeps its lease.

**What you may notice:** `JobFailed` and `JobRetrying` are no longer emitted
around a pause. Alerting that counted those events will see them stop.

### 3. Fan-out sub-job options are honoured

**Before:** `fanout.Sub` accepted the full option set and silently dropped
`Determinism`, `Delay`, `RunAt` and `IdempotencyKey`; `Retries(0)` was overridden
with the default; `WithFanOutRetries` was dead.

**After:** all of these take effect.

**What you may notice:** sub-jobs that were quietly retried three times now
respect an explicit `Retries(0)`. Sub-jobs given a `Delay` now actually wait.
Passing a dedup option (`Unique`, `IdempotencyKey`, `UniqueFor`) to `Sub` logs a
`WARN` — those are parent-level concepts and remain ignored. It is a warning
rather than an error in v4 deliberately: turning a silently-wrong call into a
hard failure on upgrade would convert a latent bug into an outage. v5 makes it an
error.

### 4. Fractional rate limits are now accurate

**Before:** `RateLimit` over-admitted by up to ~67% (and ~2x fleet-wide) for any
`PerSecond` that was neither an integer nor `1/n`.

**After:** the configured rate is enforced within 0.5%.

**What you may notice:** throughput on fractional rates **drops** — by up to 49%
in the worst case. That is the rate you configured; you were previously getting
more than you asked for. If you tuned against the old behaviour, raise the limit
deliberately.

### 5. Backlog-age alerts stop firing on scheduled jobs

**Before:** backlog age was `MIN(created_at)` over pending jobs with no due-ness
predicate, so a single scheduled job pinned the age at "hours old" forever.

**After:** the age reflects only jobs that are actually due; a queue holding only
future work reports no age.

**What you may notice:** an existing backlog-age alert that has been firing
constantly will resolve, and the series may go stale for queues with nothing due.
That is the metric becoming useful, but it will look like a monitoring change.
Affects the `jobs.backlog.oldest_age` gauge, the dashboard card, and `sdj queues`.

---

## Also in v4.6.1

Non-behavioural, listed so the diff is not surprising:

- **Timezone-aware schedules.** `Cron` honours an explicit `CRON_TZ=`/`TZ=`
  prefix instead of silently forcing UTC, and `CronIn`/`DailyIn`/`WeeklyIn` take
  a `*time.Location`. A schedule that relied on the prefix being ignored will now
  fire at the hour it asked for.
- **Scheduler no longer hot-loops** at 10 Hz when a `Unique` scheduled job
  overruns its own previous fire.
- **Dashboard stats** are computed with aggregates rather than by loading up to
  20,000 job rows per minute, and no longer silently truncate. Adds migration
  **v38** (`job_stats` timestamp index).
- **Sub-path mounting works.** The dashboard now boots when mounted under a
  prefix, which every doc prescribed and which previously produced a blank page.
- **`Queue.NewWorker` logs an ERROR** naming any argument that is not a
  `WorkerOption` instead of silently discarding it. v5 makes this a compile-time
  signature.

## Rollback

v4.6.x adds three forward-only migrations (v36 `checkpoints.span_end`, v37
`idx_concurrency_slots_job_id`, v38 `job_stats` index). All three are additive —
a new column with a default and two indexes — so an older binary runs correctly
against the newer schema and rolling back the application is safe. No migration
in this line rewrites data.
