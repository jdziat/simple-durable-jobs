# Upgrading

## Upgrading through the v4.6 / v4.7 line

Three releases matter here.

- **v4.6.0** closed a silent data-corruption bug in nested `Call()` replay and an
  authentication bypass in the embedded dashboard, and cleared two reachable
  advisories.
- **v4.7.0** closed a set of durability defects — stranded dequeue claims,
  dispositions reported but never written, backlog age counting not-yet-due jobs,
  a MySQL charset crashloop, and an unfenced concurrency-slot release. None of it
  changed behaviour you could have been relying on.
- **The next release** (this document's subject) changes runtime behaviour in
  several places, all of them cases where the old behaviour was wrong — but wrong
  in ways a deployment may have been tuned around. Those are listed below.

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

## Behaviour changes in the next release

> **This section describes only what has actually landed.** Each behaviour change
> is added here by the commit that implements it — a release note written ahead of
> the code is a release note that lies, which is the exact defect class this work
> exists to remove. An earlier draft of this file documented five changes that
> did not exist yet; review caught it.

### Cancelling a workflow now cancels paused and waiting descendants

**Before:** `CancelJob` on a fan-out parent skipped descendants in `paused` or
`waiting`. A paused child survived its parent's terminal cancellation and stayed
resumable — the dashboard's Resume button would run work you had explicitly
cancelled — and the fan-out row permanently violated the documented
`completed + failed + cancelled == total` invariant.

**After:** cancellation reaches the whole subtree.

**What you may notice:**

- Jobs that previously lingered after a cancel now end `cancelled`. If you relied
  on pausing a child to *protect* it from a parent cancel, that never worked the
  way it appeared to — the child was only reachable by manual resume.
- `CancelJob` on a **directly paused job** (not part of a fan-out) now succeeds
  and lands `cancelled`, where it previously returned an error. Cancelling a
  paused job is no longer a two-step resume-then-cancel.
- `ResumeJob` is correspondingly narrowed: a fan-out completion no longer
  silently un-pauses a parent an operator deliberately paused. The dashboard's
  own Resume is unaffected — it goes through `UnpauseJob`, which restores the
  pre-pause status.

### Aggressive pause no longer burns a retry or dead-letters

**Before:** `Worker.Pause(PauseModeAggressive)` cancelled in-flight handler
contexts and wrote nothing durable, so the resulting `context.Canceled` fell into
the ordinary failure path — burning an attempt and, at the default `MaxRetries`
with the attempt already advanced, permanently **dead-lettering** a job that was
merely paused. The heartbeat also stopped for a job that was still running, so
its lease could lapse and the stale-lock reaper could hand the job to a peer
while the original handler was still executing it.

**After:** a pause-cancelled job is released to `pending` with its attempt intact,
and a still-running job keeps its lease until the handler actually returns.

**What you may notice:** `JobFailed` and `JobRetrying` are no longer emitted
around a pause. Alerting that counted those events will see them stop. Resume
simply re-dispatches the job.

### A blocked `Unique` schedule is skipped, not retried at 10 Hz

**Before:** a scheduled job declared with `queue.Unique(key)` dedups against its
own still-running previous fire. The scheduler treated that as a failure — logged
an `ERROR`, did not advance the durable cursor, and retried 100 ms later — for the
**entire runtime of the previous instance**. One transaction and one error log
per tick.

**After:** a unique-key dedup is a deliberate skip. The claim commits so the
cursor advances and peers stop re-attempting the boundary, and it logs at `INFO`.
Genuine failures now back off per schedule (100 ms doubling to 30 s) instead of
retrying every tick, and a schedule that can never fire — an unsatisfiable cron
such as `0 0 30 2 *` — is logged once and skipped rather than spinning forever.

**What you may notice:** a boundary blocked by a long-running instance is now
**skipped outright** rather than eventually firing late (and then producing a
burst of overdue fires). That is the correct reading of `Unique`, and matches
cron+flock and Quartz's DoNothing policy — but if you relied on the delayed fire
arriving eventually, it no longer does.

A skipped boundary advances the cursor but deliberately does **not** stamp the
schedule's last-fire time, so the dashboard's overdue/health indicator still
shows the schedule as not having run. A schedule blocked for hours reads as
blocked, not as healthy.

### Cron honours an explicit timezone prefix

**Before:** `Cron` accepted a crontab-style `CRON_TZ=`/`TZ=` prefix, let the
parser resolve it, and then **overwrote the result with UTC** — so a
timezone-aware schedule fired hours off with no error.
`Cron("CRON_TZ=America/New_York 0 9 * * *")` fired at 09:00 **UTC**, four hours
late in summer and five in winter. A prefix with no schedule fields after it
(`Cron("CRON_TZ=America/New_York")`) **panicked** with a slice-bounds error, in a
constructor that returns an error.

**After:** the prefix is honoured, an unresolvable timezone name is an error
rather than a silent fallback, and no input panics.

**What you may notice:** a schedule that carries a prefix **moves to the hour it
asked for**. If you compensated for the old behaviour by shifting the expression,
remove the compensation. Expressions without a prefix are unchanged and remain
UTC — deliberately, since the underlying parser would otherwise default them to
the host's timezone.

Also adds `CronIn`, `DailyIn`, `WeeklyIn` (and `MustCronIn`) for callers holding a
`*time.Location` rather than a name. `Daily`/`Weekly` now advance by rolling the
calendar **day** rather than the instant, so in a DST zone they fire exactly once
per day — the old form could fire twice on a spring-forward day.

### Fan-out sub-job options are honoured

**Before:** `fanout.Sub` accepted the full `queue.Option` set and silently dropped
`Determinism`, `Delay`, `RunAt` and `IdempotencyKey`. It also stamped a retry
count on every child unconditionally, which made the fan-out default unreachable
— so `WithFanOutRetries(n)` was **completely dead** for anything built with
`Sub()`, and an explicit `Retries(0)` was overridden with the default.

**After:** all of these take effect.

**What you may notice:**

- A sub-job given `jobs.Retries(0)` now runs **once**, where it previously ran up
  to four times. A deployment tuned around those extra attempts will see more
  terminal failures — that is the option doing what it says.
- `jobs.WithFanOutRetries(n)` starts working for `Sub()`-built children.
- Sub-jobs given a `Delay` or `RunAt` now actually wait, and one asked to replay
  deterministically now does.
- **Hand-written `fanout.SubJob{}` literals** with no explicit `Retries` go from
  3 retries to 2. The fan-out default is now `queue.DefaultJobRetries`, matching
  what `Sub()`-built children already received — chosen so the common path does
  not move. Set `Retries` explicitly if you relied on 3.
- Passing a dedup option (`Unique`, `IdempotencyKey`, `UniqueFor`) to `Sub` logs
  one `WARN` per fan-out. Those are parent-level concepts and remain ignored:
  children carry a fan-out-owned unique key so parent replay stays idempotent.
  It is a warning rather than an error in v4 deliberately — turning a
  silently-wrong call into a hard failure on upgrade would convert a latent bug
  into an outage. v5 makes it an error.

### Fractional rate limits are now accurate

**Before:** the fleet rate limiter derived a window that only required
`PerSecond * window >= 1`, but the storage gate admits `ceil(PerSecond * window)`
units per window — which is exact only when that product is a **whole number**.
Every rate that was neither an integer nor `1/n` therefore ran fast. Measured
against the real gate on SQLite, Postgres and MySQL:

| Configured | Enforced (before) | Over |
| --- | --- | --- |
| 0.011 /s | 0.022 /s | **+99.8%** |
| 0.3 /s | 0.5 /s | +66.7% |
| 1.2 /s | 2 /s | +66.7% |
| 2.5 /s | 3 /s | +20% |
| 7.3 /s | 8 /s | +9.6% |

**After:** the configured rate is honoured within 0.5%.

**What you may notice:** throughput on fractional rates **drops** — by up to half
in the worst case. That is the rate you configured; you were previously getting
more than you asked for. If you tuned against the old behaviour, raise the limit
deliberately.

Rates that were already exact — whole numbers and `1/n` — derive the same window
as before and do not move.

### `NewWorker` reports arguments it cannot use

**Before:** `Queue.NewWorker` takes `...any` (the facade cannot name
`worker.WorkerOption` without an import cycle) and silently discarded anything
that was not a `WorkerOption` — a `queue.Option`, an option from the wrong
constructor, a bare value. The worker ran on defaults and said nothing.

**After:** one `ERROR` per discarded argument, naming its position and concrete
type.

**What you may notice:** a new startup error on a worker that has been quietly
misconfigured. It is a log, not a panic — a patch upgrade should not turn a
running process into a crash. v5 replaces this with a typed signature.

## Rollback

This line adds two forward-only migrations: **v36** (`checkpoints.span_end`,
shipped in v4.6.0) and **v37** (`idx_concurrency_slots_job_id`, shipped in
v4.7.0). Both are additive — a new column
with a default, and an index — so an older binary runs correctly against the
newer schema and rolling back the application is safe. No migration in this line
rewrites data. Any further migration is listed here by the packet that adds it.
