# Upgrading

## Upgrading through the v4.6 / v4.7 / v4.8 line

Three releases matter here.

- **v4.6.0** closed a silent data-corruption bug in nested `Call()` replay and an
  authentication bypass in the embedded dashboard, and cleared two reachable
  advisories.
- **v4.7.0** closed a set of durability defects — stranded dequeue claims,
  dispositions reported but never written, backlog age counting not-yet-due jobs,
  a MySQL charset crashloop, and an unfenced concurrency-slot release. None of it
  changed behaviour you could have been relying on.
- **v4.8.0** (this document's subject) changes runtime behaviour in several
  places, all of them cases where the old behaviour was wrong — but wrong in ways
  a deployment may have been tuned around. Those are listed below.

No API break: `gorelease` reports every change compatible, and the only additions
are new options, new event aliases and new storage methods. That is *signature*
compatibility — one exported method's observable behaviour does change:
`EnqueueScheduledFire` now COMMITS the schedule's claim on a deduplicated fire and
reports `claimed = true`, where it previously rolled the claim back. (v4.7.0 also
returned `ErrDuplicateJob` here, so the error value itself is not new; what changed
is that the boundary is now consumed rather than retried at 10 Hz.) Described
below.

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

## Behaviour changes in v4.8.0

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
  pre-pause status. If you call `core.Storage.ResumeJob` **directly** (it is part
  of the exported interface), note that it now returns `(false, nil)` for a
  `paused` job instead of resuming it — a silent no-op, not an error. Use
  `UnpauseJob` for operator resume.

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

This is keyed on the handler **returning the cancellation**: the release path
requires an error satisfying `errors.Is(err, context.Canceled)`. A handler that
swallows cancellation and returns some other error — a driver or HTTP stack that
wraps it into a non-`Is`-able type, or one that maps it to a domain error — still
travels the ordinary failure path and still burns the attempt. Return (or wrap)
`ctx.Err()` if you want the pause treated as a pause.

**What you may notice:** `JobFailed` and `JobRetrying` are no longer emitted
around a pause. Alerting that counted those events will see them stop. Resume
simply re-dispatches the job.

`Queue.OnJobWaiting` hooks now also fire for each aggressively-paused job. That is
how the attempt's tracing span is closed — without it every paused job leaked one —
but if you registered that hook to account for fan-out and signal parks, you will
see new entries. The hook receives an already-cancelled job context on this path.

The heartbeat change is **not** limited to pause. It is detached from the job's
own context, so any cancellation that leaves a handler running — including
`Worker.Stop()` in a process that stays alive — now keeps the lease renewed until
the handler returns. That is the correct trade, since it is what prevents the
reaper handing a still-executing job to a peer, but on graceful shutdown a
handler that ignores cancellation will hold its lease indefinitely rather than
letting it lapse.

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
`Cron("CRON_TZ=America/New_York 0 9 * * *")` fired at 09:00 **UTC**, which is
05:00 in New York in summer and 04:00 in winter — four and five hours **early**,
not late. A prefix with no schedule fields after it
(`Cron("CRON_TZ=America/New_York")`) **panicked** with a slice-bounds error, in a
constructor that returns an error.

**After:** the prefix is honoured, an unresolvable timezone name is an error, and
no input panics. (v4.7.0 already errored on an unknown zone — `provided bad
location Not/AZone` — so that part is a better message, not new behaviour. The
genuine silent fallback is the UTC overwrite of a **valid** zone, described
above.)

**What you may notice:** a schedule that carries a prefix **moves to the hour it
asked for**. If you compensated for the old behaviour by shifting the expression,
remove the compensation. Expressions without a prefix are unchanged and remain
UTC — deliberately, since the underlying parser would otherwise default them to
the host's timezone.

**During a rolling deploy, a prefixed schedule can fire TWICE in one day.** The two
versions disagree about which instant the next boundary is, and they share one
cursor: the old binary claims the boundary at the UTC hour, the new one then claims
the boundary at the requested local hour, and both are "next" by their own
reckoning. Reproduced by alternating the real binaries against one SQLite cursor
with `Cron("CRON_TZ=America/New_York 0 9 * * *")` — two fires on the same calendar
day, at every old→new handover. It clears once the rollout completes. If a double
run of that job would be harmful, either declare it `queue.Unique(...)` for the
duration or pause the schedule across the deploy. Unprefixed schedules are
unaffected, since neither version moves them.

Also adds `CronIn`, `DailyIn`, `WeeklyIn` (and `MustCronIn`) for callers holding a
`*time.Location` rather than a name. `DailyIn`/`WeeklyIn` advance by rolling the
calendar **day** rather than the instant, so in a DST zone they fire exactly once
per day — the old form could fire twice on a spring-forward day. **`Daily` and
`Weekly` are unchanged**: they are `DailyIn(time.UTC, …)`/`WeeklyIn(time.UTC, …)`,
and UTC has no DST, so nothing an existing `Daily(9, 0)` caller has observes any
difference.

### Fan-out sub-job options are honoured

**Before:** `fanout.Sub` accepted the full `queue.Option` set and silently dropped
`Determinism`, `Delay` and `RunAt`. It also stamped a retry count on every child
unconditionally, which made the fan-out default unreachable — so
`WithFanOutRetries(n)` was **completely dead** for anything built with `Sub()`, and
an explicit `Retries(0)` was overridden with the default.

**After:** `Determinism`, `Delay`, `RunAt` and the retry options take effect.

The dedup options (`Unique`, `IdempotencyKey`, `UniqueFor`) still do **not**, and
that is deliberate — a fan-out child carries a fan-out-owned unique key so parent
replay stays idempotent, and a caller-supplied one cannot be honoured without
breaking that. What changed is that they are no longer silent: passing one logs a
`WARN`. See the last bullet below.

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
  not move. Set `Retries` explicitly if you relied on 3. A literal that sets `Retries` or
  `Priority` to a **non-zero** value keeps it; a zero takes the fan-out default,
  because a plain struct literal cannot express "explicitly zero" — there is no
  field to distinguish it from an omission. Use `jobs.Sub(..., jobs.Retries(0))`
  when you mean zero.
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

**After:** the configured rate is honoured within 0.5% for every rate a deployment
would plausibly set — measured by sweeping, not by sampling, from about
3.2×10⁻¹⁰/sec (one job per century) up to 10⁹/sec. Below that floor the
hundred-year window clamp binds and the limiter runs fast; far below it the
`float64`→`Duration` conversion overflows and it falls back to a one-second
window. Those inputs are absurd rather than merely unusual, and the code comment on
`maxRateLimitWindow` states the behaviour precisely.

**What you may notice:** throughput on fractional rates **drops** — by up to half
in the worst case. That is the rate you configured; you were previously getting
more than you asked for. If you tuned against the old behaviour, raise the limit
deliberately.

Whole-number rates derive exactly the same window as before and do not move
(verified for 1..20000).

Some `1/n` rates DO move, by at most one second: the old formula rounded up on the
float representation of `1/n`, so those were never exact to begin with. Measured
over n = 1..20000, 2785 of them shift (13.9%); the first is `1/49`, whose window
goes from 50s to 49s.

Those movers split almost evenly — 1421 end closer to the rate you configured and
1364 end very slightly farther, the first at `1/93` (a 93s window losing one
millisecond). The worst drift away is **1.1×10⁻⁵**, i.e. 0.001%, which is two
orders of magnitude inside the 0.5% bound above and far below what any limiter can
observe. (An earlier draft of this file said "17 movers, around n ≈ 16300, about
6×10⁻⁸". That was wrong in all three numbers; the figures here are measured
against the shipped derivation.)

Every round reciprocal an operator actually writes — 1/2, 1/3, 1/4, 1/5, 1/6,
1/10, 1/12, 1/15, 1/20, 1/30, 1/45, 1/60, 1/90, 1/120, 1/180, 1/300, 1/600, 1/900,
1/1800, 1/3600, 1/86400 — is unchanged.

An explicitly set `RateLimitConfig.Window` is now floored to a whole millisecond
and clamped, where it was previously used verbatim. That alignment is not
cosmetic: `window_start` is `now.Truncate(window)` and the column is `datetime(3)`
on MySQL, so a window like `1500µs` produced a start MySQL rounded on write, after
which the consume's own `WHERE window_start = ?` matched nothing and every
rate-limited job bounced forever. A window that is already a whole number of
milliseconds — which every documented example is — is unchanged. A window
**below** one millisecond cannot be floored to a usable value and falls back to
the one-second default rather than to 1ms, so `Window: 500 * time.Microsecond`
becomes `1s` — a 2000× jump. Such a window never worked on MySQL anyway.

**During a rolling deploy, a fractional rate briefly runs FASTER, not slower.**
The two versions derive different windows for the same `PerSecond`, and
`window_start` is `now.Truncate(window)`, so the old and new binaries key
different rows in `rate_limit_windows` and each gets a full independent budget.
The enforced rate while both are running is the **sum**. Measured against the real
storage gate at `PerSecond: 0.3`: v4.7.0 alone admits about 0.533/sec (its own
overshoot) and v4.8.0 alone about 0.333/sec, so a mixed fleet admits roughly the
**sum** — measured between **0.83 and 0.93/sec, i.e. 2.8× to 3.1×** what you
configured.

It is a range, not a constant, because each version's `window_start` is
`truncate(now, window)` on its own grid and the two grids drift relative to each
other; where a 30-second sample lands on them changes the count by an admission or
two. (An earlier draft of this file gave a single figure and explained the
shortfall as the two grids sharing a `window_start`. That cannot be the reason: the
4s and 3.333s grids coincide only once every ~3.7 hours.) It clears the moment the rollout completes, but it
persists indefinitely if you leave a canary on the old version, so if the limit
exists to protect a third party, finish the rollout promptly or pause it.
Whole-number rates derive the same window on both versions and are unaffected.

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

### `EnqueueScheduledFire` reports a deliberate skip as an error value

**Before:** a scheduled fire blocked by an active `Unique` key was retried in a
tight loop (see above).

**After:** the claim is committed and the method returns
`(claimed, jobs.NilUUID, jobs.ErrDuplicateJob)`.

**What you may notice:** this is an exported method whose **error contract**
changed. It is signature-compatible, but a caller that treats any non-nil error
as a failure will log one per deduplicated fire. Test with
`errors.Is(err, core.ErrDuplicateJob)` and treat it as success — the claim was
committed and the schedule advanced. Note the direction: such a caller was
previously logging one failure per 100ms **tick** for the entire runtime of the
blocking job, because the fire was retried in a tight loop. This is strictly less
noise, not more.

### `run_at` is stored on one clock face

**Before:** SQLite has no datetime type — the driver writes every timestamp as
TEXT carrying its offset, and SQLite compares those strings **lexically**. Every
"is this job due yet" predicate binds this process's wall clock with the local
offset, so a `run_at` supplied on a different clock face (`jobs.At` with a UTC
time, a parsed RFC 3339 `...Z`, a timestamp from another service) was compared
character-by-character against a differently-offset string. The job became
eligible early or late by the full delta between the two zones — up to 14 hours.

**After:** `run_at` — from enqueue and from `Storage.Fail`'s `retryAt` — is
re-pointed at the same instant on this process's local clock face before it is
written. The instant never changes; only the rendering does. A no-op on
Postgres and MySQL.

Schedule boundaries get the same treatment. `last_fire_at` is compared the same
way, and `DailyIn` / `WeeklyIn` produce boundaries in *their* location — so on
SQLite a `DailyIn(America/New_York, 13, 0)`
boundary rendered `13:00:00-04:00` sorted **below** a `16:00:00+00:00` cursor it
was genuinely an hour after, the claim matched nothing, and the schedule silently
never fired. (A `CRON_TZ=` prefix does *not* produce a foreign face: robfig's
`Next` returns its answer in the **cursor's** location, so a cron boundary always
carries whatever face it was asked about — measured under three host zones. The
prefix fixes which *hour* fires, not which face it is stored on.)

**What you may notice:** on SQLite, delayed jobs and non-UTC schedules enqueued
**after** the upgrade now fire when you asked.

**Rows already in the database are NOT rewritten**, deliberately — an in-place
rewrite is what made the original design of this fix dangerous. A `run_at` already
stored on a foreign clock face keeps it, so a delayed job enqueued **before** the
upgrade can still fire at the wrong time, in **either direction** and by up to the
full offset DELTA between the two faces, not by one offset — so the bound is 26
hours, not 14, since the stored and local offsets can sit at opposite ends of the
range (`+14:00` against `-12:00`):

The direction follows the **stored offset relative to this process's local
offset** — not the host's zone, so a UTC host is not exempt. The comparison is on
the rendered digits, `T + offset_stored` against `now + offset_local`, so:

- A stored offset **behind** this process's offset renders SMALLER digits, sorts
  below the bind, and the job fires **early** — a `-08:00`-faced row on a UTC
  host, or a UTC-faced row under `TZ=Asia/Tokyo`. Early is usually the direction
  that matters: "send this in twelve hours" going out now.
- A stored offset **ahead of** this process's offset renders LARGER digits, sorts
  above the bind, and the job fires **late** — a `+09:00`-faced row on a UTC host.

The error appears once the offset difference exceeds the delay, so a row whose
stored offset is 9 hours behind fires early for anything scheduled less than 9
hours out. An operator on a UTC host holding rows written by services in other
zones is affected in **both** directions.

This is pre-existing behaviour and not a regression — v4.7.0 does the same thing to
its own rows, and a v4.7.0-seeded database produces a character-identical set of
mis-fires under v4.7.0 and v4.8.0 — but it is not repaired by upgrading. If you
have a backlog of delayed jobs enqueued with a non-local `run_at`, re-enqueue them;
that fixes both directions. Postgres and MySQL are unaffected throughout.

One transient side effect on SQLite: a deployment that consistently enqueued with a
single foreign zone previously held **consistent** faces in `run_at`, and after the
upgrade holds a **mixture** — new rows local-faced, old rows foreign-faced. Because
the dequeue order is `COALESCE(run_at, created_at)` compared as text, the relative
FIFO ordering **among delayed jobs** is perturbed until the pre-upgrade rows drain.
Eligibility for new rows is correct throughout; only the order in which two already
eligible delayed jobs are picked up can differ.

### The dashboard counts queue depth instead of paging job rows

**Before:** every minute the stats collector fetched up to 10,000 **full job
rows** per status — payload columns included, and codec-decoded — purely to count
them, and then silently truncated at the cap. Past 10,000 pending jobs the depth
chart showed a number that was simply wrong, during exactly the backlog incident
an operator opens the dashboard for.

**After:** one `GROUP BY` per sample when the storage supports it (`GormStorage`
does), restricted to the two statuses the sample actually reads. A custom
`core.Storage` without the aggregate still uses the row scan, and now **logs a
warning** when it truncates rather than truncating in silence.

The `WHERE status IN ('pending','running')` is load-bearing, not tidiness. An
aggregate over every status has no bound on table size — and the default
retention keeps completed jobs for 30 days, so "a few pending, millions
completed" is the normal shape. Measured on live databases at 300k rows,
unfiltered is a parallel sequential scan on Postgres (4,935 buffers, 32 ms) and
on MySQL a full scan of `idx_jobs_dequeue_eligible`, the index the claim path
depends on; filtered is an index scan (609 buffers, 0.5 ms). Every 60 seconds, in
every process that mounts the dashboard, whether or not anyone is looking at it.

The filter makes the cost proportional to queue **depth** instead of to **table
size**. It is not a promise of an index scan in every shape: with a genuinely
large live backlog the planner may still choose a sequential scan — but then it is
proportional to the backlog you actually have, rather than to your entire job
history.

**What you may notice:** the depth chart's numbers change — upward — on any queue
that was over the cap; they were an undercount.

A whole queue can also **appear** for the first time. The old scan took the first
10,000 rows per status with no `ORDER BY`, so which rows came back was
planner-dependent and a small queue could fall entirely outside the cap.
Reproduced on Postgres and MySQL with 30,000 pending in one queue and 1 in
another: the old path reported only the large queue, the aggregate reports both. If
you carry a backlog past 10,000 — the deployment this change exists for — expect
queues to show up that the chart was silently omitting.

The sampling rule itself is unchanged: only queues with pending or running work are
sampled, so a fully drained queue still stops being recorded. Migration **v38** indexes `job_stats(timestamp)`, which the retention
prune and the all-queues history read both scan by and could not previously use.

### The throughput chart measures one process, and now says so

No code change — a documentation fix for something the dashboard has always done.
The completed/failed/retried series is fed by `Queue.Events`, an **in-process**
bus, so under the multi-worker topology the docs recommend it reflects only the
process serving the dashboard and under-reports the fleet. It can under-report
within that process too: subscribers get a 100-event buffer and `Emit` drops
rather than blocks when it fills. Queue **depth** is read from the database and
is fleet-wide. See "What the throughput series does
and does not measure" in the Embedded UI guide; for fleet-wide throughput use the
OpenTelemetry metrics, which every process exports.

### The dashboard boots under a sub-path mount

**Before:** the bundled dashboard referenced its assets **root-absolutely**
(`/assets/index-*.js`) and its RPC client built every call from
`window.location.origin`. Mounted the way the `Handler` godoc, the README and six
docs pages all prescribe —

```go
mux.Handle("/jobs/", http.StripPrefix("/jobs", ui.Handler(storage)))
```

— the browser then requested `/assets/index-*.js`, which is outside the
`"/jobs/"` pattern, so the surrounding mux returned 404 and no script ever ran.
The dashboard was a **blank page** at its own documented mount. `index.html` also
referenced a `favicon.svg` that was not in the bundle at all, so that 404'd even
at the root mount.

**After:** assets and the RPC base are resolved relative to the URL the page was
served from, so one build works at any mount point — sub-path or root — with
nothing to configure. The favicon ships.

**What you may notice:** if you mounted at a sub-path and worked around the blank
page (proxy rewrites, an extra route for `/assets/`, or by mounting at the root
instead), that workaround is no longer needed. Nothing breaks if you keep it.

Two consequences worth knowing:

- Register the pattern **with a trailing slash**, as every example does.
  `ServeMux` redirects `/jobs` to `/jobs/` for a `"/jobs/"` pattern, and that
  redirect is what makes the relative URLs resolve. A router that serves the
  shell at `/jobs` without redirecting will not.
- An unknown extension-less path under the mount now returns **302 to the mount
  root** (with a relative `Location`) instead of 200 with the shell. Serving the
  shell at `/jobs/a/b` would make the browser resolve `./assets/...` into a
  directory that does not exist — a blank page. The app is hash-routed
  (`/jobs/#/queues`), so no real route is affected.

## Rollback

Across the whole v4.6 → v4.8 line there are three forward-only migrations. Only
the last is new in **v4.8.0**; the other two are already in the releases named:

| Migration | Adds | First shipped in |
| --- | --- | --- |
| **v36** | `checkpoints.span_end` | v4.6.0 |
| **v37** | `idx_concurrency_slots_job_id` | v4.7.0 |
| **v38** | `idx_job_stats_timestamp` | v4.8.0 |

All three are additive — one column with a default, and two indexes — so an older
binary runs correctly against the newer schema. No migration in this line rewrites
data. Verified in both directions on SQLite, live Postgres and live MySQL: the
v4.7.0 binary migrates against a v38 ledger and completes a full job lifecycle.
Any further migration is listed here by the packet that adds it.

**One caveat, and it is not about the schema.** Rolling back restores the old
*code*, so any behaviour fixed above reverts with it. The one that can cost you
work rather than just correctness is on **SQLite**: v4.7.0's lexical
schedule-cursor comparison comes back, so a non-UTC or sub-second schedule can
start stalling again on rows v4.8.0 had just been advancing correctly.

Whether you see it, and on how many schedules, depends on TWO things: how far the
host's offset sits from the stored clock face, AND where the wall clock currently
sits relative to the next boundary. Both matter, and the second is why it is not a
per-zone property — sampling across the day, Asia/Tokyo stalls 1–2 schedules and
Europe/Berlin stalls 1 in its morning hours and none later, and a stalled schedule
self-heals once the boundary's date advances past the cursor's. Do not read "my
zone was fine when someone measured it" as exemption. That is
v4.7.0's own pre-existing bug re-entering, not damage the rollback does — but "the
rollback is safe" is a statement about the schema, not about schedules. Postgres
and MySQL are unaffected.
