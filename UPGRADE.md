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
    AND c.call_type <> 'fanout'
    AND c.call_type <> '_sleep'
    AND c.call_type NOT LIKE 'signal:%'
    AND c.call_type NOT LIKE 'signaltimeout:%'
    AND c.call_type NOT LIKE 'signalpeek:%'
    AND c.call_type NOT LIKE 'signaldrain:%'
    AND j.status NOT IN ('completed', 'cancelled', 'failed')
  GROUP BY j.id, j.type
  HAVING count(c.id) > 1;
  ```

  A worker replaying such checkpoints also logs one `WARN` per run naming the job.

The listing is a deliberate over-approximation — nothing recorded tells us whether
a legacy call actually nested, so flat workflows with two or more calls appear
too.

#### Repairing a listed job

**`Requeue` alone does not work here, and earlier revisions of this document were
wrong to say it did.** `Requeue` is indeed the only operation that clears
checkpoints, but it accepts a job only when its status is already `failed` or
`cancelled` — and this listing deliberately excludes exactly those three terminal
statuses. The two sets are disjoint, so requeueing a listed job returns `false`
and leaves its checkpoints in place. That holds for genuine positives as much as
for over-approximated ones, and the per-run `WARN` names the same inoperative
remedy.

To clear a listed job's checkpoints you must make it terminal first:

```go
// Restarts the workflow from the beginning.
if err := storage.CancelJobTerminal(ctx, jobID); err != nil { /* ... */ }
ok, err := q.Requeue(ctx, jobID)
```

Two things to weigh before doing that:

- It **restarts** the workflow rather than resuming it. Every completed step runs
  again, so this is only appropriate where those steps are safe to repeat.
- Requeueing clears checkpoints but does **not** un-consume signals a previous run
  already consumed. A signal-driven workflow can therefore re-park on a signal it
  will never receive again.

Draining nested workflows before you upgrade remains the only remedy that loses
nothing, which is why it is the first recommendation above.

#### Do not drop the `call_type` exclusions

Only `Call()` records a span, so a checkpoint written for a built-in durable
operation — a fan-out, a durable sleep, any of the signal operations — carries
`span_end = 0` in **every** version, including the one you just upgraded to.
Without the exclusions this query lists healthy current-version workflows and a
worker logs the pre-upgrade `WARN` for them on every single replay. Note also
that `checkpoints.span_end` is added by the v4.6 migration, so this query can
only run *after* upgrading — by which point current-version checkpoints are
guaranteed to be present, and a workflow that merely calls `Sleep` twice would
qualify.

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

### A waiting job is only resumed for the signal it is waiting on

**Before:** the signal-resume poll woke a job in `waiting` whenever it had **any**
unconsumed signal, with no correlation to the name the handler was parked on. A
pending signal the handler will never consume therefore resumed the job on every
tick — 5 s by default — for the entire life of the wait: the job was
re-dispatched, the handler replayed from the top, its wait found nothing and
re-suspended, and the surplus signal was still pending for the next tick.

No typo was needed to hit it. At-least-once delivery is the documented producer
contract, so a retried caller could deliver `"a"` twice while the handler had
moved on to `"b"`; or a signal was simply sent early for a later phase. Each
replay re-ran handler code not behind a `Call` or phase checkpoint and burned a
dispatch plus a fleet rate-limit token. A durable `Sleep` was affected the same
way: any buffered signal replayed the sleeping job every tick until its deadline,
which the resume query's own doc comment already said must not happen.

**After:** a job records the signal name it suspended on, and the poll wakes it
only for that name. A durable sleep records a reserved internal name that no user
signal can match (`validateName` rejects names starting with `_`), so only its
`run_at` deadline wakes it.

**What you may notice:**

- Nothing, if your producers only ever send signals a handler awaits. The signal
  you are parked on still wakes you immediately, and a surplus signal alongside it
  does not suppress that.
- Handler executions for signal-waiting jobs drop sharply if you were hitting
  this. What looked like retry churn or an idle-loop cost was this.
- **If you relied on a signal of one name nudging a handler parked on another**,
  that no longer happens. It only ever "worked" by replaying the whole handler,
  so any effect depended on non-checkpointed code re-running.

Migration **v39** adds `jobs.waiting_signal_name` as `NOT NULL DEFAULT ''`. Empty
means "not recorded" and keeps the old permissive behaviour, so every job already
parked across the upgrade resumes exactly as it did before — there is no window in
which a waiting job becomes unwakeable. Only jobs that suspend after the upgrade
get the correlated treatment. Fan-out suspends deliberately record nothing, since
they are not waiting on a signal at all and are resumed by the fan-out join.

**On MySQL** the column must share `signals.name`'s collation
(`utf8mb4_0900_as_cs`) — the resume poll compares the two, and a mismatch fails
with error 1267 rather than degrading quietly. A pre-AutoMigrate step creates the
column with that collation directly, so the normal upgrade is a fast
`ADD COLUMN`. A database that somehow already has the column with the wrong
collation is repaired by v39 with `ALTER TABLE jobs MODIFY`, and on MySQL a
collation change is neither `INSTANT` nor `INPLACE` — that repair rebuilds the
`jobs` table under a lock. You should not hit it on a normal upgrade; if you have
a very large `jobs` table and want to be certain, check before upgrading:

```sql
SELECT COLLATION_NAME FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'jobs'
  AND COLUMN_NAME = 'waiting_signal_name';
```

No row means the fast path applies. `utf8mb4_0900_as_cs` means there is nothing
to do. Anything else means the rebuild will run, so schedule accordingly.

Because signal names are matched case-sensitively (`signals.name` is already
`as_cs`), a wait on `"Approval"` is **not** satisfied by a signal named
`"approval"` — unchanged from before, and the correlation deliberately matches the
consume rather than being looser than it.

For a custom `core.Storage`: recording the name is an **optional capability**
(`core.SignalWaitMarker`), not a new method on `core.Storage`, so an existing
implementation still compiles and keeps the previous permissive behaviour.

### `WaitForSignalTimeout` no longer accepts a signal that arrived after its deadline

**Before:** the wait consumed any pending signal *before* checking its own
deadline. A signal that arrived after the deadline was therefore delivered as
though it had arrived in time (`timedOut = false`), and nothing bounded how late
it could be.

This needed something to have prevented the timeout from firing on schedule — a
worker outage spanning the deadline, a saturated fleet, a paused queue. The job's
`run_at` fires the timeout only if a worker is there to act on it; otherwise the
next replay happened later and whatever signal had arrived in the meantime won.

**After:** when the deadline has already passed, only a signal that arrived
*before* it can satisfy the wait. A later one leaves the wait timed out and stays
**pending**, so a subsequent `WaitForSignal` on the same name can still consume it.

**What you may notice:**

- A handler that used to receive a late signal after an outage now sees
  `timedOut = true` for that wait. That is the documented contract; the previous
  behaviour silently ignored the timeout you asked for.
- The signal is not lost. It remains pending for the next waiter on that name, so
  a workflow that re-waits still sees it.

The comparison is deliberately against the **signal's arrival time**, not against
"now". A signal that genuinely arrived before the deadline is still delivered even
when the replay only happens afterwards — checking now-vs-deadline instead would
have wrongly timed those out. Since `created_at` is stamped by the sending process
and the deadline by the waiting one, a signal within clock skew of the boundary may
fall either way; that residual is bounded by skew, where the defect it replaces was
unbounded.

### A fan-out whose parent is already terminal no longer retries the resume

**Before:** when a fan-out completed, the worker tried to resume the waiting
parent, and if the parent was not resumable it retried four more times on a
background goroutine and then logged a `WARN` saying it was "relying on the
stalled-parent backstop".

With `CancelOnFail = false` (the default) that is the ordinary steady state, not an
error: the fan-out settles early on a failure, the parent runs on to a terminal
status, and every sibling that then finishes naturally arrives at the same place.
Each one drove five `ResumeJob` writes that could not succeed and logged a warning
pointing at a stall that did not exist — for a parent no backstop will ever touch.

**After:** a parent that is already terminal is recognised as never-resumable. One
inline attempt, no retries, and a `DEBUG` line instead of a `WARN`.

**What you may notice:** the "relying on the stalled-parent backstop" warning stops
appearing for healthy `CancelOnFail = false` fan-outs. It still fires when a
non-terminal parent genuinely fails every retry, which is the case it was written
for. A parent whose status cannot be read is still retried, since a few wasted
writes are cheaper than stranding a waiting parent.

### A schedule that falls behind fires once, not once per missed boundary

**Before:** the "at most one catch-up fire" rule was only applied when a worker
first saw a schedule. After that the durable cursor was never re-read, so a
storage outage the worker *survived* — a failover, pool exhaustion, a lock-wait
timeout — left the in-memory cursor stale by one boundary per period. When storage
came back the scheduler fired **every** missed boundary, one per 100 ms tick, each
a real enqueue. The failure backoff made it worse, since more boundaries elapsed
while it waited.

**After:** the same clamp runs on every tick. Zero or one boundary due behaves
exactly as before; two or more collapse to a single catch-up fire and normal
cadence resumes. An `INFO` line records that it happened.

**What you may notice:** after an outage, a schedule produces one catch-up job
instead of a burst proportional to the outage. If you were relying on that burst
to backfill every missed interval, it was never the documented behaviour — the
cold-start path has always collapsed them, and this only makes the warm path agree.

### `Unique` keeps deduplicating while its holder is waiting or paused

**Before:** the guard matched only `pending` or `running` holders, so a job parked
in `waiting` (on a signal or a fan-out) or sitting in `paused` silently stopped
deduplicating and a second job with the same key was admitted.

The damage was not two handlers running at once — the partial unique index refuses
the second row the moment either becomes runnable. The damage is that it refuses
the WRONG one: the interloper is admitted as `pending`, and when the original
holder's signal finally arrives, its `waiting` → `pending` resume collides with the
index and fails. The job actually doing the work is the one that cannot proceed,
until the interloper it never asked for reaches a terminal status.

**After:** the guard is held in every non-terminal status — `pending`, `running`,
`retrying`, `waiting`, `paused` — and still releases on `completed`, `failed` and
`cancelled`, exactly as documented. `IdempotencyKey`/`UniqueFor` have always
treated a parked job as still in progress; the two mechanisms now agree.

**What you may notice:** an `Enqueue` with `Unique(key)` that previously succeeded
while the holder was parked now returns `ErrDuplicateJob`. That is the documented
contract ("the guard releases as soon as the existing job reaches completed,
failed, or cancelled"); the old behaviour also produced the resume failure above,
so code relying on it was already racing its own holder. The reference page said
"pending-or-running" in one sentence and the terminal rule in another — those were
never the same rule, and the terminal one is now authoritative.

No migration. The partial unique index is unchanged and remains the
`pending`/`running` backstop it was added for (Postgres's absent-row `FOR UPDATE`
gap); the application predicate is simply stricter than it now, which is the safe
direction and is what removes the collision.

### Reusing a phase name within one run is now an error

**Before:** a phase checkpoint is identified by `{call_index: -1, call_type:
phaseName}`, so two different phases sharing a name were the SAME checkpoint. The
second `SavePhaseCheckpoint` upserted over the first, and on replay BOTH were
skipped as already-done — including the one whose body never ran. The job completed
with an effect missing and no error anywhere. Call checkpoints are unaffected: they
carry a real ascending index.

**After:** saving a second phase under a name the same run has already used returns
`jobctx.ErrDuplicatePhaseName` and fails the job terminally (wrapped in
`core.NoRetry` — with retries available, the retry replays the first phase's
checkpoint for both phases and completes, turning the loud error back into the
silent corruption). Saving the same name again in a LATER run is replay and remains
allowed: only names written by the current execution reserve.

**What you may notice:**

- Using one phase name as a progress cursor in a loop (`for … { Save("cursor", i) }`)
  now fails. That only ever "worked" through the upsert, and every write after the
  first was destroying the previous one.
- A child handler that saves a fixed phase name and is invoked twice via `Call` in
  one run now fails — nested calls share the parent's phase namespace. That case is
  already broken today (both invocations share one record); the error surfaces it
  and forces per-invocation names.
- `SavePhaseCheckpointTx` claims the name when the row is written, and a later
  ROLLBACK cannot un-claim it. Redo a rolled-back phase by returning the error and
  letting the job replay, not by retrying inside the same run.

**What it deliberately does not catch:** because `SavePhaseCheckpoint` writes its
result back into the in-run call state, the most likely copy-paste shape never
reaches a second save — phase 2's own `LoadPhaseCheckpoint` hits phase 1's
write-back, so phase 2 is skipped on the very first run and no guard fires. A
Load-side guard cannot distinguish that from the documented same-run read-back
pattern. The transactional path (`SavePhaseCheckpointTx`, which has no write-back)
is fully covered.

### Registering a handler name twice with different types is now an error

**Before:** `Register`/`RegisterE` — and therefore `typed.Define`/`DefineE` — was
last-write-wins. A second registration under the same name silently replaced the
first.

That was not merely untidy. After the collision, a typed `Call` through the FIRST
definition JSON-round-tripped the caller's argument into the SECOND definition's
argument struct. With no field names in common that decodes cleanly to the zero
value, so the callee received a zero argument, its result decoded back to a zero
result, and the job **completed with a nil error on every observable surface**.
Only `Def.Enqueue` was protected (`ErrJobArgsMismatch`); `Def.Call`,
`EnqueueRemote`, and any job already queued under the first definition were not.

**After:** re-registering a name whose argument or result type differs returns an
error naming both signatures. Re-registering with the SAME types is unchanged and
still allowed, so rebuilding a queue from the same definitions keeps working.

**What you may notice:** a program that registered two different handlers under
one name now fails at registration instead of at some later, silent point. If you
see this error, one of the two registrations was already being discarded.

`Schedule` has always refused its duplicate outright ("schedule already registered
for %q"); handler names were the outlier.

### A cron expression naming two timezones is now rejected

**Before:** `Cron("CRON_TZ=UTC TZ=Asia/Tokyo 0 9 * * *")` was accepted. robfig's
parser understands `TZ=`/`CRON_TZ=` itself, so it stripped the inner name and set
the location from it — and this package then overwrote that with the OUTER name.
One of the two timezones you wrote was silently discarded and the job fired in the
other, with no error and no log line.

**After:** an expression carrying more than one timezone prefix returns an error
naming the problem. Exactly one `CRON_TZ=` or `TZ=` prefix, or none, is unchanged.

**What you may notice:** nothing, unless you had such an expression — in which case
it was already firing in a timezone you did not choose, and now it fails loudly at
schedule time instead.

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

**A prefixed schedule can fire TWICE on the day you upgrade — and stopping the old
version first does NOT avoid it.** The two versions disagree about which instant
the next boundary is and they share one cursor, so the old binary claims the
boundary at the UTC hour and the new one then claims the boundary at the requested
LOCAL hour, each "next" by its own reckoning.

Reproduced with the real binaries against one SQLite cursor using
`Cron("CRON_TZ=America/New_York 0 9 * * *")`, in BOTH deployment styles:

- **Rolling** — two fires on the same calendar day at every old→new handover.
- **Stop-the-world** — run v4.7.0 alone through the day (fires 09:00Z), stop it
  completely, then start v4.8.0: it immediately catches up the 13:00Z boundary the
  old binary skipped, so the job runs twice on the cutover day. Verified at
  cutovers of 10:00Z, 14:00Z, 20:00Z and at next-midnight; every one double-fires.

The extra fire is a **catch-up of a genuinely missed local boundary**, which is
correct behaviour once you accept that the schedule was firing at the wrong hour
before — but it happens regardless of how you sequence the deploy, so "cut over
cleanly" is not a mitigation.

**If a double run would be harmful, use a WINDOWED dedup for the cutover:**

```go
jobs.IdempotencyKey("nightly-report-"+day, 24*time.Hour)   // or jobs.UniqueFor(24*time.Hour)
```

`queue.Unique` alone does **not** cover this, and an earlier draft of this file
wrongly said it did. `Unique` means "only one ACTIVE job with this key" — the
dedup matches `status IN ('pending','running')` — and the two fires here are hours
apart, so the first has long since completed and the second enqueues normally. A
windowed dedup is keyed on time rather than liveness, which is what a
boundary-catch-up needs; the scheduler already forwards these options.

That draft also suggested pausing the schedule, which is not an operation this
library has (there is `PauseJob`, `PauseQueue` and `Worker.Pause`, but nothing that
pauses a schedule). Pausing the *queue* does not help either — the scheduler still
claims and enqueues the boundary — and removing and re-adding the schedule does not
skip it, because the scheduler deliberately performs one catch-up when it seeds a
new cursor.

Unprefixed schedules are unaffected, since neither version moves them.

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
The enforced rate while both are running is the **sum of the two**.

At `PerSecond: 0.3`, v4.7.0 derives a 4s window and admits `ceil(0.3×4)/4` =
**0.500/sec**; v4.8.0 derives 3.333s and admits `ceil(0.3×3.333)/3.333` =
**0.300/sec**. A mixed fleet therefore sustains **0.800/sec — 2.67× what you
configured** — for as long as both versions are running. Verified over 600-second
samples across twelve phase offsets: 0.800–0.805/sec, essentially no spread.

A short sample taken right at the start of the overlap reads higher — up to
0.93/sec — because each version's already-in-progress window still has its full
budget to spend. That is a measurement artifact of the first ~30 seconds, not a
property of the rollout; it settles to 2.67×. (Two earlier drafts of this
paragraph reported that transient as the steady-state figure, once as 2.9× and
once as a 2.8×–3.1× "range". Both were 30-second samples.)

It clears the moment the rollout completes, but persists indefinitely if you leave
a canary on the old version, so if the limit exists to protect a third party,
finish the rollout promptly or pause it. Whole-number rates derive the same window
on both versions and are unaffected.

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
**after** the upgrade now fire when you asked — **provided every process that
enqueues and every process that dequeues runs in the same timezone.**

That precondition is new, and on SQLite it is the one thing to check before
upgrading. `run_at` is re-pointed at the **writing process's** local clock face,
so a reader in a different zone compares it against a differently-offset string
and gets the wrong answer — for NEW rows, permanently, not as a backlog that
drains. Reproduced: a `TZ=Asia/Tokyo` process enqueues with `jobs.At(t.UTC())`,
and a `TZ=UTC` process does not pick the job up until roughly nine hours after it
was due; the mirror direction fires early. The same applies to one process whose
host timezone changes between writing and reading a row — a base-image bump, or
adding `Environment=TZ=` to a unit file.

v4.7.0 stored whatever face the *application* supplied, so a UTC-supplying app
read by UTC workers was already correct; **that specific combination regresses.**
Every other combination is fixed or unchanged, which is why this ships. If your
SQLite deployment spans timezones, set one `TZ` across the fleet before upgrading
— that is the only configuration in which both versions are correct.

Postgres and MySQL are unaffected: both store an instant and compare instants.

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
