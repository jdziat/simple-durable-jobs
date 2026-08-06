# Upgrading

## Upgrading through the v4.6 / v4.7 / v4.8 / v4.9 line

Four releases matter here.

- **v4.6.0** closed a silent data-corruption bug in nested `Call()` replay and an
  authentication bypass in the embedded dashboard, and cleared two reachable
  advisories.
- **v4.7.0** closed a set of durability defects — stranded dequeue claims,
  dispositions reported but never written, backlog age counting not-yet-due jobs,
  a MySQL charset crashloop, and an unfenced concurrency-slot release. None of it
  changed behaviour you could have been relying on.
- **v4.8.0** changes runtime behaviour in several
  places, all of them cases where the old behaviour was wrong — but wrong in ways
  a deployment may have been tuned around. Those are listed below.
- **v4.9.0** adds one hook and one CLI exit code, and fixes four defects. It needs
  no migration and no code change on your side; the two additive surfaces are
  described in its own section below.

No API break: `gorelease` reports every change compatible against a v4.7.0 base.
The additions are wider than "options and storage methods", so here are the ones
that matter to callers: five new methods on `*storage.GormStorage`; new checkpoint-type constants
and helpers on `core` (`IsCallCheckpointType`,
`BuiltinCheckpointTypeSQLExclusion`, `ActiveDedupStatuses`, `SignalWaitMarker`);
timezone-aware schedule constructors (`jobs.CronIn`, `MustCronIn`, `DailyIn`,
`WeeklyIn`, and the same four in `pkg/schedule`); accessors on `queue.Options`
and `queue.Queue`; `jobctx.ErrDuplicatePhaseName`; and new exported **fields** on
`fanout.SubJob`, `core.Job`, `core.Checkpoint` and `core.DeadLetterFilter`. There
is no new `queue.Option` constructor and no new event alias.

`gorelease` additionally reports three names this list omits because they are not
API you would call: two test-only helpers in `pkg/call`
(`ResultShapeStringForTest`, `ResultFingerprintForTest`) and one generated getter,
`(*jobsv1.ScheduledJobInfo).GetNeverFires`.

All of that is additive, but note the one source-level consequence `gorelease`
does not model: an **unkeyed** composite literal of a struct that gained a field
stops compiling and has to switch to field names. `fanout.SubJob` gained five
(`RetriesSet`, `Determinism`, `Delay`, `RunAt`, `DedupOptionsIgnored`), and it is
the one of those four structs you are most likely to have written unkeyed.

That is *signature* compatibility — one exported method's observable behaviour
does change:
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
  SELECT j.id, j.type, count(c.id) AS call_checkpoints   -- Postgres
  -- MySQL:  SELECT BIN_TO_UUID(j.id), j.type, count(c.id) AS call_checkpoints
  -- SQLite: SELECT hex(j.id),         j.type, count(c.id) AS call_checkpoints
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

  **Pick the right first column for your dialect.** `jobs.id` is a native `uuid`
  column only on Postgres. On MySQL it is `binary(16)` and on SQLite a `blob`, so
  a plain `SELECT j.id` hands your client 16 raw bytes rather than
  `019f...-...` — not something you can paste into the `CancelJobTerminal` /
  `Requeue` repair below. Use `hex(j.id)` on SQLite; it returns the unhyphenated
  32-character form, which `core.ParseUUID` accepts as-is (in either case), so it
  pastes straight in. On MySQL use `BIN_TO_UUID(j.id)` with the **default** swap
  flag: ids are written with `uuid.MarshalBinary`'s byte order, the same order
  `UUID_TO_BIN(x)` produces, so `BIN_TO_UUID(j.id, 1)` returns a
  plausible-looking but **wrong** id.

  The Go helper is unaffected — it scans into `core.UUID`, which decodes to the
  canonical form, as does the `WARN` a worker logs.

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

To clear a listed job's checkpoints you must make it terminal first — but check
whether the listed job is a fan-out **sub-job** before you touch it.

`Requeue` refuses a sub-job outright, returning `core.ErrCannotRequeueSubJob`
before it resets anything ("cannot requeue a fan-out sub-job directly; requeue
its parent"). So on a sub-job the
cancel below succeeds and the requeue does not — you terminally cancel live work,
the legacy checkpoints stay exactly where they were, and the parent is left
holding a cancelled child. That is strictly worse than doing nothing. Note also
that the detector will usually list only the sub-job: a parent's `fanout`
checkpoint is excluded by `call_type`, so the parent rarely reaches the
`count > 1` threshold and the listing gives you no hint that a tree is involved.

Check first:

```sql
SELECT id, parent_job_id, fan_out_id FROM jobs WHERE id = ?;
```

**If `fan_out_id` is NULL**, the job is a root and the two-step below applies to
it directly.

**If `fan_out_id` is set**, repair the root of its workflow instead. Walk
`parent_job_id` upward until you reach a job whose `parent_job_id` is NULL
(fan-outs nest, so this can be more than one hop) and apply the two-step to
*that* job. Requeueing a root deletes its entire fan-out subtree — every
descendant fan-out record, sub-job and checkpoint at any depth — and re-dispatches
a fresh tree. Two consequences to weigh: cancelling the root terminally cancels
every live descendant, not just the one the listing named; and the requeue
DELETES the sub-job rows outright, so whatever history they carried is gone.

Either way, both steps are required. `Requeue` accepts a job only when its status
is already `failed` or `cancelled`, and a fan-out root parked in `waiting` is
neither — a bare `Requeue` on it returns `(false, nil)`, silently, having done
nothing:

```go
// Restarts the workflow from the beginning.
if err := storage.CancelJobTerminal(ctx, targetID); err != nil { /* ... */ }
ok, err := q.Requeue(ctx, targetID) // ok == false means nothing was cleared
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

**Check the fix's scope against your own wiring.** Only middleware passed to
`ui.WithMiddleware` moved inside the h2c handler. If you authenticate instead by
wrapping the handler `ui.Handler(...)` **returns** — the ordinary
`mux.Handle("/jobs/", authMW(ui.Handler(...)))` chain — that wrapper is still
outside the hijack in v4.6.0, exactly as it was before, and is still consulted
only on the upgrade request.

Whether that is exploitable depends on the other half of your configuration,
because the dashboard's own Connect interceptor runs *inside* the hijack:

- With **no auth option at all**, the interceptor denies every RPC, so
  post-upgrade streams are refused. Upgrading is enough.
- With **`ui.WithAuthorizer`**, your authorizer runs inside the hijack and *is*
  consulted on every stream — the upgrade cannot bypass it. But it is consulted
  without whatever your outer middleware would have put in the context, because
  that middleware is the thing being bypassed. `Authorize` receives only a
  context and an `Action`; the principal is optional and arrives via
  `ui.WithPrincipal`. So check yours fails CLOSED on a missing principal: one
  that decides from the `Action` alone — "allow the view actions, deny
  mutations", which the API invites — still allows every read on a hijacked
  connection.
- With `ui.WithInsecureAllowUnauthenticated()` — the natural pairing when your
  auth lives outside the library — the interceptor allows everything, so an
  attacker who can reach the shell reads every job payload and reaches the
  mutating RPCs. Upgrading does **not** close this.

If that last shape is yours, move the middleware into `ui.WithMiddleware` or pass
`ui.WithoutH2C()` — both available once you are on v4.6.0 or later. No release
closes it for you.

If you mount the dashboard, upgrade — there is no in-library mitigation on
v4.5.1 or earlier, because the escape hatch (`ui.WithoutH2C()`) ships in v4.6.0
itself, in the very commit that fixes the bypass. Until you can upgrade, the
mitigation has to be external: put an authenticating reverse proxy in front of
the dashboard so requests are authenticated before they reach the library, and/or
configure that proxy to strip or refuse requests carrying `Upgrade: h2c` (with
`Connection: Upgrade` and `HTTP2-Settings`). Terminating TLS by itself is *not*
enough — x/net's h2c handler decides to hijack from the request headers alone and
does not check whether the connection is encrypted.

On v4.6.0+ the bypass is already closed for middleware passed to
`ui.WithMiddleware`: the middleware now wraps the handler *inside* h2c, so it
runs on every stream. `ui.WithoutH2C()` is a separate, optional hardening knob
for operators who terminate HTTP/2 themselves (TLS, or Go 1.24+
`srv.Protocols.SetUnencryptedHTTP2`) and want no connection hijacked inside the
library at all. Its own godoc says so: it is about who owns protocol negotiation,
not about authentication, and it is not required to get the fix.

v4.6.0 also cleared two reachable advisories: **GO-2026-5970** (`golang.org/x/text`,
reachable in every default configuration) and **GO-2026-5506** (`go.opentelemetry.io/otel`).

---

## Behaviour changes in v4.9.0

v4.9.0 is a **minor**, not a patch, for two reasons — both additive, neither
requiring any change on your side:

- it adds one exported hook to `pkg/queue` (`OnAttemptEnd` / `CallAttemptEndHooks`), and
- `sdj dlq requeue` gains a non-zero exit code for a case that previously exited 0.

Everything else in the release is a defect fix or a repair to the project's own
release-gating test harness, which is not shipped to you.

### `sdj dlq requeue` exits 4 when it skipped rows it matched

**Before:** a bulk requeue exited **0** whether it drained the queue or requeued
some rows and left others behind. Sub-jobs of a live fan-out, and rows that are no
longer dead-lettered by the time the write runs, are skipped by design — but the
exit code did not say so, so

```sh
sdj dlq requeue --queue payments && clear-alert
```

cleared the alert over a queue that was still stuck.

**Now:** if any matched row was skipped, the command exits **4** and prints the
skipped counts. A clean drain still exits 0.

**What to check:** an automation that treats any non-zero exit from `dlq requeue`
as a hard failure will now surface partial drains. That is the point, but if you
run it from a `set -e` script and would rather keep going, handle 4 explicitly:

```sh
sdj dlq requeue --queue payments || [ $? -eq 4 ]
```

**Deliberately unchanged:** "matched nothing at all" still exits **0**. It
includes the ordinary already-drained queue, so giving it a code of its own would
break the same `&& clear-alert` invocation this change exists to serve. It prints
a note on stderr instead. There is no exit 3.

### `OnAttemptEnd`: a hook for attempts that ended without a disposition

Purely additive; nothing fires differently unless you register it.

An attempt can end without persisting any disposition — no complete, fail, retry or
waiting write landed, because the row stopped being that worker's before one could.
Cancelled jobs, jobs released by graceful shutdown, and jobs whose lease was lost
all take this path. `OnAttemptEnd` reports exactly that population.

It exists because those attempts were previously invisible, and the obvious place
to put them — `OnJobWaiting` — is the wrong one: that hook documents parked
workflows, and adding cancelled and shutting-down jobs to it would silently change
what an already-registered callback counts. **`OnJobWaiting`'s population is
unchanged in this release.**

One boundary worth knowing if you build metrics on it: a job whose type the worker
has no handler for does **not** reach this hook. The handler lookup happens before
the observability span is created, so there is no attempt-end to report. Those jobs
terminally fail and fire `OnJobFail` (which is itself a fix in v4.8.0).

If you use `pkg/otel`, you need do nothing — it registers on this hook itself, which
is what stops a cancelled job leaking its `job.process` span.

---

## Behaviour changes in v4.8.0

> **This section describes only what has actually landed.** Each behaviour change
> is added here by the commit that implements it — a release note written ahead of
> the code is a release note that lies, which is the exact defect class this work
> exists to remove. An earlier draft of this file documented five changes that
> did not exist yet; review caught it.

### Cancelling a workflow now cancels paused and waiting descendants

**Before:** `CancelJob` on a fan-out parent skipped descendants in `paused`. (Its
predicate was `pending`/`waiting`/`running`, so a `waiting` descendant was
already cancelled.) A paused child survived its parent's terminal cancellation
and stayed resumable — the dashboard's Resume button would run work you had
explicitly cancelled — and the fan-out row permanently violated the documented
`completed + failed + cancelled == total` invariant.

The automatic sibling sweep was worse. `core.Storage.CancelSubJobs` — which runs
when a fan-out ends `failed` and its `CancelOnFail` is set — skipped **both**
`paused` and `waiting`, so a sibling suspended in a durable `Sleep`, a
`WaitForSignal`, or its own nested fan-out survived the cancel, woke up later and
ran.

**After:** `CancelJob` reaches the whole descendant subtree, `paused` and
`waiting` descendants included. All three predicates now share one
`cancellableChildStatuses` set.

**One gap this release deliberately does not close.** The widening applies to the
*statuses* every cancel path treats as cancellable, not to their *reach*. The
fail-fast sweep a fan-out runs when `CancelOnFail = true` (`CancelSubJobs`) now
cancels `waiting` and `paused` siblings, but it still only touches a fan-out's
**direct** children — it does not walk a cancelled sibling's own nested fan-out.
A `waiting` sibling is, by construction, one suspended on a nested fan-out or
`Call`, i.e. exactly the population that has descendants, and those grandchildren
stay `pending`: a worker will dequeue and run them under an already-terminal
ancestor. This is still strictly better than before, when the sibling itself woke
up and ran. If you use `CancelOnFail = true` with nested fan-outs, cancel the
parent with `CancelJob` rather than relying on the fail-fast sweep.

**What you may notice:**

- Jobs that previously lingered after a cancel now end `cancelled`. If you relied
  on pausing a child to *protect* it from a parent cancel, that never worked the
  way it appeared to — the child was only reachable by manual resume.
- If you set `CancelOnFail = true` on a fan-out, siblings suspended in `waiting`
  or `paused` when it fails now end `cancelled` instead of waking up later. This
  applies even if you never call `CancelJob`.
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
dispatch plus a fleet rate-limit token.

A durable `Sleep` is covered by the same correlation, but **was not being
replayed** before it. Both resume paths already skipped a sleeping job whose
deadline had not passed: the poll checked `signal.WaitingOnFutureSleep` (with a
memo cache in front of it) on every candidate, and `Signal`'s immediate-wake fast
path did the same. That guard has been there since durable timers shipped, so no
released version replayed a sleep on a buffered signal.

**After:** a job records the signal name it suspended on, and the poll wakes it
only for that name. A durable sleep records a reserved internal name that no user
signal can match (`validateName` rejects names starting with `_`), so only its
`run_at` deadline wakes it.

For sleeps that is a saving rather than a fix: the resume query no longer returns
a sleeping job at all, so the poll skips the per-job checkpoint read it used to
do on every tick, and the narrow case where that read fails —
`WaitingOnFutureSleep` reports false on a read error and the job is resumed
anyway — can no longer be reached. Jobs already parked across the upgrade carry
the empty, permissive `waiting_signal_name`, so they keep relying on the
worker-side guard, which stays in place for them.

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

For a custom `core.Storage` there are two separate things here, and only one of
them is optional.

Recording the name on an **indefinite** wait is an optional capability
(`core.SignalWaitMarker`), not a method on `core.Storage`. A storage that does
not implement it records nothing, and the resume poll stays permissive for those
jobs — the previous behaviour.

**The signal capability set itself, however, gained a required method.**
Alongside the v4.7 methods, `pkg/signal` now also requires:

```go
SaveCheckpointAndMarkWaitingForSignal(ctx context.Context, cp *core.Checkpoint,
    jobID core.UUID, workerID string, d time.Duration, signalName string) error
```

A storage that implemented v4.7's signal capability but not this method still
**compiles** — the capability interface is unexported and satisfied
structurally, so neither the build nor `gorelease` flags it — but it now fails
the capability check at runtime. `WaitForSignal`, `WaitForSignalTimeout`,
`CheckSignal`, `DrainSignals`, `Sleep` and `SleepUntil` all return
`core.ErrStorageNoSignals`. Signals and durable timers stop working entirely for
that backend, and the optional marker never comes into play, because the
capability check runs first.

Add the method before upgrading. Forwarding it to your existing
`SaveCheckpointAndMarkWaiting` and ignoring `signalName` reproduces the previous
behaviour exactly — an empty recorded name is what "not recorded" means.

A custom `core.Storage` that never implemented the signal capability at all is
unaffected: it returned `core.ErrStorageNoSignals` before this release and still
does.

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

### A live `IdempotencyKey`/`UniqueFor` window is no longer ended early by retention

**Before:** retention deleted a terminal job row on its own schedule and took the
job's `unique_locks` row with it, regardless of how much of the dedup window was
left. An operator who wrote `jobs.IdempotencyKey("invoice-42", 90*24*time.Hour)`
to mean "never charge this invoice twice within 90 days" lost the guard on day 30
under the stock completed-job window — day **7** under the documented
`jobs.DefaultRetention()` preset. The replayed request was not deduped: `Enqueue`
returned a new job id, a second job row was inserted, and the handler ran again.

Retaining the lock row alone would not have fixed it. `EnqueueWithUniqueLock`
treated a lock whose referenced job row was missing as **stealable** — on the
reasoning that a vanished row meant the deduped work never ran — so a surviving
window was stolen anyway. The commonest way for a job row to vanish is that it
succeeded and was collected, which made that inference the exact inverse of the
guarantee it was protecting.

**After:** two changes, both of which are needed.

1. Retention never deletes a terminal job that a still-live `unique_locks` row
   references. Once the window lapses, the next pass collects the job row and the
   lock row together.
2. A missing job row is no longer a steal trigger. Only a `failed` or `cancelled`
   reference steals — that work really will never complete. Releasing a window is
   now an explicit act of whoever removes the job: `DeleteJob`,
   `DeleteWorkflowSubtree`, `PurgeJobs` and `Requeue`'s subtree replay all delete
   the lock with the row, and an active-unique collision that prevents the job
   from being inserted at all releases the window it just took. A dangling live
   window therefore fails **closed**.

**What you may notice:** a terminal job guarded by a long window is retained for
`max(retention window, window TTL)` instead of the retention window alone, so
`jobs` can hold more rows than before if you use long TTLs. Growth is still
bounded — by the TTL you chose. If you were relying on retention to end windows
early, shorten the TTL; that is the knob that was always meant to control it.
Deleting the job explicitly still releases its window immediately.

Two related bounds went in with it: `RetentionBatchSize` and
`UniqueLockSweepBatchSize` now clamp to 10000 (they accepted any value before),
and both sweeps chunk their delete statements internally. A batch size above the
driver's bind-parameter ceiling (SQLite ~32k, Postgres 65535) previously made
**every** pass fail with `too many SQL variables` and delete nothing, so the sweep
died silently — worst exactly during the backlog the batch size was raised for.

No migration; no schema change.

### `Retries(0)` now actually means "run once"

**Before:** a job enqueued with `jobs.Retries(0)` was persisted with
`max_retries = 3` and its handler ran **three times**. `core.Job.MaxRetries`
declared a GORM `default:3` tag, and GORM substitutes a field's declared default
for any zero value — `Select()` does not override this. Everything above that layer
was already correct (`queue.Options` tracks whether `Retries` was applied, and
fan-out checks it so an explicit 0 is not mistaken for "unset"); all of it was
inert one layer below.

**After:** the tag stays and the `max_retries` column definition is byte-identical
to every shipped release — v39 and v40 add columns elsewhere (see the migration
table below), but nothing touches this one; storage instead records which jobs
asked for zero retries *before* the
insert — GORM substitutes the declared default for a zero value and writes it
back into the caller's struct, so a check made afterwards reads state that has
already been overwritten — and writes the intended 0 back inside the same
transaction. The row is never visible carrying a value its author did not ask
for. If you enqueued non-idempotent work with `Retries(0)` and saw it run more
than once, that is why.

**Nothing changes for hand-written SQL.** An earlier attempt at this fix removed
the column's `DEFAULT 3`, which turned out to be far worse than the bug: AutoMigrate
then sees a changed column definition and REBUILDS the SQLite `jobs` table, taking
the indexes created by versioned migrations with it — measured at 14 indexes before
the upgrade and 4 after. Keeping the tag is what avoids that, which is why the fix
lives in the write path instead. A writer that omits `max_retries` entirely still
gets 3 from the column default, exactly as before.

**If you enqueue through `core.Storage` directly, read this.** Go cannot tell an
`int` field set to 0 from one never touched, so the write path cannot infer intent
from the value — and inferring it would silently turn retries **off** for every
application whose enqueue looks like this:

```go
store.Enqueue(ctx, &core.Job{Type: "charge", Queue: "default", Args: args})
```

That job's `MaxRetries` is 0 because nobody mentioned retries. It keeps the column
default of **3**, exactly as on every shipped release. To ask for zero from a
hand-built `core.Job`, set the companion flag the struct now exports — the same
idiom `fanout.SubJob{Retries: 0, RetriesSet: true}` already uses:

```go
store.Enqueue(ctx, &core.Job{Type: "charge", MaxRetries: 0, MaxRetriesSet: true})
```

`jobs.Retries(0)` and the fan-out builders set it for you, so nothing changes for
callers going through `queue.Enqueue` or `fanout.Sub`. `MaxRetriesSet` is not
persisted (`gorm:"-"`), so a job **read back** from the database carries `false`
with whatever `max_retries` the row holds; re-enqueuing such a job verbatim and
wanting to keep a stored zero means setting it again.

### A `Call` whose result type changed is caught instead of returning zero

**Before:** changing a `Call`'s NAME between runs was caught loudly as a
determinism violation, but changing its RESULT TYPE was not caught at all. The
stored JSON decoded into the new type — unknown keys ignored, absent ones left
zero — so the caller received an empty result with a **nil error** and the workflow
completed carrying it.

**After:** each checkpoint records a fingerprint of its result type's JSON shape at
write time (migration **v40** adds `checkpoints.result_shape`). On replay a
mismatch is a determinism violation, and under `BestEffortReplay` a warning —
matching how a name mismatch already behaves.

The fingerprint is **structural**, not nominal: it is the set of JSON field names
and kinds *as `encoding/json` actually serializes them*, so renaming the type,
moving it between packages, reordering its fields, promoting fields through an
embedded struct, renaming or inlining that embedded struct, widening `int` to
`int64`, or adding unexported / `json:"-"` fields does not trip replay. Changing
the field set does.

**What you may notice:** a workflow whose handler's return type changed now fails
replay with a clear message instead of silently completing with an empty result. If
you see it, that workflow was already producing wrong data.

**How the shape is computed, and what that means.** The fingerprint is not derived
from the Go type by hand. A representative value of the result type is marshalled
with `encoding/json` itself and the shape is read off the JSON that comes back —
member names, nesting, and scalar kinds. It therefore cannot disagree with the
encoder about promotion, embedded structs, tags, `,string`, `json.Number`, or a
custom `MarshalJSON`: whatever the encoder emits is what gets fingerprinted.

**The limit, stated plainly.** The shape captures STRUCTURE, not values. Two types
that serialize to the same structure fingerprint the same, so these changes are
NOT caught:

- one string-valued form swapped for another — `time.Time` for a `string`, one
  `MarshalJSON` that emits `"USD:500"` for one that emits `"hello"`, or a
  `,string` option added to a **string** field (which double-quotes on the wire,
  but is still a JSON string);
- swapping one struct that serializes as `{}` for another that does, since both
  record the empty shape. Note that "has no exported fields" is *not* the same
  test: `encoding/json` promotes the exported members of an **unexported
  embedded** struct — and promotes that embed's `MarshalJSON`, which can make the
  whole value a JSON string rather than an object — so a struct with no exported
  field of its own can still have a real shape, and swapping it IS caught. An
  embed contributes nothing only when it is tagged `json:"-"` or has no exported
  members of its own;
- **any change to a result type that contains an `interface` member the shape
  walk reaches** — including `Call[any]` itself — since an interface has no
  concrete value to inspect. Such a type records no shape at all and is **not
  guarded at all**, see below. (A plain *unexported* interface field is not
  reached and does not disarm the guard; a `json:"-"` tag does not rescue an
  exported one. Both are spelled out below.);
- **any change to a result type that nests deeper than 32 JSON levels** — such a
  type records no shape at all and is skipped entirely, see below;
- **any change to a result type containing a `json.RawMessage` member anywhere in
  it** — a `RawMessage` holds arbitrary JSON, so like an `interface` its wire form
  belongs to the value rather than the type. Such a type records no shape and is
  not guarded at all;
- **any change to a result type containing a slice, array or map type that
  declares its own `MarshalJSON`** — such a marshaler renders the container's
  CONTENTS, and the probe can only ever build one element (an array only ever gets
  index 0), so the wire form belongs to the value rather than to the type, exactly
  as for an `interface` member or a `json.RawMessage`. Such a type records no
  shape and is not guarded at all. A container declaring only `MarshalText` is
  unaffected — `encoding/json` renders its output as a JSON string whatever the
  contents — so a `type UUID [16]byte`, a hex `[]byte` and `net.IP` keep their
  `string` shape, and a container with no marshaler of its own (`[]string`,
  `map[string]int`, `[2]int`, `[]T`) is untouched;
- **any change to a result type containing a marshaler that VALIDATES on a member
  `encoding/json` serializes** — also below;
- **any change to a result type carrying a `,omitzero` member that `encoding/json`
  would DROP at the probe's value** — which is broader than "a type the probe
  cannot populate". It covers `time.Time`, `netip.Addr`, `big.Int` and anything
  else made only of unexported fields, but ALSO any member with an `IsZero()`
  method that reports true for the probe — including a type whose payload fields
  the probe fills perfectly. The canonical `omitzero` shape is exactly that: an
  optional wrapper with an exported value and an unexported `set` flag that
  `IsZero()` reads. Such a type records no shape and is not guarded at all, see
  below.
- **any change to a result type containing a map whose KEY's rendered name comes
  from a marshaler** — a key that is not of string, integer or `uintptr` kind, or
  one of integer/`uintptr` kind that declares `MarshalText` (the ordinary
  `map[Status]int` enum-keyed count map). Such a type records no shape and is not
  guarded at all, see below. A string-kind key is unaffected even when it declares
  `MarshalText`, because `encoding/json` never consults it.

Those replay exactly as they did before this change: no worse, no better. The
check is deliberately biased this way — a false rejection wedges a healthy
workflow, whereas a miss only leaves the old behaviour in place.

**Types whose shape cannot be computed are skipped — and "skipped" now means the
whole type, at every depth.** A result type that `encoding/json` cannot marshal at
all (a channel or func member) records **no** shape and is skipped entirely: a type
whose shape cannot be computed must never be able to fail a replay. There is
exactly one thing that can happen when the shape walk cannot continue, and it is
this one.

**Depth is one of those cases.** The walk descends at most 32 JSON nesting levels.
A pointer and an untagged embedded struct add none of their own — `encoding/json`
dereferences the one and promotes the other's members into the parent — so
neither spends budget, and the byte-neutral refactors `T` → `*T` and "group these
members into an embed" cannot move the fingerprint. A type that genuinely nests
past 32, which in practice means a self-referential one (a tree node, a linked
list), records **no** shape and is not guarded at all. Thirty-two is measured, not
guessed: the deepest struct type declared anywhere in this repository — including
every example and every Go snippet in these docs — is 5 levels, and the deepest
type actually used as a `Call` result is 2.

Earlier revisions instead *truncated* at the cap, substituting a stand-in value
for the member that landed on the boundary and letting the encoder decide what to
emit for it. That produced a **false fire** every time, because what the encoder
emits depends on the member's Go representation rather than on the wire form: with
`omitempty`, a substituted zero `int` is DROPPED while a substituted non-nil
`*int` is KEPT, so boxing a deep member in a pointer — which cannot move a byte —
moved the fingerprint and replay refused the deploy.

**An `interface` member is the same case, and it is the one that costs you
coverage.** It used to be recorded as `null`, which meant a nil interface was
substituted and handed to the encoder — and `encoding/json` treats a nil interface
as EMPTY, so with `,omitempty` the member was DROPPED from the shape instead.
Adding `,omitempty` to a `Meta any` field your handler always populates is
byte-identical on the wire, decodes losslessly, and used to be **refused on
replay**. The same substitution also hid the member's outright deletion, because a
dropped member and an absent one look identical.

There is no value that fixes this — a nil leaks as `null`, a zero leaks through
`omitempty` one way and a non-nil pointer leaks through it the other — so a result
type containing an interface **anywhere in it** now records no shape and is
skipped, exactly like a type past the depth cap.

**Be clear about what that costs.** The whole type loses the guard, not just the
interface member. If your result is

```go
type Result struct {
    Meta  any    `json:"meta"`
    Order string `json:"order"`
    Total int    `json:"total"`
}
```

then renaming `order` to `reference`, or dropping `total`, is no longer caught
either — before, those members were still compared. `Call[any]` is likewise
unguarded, so tightening it to a concrete type replays as it did before this
feature existed. If you want the guard back, move the free-form part out of the
result type: return a `map[string]string` or a concrete struct instead of an
`any`. A `json.RawMessage` is **not** a way to get the guard back — it holds
arbitrary JSON, so like an `any` it is unknowable from the type and its presence
makes the whole result type unguarded.

The one remaining substitution is a zero-nesting cyclic type, which has to hand
back *some* value of itself or the walk cannot terminate. Everywhere else the
walk now stops rather than substituting — because substituting a value at a
boundary and letting `encoding/json` decide its fate is what produced every false
rejection this check has ever had: a zero leaks through `omitempty`, a non-nil
pointer leaks through it the other way, and a nil leaks as `null`, so two types
that serialize identically could take different paths and disagree.

The same rule applies when a `MarshalJSON` or `MarshalText` **rejects the
fabricated probe** rather than the type. `net.IP` refuses any length other than
0, 4 or 16; an enum's `MarshalJSON` refuses a value that is not one of its cases.
Both marshal perfectly on real data — only the synthesized probe offends them. A
result type records **no** shape when such a marshaler sits on a member
`encoding/json` actually serializes — at any nesting depth, including deeper than
the probe would otherwise descend — and replay then skips the check for that type
completely.

That is a miss, chosen deliberately over the alternative. Substituting a value
the marshaler *would* accept was tried twice and both times produced a **false
fire**, because the encoder reinterprets whatever value it is handed: with
`omitempty`, a slice-backed member (`net.IP`) drops out of the shape entirely
while a struct-backed one (`netip.Addr`) does not, so the byte-identical
`net.IP` → `netip.Addr` modernization moved the fingerprint and rejected replays
that would have succeeded. A vanished member is also indistinguishable from a
member that was *removed* from the type, which is the very change the guard
exists to catch.

**`,omitzero` on a member the probe cannot populate is the same case one level
out.** Go 1.24's `omitzero` option drops a member whose value is the zero of its
type — or whose `IsZero()` reports true. The probe cannot set unexported fields,
so a member made only of them (`time.Time`, `netip.Addr`, `big.Int`, a decimal
carried as a struct) is built at its ZERO, which is exactly what `omitzero`
drops: the member vanishes from the probe's JSON while production, which never
carries a zero timestamp, always emits it. The recorded shape would then describe
the probe rather than the type, so the type records **no** shape instead.

Without that, the wire-neutral tag edit `json:"created"` →
`json:"created,omitzero"` on an always-set timestamp moved the fingerprint from
`{created:string,n:number}` to `{n:number}` — both non-empty, so the fail-open
skip did not apply — and replay refused a checkpoint that decodes perfectly. The
same edit on an ordinary member the probe DOES populate (a string, a number, a
non-nil pointer without an `IsZero`) changes nothing: those keep their shape and
stay guarded.

**A member the encoder never touches does not disarm the guard.**
`encoding/json` never hands a `json:"-"` field or a plain unexported one to its
marshaler, so the probe is never rejected, the shape is recorded, and the type
stays fully guarded. ``struct{ IP net.IP `json:"-"`; Order string `json:"order"` }``
records `{order:string}`, and renaming `order` there IS refused on replay. The
same is true of a `chan` or `func` member behind such a tag — the rule above is
about a type `encoding/json` cannot marshal *as written*, not one that merely
declares such a member.

The `interface`, `json.RawMessage` and container-with-its-own-`MarshalJSON` cases
are the exception to that exception: they stop the shape WALK itself rather than
offending the encoder, so a `json:"-"` tag does not rescue them — an exported
``Meta any `json:"-"` `` still records no shape, and so does an exported
``Seen LastSeen `json:"-"` `` whose `LastSeen` is a slice type declaring
`MarshalJSON`. Only a member the walk never enters does, which means an
ordinary unexported field: ``struct{ hidden any; Order string `json:"order"` }``
records `{order:string}` and stays guarded. An unexported **embedded** field is
still entered by the shape walk — for an embedded struct because `encoding/json`
promotes its exported members through it, and for anything else simply because
the walk descends into every embed — so an unexported embedded interface stops
the walk too, even though `encoding/json` itself ignores it
(``struct{ ifc; Order string `json:"order"` }`` marshals to `{"order":"x"}`).

The skip is **symmetric**: it applies whether the shape recorded on the
checkpoint is empty or the shape of the type being replayed *into* is empty. So
neither direction of such a refactor can wedge a live workflow — and that is what
makes every "records no shape" case above free of risk. A type that trips one of
them can neither be refused on replay nor refuse another type; the only cost is
that it stops being guarded, which leaves it exactly where it was before this
feature existed.

One smaller case joins the list for the same reason: a **map whose rendered key
name comes from a marshaler rather than from the key itself**. That is two
groups, and the rule is *not* the key's kind alone:

- a key **not of string, integer or `uintptr` kind** — a struct, pointer, float,
  bool or array key, which can reach JSON at all only through an
  `encoding.TextMarshaler`;
- a key of **integer or `uintptr` kind that declares `MarshalText`** — the
  ordinary `type Status int` enum behind a count map such as
  `ByStatus map[Status]int`.

Both record no shape. In both the rendered key name is whatever the marshaler
makes of the FABRICATED probe value rather than a property of the type — a
pointer key is fabricated nil and renders to an empty name, and an integer key is
fabricated `K(1)` and renders whatever `1` happens to spell — which was another
false-fire source, and the second group was the worst of them: **inserting one
constant at the front of an enum's `iota` block moved the fingerprint** even
though every persisted key is written by NAME, every name stayed attached to its
own state, and the handler emitted byte-identical JSON. The result type
declaration had not changed and replay was still refused with "written from a
different result type". Both forms now record no shape, so neither can disagree
with anything.

Do not read that as `map[K]V` and `map[*K]V` being interchangeable. They are not:
`encoding/json` resolves a key of string kind by its raw string BEFORE it looks
for a marshaler, so `map[Currency]int{"USD": 5}` renders `{"USD":5}` while the
pointer form goes through `MarshalText`. Swapping one for the other changes the
wire, and is a real type change rather than a refactor.

A **string-kind** key stays guarded even when it declares a `MarshalText`, and
that boundary is deliberate rather than an oversight: `encoding/json` never
consults the marshaler for a string key at all (`map[Currency]int{"USD": 5}`
marshals as `{"USD":5}`, not as whatever `Currency.MarshalText` returns), so
nothing fabricated can reach the shape and disarming it would buy a larger blind
spot for no false fire. Plain string- and integer-keyed maps with no `MarshalText`
on the key — which is nearly all of them — are unaffected and stay guarded.

If you want a result type covered by this guard, keep `interface`/`any` members
and `json.RawMessage` members out of it entirely — wrap the value in a plain
struct, or expose the field as a `string` — keep validating marshalers off
members `encoding/json` serializes (tagging one `json:"-"` is enough; it keeps
the guard), key your maps by plain strings or integers — a `MarshalText` on an
integer key type disarms it, one on a string key type does not — and keep it from
nesting without end, which for a tree or list result usually means returning a
flattened form.

One further bound is not about nesting at all, and it has no tidy rule of thumb.
The probe also gives up after constructing 100,000 values (`maxShapeNodes` in
`pkg/call/result_fingerprint.go`), which a **wide** type reaches far inside the
32-level cap. The cost is the product of the struct-typed member counts down a
path, so — measured on synthetic types whose every level has the same number of
struct-typed members — ten members run out at 5 JSON levels, nine, eight or seven
at 6, six at 7, five at 8, four at 9 and three at 11, every one of them a finite,
non-recursive
type well inside the depth cap. Only struct-typed MEMBERS multiply: a slice,
array or map is probed with a single element, so `[]Sub` and `map[string]Sub`
cost the same as one `Sub`, and scalar members cost one apiece. Nothing in
practice comes close — one of the larger generated types in this repository,
`ui/gen/jobs/v1`'s `ListWorkflowsResponse`, records a 651-character shape — but a
genuinely wide result type can exhaust it. Like every other case it then records no
shape and fails open, so it costs coverage rather than correctness: such a type
is not guarded, and cannot refuse a replay either.

Checkpoints written before v40 carry an empty shape and are **not** checked, so work
already in flight replays exactly as before — the same degradation `span_end = 0`
already gets.

*Implementation note, because three earlier attempts failed here.* This is
deliberately a write-time fingerprint rather than an inspection of the stored bytes.
Strict decoding, per-key probes and null probes each hard-failed replays whose type
had NOT changed — a dropped field, a nested field under an all-zero value, a
required-fields `UnmarshalJSON`. That is one problem, not three: a type change and a
legitimately-different-but-valid payload are indistinguishable in the bytes. Both
sides now compute the fingerprint from the TYPE, so an unchanged type cannot false
fire.

### The dead-letter list is actually newest-dead-first, and `DeadLetteredAt` reads back in UTC

**Before:** on **SQLite**, `dead_lettered_at` was written with a bare `time.Now()`,
so it carried the offset of whichever process wrote it. `ListDeadLettered` orders
by that column, and on SQLite a timestamp column is TEXT — so the ORDER BY was a
LEXICAL compare across mixed clock faces, not an ordering of instants. A job that
died an hour LATER could sort below one that died earlier, which for the shipped
page size pushed the newest dead job off page 1 of the triage view. Two ways in,
neither hypothetical: one worker rendering two offsets across a DST fall-back, and
two processes in different zones against one file (the CLI and the standalone UI
binaries are documented second processes).

**After:** both writers of the column — `Fail`'s retry-exhausted branch and
`FailTerminalWithResult` — store it on a single face, so the existing ORDER BY
sorts by instant. Postgres and MySQL store a real instant and were never affected.

**What you may notice.** On SQLite, `core.Job.DeadLetteredAt` now reads back with
`Location` UTC, while `CreatedAt` and `StartedAt` still read back Local. If you
format or compare it without `.Local()`, the wall time you print changes after the
upgrade. The instant is the same; only the face differs.

**Sorting by other timestamps still orders wall faces on SQLite.** `created_at`,
`run_at` and `started_at` are whitelisted sort keys with the same underlying
hazard, and they are NOT fixed — `ORDER BY run_at DESC` still inverts across a
DST fall-back. That is deliberate and measured: normalizing those through
`julianday()` is instant-correct but costs SQLite the index it was walking in
order, measured at 487-554x on the filtered dashboard queries (200k rows,
`LIMIT 50`). `dead_lettered_at` could be fixed on the WRITE side instead;
`created_at` cannot, because it is half the dequeue eligibility fence
(`COALESCE(run_at, created_at) <= now`, compared against a process-local bind),
and changing its face would mis-read every already-stored row.


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

**After:** re-registering a name whose argument or result type differs is
refused, with an error naming both signatures. `RegisterE` and `typed.DefineE`
**return** it; `Register` and `typed.Define` **panic** with it, as they already do
for every other registration error. On the path every example in these docs uses,
that means a colliding pair now takes the process down where the registration
runs — for most programs, boot — instead of failing silently much later. There is
nothing to catch: audit your registrations before you deploy, or move the call to
`RegisterE`. Re-registering with the SAME types is unchanged and still allowed,
so rebuilding a queue from the same definitions keeps working.

**What you may notice:** a program that registered two different handlers under
one name now fails at registration instead of at some later, silent point. If you
hit this, one of the two registrations was already being discarded.

`Schedule` has always refused its duplicate outright ("schedule already registered
for %q"); handler names were the outlier.

### A cron expression naming two timezones is now rejected

**Before:** every released version, v4.7.0 included, rejected
`Cron("CRON_TZ=UTC TZ=Asia/Tokyo 0 9 * * *")` outright. It handed the whole
string to robfig's parser, which strips only the FIRST prefix and then fails
field normalisation with `expected exactly 5 fields, found 6:
[TZ=Asia/Tokyo 0 9 * * *]`. `MustCron` panicked at startup on it. **No deployed
schedule was ever firing in an unchosen zone because of this**; the
accept-and-silently-discard state existed only between two commits on this
branch, and never in a release.

**After:** the prefix support added in this release strips the outer prefix here
and would hand `TZ=Asia/Tokyo 0 9 * * *` to robfig — which resolves the INNER
name, only to have it overwritten with the OUTER one, silently discarding one of
the two. Picking one is not a defensible resolution of ambiguous input, so an
expression carrying more than one timezone prefix returns an error naming the
problem.

An expression carrying exactly one prefix is still accepted here, but it is
**not** otherwise unchanged — see *Cron honours an explicit timezone prefix*
below, which moves such a schedule to the hour it asked for and can make it fire
twice on the day you upgrade. Expressions with no prefix at all are unchanged:
`Cron("0 9 * * *").Next` returns the identical instant on both versions. (One
single-prefix form is newly rejected rather than merely moved:
`Cron("CRON_TZ=  0 9 * * *")` — an empty name before the fields — returned a nil
error on v4.7.0 and produced a live 09:00 UTC schedule; it now fails with
`carries an empty timezone name`, and `MustCron` on it PANICS at startup.)

**What you may notice:** nothing. If you had a two-timezone expression it was
already failing — `Cron` returned an error and `MustCron` panicked at startup —
so it never reached production. The only change is the message: robfig's
misleading `expected exactly 5 fields, found 6` becomes `cron expression ... names
more than one timezone; use exactly one CRON_TZ= or TZ= prefix`.

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

**If a double run would be harmful, use a WINDOWED dedup for the cutover — sized
below the schedule period, and removed once the cutover is past:**

```go
// TEMPORARY: covers the cutover day only. Remove after the first clean day.
jobs.IdempotencyKey("nightly-report-cutover", 6*time.Hour) // or jobs.UniqueFor(6*time.Hour)
```

`queue.Unique` alone does **not** cover this, and an earlier draft of this file
wrongly said it did. `Unique` means "only one ACTIVE job with this key", and
active is `core.ActiveDedupStatuses` — `pending`, `running`, `retrying`,
`waiting` and `paused` — not just `pending`/`running`: a job in retry backoff, or
a workflow parked on a signal or a fan-out, still holds its key, and only
`completed`, `failed` and `cancelled` release it. (`status IN ('pending','running')`
is the narrower predicate of the partial unique *index* that backs the check — a
generated-column index on MySQL — not the dedup query itself.) In the ordinary
case the two fires are hours apart and the first has reached a terminal status,
so the second enqueues normally and `Unique` does nothing for you. If the first
fire is a long-lived workflow still parked in `waiting`, or still retrying,
`Unique` swings the other way and *drops* the second fire — also not what you
want. A windowed dedup is keyed on time rather than liveness, which is what a
boundary-catch-up needs; the scheduler already forwards these options.

Three things to know before you reach for it.

- **The key does not vary per fire.** `Schedule` materialises its options once at
  registration and the scheduler forwards the stored literal, so a key built from
  a date (`"nightly-report-"+day`) is frozen at process start and is a constant
  for the process's lifetime. Use a fixed key that says what it is.

- **Size the window strictly BELOW the schedule period.** The window is anchored
  to the previous fire's actual *enqueue* instant — `expires_at` is stamped
  `now + ttl` at insert — not to the boundary, so a TTL equal to the period
  silently suppresses a later, legitimate fire whenever the next boundary lands
  less than a TTL after the last enqueue. On a daily schedule with a 24 h TTL
  that is roughly a coin flip every day, since the 100 ms scheduler tick's phase
  relative to the boundary varies, and it is *guaranteed* on a DST spring-forward
  day, where `CRON_TZ=America/New_York 0 9 * * *` boundaries are only 23 h apart
  (2026-03-07T14:00Z → 2026-03-08T13:00Z). Pick a TTL that just spans the two
  cutover fires. That gap is the zone's UTC offset — a few hours for the
  Americas, up to ~14 h elsewhere — so size it for *your* zone, not from the
  example.

- **A suppressed fire is silent, and the schedule still reads healthy.** Unlike
  the `Unique` skip described above, an `IdempotencyKey`/`UniqueFor` dedup
  returns the original job ID with a **nil error**, so the durable cursor
  advances *and* the last-fire marker is stamped for a boundary at which nothing
  ran. Nothing logs, and the dashboard's overdue/health indicator stays green.
  This is the reason to keep the window short and to take it back out.

One further caveat: the window is released early if the first fire ended `failed`
or `cancelled` — the lock is stolen so the re-enqueue admits fresh work — so on a
day where the first fire failed, this does not suppress the second run.

That same earlier draft also suggested pausing the schedule, which is not an operation this
library has (there is `PauseJob`, `PauseQueue` and `Worker.Pause`, but nothing that
pauses a schedule). Pausing the *queue* does not help either — the scheduler still
claims and enqueues the boundary — and removing and re-adding the schedule does not
skip it, because the scheduler deliberately performs one catch-up when it seeds a
new cursor.

Unprefixed schedules are unaffected, since neither version moves them.

Also adds `CronIn`, `DailyIn`, `WeeklyIn` (and `MustCronIn`) for callers holding a
`*time.Location` rather than a name. `DailyIn`/`WeeklyIn` advance by rolling the
calendar **day** rather than the instant, so in a DST zone they fire exactly once
per day — the old form could fire twice on a spring-forward day.

The fire is the **earliest instant on that calendar day whose clock in `loc` has
reached `hour:minute`**, which pins all three DST cases:

| the requested reading | what fires |
| --- | --- |
| exists (the usual case) | that instant |
| does not exist — the clock jumped over it (spring forward) | the instant of the jump. `DailyIn(newYork, 2, 30)` fires at **03:00 EDT** |
| exists twice (fall back) | the **first**, earlier occurrence |

Do not reach for `time.Date` to predict any of this. It resolves only the first
case: for a reading inside a gap it answers an hour *early* in `America/New_York`
(02:30 → 01:30 EST), an instant on *neither* side of the gap in
`Australia/Lord_Howe` (02:15 → 02:45), and an instant on the *previous calendar
day* in the zones whose spring-forward is at midnight (`America/Santiago` 00:00 →
23:00 the day before); and for a repeated reading it answers the first occurrence
in `America/New_York` but the second in `Europe/Berlin`. `WeeklyIn` always lands
on the requested calendar weekday, and `Next` always returns an instant strictly
after its argument, so a schedule can never stall.

A calendar day that cannot hold the fire at all is skipped rather than pushed onto
the next day: a location can skip the tail of a day (`Africa/Algiers` jumped
1971-04-25 23:00 straight to the 26th) or a whole day (`Pacific/Kwajalein` skipped
1993-08-21 crossing the date line).

**`Daily` and `Weekly` are unchanged**: they are
`DailyIn(time.UTC, …)`/`WeeklyIn(time.UTC, …)`, and UTC has no DST, so nothing an
existing `Daily(9, 0)` caller has observes any difference.

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
  not move. Set `Retries` explicitly if you relied on 3. A literal that sets
  `Retries` or `Priority` to a **non-zero** value keeps it; a bare zero takes the
  fan-out default, because the zero is indistinguishable from an omission. To say
  "explicitly zero" in a literal, set the companion flag the struct now exports:
  `SubJob{Retries: 0, RetriesSet: true}` (and `PrioritySet` for priority), which
  `pkg/fanout` honours. `jobs.Sub(..., jobs.Retries(0))` sets it for you.
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
Every rate that was neither an integer nor an *exactly representable* `1/n`
therefore ran fast — and `1/n` is exact less often than it looks, because the old
formula rounded up on the float representation of `1/n`: `1/(1/49)` is
`49.000000000000007`, so it derived a **50 s** window and the gate admitted two
units in it. Over n = 1..20000, 1421 reciprocals (7.1%) ran fast, and there is no
mild case among them — every one enforced very nearly **double** the configured
rate, between **+96.0%** (`1/49`: 0.0204/s configured, 0.0400/s enforced) and
**+99.99%** (`1/16375`). Every round reciprocal an operator actually writes was
exact. Measured against the real gate on SQLite, Postgres and MySQL:

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

Some `1/n` rates DO move. Measured over n = 1..20000, 2785 shift (13.9%), the
first being `1/49` (a 50s window becoming 49s). They split into two groups that
are nothing alike. **1421 are the inexact reciprocals described above** — their
window shrinks by one second and their enforced rate roughly halves, back onto
the rate you configured; if you tuned against the old behaviour, these are the
ones to re-tune. The other 1364 were already exact and stay exact for practical
purposes, losing at most a millisecond of window.

Among those 1364, the worst drift away from the configured rate is
**1.1×10⁻⁵**, i.e. 0.001% (at `1/93`, a 93s window losing one millisecond) — two
orders of magnitude inside the 0.5% bound above and far below what any limiter
can observe. That figure bounds the harmless group only; it says nothing about
the 1421, whose whole point is that they move by roughly a factor of two.
(An earlier draft of this file said "17 movers, around n ≈ 16300, about
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
eligible early or late by the full delta between the two clock faces — **up to 26
hours**, not 14. Both faces are arbitrary (the supplied offset and this process's
local offset), so they can sit at opposite ends of the range, `+14:00` against
`-12:00`; 14 hours is the bound only when one of the two faces is UTC. Ordinary
pairs already exceed it — a `+09:00` value read by a `-07:00` process mis-orders
by 16 hours.

**After:** `run_at` — from enqueue and from `Storage.Fail`'s `retryAt` — is
re-pointed at the same instant on this process's local clock face before it is
written. The instant never changes; only the rendering does. A no-op on
Postgres and MySQL.

Schedule boundaries have the same bug and are fixed too, but by a **different
mechanism** — one that carries none of the same caveats. `last_fire_at` is *not*
re-pointed; it keeps whatever face the boundary carries. The claim's cursor
comparison was made face-aware instead: when the stored cursor and the incoming
boundary carry the same trailing offset the two are compared as raw text, exact
to the nanoseconds the driver wrote; when the offsets differ, both sides are
parsed with `julianday(…)` and compared as instants, to millisecond resolution.
(That expression was `strftime('%Y-%m-%d %H:%M:%f', …)` in an earlier draft of this
wave and is **not** face-independent — see the note below.) Normalizing writes the way `run_at` is normalized was
tried and rejected — it truncates to milliseconds and stalled sub-millisecond
`Every` schedules that v4.7.0 advanced (`Every(100µs)` went 20/20 → 2/20).

`last_fire_at` was compared lexically before, and `DailyIn` / `WeeklyIn` produce
boundaries in *their* location — so on SQLite a `DailyIn(America/New_York, 13, 0)`
boundary rendered `13:00:00-04:00` sorted **below** a `16:00:00+00:00` cursor it
was genuinely an hour after, the claim matched nothing, and the schedule silently
never fired. (A `CRON_TZ=` prefix does *not* produce a foreign face: robfig's
`Next` returns its answer in the **cursor's** location, so a cron boundary always
carries whatever face it was asked about — measured under three host zones. The
prefix fixes which *hour* fires, not which face it is stored on.)

Because that fix is in the predicate and not at write time, it needs no migration
and does not depend on which constructor wrote the row: **cursors already in the
database are repaired by upgrading**, unlike `run_at`. The one residual is narrow
— a cross-face pair less than 1 ms apart collapses in the normalizing branch,
which requires a sub-millisecond schedule whose process timezone changed between
two fires.

**Why `julianday()` and not `strftime()`.** The first cut of both this comparison
and the dashboard's `since`/`until` window normalized to TEXT, on the stated
premise that `strftime('%f')` renders one instant identically whatever offset it
carries. It does not. SQLite keeps the raw clock it parsed *and* a millisecond
julian-day integer; a **non-zero** offset has to be applied to the integer, which
invalidates the raw fields and re-renders everything from the rounded value, while
a **zero** offset — what the driver writes for a UTC value — prints the raw clock
as parsed. So the same instant rendered two ways, and three bands measurably
diverged: the last ~500 µs of a minute (1 ms apart), exact half-millisecond values
(1 ms apart), and the last ~500 µs of a day whose day-of-month is ≥ 29, where the
date advances while the clock still prints `23:59:59.999` — **nearly 24 hours**
apart. `julianday()` is derived from the parsed instant by one arithmetic path and
is the same number on every face; it accepts and rejects exactly the same inputs.
If you are on a pre-release build of this wave, the symptoms were a dashboard page
that came back empty for a `until` bound sitting exactly on a job's `created_at`,
a `CountDeadLettered` that under-counted, and a schedule that stopped for a day.

**What you may notice:** on SQLite, delayed jobs enqueued **after** the upgrade
now fire when you asked — **provided every process that enqueues and every
process that dequeues runs in the same timezone.** Non-UTC schedules are fixed
unconditionally and are not subject to that precondition; see above.

That precondition is new, and on SQLite it is the one thing to check before
upgrading. `run_at` is re-pointed at the **writing process's** local clock face,
so a reader in a different zone compares it against a differently-offset string
and gets the wrong answer — for NEW rows, permanently, not as a backlog that
drains. Reproduced: a `TZ=Asia/Tokyo` process enqueues with `jobs.At(t.UTC())`,
and a `TZ=UTC` process does not pick the job up until roughly nine hours after it
was due; the mirror direction fires early. The same applies to one process whose
host timezone changes between writing and reading a row — a base-image bump, or
adding `Environment=TZ=` to a unit file.

v4.7.0 stored whatever face the *application* supplied, so it was correct exactly
when the supplied offset matched the **reading** process's offset — a
UTC-supplying app read by UTC workers, but equally a `+09:00`-supplying app read
by `TZ=Asia/Tokyo` workers. v4.8.0 is correct exactly when the **writing**
process's offset matches the reading process's. **Any deployment whose
application already supplied the readers' own clock face, but enqueues from a
process in a different zone, therefore regresses** — an app supplying
`+09:00`-faced times with workers on `TZ=Asia/Tokyo` and an enqueuer on `TZ=UTC`
is correct on v4.7.0 and fires nine hours early on v4.8.0; the mirror (a
UTC-supplying app with UTC workers and a `TZ=Asia/Tokyo` enqueuer) fires nine
hours late. A deployment whose enqueuing and dequeuing processes share a
timezone is fixed or unchanged, an all-UTC one included, which is why this ships.

If your SQLite deployment spans timezones, set one `TZ` across the fleet before
upgrading. Note what that buys: it makes v4.8.0 correct, not both versions —
v4.7.0 still mis-fires under a single-`TZ` fleet whenever the application
supplies a face other than that fleet's own.

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
on MySQL a full scan of `idx_jobs_dequeue_eligible`, one of the hot-path indexes
on the same `jobs` table — note that on MySQL the claim path itself is served by
`idx_jobs_dq_ready (status, dq_ready, priority DESC, dq_eligible_at, queue)`;
`idx_jobs_dequeue_eligible` is the claim index only on Postgres and SQLite.
Filtered is an index scan (609 buffers, 0.5 ms). Every 60 seconds, in
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
OpenTelemetry metrics in `pkg/metrics` and aggregate them at your collector. They
are **opt-in** — the library never turns them on for you. Each process you want
counted has to call `jobsmetrics.Instrument(queue)` itself, with a MeterProvider
configured (`Instrument` otherwise falls back to the global
`otel.GetMeterProvider()`, which is a no-op if you never set one). A process that
mounts the dashboard but never calls `Instrument` exports nothing at all. See the
Metrics guide.

### The dashboard boots under a sub-path mount

**Before:** the bundled dashboard referenced its assets **root-absolutely**
(`/assets/index-*.js`) and its RPC client built every call from
`window.location.origin`. Mounted the way the `Handler` godoc, the README and
five docs pages all prescribe —

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

Across the whole v4.6 → v4.9 line there are six forward-only migrations. The last
four are new in **v4.8.0**; the other two are already in the releases named.
**v4.9.0 adds none**, so upgrading from v4.8.0 to v4.9.0 does not touch the schema
at all:

| Migration | Adds | First shipped in |
| --- | --- | --- |
| **v36** | `checkpoints.span_end` | v4.6.0 |
| **v37** | `idx_concurrency_slots_job_id` | v4.7.0 |
| **v38** | `idx_job_stats_timestamp` | v4.8.0 |
| **v39** | `jobs.waiting_signal_name` | v4.8.0 |
| **v40** | `checkpoints.result_shape` | v4.8.0 |
| **v41** | `idx_jobs_unique_key` (Postgres/SQLite; MySQL already had it) | v4.8.0 |

All six are additive — three columns, each `NOT NULL` with a default, and three
indexes — so an older binary runs correctly against the newer schema. No migration
in this line rewrites data.

**Plan v41 on a large `jobs` table.** It builds a plain index on `jobs(unique_key)`
to make `Unique`/`IdempotencyKey` dedup lookups index-served instead of scanning
the active set (measured on SQLite at 300k rows with 1% key density: 76ms → 0.05ms
per lookup). On **Postgres a standard index build locks out writes to `jobs` until
it completes** — reads are unaffected — so on a large table schedule the upgrade
in a window where a write pause is acceptable, or create the index yourself
concurrently before deploying:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_unique_key ON jobs (unique_key);
```

Migration 41 uses `CREATE INDEX IF NOT EXISTS`, so a pre-created index makes it a
no-op. SQLite is single-writer regardless, and MySQL has carried this index since
migration 12, so neither is affected.

**How that is verified, and one thing it does NOT cover.** The v4.7.0 binary was
run against a **v38** ledger on SQLite, live Postgres and live MySQL, migrating in
both directions and completing a full job lifecycle. For the two columns added
after that (v39, v40) the schema-level property is pinned by a test rather than a
manual run: `TestOlderBinaryInsertsWithoutTheNewColumns` issues an `INSERT` that
does not mention the new column — exactly what a binary compiled before it existed
emits — and requires it to succeed and read back, on all three dialects. That is
what "additive" has to mean at write time; a `NOT NULL` column without a usable
default would break it in production rather than at migration time.

That covers `INSERT`. It does **not** make a pre-v39 binary behave correctly on
`UPDATE`, and for `jobs.waiting_signal_name` there is a real gap worth planning
around:

> A v4.8 worker clears `waiting_signal_name` on every SIGNAL park — `MarkWaiting`
> (the unnamed wait, and the legacy fan-out re-park) writes `''`, while
> `MarkWaitingForSignal` and the deadline form write the awaited name — precisely
> so a stale name from an earlier named wait cannot narrow a later one. The one
> exception is `SuspendForFanOut`, the atomic fan-out park, which does not write
> the column at all: a fan-out parent that earlier completed a named wait stays
> parked with that name, and nothing on the resume or re-claim path clears it
> either. That costs nothing today — while the fan-out is pending the parent is
> excluded from the signal-resume poll, and once the fan-out reaches a terminal
> status the parent is resumed by the fan-out completion path regardless of the
> stale name — but do not read a non-empty `waiting_signal_name` on a waiting
> fan-out parent as evidence of an un-upgraded worker.
>
> A pre-v39 worker has no such column in its update set, so it physically cannot
> clear it. In a MIXED fleet the sequence is: a new worker parks a job on
> `WaitForSignal("approval")`; the signal arrives and the job resumes; an old
> worker later parks the same job on an UNNAMED wait, leaving `'approval'` in
> place; a subsequent signal is then matched against the stale name.
>
> This needs no rollback — an ordinary rolling upgrade is enough. **Finish
> upgrading every worker before relying on named signals**, or drain named waits
> across the upgrade window. Once all workers are v4.8 every signal park clears
> the column and the mixed-fleet hazard is gone.

**v39 has one dialect caveat**, covered in its own section above: on MySQL the
column must carry an `as_cs` collation to match `signals.name`. A pre-AutoMigrate
step adds it with that collation, so on any database that already has a `jobs`
table the upgrade is a fast `ADD COLUMN` and v39 finds nothing to repair — its
`ALTER TABLE jobs MODIFY` rebuild-under-lock does not run. That rebuild fires
only on a brand-new database, where `jobs` is empty and it is therefore free, or
on a database that somehow holds the column with the wrong collation; and since
v39 and the pre-AutoMigrate step ship together in v4.8.0, no database upgrading
from a released version can be in that state. **No extra MySQL maintenance window
is needed for v39.** If you have a very large `jobs` table and want certainty
before you schedule, run the `information_schema` query in that section: no row
means the fast path applies.

Any further migration is listed here by the packet that adds it.

**One caveat, and it is not about the schema.** Rolling back restores the old
*code*, so any behaviour fixed above reverts with it. The one that can cost you
work rather than just correctness is on **SQLite**: v4.7.0's lexical
schedule-cursor comparison comes back, so any schedule whose next boundary is
rendered on a **different clock face from its stored cursor** can start stalling
again on rows v4.8.0 had just been advancing correctly.

The exposure is narrower than it first looks, and it is worth being precise
because the scary-sounding case is the one that does *not* happen.

**A schedule that has already fired under v4.8.0 is not exposed.**
`ClaimScheduledFireTx` stores the boundary itself into `last_fire_at`, and
`Daily`/`Weekly`/`Cron` compute boundaries on UTC's face — so after one fire the
cursor and the next boundary carry the same offset and the restored lexical
compare is exact. The host's timezone does not matter. Measured on SQLite under
`TZ=Asia/Tokyo`, sweeping all 24 hours-of-day: **0/24** `Daily(9, 0)` cursors that
v4.8.0 had advanced stalled on rollback.

What *can* stall is a schedule still holding the **host-faced anchor written at
first registration**, which has not fired yet — `SeedScheduledFire` writes
`time.Now()` on the host's face, and a UTC-pinned boundary does not match it
(9/24 in the same sweep). That costs at most the first boundary and then
self-recovers, and it is not something v4.8.0 created: v4.7.0 seeds its own fresh
rows exactly the same way.

A `DailyIn`/`WeeklyIn` cursor left on a foreign face is the other shape, but those
constructors do not exist on v4.7.0, so you have to delete them to compile anyway.
(`CronIn` and a `CRON_TZ=` prefix are *not* exposed — robfig's `Next` returns its
answer in the cursor's own location, as noted above.)

**Sub-second schedules are not affected**, and were never the trigger: when the
two faces match — which `Every` guarantees, since it seeds from the host clock
and its `Next` preserves that location — the lexical compare is exact to the
nanoseconds the driver wrote. Measured against v4.7.0, `Every(100µs)` advanced
20/20. (Millisecond truncation was a property of a normalize-always variant that
was rejected and never shipped.)

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
