---
title: "Guarantees & Production Readiness"
weight: 0
---

This page is the honest contract: what Simple Durable Jobs guarantees, what it
asks of you in return, and how to configure it for production. Read it before
you put revenue-critical work on the queue.

> **No warranty — use at your own risk.** Simple Durable Jobs is free,
> open-source software provided "AS IS", without warranty of any kind, as set
> out in the [LICENSE](https://github.com/jdziat/simple-durable-jobs/blob/main/LICENSE).
> The properties described on this page are the library's *design contract*,
> exercised by the automated test suite described below — they are not a
> guarantee of fitness for any particular purpose and create no warranty. You
> are responsible for validating the library against your own requirements.
>
> **Not for high-risk use.** This software is general-purpose infrastructure and
> is **not designed or tested for safety-critical or high-risk uses** — including
> medical, life-support, aviation, automotive, nuclear, emergency-response, or
> financial-settlement systems — where failure could lead to death, injury, or
> severe financial or environmental harm. Do not use it for such purposes.

## Execution semantics: at-least-once

Handlers run **at least once**. A job is leased with a time-bounded lock; if the
owning worker crashes, the lock eventually expires and another worker re-runs
the job from its last durable checkpoint. The same is true for nested
`Call()` steps inside a workflow: a result is checkpointed once, but a crash
*after* the nested handler runs and *before* its checkpoint is saved will run
that step again on replay.

A completed `Call()` or `SavePhaseCheckpoint` writes its checkpoint on a
cancellation- and deadline-immune detached context (a fixed ~5s budget), so a
per-job `Timeout` that fires *as the activity returns* will not lose the
checkpoint and re-run the step. That detached window narrows the crash gap to a
genuine process/machine death — the case the statement above describes.

**Your handlers — and every `Call()` step — must be idempotent.** Use a unique
key, an upsert, or an external dedup guard for any side effect that must not
happen twice (charging a card, sending an email, calling a non-idempotent API).
For phase effects in the same database as the jobs storage, see
[Transactional Checkpoints]({{< relref "/docs/advanced/transactional-checkpoints" >}})
to commit the effect and phase checkpoint together.
This is not a limitation unique to this library; it is the same contract River
and Temporal activities place on you. Exactly-once *effects* are achieved by
making *effects* idempotent, not by the queue pretending a crash never happened.

What the library does guarantee:

These properties are the library's design contract, verified by the test suite
described below; they are provided on the AS-IS basis in the LICENSE and are not
a warranty of fitness for any particular use.

- **Durability** — an enqueued job is committed to the database before
  `Enqueue` returns; it survives process and machine restarts.
- **No lost jobs** — the library is designed so that a leased job is either
  completed, failed (terminally), retried, or returned to the queue by the
  stale-lock reaper, and the chaos suite asserts that jobs do not silently
  disappear.
- **Single active owner** — the library is designed so that at any instant at
  most one worker holds a job's lock. Lock timing is anchored to the **database
  clock** on Postgres/MySQL, and the chaos suite asserts that worker clock skew
  does not cause a live lock to be reclaimed early.
- **Checkpointed workflows** — completed `Call()` steps are not re-executed on
  replay (subject to the at-least-once window above), and fan-in counters are
  updated atomically with the sub-job's terminal transition. The fan-out's
  *status* advance commits in that same terminal transaction; the library is
  designed so that a terminal fan-out is not observable as `status=pending` with
  terminal counts, and the chaos suite asserts that invariant. If a worker
  crashes between the final sub-job's terminal write and resuming the waiting
  parent, a recovery sweep (`GetCompletablePendingFanOuts`, gated by
  `WithFanOutRecoveryStaleAge`, default 2m) heals any parent stranded in
  `waiting`.
- **Atomic signal consumption** — consuming a signal
  (`WaitForSignal`/`WaitForSignalTimeout`/`DrainSignals`) persists the consume
  and its replay checkpoint in a single transaction; the library is designed so
  that a crash mid-wait does not wedge a waiting job or drop a drained signal,
  and the chaos suite asserts that the transaction either rolls back cleanly
  (nothing consumed) or commits both.

## Backend support tiers

| Backend | Tier | Use for |
| --- | --- | --- |
| **PostgreSQL** | Production (recommended) | Multi-worker, distributed deployments. Uses `FOR UPDATE SKIP LOCKED`. |
| **MySQL 8+** | Production | Multi-worker. Active-job uniqueness is enforced with a generated-column unique index. |
| **SQLite** | Development / single-process only | Local dev, tests, single-instance tools. **Not safe for multiple concurrent worker processes.** |

SQLite is single-writer. Run exactly one worker process against it, and open it
with the concurrency-safe DSN:

```go
sqlite.Open("jobs.db?_journal_mode=WAL&_busy_timeout=5000&_txlock=immediate")
```

## Crash-recovery latency (tune this!)

By default a crashed worker's in-flight job is **not reclaimed for ~45 minutes**.
That conservative default avoids reclaiming long-running jobs prematurely, but it
is almost certainly wrong for short jobs. The relevant knobs:

- `WithLockDuration(d)` — how long a lease is held before it is considered
  expired (default 45m). Set this to comfortably exceed your longest job plus a
  couple of heartbeat intervals.
- `WithStaleLockAge(d)` — how long the owning worker must have made **no
  contact** before the reaper reclaims the job (default 45m). The anchor is
  `COALESCE(last_heartbeat_at, started_at, locked_until)` — the worker's last
  contact (a live heartbeat, else when it started, else the lease as a last
  resort), measured as *time since last contact*, not lease expiry — so reclaim
  latency is roughly `StaleLockAge`, regardless of how far an in-flight lease has
  been pushed out.
  `StaleLockAge` also governs the heartbeat interval: it is clamped to
  `StaleLockAge/3` (with a 200ms floor), so a live worker refreshes its
  last-contact timestamp several times within the stale window. The reaper is
  designed so that an active job is not falsely reclaimed, and the chaos suite
  asserts that behavior.
- `WithStaleLockInterval(d)` — how often the reaper runs (default 5m).

For sub-minute jobs, something like `WithLockDuration(2*time.Minute)`,
`WithStaleLockAge(2*time.Minute)`, `WithStaleLockInterval(10*time.Second)` gives
fast recovery without flapping. Shortening `StaleLockAge` also shortens the
heartbeat interval (via the `StaleLockAge/3` clamp), keeping the last-contact
anchor fresh; a legitimately long job that keeps heartbeating is not reclaimed
mid-flight.

## Failed jobs are the dead-letter set

There is no separate dead-letter table. A job that exhausts its retries (or is
cancelled) stays in `failed` status and is queryable:

```go
failed, _ := q.Storage().GetJobsByStatus(ctx, jobs.StatusFailed, 100)
```

Replay one with `jobs.Requeue` (checkpoints are preserved, so a workflow resumes
from its last successful step):

```go
ok, err := jobs.Requeue(ctx, q, jobID)
```

Tune the retry backoff cap with `WithMaxRetryBackoff(d)` (default 1 minute) so a
flapping dependency is not hammered.

## Determinism modes

`Determinism(mode)` controls how strictly `Call()` replay is validated:

- **`ExplicitCheckpoints`** (default) — errors when a replayed `Call`'s type does
  not match the checkpoint at that index.
- **`Strict`** — also fails the job terminally if the replay does not reach every
  recorded `Call` checkpoint (the handler's `Call` sequence changed). Use it to
  catch nondeterministic handlers early.
- **`BestEffort`** — logs mismatches and re-executes instead of erroring.

## Schema migrations

`Migrate()` runs GORM `AutoMigrate` (additive column/index creation) and then
applies ordered, versioned migrations recorded in a `schema_migrations` ledger,
so index reworks and constraint changes ship safely to existing databases.
Always run `Migrate()` on deploy; it is idempotent.

## How these guarantees are verified

Every change runs the full suite under the race detector on SQLite, Postgres,
and MySQL. A nightly **Stress & Chaos** workflow additionally runs long stress
tests on all three backends and a multi-worker crash/recovery chaos harness on
Postgres and MySQL that kills workers at random and then asserts the durability
invariants (exactly-once effects, no wedged jobs, fan-in count integrity,
unique-key dedup, single-scheduler firing). A short chaos smoke runs on every
pull request.
