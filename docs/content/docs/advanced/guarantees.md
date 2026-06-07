---
title: "Guarantees & Production Readiness"
weight: 0
---

This page is the honest contract: what Simple Durable Jobs guarantees, what it
asks of you in return, and how to configure it for production. Read it before
you put revenue-critical work on the queue.

## Execution semantics: at-least-once

Handlers run **at least once**. A job is leased with a time-bounded lock; if the
owning worker crashes, the lock eventually expires and another worker re-runs
the job from its last durable checkpoint. The same is true for nested
`Call()` steps inside a workflow: a result is checkpointed once, but a crash
*after* the nested handler runs and *before* its checkpoint is saved will run
that step again on replay.

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

- **Durability** — an enqueued job is committed to the database before
  `Enqueue` returns; it survives process and machine restarts.
- **No lost jobs** — a leased job is either completed, failed (terminally),
  retried, or returned to the queue by the stale-lock reaper. It never silently
  disappears.
- **Single active owner** — at any instant at most one worker holds a job's
  lock. Lock timing is anchored to the **database clock** on Postgres/MySQL, so
  clock skew between workers cannot cause a live lock to be reclaimed early.
- **Checkpointed workflows** — completed `Call()` steps are not re-executed on
  replay (subject to the at-least-once window above), and fan-in counters are
  updated atomically with the sub-job's terminal transition.

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
- `WithStaleLockAge(d)` — how long an expired lock must remain expired before the
  reaper reclaims it (default 45m).
- `WithStaleLockInterval(d)` — how often the reaper runs (default 5m).

For sub-minute jobs, something like `WithLockDuration(2*time.Minute)`,
`WithStaleLockAge(2*time.Minute)`, `WithStaleLockInterval(10*time.Second)` gives
fast recovery without flapping. The heartbeat extends the lock while a handler is
actively running, so a legitimately long job is not reclaimed mid-flight.

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
