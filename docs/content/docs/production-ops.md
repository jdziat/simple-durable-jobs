---
title: "Production Operations"
weight: 6
toc: true
---

This guide covers the operational surface for running Simple Durable Jobs in a
fleet: deployment shape, migrations, worker drain, health probes, throughput
tuning, metrics, dead-letter triage, and retention.

## Deployment Topology

A production deployment usually has three pieces:

- Application processes that enqueue jobs through `Queue.Enqueue`,
  `Queue.EnqueueBatch`, scheduled jobs, or transactional enqueue.
- Worker processes that share the same database and run `Worker.Start(ctx)`.
  Workers can be embedded in the app process or deployed as separate headless
  worker services.
- The same SQL database used by the rest of the application. PostgreSQL and
  MySQL are the production multi-worker backends; SQLite is best for local
  development and small single-process deployments.

Run `Migrate(ctx)` before producers and workers depend on a new schema. It is
safe to call from every process during startup: `GormStorage.Migrate` holds a
fleet-wide migration lock for PostgreSQL and MySQL, serializes in-process
callers with a process mutex, temporarily floors tiny pools so lock and work
connections can coexist, and records each version in `schema_migrations`.
PostgreSQL uses `pg_advisory_lock`; MySQL uses `GET_LOCK("sdj_migrate", 60)`.
SQLite relies on its single-writer behavior.

```go
store := jobs.NewGormStorage(db)
if err := store.Migrate(ctx); err != nil {
	return err
}

q := jobs.New(store)
w := jobs.NewWorker(q,
	jobs.WorkerQueue("default", jobs.Concurrency(20)),
	jobs.WithDrainTimeout(30*time.Second),
)
```

## Graceful Drain

`Worker.Start(ctx)` blocks until `ctx` is cancelled. On cancellation the worker
stops dispatching new jobs, closes the internal dispatch channel, and waits for
in-flight handlers to finish for `DrainTimeout`.

`WithDrainTimeout(d)` controls that grace window. The default is 30 seconds. A
non-positive value aborts immediately by cancelling handler contexts. If the
timeout expires, the worker cancels remaining handlers and waits for them to
return.

Use a drain timeout long enough for normal handlers to observe `ctx.Done()`,
finish idempotently, and persist results or checkpoints. Handlers should pass
their context into outbound calls; a handler that ignores cancellation can still
hold shutdown until the drain timeout is reached.

## The `sdj` CLI

The `sdj` binary is a standalone operations tool for GORM storage. Global usage:

```bash
sdj [--driver sqlite|postgres|mysql] --dsn <dsn> <command> [options]
sdj --version
sdj version
```

Global flags:

| Flag | Description |
|---|---|
| `--driver` | Database driver: `sqlite`, `postgres`, or `mysql`. Default: `sqlite`. |
| `--dsn` | Database connection string. Required for storage commands. SQLite DSNs are passed through `jobs.SafeSQLiteDSN`. |

Commands:

```bash
# Apply idempotent storage migrations.
sdj --driver sqlite --dsn ./jobs.db migrate
sdj --driver postgres --dsn postgres://user:pass@localhost/db migrate
sdj --driver mysql --dsn 'user:pass@tcp(localhost:3306)/db?parseTime=true&loc=UTC' migrate

# Print queue pending depth, DLQ depth, oldest pending timestamp, and backlog age.
sdj --driver postgres --dsn postgres://user:pass@localhost/db queues

# List dead-lettered jobs.
sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq list
sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq list --queue emails --type send-email --limit 100 --offset 0
sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq list --queue emails --tenant acme --metadata env=prod
sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq list --json
sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq list --ids-only

# Requeue one dead-lettered job by id, or a filtered dead-lettered batch.
sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq requeue <jobID>
sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq requeue --queue emails --tenant acme

# Ping storage and print OK on success.
sdj --driver postgres --dsn postgres://user:pass@localhost/db health
```

Each subcommand accepts `-h` for its command-specific usage. `dlq list` prints a
table by default; `--json` emits a JSON array and `--ids-only` emits one job ID
per line for scripts. `dlq list --tenant` and repeated `--metadata key=value`
filters narrow triage to one tenant or metadata slice. `dlq requeue` succeeds
only when the target job exists and is failed or cancelled; fan-out sub-jobs
must be handled by requeueing their parent. `dlq requeue --queue` and
`--tenant` requeue every matching dead-lettered job.

## Health Probes

Headless workers can expose probes with `Worker.HealthHandler()`:

```go
mux := http.NewServeMux()
mux.Handle("/", worker.HealthHandler())
go http.ListenAndServe(":8080", mux)
```

The handler registers two endpoints:

| Endpoint | Meaning | Database work | Status behavior |
|---|---|---|---|
| `/healthz` | Liveness | None | Always returns `200 OK`. |
| `/readyz` | Readiness | Calls `Ping(ctx)` when storage implements `storage.Healther`. | Returns `200 OK` when `Ping` succeeds or the storage backend has no `Healther`; returns `503 Service Unavailable` when `Ping` fails. |

`GormStorage` implements `storage.Healther` with `Ping(ctx) error`, backed by
`sql.DB.PingContext`.

Kubernetes probes can point at the two endpoints separately:

```yaml
livenessProbe:
  exec:
    command: ["wget", "-qO-", "127.0.0.1:8080/healthz"]
  periodSeconds: 10
readinessProbe:
  exec:
    command: ["wget", "-qO-", "127.0.0.1:8080/readyz"]
  periodSeconds: 10
  failureThreshold: 3
```

Operator pause is not a readiness failure. `Worker.Pause(...)` is a reversible
control-plane state, so a paused worker still returns `200 OK` from `/readyz` as
long as storage is reachable. This avoids orchestrators restarting a worker that
an operator deliberately quiesced.

For local state without HTTP, `Worker.Health()` returns `WorkerHealth` with
`RunningCount`, `Paused`, and `Started`.

## Throughput Tuning

The worker dispatch path is work-conserving: on each poll tick it drains
available capacity by repeatedly dequeuing and dispatching jobs while progress
is made. The tick is the idle backstop, not the throughput ceiling.

The drain loop is bounded. It re-checks cancellation and pause, recomputes queue
capacity, stops when no work is dispatched, stops when released jobs reach the
tick's release budget, stops when the poll-interval wall-clock budget elapses,
and has a hard iteration cap.

`WithDequeueBatchSize(n)` sets the per-poll claim cap. Default: `10`. Values are
clamped to `[1, 1000]`. `WithPollInterval(d)` sets the idle poll interval.
Default: `100ms`; positive values below `50ms` are clamped up to `50ms`; non-
positive values are ignored.

```go
w := jobs.NewWorker(q,
	jobs.WorkerQueue("default", jobs.Concurrency(50)),
	jobs.WorkerQueue("critical", jobs.Concurrency(20)),
	jobs.WithDequeueBatchSize(50),
	jobs.WithPollInterval(100*time.Millisecond),
)
```

When `GormStorage` supports `DequeueBatchPerQueue(ctx, workerID, budgets)`, the
worker computes a per-queue claim budget from each queue's remaining capacity.
The storage scan still follows the global `priority DESC, COALESCE(run_at, created_at) ASC` (eligibility-time) order;
rows from a queue that has already hit its budget are skipped with the same
skip-list mechanism used for paused rows. This keeps cross-queue global priority
while preventing a hot queue from over-claiming and then releasing surplus rows.

Rollback escape hatch: set `jobs.WithDequeueBatchSize(1)` and use a slower
`jobs.WithPollInterval(...)` to approximate the conservative single-claim,
slow-poll behavior. This is useful during incident response if a database is
under unexpected dequeue pressure.

## Dequeue Index

The dequeue hot path claims the highest-priority *eligible* job — pending, with
its scheduled time reached — using `FOR UPDATE SKIP LOCKED`, ordered by
`priority DESC` then eligibility time. A single index, `idx_jobs_dequeue_eligible`,
serves both ready and delayed/scheduled jobs by folding `run_at` into the
eligibility key (`COALESCE(run_at, created_at)`):

- **PostgreSQL / SQLite** — partial index on
  `(priority DESC, COALESCE(run_at, created_at), queue) WHERE status = 'pending'`.
  Terminal rows are excluded from the index entirely.
- **MySQL** — `dq_eligible_at` is a `STORED` generated column
  `IF(status = 'pending', COALESCE(run_at, created_at), NULL)`. MySQL has no
  partial indexes, so `status` leads the index as an equality. A single
  index cannot both range-prune eligibility *and* provide the `priority DESC`
  order, so a large steady-state **ready** backlog used to filesort. The
  dequeue now also filters a stored `dq_ready` boolean (see below) and is served
  by `idx_jobs_dq_ready (status, dq_ready, priority DESC, dq_eligible_at, queue)`:
  equality on `(status, dq_ready)` then the index's own priority order — **no
  filesort**, the claim reads one row and stops.

This replaces the earlier `idx_jobs_dequeue` / `idx_jobs_dequeue_order` indexes,
which were dropped: they could not serve delayed jobs. The multi-backend CI suite
EXPLAINs the real dequeue query on PostgreSQL and MySQL and fails the build on a
`Sort` / `Using filesort` plan or the wrong index, so a future column-order
regression cannot ship.

### The `dq_ready` hint and the ready-promoter

`dq_ready` is a stored boolean meaning "pending and eligible to run *now*"
(`run_at` is null or in the past). It is a **pure performance hint** that lets
MySQL separate ready from not-yet-due jobs in the index — Dequeue still filters
`dq_eligible_at <= now`, so a stale flag can never cause an incorrect or early
dequeue; it can only cost latency. Readiness depends on wall-clock time, which a
generated column cannot express, so each worker runs a small **ready-promoter**
loop (`WithReadyPromoteInterval`, default = the poll interval) that flips
`dq_ready` true for pending jobs whose `run_at` has passed. It is the backstop
that makes a delayed/scheduled job dequeue-visible, so it always runs and cannot
be disabled; it is idempotent and bounded (PostgreSQL/SQLite use the partial
index `idx_jobs_dq_unready`; MySQL uses the `(status, dq_ready)` prefix of
`idx_jobs_dq_ready`), so it scans only the not-yet-eligible set, never the whole
ready backlog. The chaos suite's `INV-READY-NO-STUCK` invariant asserts no
eligible job is ever left unready.

## Schema Migrations At Scale

`sdj migrate` — and the automatic `Migrate` on worker startup — is idempotent and
applies every versioned migration under a fleet-wide advisory lock. On a fresh or
small database every migration is effectively instant. A few migrations applied to
an **already-large** `jobs` table take locks proportional to table size; on large
installs apply these out-of-band (during a maintenance window, outside the fleet
lock) before rolling out the upgrade:

- **New index builds.** Versioned index migrations use plain `CREATE INDEX`, which
  on PostgreSQL takes a `SHARE` lock that blocks writes for the build duration. If
  you add an index migration against a large `jobs` table, build it first with
  `CREATE INDEX CONCURRENTLY` out-of-band — it cannot run inside the transactional
  migration runner — then deploy.
- **MySQL collation migration (v15).** v15 pins `unique_key` /
  `active_unique_key` to `utf8mb4_0900_as_cs` (case- and accent-sensitive dedup,
  for cross-engine parity). On MySQL this is a table-copy `ALTER`: instant on a
  fresh table, but minutes under the fleet lock at millions of rows. On large
  installs run it during a maintenance window or via an online-DDL tool
  (`pt-online-schema-change` / `gh-ost`) before upgrading.
- **MySQL queue/tenant collation + signal index rebuild (v25).** v25 pins
  `queue` and `tenant` to `utf8mb4_0900_as_cs`, so on MySQL these become
  **case-sensitive** — `"Default"` and `"default"` are now *distinct* queues
  (matching PostgreSQL, which was already case-sensitive). If a MySQL-only
  deployment relied on case-insensitive queue/tenant matching, normalize your
  queue/tenant names before upgrading. v25 also right-sizes `dq_eligible_at` to
  `datetime(3)` and rebuilds `idx_signals_pending` to include `created_at` — all
  table-copy/index `ALTER`s on MySQL, so the same maintenance-window guidance as
  v15 applies on large installs.
- **MySQL foreign keys (v14).** v14 adds `ON DELETE CASCADE` foreign keys from
  `checkpoints` / `signals` / `fan_outs` to `jobs(id)`. Two MySQL-specific
  consequences: (1) a child-row `INSERT` takes a shared lock on its parent `jobs`
  row, so a checkpoint or signal write can briefly contend with a concurrent
  status transition of the same job under heavy load; (2) `TRUNCATE jobs` now
  fails (`Cannot truncate a table referenced in a foreign key constraint`) —
  truncate the child tables first, or `SET FOREIGN_KEY_CHECKS = 0` for a full
  reset. PostgreSQL's FK locks are `FOR KEY SHARE` and do not block the status
  update; SQLite uses application-level cascade (its DSN runs with
  `foreign_keys=OFF`).

## Metrics And Alerts

Metrics are opt-in through `pkg/metrics`:

```go
handler, meterProvider, err := jobsmetrics.NewPrometheusHandler()
if err != nil {
	return err
}
defer meterProvider.Shutdown(ctx)

jobsmetrics.Instrument(q, jobsmetrics.WithMeterProvider(meterProvider))
http.Handle("/metrics", handler)
```

Storage-side gauges:

| Metric | Unit | Attributes | Source |
|---|---|---|---|
| `jobs.queue.depth` | `{job}` | `queue`, `outcome=pending\|running` | Storage queue counts. |
| `jobs.queue.backlog.oldest_age` | `s` | `queue` | Age of the oldest pending job by queue. |
| `jobs.dead_letter.depth` | `{job}` | `queue` | Dead-lettered job count by queue. |

Worker-side gauge:

| Metric | Unit | Attributes | Source |
|---|---|---|---|
| `jobs.queue.saturation` | `1` | `queue`, `worker.id` | `InstrumentQueueSaturation(workerID, capacities, running, ...)`, where saturation is running jobs divided by configured capacity for that worker. |

Alert on backlog and DLQ depth from the storage-side gauges:

```promql
max by (queue) (jobs_queue_backlog_oldest_age) > 300
max by (queue) (jobs_dead_letter_depth) > 0
```

For saturation, do not `sum` workers. `jobs.queue.saturation` is emitted per
worker with `worker.id`; summing two workers at 50% would report a fake 100%.
Use an average by queue:

```promql
avg by (queue) (jobs_queue_saturation) > 0.9
```

OpenTelemetry exporters may expose dotted metric names differently for a given
backend. Prometheus commonly converts dots to underscores, as shown above.

## DLQ Runbook

Dead-lettered jobs are terminal failed jobs with explicit DLQ metadata. They
remain in the `jobs` table and are replayed with `Requeue`, not copied to a
separate archive table.

1. Check queue impact:

   ```bash
   sdj --driver postgres --dsn "$DATABASE_URL" queues
   ```

2. Inspect jobs:

   ```bash
   sdj --driver postgres --dsn "$DATABASE_URL" dlq list --queue emails --limit 50
   sdj --driver postgres --dsn "$DATABASE_URL" dlq list --queue emails --tenant acme --metadata env=prod
   sdj --driver postgres --dsn "$DATABASE_URL" dlq list --queue emails --ids-only
   ```

3. Fix the handler code or external dependency that caused the terminal
   failure.

4. Requeue one job:

   ```bash
   sdj --driver postgres --dsn "$DATABASE_URL" dlq requeue <jobID>
   ```

   Or requeue a filtered batch:

   ```bash
   sdj --driver postgres --dsn "$DATABASE_URL" dlq requeue --queue emails --tenant acme
   ```

Requeue clears DLQ metadata, resets execution state, and deletes checkpoints so
the workflow starts from the beginning. The dashboard Requeue button instead
clears the dead-letter state and resumes from existing checkpoints. Handlers
still must be idempotent. See [Dead-Letter Queue]({{< relref
"/docs/advanced/dead-letter-queue" >}}) for the full API and retention caveats.

## Retention And GC

Retention is enabled by default on workers with generous windows: completed
jobs are deleted after 30 days, failed and cancelled jobs after 90 days, and
consumed signals after 7 days. Disable it only if you manage retention
externally or must keep terminal rows indefinitely:

```go
w := jobs.NewWorker(q,
	jobs.RetentionDisabled(),
)
```

Tune the windows explicitly when your operational policy needs different
retention:

```go
w := jobs.NewWorker(q,
	jobs.WithRetention(
		jobs.RetentionCompletedAfter(7*24*time.Hour),
		jobs.RetentionFailedAfter(30*24*time.Hour),
		jobs.RetentionConsumedSignalsAfter(7*24*time.Hour),
		jobs.RetentionInterval(time.Hour),
		jobs.RetentionBatchSize(1000),
	),
)
```

`jobs.DefaultRetention()` remains a conservative preset: completed jobs 7 days,
terminal failed/cancelled jobs 30 days, consumed signals 7 days.

Set `RetentionFailedAfter` long enough for operators to inspect and requeue DLQ
rows. Retention deletes are permanent. See [Retention GC]({{< relref
"/docs/advanced/retention-gc" >}}) for checkpoint GC, batch sizing, and storage
support details.

### Retention, Bloat & VACUUM

Delete-based retention removes old rows from `jobs`, `checkpoints`, and
`signals`, but PostgreSQL keeps the deleted row versions as dead tuples until
VACUUM reclaims them. On high-write queues, tune autovacuum per table instead
of relying only on the database-wide defaults:

> **Why `jobs` bloats fastest.** A job's `status` transitions (pending → running
> → completed/failed) are **non-HOT by construction**: PostgreSQL treats a
> partial index's predicate columns as HOT-blocking, and `status` is a
> predicate/key column of the dequeue, stale-lock, retention, and active-unique
> indexes. Every status change therefore writes fresh index tuples (and dead old
> ones), independent of how few indexes name `status` directly — which is exactly
> why aggressive autovacuum on `jobs` matters. (The redundant full `idx_jobs_status`
> was dropped in v20 to cut per-write index maintenance; the inherent non-HOT
> cost on the remaining partial indexes is unavoidable for an indexed queue.)

```sql
ALTER TABLE jobs SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold = 1000
);

ALTER TABLE checkpoints SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold = 1000
);

ALTER TABLE signals SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold = 1000
);
```

Very high-volume installs can avoid most delete bloat by partitioning retention
tables by time and dropping old partitions instead of deleting rows in batches.
That design is application-specific, but the goal is the same: remove cold data
without forcing VACUUM to chase millions of dead row versions.

Watch dead tuples with `pg_stat_user_tables`:

```sql
SELECT relname, n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname IN ('jobs', 'checkpoints', 'signals')
ORDER BY n_dead_tup DESC;
```

Sustained growth in `n_dead_tup` after retention runs means autovacuum is not
keeping up; lower the scale factor or threshold, increase autovacuum capacity,
or move high-volume history to partition-drop retention.

## Operational Coverage

This page covers these operational surfaces:

| Surface | Section |
|---|---|
| `idx_jobs_dequeue_eligible` (ready + delayed dequeue) | [Dequeue Index](#dequeue-index) |
| `CREATE INDEX CONCURRENTLY` / v15 collation / v14 FK out-of-band | [Schema Migrations At Scale](#schema-migrations-at-scale) |
| `jobs.queue.depth`, `jobs.queue.backlog.oldest_age`, `jobs.dead_letter.depth`, `jobs.queue.saturation` | [Metrics And Alerts](#metrics-and-alerts) |
| `storage.Healther`, `GormStorage.Ping(ctx)` | [Health Probes](#health-probes) |
| Work-conserving drain loop | [Throughput Tuning](#throughput-tuning) |
| `DequeueBatchPerQueue(ctx, workerID, budgets)` fair-share claim budget | [Throughput Tuning](#throughput-tuning) |
| `Worker.Health()`, `Worker.HealthHandler()`, `/healthz`, `/readyz` | [Health Probes](#health-probes) |
| `sdj migrate`, `sdj queues`, `sdj dlq list`, `sdj dlq requeue`, `sdj health` | [The `sdj` CLI](#the-sdj-cli) |
