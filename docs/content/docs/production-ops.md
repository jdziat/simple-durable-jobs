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
sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq list --json
sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq list --ids-only

# Requeue one dead-lettered job by id.
sdj --driver postgres --dsn postgres://user:pass@localhost/db dlq requeue <jobID>

# Ping storage and print OK on success.
sdj --driver postgres --dsn postgres://user:pass@localhost/db health
```

Each subcommand accepts `-h` for its command-specific usage. `dlq list` prints a
table by default; `--json` emits a JSON array and `--ids-only` emits one job ID
per line for scripts. `dlq requeue` succeeds only when the target job exists and
is failed or cancelled; fan-out sub-jobs must be handled by requeueing their
parent.

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
The storage scan still follows the global `priority DESC, created_at ASC` order;
rows from a queue that has already hit its budget are skipped with the same
skip-list mechanism used for paused rows. This keeps cross-queue global priority
while preventing a hot queue from over-claiming and then releasing surplus rows.

Rollback escape hatch: set `jobs.WithDequeueBatchSize(1)` and use a slower
`jobs.WithPollInterval(...)` to approximate the conservative single-claim,
slow-poll behavior. This is useful during incident response if a database is
under unexpected dequeue pressure.

## Dequeue Index

Migration v9 (`dequeue_order_index`) adds `idx_jobs_dequeue_order` for
multi-queue priority scans.

The index is order-first:

- MySQL: `(status, priority DESC, created_at ASC, queue)`
- PostgreSQL and SQLite: partial index on `(priority DESC, created_at ASC, queue)
  WHERE status = 'pending'`

The existing `idx_jobs_dequeue` remains for queue-filtered scans. The new index
lets planners walk pending jobs in global priority order and filter queues from
the index, which avoids sort-heavy plans on the `ORDER BY priority DESC,
created_at ASC LIMIT ...` dequeue hot path. The P1 EXPLAIN tests document the
reason: MySQL moved from a full scan with `Using filesort` to
`idx_jobs_dequeue_order` with `Using index condition`; PostgreSQL moved to an
`Index Scan using idx_jobs_dequeue_order` without a Sort node.

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
   sdj --driver postgres --dsn "$DATABASE_URL" dlq list --queue emails --ids-only
   ```

3. Fix the handler code or external dependency that caused the terminal
   failure.

4. Requeue one job:

   ```bash
   sdj --driver postgres --dsn "$DATABASE_URL" dlq requeue <jobID>
   ```

Requeue clears DLQ metadata, resets execution state, and deletes checkpoints so
the workflow starts from the beginning. Handlers still must be idempotent. See
[Dead-Letter Queue]({{< relref "/docs/advanced/dead-letter-queue" >}}) for the
full API and retention caveats.

## Retention And GC

Retention is disabled by default. Without retention, completed, failed,
cancelled, and consumed-signal rows accumulate forever. Configure it on workers:

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

`jobs.DefaultRetention()` is an explicit opt-in preset: completed jobs 7 days,
terminal failed/cancelled jobs 30 days, consumed signals 7 days.

Set `RetentionFailedAfter` long enough for operators to inspect and requeue DLQ
rows. Retention deletes are permanent. See [Retention GC]({{< relref
"/docs/advanced/retention-gc" >}}) for checkpoint GC, batch sizing, and storage
support details.

## Operational Coverage

This page covers these operational surfaces:

| Packet | Surface | Coverage |
|---|---|---|
| P1 | `idx_jobs_dequeue_order` / migration v9 `dequeue_order_index` | [Dequeue Index](#dequeue-index) |
| P2 | `jobs.queue.depth`, `jobs.queue.backlog.oldest_age`, `jobs.dead_letter.depth`, `jobs.queue.saturation` | [Metrics And Alerts](#metrics-and-alerts) |
| P3 | `storage.Healther`, `GormStorage.Ping(ctx)` | [Health Probes](#health-probes) |
| P4 | Work-conserving drain loop | [Throughput Tuning](#throughput-tuning) |
| P5 | `DequeueBatchPerQueue(ctx, workerID, budgets)` fair-share claim budget | [Throughput Tuning](#throughput-tuning) |
| P6 | `Worker.Health()`, `Worker.HealthHandler()`, `/healthz`, `/readyz` | [Health Probes](#health-probes) |
| P7 | `sdj migrate`, `sdj queues`, `sdj dlq list`, `sdj dlq requeue`, `sdj health` | [The `sdj` CLI](#the-sdj-cli) |
