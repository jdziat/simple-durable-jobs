---
title: "Benchmarks"
weight: 7
---

## Methodology

These are single-machine measurements for relative guidance on this library's own storage backends. They are not a performance-lab cross-vendor comparison, and they should not be read as River or Asynq comparison numbers because those systems were not benchmarked under identical conditions.

Run date: June 6, 2026. Branch: `feat/adoption-wave` at `90e2907`. Command: `BENCH_COUNT=3 TEST_DATABASE_URL='postgres://jobs:jobs@localhost:15433/jobs_test?sslmode=disable' TEST_MYSQL_URL='root:jobs@tcp(localhost:13307)/jobs_test?parseTime=true' scripts/bench.sh sqlite postgres mysql`.

Host: AMD EPYC 9R14 96-Core Processor, `nproc=192`, `Linux 6.8.0-124-generic x86_64 GNU/Linux`, `go version go1.25.11 linux/amd64`.

Database versions: SQLite `3.50.2`; PostgreSQL `16.11 on x86_64-pc-linux-musl` with `max_connections=100`; MySQL `8.0.42` with `max_connections=151`.

Database setup: SQLite used a temp file through `jobs.SafeSQLiteDSN`, which enables WAL, `busy_timeout=5000`, and `txlock=immediate`. Postgres and MySQL used the repository `docker-compose.yml` containers on task-provided host ports `15433` and `13307`. The compose file does not override server `max_connections`; the measured values above are from SQL queries. The benchmark client pool was capped at 16 open connections and 8 idle connections for every backend so a 192-thread host would not swamp the shared dev containers.

Benchmark shapes: `BenchmarkEnqueue` enqueues one job per operation through the public queue API. `BenchmarkEnqueueBatch` enqueues 100 jobs per operation and reports jobs/sec. `BenchmarkEndToEnd` drains a saturated queue of 1000 pre-enqueued no-op jobs per benchmark operation, starts one worker with concurrency 16 and `WithPollInterval(50ms)`, and reports jobs/sec from first handler start to last completion. With the explicit single-row dequeue path (`WithDequeueBatchSize(1)`), each claim asks storage for one job; the work-conserving dispatcher may still repeat those single-row claims while capacity remains. `BenchmarkEndToEndBatchDequeue` uses the same saturated-queue drain shape plus `WithDequeueBatchSize(10)`. `BenchmarkWorkflowSteps` runs `b.N` workflow jobs, each doing 5 checkpointed `jobs.Call` steps, and reports steps/sec. `BenchmarkDispatchLatency` starts an already-running idle worker first, then enqueues one job at a time and records enqueue-to-handler-start latency before proceeding to the next sample.

The worker default poll interval is 100ms. These benchmarks use 50ms because that is the worker poll-interval floor: `WithPollInterval` clamps any positive value below 50ms up to that floor (and ignores a non-positive value, keeping the existing one). Idle-worker dispatch latency is poll-bound: p50 is roughly half to one poll interval on this host, and the tail is roughly one poll interval plus scheduling noise. It is tunable with `WithPollInterval`, but lower polling means more database load. Dispatch-latency p95 and p99 are based on about 25 samples per run, so treat the tail values as a smoke signal rather than a statistically deep latency distribution. SQLite has a single-writer ceiling even with WAL, so use Postgres or MySQL for heavy multi-process write concurrency.

The CI benchmark matrix covers SQLite and Postgres. MySQL numbers below are measured locally only against the repository's MySQL container on `localhost:13307`; there is no checked-in CI MySQL baseline.

## Throughput Knobs

The explicit single-row dequeue path (`WithDequeueBatchSize(1)`) is the conservative claim mode: each storage request claims one job. Pair it with a slower `WithPollInterval(...)` when you want to approximate the older slow-poll behavior during rollback.

`WithDequeueBatchSize(10)` is now the worker default, and for throughput-sensitive deployments raising it further helps. At batch size 10, the same 50ms poll can claim roughly `10 / 0.050s = 200 jobs/sec` before storage and handler work dominate. That arithmetic explains why the default EndToEnd row is similar across backends, while enqueue, batch enqueue, workflow, and SQLite's batched-drain shape are not near-identical.

## SQLite Results

| Benchmark | jobs/sec | ns/op | B/op | allocs/op |
| --- | --- | --- | --- | --- |
| Enqueue | `1139`, `1023`, `1193` | `877802`, `977545`, `837897` | `16562`, `16528`, `16525` | `145`, `145`, `145` |
| Batch enqueue | `7633`, `7920`, `8379` | `13100984`, `12626447`, `11934574` | `611989`, `612006`, `611782` | `4322`, `4322`, `4321` |

| Benchmark | Captured results |
| --- | --- |
| End-to-end | `20.02 jobs/sec`, `20.02 jobs/sec`, `20.02 jobs/sec` |
| End-to-end batch dequeue | `156.9 jobs/sec`, `156.0 jobs/sec`, `156.7 jobs/sec` |
| Workflow steps | `432.3 steps/sec`, `483.0 steps/sec`, `417.8 steps/sec` |
| Dispatch latency | p50 `41.745 ms`, `41.821 ms`, `41.757 ms`; p95 `43.062 ms`, `42.394 ms`, `41.963 ms`; p99 `43.894 ms`, `44.120 ms`, `43.931 ms` |

## PostgreSQL Results

| Benchmark | jobs/sec | ns/op | B/op | allocs/op |
| --- | --- | --- | --- | --- |
| Enqueue | `412.4`, `415.8`, `398.4` | `2424769`, `2405088`, `2510262` | `14655`, `14553`, `14604` | `126`, `126`, `126` |
| Batch enqueue | `10214`, `10635`, `9690` | `9790897`, `9402811`, `10320289` | `962180`, `962344`, `961305` | `6460`, `6461`, `6460` |

| Benchmark | Captured results |
| --- | --- |
| End-to-end | `20.02 jobs/sec`, `20.02 jobs/sec`, `20.02 jobs/sec` |
| End-to-end batch dequeue | `201.3 jobs/sec`, `201.4 jobs/sec`, `201.3 jobs/sec` |
| Workflow steps | `936.8 steps/sec`, `937.6 steps/sec`, `921.6 steps/sec` |
| Dispatch latency | p50 `38.694 ms`, `38.931 ms`, `38.381 ms`; p95 `43.003 ms`, `41.101 ms`, `43.563 ms`; p99 `52.603 ms`, `55.167 ms`, `54.454 ms` |

## MySQL Results

MySQL is included here as a local measurement only. CI currently publishes SQLite and Postgres artifacts and compares those two backends against checked-in baselines.

| Benchmark | jobs/sec | ns/op | B/op | allocs/op |
| --- | --- | --- | --- | --- |
| Enqueue | `158.4`, `171.3`, `152.3` | `6314391`, `5838760`, `6566516` | `16542`, `16505`, `16522` | `134`, `134`, `134` |
| Batch enqueue | `5302`, `4921`, `5129` | `18861738`, `20320236`, `19497120` | `746264`, `745407`, `746482` | `3648`, `3647`, `3648` |

| Benchmark | Captured results |
| --- | --- |
| End-to-end | `20.02 jobs/sec`, `20.02 jobs/sec`, `20.02 jobs/sec` |
| End-to-end batch dequeue | `199.6 jobs/sec`, `201.7 jobs/sec`, `201.6 jobs/sec` |
| Workflow steps | `738.0 steps/sec`, `743.4 steps/sec`, `735.8 steps/sec` |
| Dispatch latency | p50 `34.743 ms`, `34.969 ms`, `34.654 ms`; p95 `44.112 ms`, `40.038 ms`, `37.569 ms`; p99 `54.062 ms`, `54.262 ms`, `54.524 ms` |

## Reproduce It

Start the external backends, then run the benchmark suite:

```bash
POSTGRES_HOST_PORT=15433 MYSQL_HOST_PORT=13307 docker compose -p sdj-test up -d --wait postgres mysql
BENCH_COUNT=3 \
  TEST_DATABASE_URL='postgres://jobs:jobs@localhost:15433/jobs_test?sslmode=disable' \
  TEST_MYSQL_URL='root:jobs@tcp(localhost:13307)/jobs_test?parseTime=true' \
  scripts/bench.sh sqlite postgres mysql
```

The script isolates each backend leg by blanking unrelated DSN environment variables before invoking `go test`, so the command above genuinely attributes SQLite, Postgres, and MySQL results to the intended backend even when both external DSNs are exported in the parent shell. Raw local output is written to `bench-results/<backend>.txt`; CI uploads the same raw files as artifacts and reports WARN-only benchstat deltas against the checked-in SQLite and Postgres baselines. The runner default is `BENCH_COUNT=3`, matching CI and the checked-in baselines.
