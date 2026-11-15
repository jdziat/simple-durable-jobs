---
title: "Benchmarks"
weight: 7
---

## Methodology

These are measurements for relative guidance on this library's own storage backends. They are not a performance-lab cross-vendor comparison, and they should not be read as River or Asynq comparison numbers because those systems were not benchmarked under identical conditions.

**Where the numbers come from.** The Benchmarks workflow runs `scripts/bench.sh` on GitHub-hosted runners (`ubuntu-latest`, 4 vCPU), with Go 1.26.11 and `BENCH_COUNT=6` (six samples per benchmark). The tables below are from 14 runs of that workflow on `main` at commit `5a0c3e0` on 2026-11-15. The 14 runs produced 28 result files, one per backend leg. Each table uses the runs of one backend that landed on one CPU model, the most frequent one for that backend: SQLite on AMD EPYC 7763 (5 runs) and PostgreSQL on Intel Xeon 6975P-C (5 runs). The other runs landed on other CPU models; their files are what the checked-in baselines are built from, not these tables. Each value is the median over all samples from those runs; the range is the lowest and highest per-run median.

**The CPU model matters.** Hosted runners land on different CPU models from run to run (five were seen in these 14 runs: AMD EPYC 7763, 9V74 and 9V45, and Intel Xeon 6975P-C and 8573C), and the same benchmark can differ by up to 2x between them. Treat each table as a result for the CPU named, not as a property of the backend. The weekly canary (below) therefore compares a run only against a baseline recorded on the same CPU model.

**Weekly canary.** The Benchmarks workflow also runs weekly on `main`. It validates the output (a partial or corrupted run fails it), reports a benchstat comparison against the checked-in baseline for the run's CPU model, and runs a coarse regression check that fails the job when a gated benchmark's median is more than 2.0x (SQLite) or 2.5x (PostgreSQL) slower than that baseline. The factors are set above the run-to-run noise measured on the same CPU model (worst observed slowdown between runs: SQLite 1.22x, PostgreSQL 1.59x), so noise alone does not fail the job. A CPU model without a recorded baseline skips the comparison with a notice. This check catches large slowdowns; a regression of 30 to 60 percent shows only in the warn-only benchstat comparison. See `benchmarks/baseline/README.md`.

`B/op` and `allocs/op` are per benchmark operation, and an operation differs by row: one job for Enqueue, one 100-job batch for batch enqueue, one 1000-job drain for the end-to-end rows, and one five-step workflow job for workflow steps.

Database setup: SQLite used a temp file through `jobs.SafeSQLiteDSN`, which enables WAL, `busy_timeout=5000`, and `txlock=immediate`. PostgreSQL ran as a `postgres:16-alpine` service container on the runner. The benchmark client pool was capped at 16 open connections and 8 idle connections for every backend.

Benchmark shapes: `BenchmarkEnqueue` enqueues one job per operation through the public queue API. `BenchmarkEnqueueBatch` enqueues 100 jobs per operation and reports jobs/sec, and `BenchmarkEnqueueBatchSizes` repeats it at 100, 500, 1000 and 5000 jobs (a batch size that errors fails the benchmark). `BenchmarkEndToEnd` drains a saturated queue of 1000 pre-enqueued no-op jobs per benchmark operation, starts one worker with concurrency 16 and `WithPollInterval(50ms)`, and reports jobs/sec from first handler start to last completion. With the explicit single-row dequeue path (`WithDequeueBatchSize(1)`), each claim asks storage for one job; the work-conserving dispatcher repeats those single-row claims while capacity remains. `BenchmarkEndToEndBatchDequeue` uses the same saturated-queue drain shape plus `WithDequeueBatchSize(10)`. `BenchmarkWorkflowSteps` runs `b.N` workflow jobs, each doing 5 checkpointed `jobs.Call` steps, and reports steps/sec. `BenchmarkDispatchLatency` starts an already-running idle worker first, then enqueues one job at a time and records enqueue-to-handler-start latency before proceeding to the next sample.

The worker default poll interval is 100ms. These benchmarks use 50ms because that is the worker poll-interval floor: `WithPollInterval` clamps any positive value below 50ms up to that floor (and ignores a non-positive value, keeping the existing one). Idle-worker dispatch latency is poll-bound: p50 is roughly half to one poll interval, and the tail is roughly one poll interval plus scheduling noise. It is tunable with `WithPollInterval`, but lower polling means more database load. Dispatch-latency p95 and p99 are based on about 25 samples per run, so treat the tail values as a smoke signal rather than a statistically deep latency distribution. SQLite has a single-writer ceiling even with WAL, so use Postgres or MySQL for heavy multi-process write concurrency.

MySQL is not in the CI matrix; its numbers below are an older local measurement (see the MySQL section).

## Throughput Knobs

The explicit single-row dequeue path (`WithDequeueBatchSize(1)`) is the conservative claim mode: each storage request claims one job. Pair it with a slower `WithPollInterval(...)` when you want to approximate the older slow-poll behavior during rollback.

The worker default is `WithDequeueBatchSize(50)`, capped at the worker's free concurrency slots, so at the default concurrency of 10 the effective per-poll claim is 10. The work-conserving dispatcher, which landed on 2026-06-08, keeps claiming while capacity remains instead of waiting a full poll interval after each claim. That is why single-row claims no longer cap at one job per poll interval. One claim per 50ms poll is 20 jobs/sec, which is the figure published in June and is what the older dispatcher measured: `BenchmarkEndToEnd` now measures single-row claim throughput under the drain loop. Raising concurrency is what allows the larger default batch.

The two end-to-end rows are different claim shapes under the same drain loop, and neither is always faster: in these runs single-row claims were faster on SQLite and batch claims of 10 were faster on PostgreSQL. The benchmarks do not isolate why, so treat the comparison as specific to the backend and CPU model in the table.

## SQLite Results

AMD EPYC 7763, 5 runs, 4 vCPU runner.

| Benchmark | jobs/sec or steps/sec, median (range of run medians) | B/op | allocs/op |
| --- | --- | --- | --- |
| Enqueue (1 job per op) | 3,998 (3,894 to 4,180) | 21,379 | 169 |
| Batch enqueue (100 jobs per op) | 26,910 (25,945 to 27,532) | 792,311 | 5,617 |
| End-to-end, single-row claims (1000 jobs) | 374 (363 to 376) | 72,006,028 | 892,316 |
| End-to-end, batch claims of 10 (1000 jobs) | 283 (279 to 290) | 43,192,924 | 511,717 |
| Workflow steps (5 checkpointed `Call`s per job) | 1,066 (1,053 to 1,090) steps/sec | 170,100 | 1,877 |

Batch enqueue by batch size:

| Batch size | jobs/sec, median (range of run medians) |
| --- | --- |
| 100 | 26,942 (26,503 to 27,602) |
| 500 | 31,014 (29,594 to 31,362) |
| 1000 | 32,766 (31,608 to 33,365) |
| 5000 | 34,880 (34,186 to 35,666) |

Dispatch latency:

| Percentile | Latency, median (range of run medians) |
| --- | --- |
| p50 | 41.1 ms (40.7 to 41.2) |
| p95 | 41.7 ms (41.6 to 42.2) |
| p99 | 43.4 ms (43.1 to 43.5) |

## PostgreSQL Results

Intel Xeon 6975P-C, 5 runs, 4 vCPU runner.

| Benchmark | jobs/sec or steps/sec, median (range of run medians) | B/op | allocs/op |
| --- | --- | --- | --- |
| Enqueue (1 job per op) | 2,193 (2,150 to 2,214) | 19,412 | 153 |
| Batch enqueue (100 jobs per op) | 18,992 (18,756 to 19,167) | 1,202,894 | 8,573 |
| End-to-end, single-row claims (1000 jobs) | 541 (535 to 587) | 86,821,556 | 1,050,278 |
| End-to-end, batch claims of 10 (1000 jobs) | 875 (866 to 882) | 63,645,972 | 563,572 |
| Workflow steps (5 checkpointed `Call`s per job) | 1,622 (1,586 to 1,678) steps/sec | 217,360 | 2,010 |

Batch enqueue by batch size:

| Batch size | jobs/sec, median (range of run medians) |
| --- | --- |
| 100 | 19,070 (18,628 to 19,200) |
| 500 | 22,633 (21,949 to 22,927) |
| 1000 | 22,842 (22,642 to 23,264) |
| 5000 | 24,004 (23,070 to 24,328) |

Dispatch latency:

| Percentile | Latency, median (range of run medians) |
| --- | --- |
| p50 | 40.9 ms (40.6 to 41.2) |
| p95 | 42.2 ms (41.9 to 42.7) |
| p99 | 44.4 ms (43.8 to 44.8) |

## MySQL Results

MySQL was last measured on 2026-06-06 (Go 1.25.11, a 192-core local host, local `docker compose` containers). That run pre-dates the work-conserving dispatcher and the larger default batch, so its end-to-end rows no longer describe current behavior and are omitted. The remaining rows are indicative only and have not been re-measured; MySQL is not in the CI matrix.

| Benchmark | jobs/sec or steps/sec (June 2026) |
| --- | --- |
| Enqueue | `158.4`, `171.3`, `152.3` |
| Batch enqueue (100 jobs per op) | `5302`, `4921`, `5129` |
| Workflow steps | `738.0`, `743.4`, `735.8` |

Dispatch latency, June 2026 (idle-worker pickup is poll-bound, so the dispatcher change does not affect it): p50 `34.7 ms`, p95 `37.6` to `44.1 ms`, p99 `54.1` to `54.5 ms`.

## Reproduce It

Run the suite against SQLite with no setup, or start the external backends first:

```bash
POSTGRES_HOST_PORT=15433 MYSQL_HOST_PORT=13307 docker compose -p sdj-test up -d --wait postgres mysql
BENCH_COUNT=6 \
  TEST_DATABASE_URL='postgres://jobs:jobs@localhost:15433/jobs_test?sslmode=disable' \
  TEST_MYSQL_URL='root:jobs@tcp(localhost:13307)/jobs_test?parseTime=true' \
  scripts/bench.sh sqlite postgres mysql
```

The script isolates each backend leg by blanking unrelated DSN environment variables before invoking `go test`, so the command above attributes SQLite, Postgres, and MySQL results to the intended backend even when both external DSNs are exported in the parent shell. It validates each output file (a partial or corrupted run fails it), takes `BENCH_COUNT` (default 6) and `BENCH_TIMEOUT` (a Go duration, default `25m`), and writes raw output to `bench-results/<backend>.txt`. CI uploads the same raw files as artifacts.

Local numbers depend heavily on the host. Docker volumes on a slow disk make Postgres and MySQL commits wait on fsync, which can dominate every write-path number, so compare local runs only with other runs on the same machine.
