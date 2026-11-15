# Benchmark baselines

Baselines are the reference runs the weekly benchmark job compares against. They are
stored per backend **and per CPU model**:

```
benchmarks/baseline/<backend>/<cpu-slug>.txt
```

`<cpu-slug>` is the run's own `cpu:` line, lowercased, with non-alphanumeric runs turned
into a dash (`scripts/bench-baseline-for.sh BACKEND FILE` prints the path). While a file
is absent the compare step and the regression check print a NOTICE and skip; they do
not fail.

## Why per CPU model

GitHub-hosted runners land on different CPU models from run to run. Two dispatch runs
on `main` minutes apart (2026-11-14 23:37 UTC) gave:

| Leg | Run 1 CPU | Run 2 CPU |
| --- | --- | --- |
| sqlite | AMD EPYC 7763 | AMD EPYC 7763 |
| postgres | AMD EPYC 9V74 | AMD EPYC 9V45 |

The same Postgres benchmarks differed by up to 2.09x between those two hosts (Enqueue
650 vs 1358 jobs/s). One baseline per backend would either skip often or report false
regressions, so each CPU model gets its own file and a run on an unrecorded CPU model
skips.

## Recording a baseline

A baseline is built from **three or more** runs on one CPU model, not one: the checks
take the median across every sample, so one unlucky run cannot shift the threshold.

1. Run the **Benchmarks** workflow on `main` (Actions, Run workflow) until a
   (backend, CPU model) pair has three runs. Never record from a branch: a regressed
   build would launder the regression into the baseline. The pool of hosted CPU models
   is wide (five were seen in 14 runs), so expect many runs.
2. Download the `bench-<backend>` artifacts and group them by the `cpu:` line.
3. For each group:

   ```
   scripts/bench-record-baseline.sh --commit <main sha> --go <go version> \
     --runner "ubuntu-latest, <cores> cores" <backend> <artifact>...
   ```

   It validates every artifact (`scripts/validate-bench-output.sh`), refuses mixed CPU
   models and fewer than three runs (`--min-runs` lowers the floor explicitly), and
   writes the one file `scripts/bench-baseline-for.sh` names, with a `commit`, `go`,
   `runner`, `date` and `runs` header that benchstat reads as file configuration.
4. A CPU model with no baseline only skips, with a NOTICE naming the file to add.

## What the checks mean

- `scripts/bench-compare.sh` runs benchstat and prints the deltas. It never fails on a
  slowdown; shared runners are noisy.
- `scripts/bench-regress.sh` is the coarse check and the only step that fails on a
  slowdown. It compares the median of the samples for each gated benchmark and fails
  when one is more than `BENCH_REGRESSION_FACTOR` times slower (2.0 in `bench.yml`).
  It reads the throughput metric (jobs/sec, steps/sec) where a benchmark reports one.
  `DispatchLatency` (pinned to the 50ms poll tick) and the churn soaks are not gated.

## Calibration of the factor

Fourteen dispatch runs on `main` (commit 5a0c3e0, 2026-11-15 UTC; dates in this file are UTC) gave 28 valid artifacts on
five CPU models. For every (backend, CPU model) pair with three or more runs, each run
was compared with the median of the OTHER runs on that CPU model (leave one out), for
every gated benchmark. The ratio is "how many times slower than the others", so 1.0 is
identical:

| Backend | comparisons | median | p95 | worst |
| --- | --- | --- | --- | --- |
| sqlite | 108 | 1.00 | 1.08 | 1.22 (EnqueueBatchSizes n=100) |
| postgres | 108 | 0.99 | 1.27 | 1.59 (Enqueue) |

Postgres is noisier than SQLite, as expected for a service container on shared runners.
The factors in `bench.yml` sit above the worst observed ratio: **2.0 for SQLite**
(1.64x headroom) and **2.5 for Postgres** (1.57x headroom). A real 3x slowdown fails on
both (see `scripts/bench-scripts_test.sh`).

Between CPU models the same Postgres benchmarks differed by up to 2.09x (two runs on
EPYC 9V74 and 9V45), which is why baselines are per CPU model.

Limits: the samples are from one afternoon, so slow drift in the runner fleet is not
captured, and the Postgres factor has only 1.57x headroom over the worst case seen. A
single red scheduled run is therefore not yet a regression: re-dispatch the workflow on
`main` once, and treat the slowdown as real only if the same benchmark fails again.
Re-derive the factors from the scheduled runs after a few months of history.
