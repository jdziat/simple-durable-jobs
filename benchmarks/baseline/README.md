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
on `main` minutes apart (2026-11-14) gave:

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
   is wide (seven were seen in 14 runs), so expect many runs.
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

Two runs on the same CPU model (sqlite leg, EPYC 7763), run 2 against run 1, medians of
six samples:

| Benchmark | ratio |
| --- | --- |
| Enqueue | 1.02 |
| EnqueueBatch | 1.01 |
| EndToEnd | 1.03 |
| EndToEndBatchDequeue | 0.98 |
| WorkflowSteps | 1.07 |

The worst same-CPU ratio is 1.07, so 2.0 leaves a wide margin against false alarms and
still catches a real slowdown (a 3x one fails; see `scripts/bench-scripts_test.sh`).

Limits of this calibration, stated plainly: it is **SQLite only** and **two runs**. Both
Postgres runs landed on different CPU models, so the Postgres same-CPU spread has not
been measured, and Postgres is more sensitive to I/O and noisy neighbours. The factor is
provisional there. After three or more runs per Postgres CPU model, re-derive it; if the
spread exceeds 1.3, give Postgres its own factor (for example 2.5) rather than letting a
noisy backend raise false alarms.
