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

1. Run the **Benchmarks** workflow on `main` (Actions, Run workflow). Never record from
   a branch: a regressed build would launder the regression into the baseline.
2. Download the `bench-<backend>` artifact. Confirm it passes
   `scripts/validate-bench-output.sh FILE BACKEND 6`.
3. Save it at the path `scripts/bench-baseline-for.sh BACKEND FILE` prints, with a header
   of `#` comment lines before the `goos:` line:

   ```
   # commit: <sha of the main commit the run built>
   # go: <go version>
   # runner: ubuntu-latest, <cores> cores
   # date: <YYYY-MM-DD>
   ```

4. Record each CPU model you see. A new model on a later run only skips (with a NOTICE
   naming the file to record) until its baseline is added.
5. Build each baseline from **three or more** runs on that CPU model, not one: validate
   each artifact on its own, then concatenate the result lines of all of them under one
   header. The checks take the median across every sample, so one unlucky run cannot
   shift the threshold. Dispatch until each (backend, CPU model) has three.

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
