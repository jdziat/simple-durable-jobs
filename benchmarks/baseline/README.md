# Benchmark baselines

`sqlite.txt` and `postgres.txt` here are the reference runs that
`scripts/bench-compare.sh` compares the weekly benchmark run against. While a file is
absent the compare step prints a NOTICE and skips; it does not fail.

## Recording a baseline

1. Run the **Benchmarks** workflow on `main` (Actions, Run workflow). Never record from
   a branch: a regressed build would launder the regression into the baseline.
2. Download the `bench-sqlite` and `bench-postgres` artifacts from that run.
3. Confirm each passes `scripts/validate-bench-output.sh FILE BACKEND 6`.
4. Commit them as `sqlite.txt` and `postgres.txt` with a header of `#` comment lines
   before the `goos:` line:

   ```
   # commit: <sha of the main commit the run built>
   # go: <go version>
   # runner: ubuntu-latest (<cores> cores)
   # date: <YYYY-MM-DD>
   # spread: <observed run-to-run spread, from the calibration runs>
   ```

A baseline only applies to the machine type it was recorded on: the compare step skips
when the `cpu:` line differs. Refresh it when the runner type changes.

## What the comparison means

CI runners are shared, so a stored baseline is noisy. Deltas are reported for
information; they do not fail the job. A coarse regression check, set above the
measured spread, is a separate step (see the benchmark review plan).
