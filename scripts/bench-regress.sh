#!/usr/bin/env bash
# bench-regress.sh - fail when a gated benchmark is much slower than the baseline.
#
# Usage: scripts/bench-regress.sh BASELINE CURRENT
# Env:   BENCH_REGRESSION_FACTOR  slowdown that fails the check (default 2.0)
#
# This is a COARSE check, not a precise one. CI runners are shared, so run-to-run
# variation is large (EnqueueBatch ranged 5.8k to 9.5k jobs/s, about 1.6x, inside one
# job). The factor must sit above the measured spread recorded in the baseline header;
# finer deltas are reported by bench-compare.sh and never fail a job.
#
# For each gated benchmark it takes the MEDIAN of the samples on each side, then:
#   - throughput benchmarks (jobs/sec or steps/sec): ratio = baseline / current
#   - latency-only benchmarks (ns/op):                ratio = current / baseline
# A ratio above the factor is a regression. Reading the throughput metric matters for
# the end-to-end benchmarks, where ns/op is the whole run, not the measured window.
#
# Not gated: DispatchLatency (pinned to the 50ms poll tick, so it measures the ticker)
# and the two churn soaks (they report rates of a fixed-length soak, not performance).
#
# Skips with a NOTICE and exit 0 when there is no baseline or its `cpu:` line differs,
# exactly as bench-compare.sh does; a missing benchmark in the current file fails.
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 BASELINE CURRENT" >&2
  exit 2
fi
baseline="$1"
current="$2"
factor="${BENCH_REGRESSION_FACTOR:-2.0}"
gated="Enqueue EnqueueBatch EnqueueBatchSizes EndToEnd EndToEndBatchDequeue WorkflowSteps"

[ -f "$current" ] || { echo "bench-regress: $current not found" >&2; exit 1; }
if [ ! -f "$baseline" ]; then
  echo "NOTICE: no baseline at $baseline; regression check skipped."
  exit 0
fi

cpu_of() { sed -n 's/^cpu:[[:space:]]*//p' "$1" | head -1 | sed 's/[[:space:]]*$//'; }
if [ "$(cpu_of "$baseline")" != "$(cpu_of "$current")" ]; then
  echo "NOTICE: baseline was recorded on a different cpu; regression check skipped."
  exit 0
fi

# samples FILE -> lines "name metric value": the throughput value when the benchmark
# reports one (higher is better), else ns/op (lower is better).
samples() {
  awk '
    /^Benchmark[A-Za-z0-9_\/=.-]+(-[0-9]+)?[ \t]+[0-9]+[ \t]/ {
      name = $1; sub(/^Benchmark/, "", name); sub(/-[0-9]+$/, "", name)
      ns = ""; tp = ""; unit = ""
      for (i = 3; i < NF; i++) {
        if ($(i+1) == "ns/op") ns = $i
        if ($(i+1) == "jobs/sec" || $(i+1) == "steps/sec") { tp = $i; unit = $(i+1) }
      }
      if (tp != "") print name, "tp", tp
      else if (ns != "") print name, "ns", ns
    }' "$1"
}

median() { sort -n | awk '{ v[NR] = $1 } END { if (NR == 0) exit 1; print (NR % 2 ? v[(NR+1)/2] : (v[NR/2] + v[NR/2+1]) / 2) }'; }

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
samples "$baseline" >"$tmp/base"
samples "$current" >"$tmp/cur"

fail=0
printf '%-34s %-8s %14s %14s %8s\n' benchmark metric baseline current ratio
for name in $gated; do
  # Every gated name plus its sub-benchmarks (EnqueueBatchSizes/n=...).
  while IFS= read -r full; do
    bm="$(awk -v n="$full" '$1 == n { print $2; exit }' "$tmp/base")"
    cm="$(awk -v n="$full" '$1 == n { print $2; exit }' "$tmp/cur")"
    if [ -z "$bm" ]; then
      echo "bench-regress: $full is missing from the baseline file; record a new baseline" >&2
      fail=1
      continue
    fi
    if [ -z "$cm" ]; then
      echo "bench-regress: $full is missing from the current file" >&2
      fail=1
      continue
    fi
    if [ "$bm" != "$cm" ]; then
      echo "bench-regress: $full reports $bm in the baseline but $cm now; the ratio would be meaningless, so record a new baseline" >&2
      fail=1
      continue
    fi
    bv="$(awk -v n="$full" '$1 == n { print $3 }' "$tmp/base" | median)"
    cv="$(awk -v n="$full" '$1 == n { print $3 }' "$tmp/cur" | median)"
    if ! awk -v b="$bv" 'BEGIN { exit !(b > 0) }'; then
      echo "bench-regress: the baseline median for $full is zero; the baseline is unusable, record a new one" >&2
      fail=1
      continue
    fi
    if [ "$bm" = tp ]; then
      ratio="$(awk -v b="$bv" -v c="$cv" 'BEGIN { printf "%.2f", (c > 0 ? b / c : 999) }')"
    else
      ratio="$(awk -v b="$bv" -v c="$cv" 'BEGIN { printf "%.2f", (b > 0 ? c / b : 999) }')"
    fi
    printf '%-34s %-8s %14s %14s %8s\n' "$full" "$bm" "$bv" "$cv" "$ratio"
    if awk -v r="$ratio" -v f="$factor" 'BEGIN { exit !(r > f) }'; then
      echo "bench-regress: REGRESSION $full is ${ratio}x slower than the baseline (limit ${factor}x)" >&2
      fail=1
    fi
  done < <(awk -v n="$name" '$1 == n || index($1, n "/") == 1 { print $1 }' "$tmp/base" "$tmp/cur" | sort -u)
done

if [ "$fail" -ne 0 ]; then
  exit 1
fi
echo "bench-regress: no gated benchmark is more than ${factor}x slower than the baseline"
