#!/usr/bin/env bash
# bench-compare.sh - compare a benchmark run against the checked-in baseline.
#
# Usage: scripts/bench-compare.sh BASELINE CURRENT
# Env:   BENCHSTAT  benchstat binary (default: benchstat)
#
# Prints a benchstat comparison. It SKIPS, with a notice and exit 0, when:
#   - the baseline file does not exist (none is recorded yet), or
#   - the baseline's `cpu:` line differs from the run's (a baseline from another
#     machine says nothing about this one; refresh it, see benchmarks/baseline/README.md).
# A skip is loud in the output so it cannot be mistaken for a clean comparison.
#
# The `-N` GOMAXPROCS suffix is stripped from both files before comparing, so a
# baseline recorded on a 4-core runner still lines up if the core count changes.
# benchstat's own exit status propagates: a file it cannot read fails the step.
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 BASELINE CURRENT" >&2
  exit 2
fi
baseline="$1"
current="$2"
benchstat="${BENCHSTAT:-benchstat}"

[ -f "$current" ] || { echo "bench-compare: $current not found" >&2; exit 1; }

if [ ! -f "$baseline" ]; then
  echo "NOTICE: no baseline at $baseline; comparison skipped (record one: benchmarks/baseline/README.md)."
  exit 0
fi

cpu_of() { sed -n 's/^cpu:[[:space:]]*//p' "$1" | head -1 | sed 's/[[:space:]]*$//'; }
base_cpu="$(cpu_of "$baseline")"
cur_cpu="$(cpu_of "$current")"
if [ "$base_cpu" != "$cur_cpu" ]; then
  echo "NOTICE: baseline was recorded on a different cpu; comparison skipped."
  echo "  baseline: ${base_cpu:-<none>}"
  echo "  this run: ${cur_cpu:-<none>}"
  echo "  Refresh the baseline from a main run on this runner type (benchmarks/baseline/README.md)."
  exit 0
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
strip() { sed -E 's/^(Benchmark[A-Za-z0-9_]+)-[0-9]+([[:space:]])/\1\2/' "$1"; }
strip "$baseline" >"$tmp/baseline.txt"
strip "$current" >"$tmp/current.txt"

"$benchstat" "$tmp/baseline.txt" "$tmp/current.txt"
