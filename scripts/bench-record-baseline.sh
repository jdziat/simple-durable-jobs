#!/usr/bin/env bash
# bench-record-baseline.sh - build one per-CPU baseline from several benchmark runs.
#
# Usage: scripts/bench-record-baseline.sh [options] BACKEND FILE...
#   --commit SHA     main commit the runs built (required)
#   --go VERSION     Go version of the runs (required)
#   --runner TEXT    runner description, e.g. "ubuntu-latest, 4 cores" (required)
#   --date YYYY-MM-DD  default: today (UTC)
#   --min-runs N     refuse fewer than N files (default 3)
# Env: BENCH_COUNT   samples per benchmark in each file (default 6)
#
# FILE is a `bench-<backend>` artifact from a Benchmarks workflow run on main. Every
# file must validate (scripts/validate-bench-output.sh) and come from the SAME cpu model.
# The result is written to the path scripts/bench-baseline-for.sh prints:
#
#   benchmarks/baseline/<backend>/<cpu-slug>.txt
#
# with one header (goos, goarch, pkg, cpu, then commit, go, runner, date, runs as
# `key: value` lines, which benchstat reads as file configuration) followed by every
# result line from every file. The checks take the median across all samples, so a
# baseline built from three or more runs is not skewed by one unlucky run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
min_runs=3
commit="" gover="" runner="" date_utc="$(date -u +%F)"

while [ $# -gt 0 ]; do
  case "$1" in
    --commit) commit="${2:-}"; shift 2 ;;
    --go) gover="${2:-}"; shift 2 ;;
    --runner) runner="${2:-}"; shift 2 ;;
    --date) date_utc="${2:-}"; shift 2 ;;
    --min-runs) min_runs="${2:-}"; shift 2 ;;
    --) shift; break ;;
    -*) echo "bench-record-baseline: unknown option $1" >&2; exit 2 ;;
    *) break ;;
  esac
done

if [ $# -lt 2 ]; then
  echo "usage: $0 [--commit SHA --go VERSION --runner TEXT --date D --min-runs N] BACKEND FILE..." >&2
  exit 2
fi
backend="$1"
shift
for need in commit gover runner; do
  if [ -z "${!need}" ]; then
    echo "bench-record-baseline: --${need/gover/go} is required" >&2
    exit 2
  fi
done
case "$min_runs" in ''|*[!0-9]*|0) echo "bench-record-baseline: --min-runs must be a positive integer" >&2; exit 2 ;; esac
count="${BENCH_COUNT:-6}"

if [ $# -lt "$min_runs" ]; then
  echo "bench-record-baseline: $# run(s) given, at least $min_runs are required (one run is one noisy sample)" >&2
  exit 1
fi

cpu_of() { sed -n 's/^cpu:[[:space:]]*//p' "$1" | head -1 | sed 's/[[:space:]]*$//'; }
first="$1"
cpu="$(cpu_of "$first")"
for f in "$@"; do
  "$ROOT/scripts/validate-bench-output.sh" "$f" "$backend" "$count" >/dev/null
  if [ "$(cpu_of "$f")" != "$cpu" ]; then
    echo "bench-record-baseline: $f is from cpu '$(cpu_of "$f")', not '$cpu'; a baseline is one cpu model" >&2
    exit 1
  fi
done

out="$("$ROOT/scripts/bench-baseline-for.sh" "$backend" "$first")"
mkdir -p "$ROOT/$(dirname "$out")"
{
  sed -n '/^goos:/p;/^goarch:/p;/^pkg:/p' "$first" | head -3
  echo "cpu: $cpu"
  echo "commit: $commit"
  echo "go: $gover"
  echo "runner: $runner"
  echo "date: $date_utc"
  echo "runs: $#"
  for f in "$@"; do grep '^Benchmark' "$f"; done
} >"$ROOT/$out"
echo "bench-record-baseline: wrote $out from $# runs ($cpu)"
