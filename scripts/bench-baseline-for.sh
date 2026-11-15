#!/usr/bin/env bash
# bench-baseline-for.sh - print the baseline path that applies to a benchmark run.
#
# Usage: scripts/bench-baseline-for.sh BACKEND CURRENT_FILE
#
# Baselines are per CPU model: benchmarks/baseline/<backend>/<cpu-slug>.txt, where the
# slug is the run's own `cpu:` line, lowercased, with every run of non-alphanumeric
# characters turned into one dash. GitHub-hosted runners land on different CPU models
# from run to run (the same leg ran on EPYC 9V74 and 9V45 in two runs minutes apart),
# and the same benchmark differs by up to 2x between them, so a single baseline per
# backend would either skip often or report false regressions.
#
# The path is printed whether or not the file exists; bench-compare.sh and
# bench-regress.sh skip loudly when it does not, and say which file to record.
set -euo pipefail

if [ $# -ne 2 ]; then
  echo "usage: $0 BACKEND CURRENT_FILE" >&2
  exit 2
fi
backend="$1"
current="$2"
case "$backend" in
  sqlite|postgres|mysql) ;;
  *) echo "bench-baseline-for: unknown backend: $backend" >&2; exit 2 ;;
esac
[ -f "$current" ] || { echo "bench-baseline-for: $current not found" >&2; exit 1; }

cpu="$(sed -n 's/^cpu:[[:space:]]*//p' "$current" | head -1 | sed 's/[[:space:]]*$//')"
if [ -z "$cpu" ]; then
  echo "bench-baseline-for: $current has no cpu: line" >&2
  exit 1
fi
slug="$(printf '%s' "$cpu" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-+//; s/-+$//')"
echo "benchmarks/baseline/${backend}/${slug}.txt"
