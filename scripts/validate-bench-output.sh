#!/usr/bin/env bash
# validate-bench-output.sh - fail when a `go test -bench` output file is not a
# complete, clean run.
#
# benchstat exits 0 on input it only partly understands, so a corrupted file does
# not fail a job by itself. The 2026-11-10 CI artifact is the case this exists for:
# slog lines landed in the middle of benchmark records and benchstat silently
# dropped most samples. This checks the file itself.
#
# Usage: scripts/validate-bench-output.sh FILE BACKEND COUNT
#   BACKEND  sqlite | postgres | mysql
#   COUNT    the -count the run used (BENCH_COUNT)
#
# A file passes when:
#   1. every line is a Benchmark result line, a go test header or footer
#      (goos, goarch, pkg, cpu, PASS, ok), a benchmark log block (--- BENCH: and
#      its indented b.Logf lines), or blank;
#   2. no line reports a failure (--- FAIL, FAIL, panic);
#   3. every required benchmark for the backend has exactly COUNT result lines.
#
# The required set is per backend because the two churn benchmarks skip on SQLite.
# The count is the same for every benchmark; derived from observed clean runs
# (scripts/testdata/bench/clean-sqlite.txt and the Postgres artifact).
set -euo pipefail

if [ $# -ne 3 ]; then
  echo "usage: $0 FILE BACKEND COUNT" >&2
  exit 2
fi
file="$1"
backend="$2"
count="$3"

[ -f "$file" ] || { echo "validate-bench-output: $file not found" >&2; exit 1; }
case "$count" in ''|*[!0-9]*|0) echo "validate-bench-output: COUNT must be a positive integer" >&2; exit 2 ;; esac

common="Enqueue EnqueueBatch EndToEnd EndToEndBatchDequeue WorkflowSteps DispatchLatency"
case "$backend" in
  sqlite) required="$common" ;;
  postgres|mysql) required="$common SaturatedRateLimitChurn SaturatedKeyedRateLimitChurn" ;;
  *) echo "validate-bench-output: unknown backend: $backend" >&2; exit 2 ;;
esac

awk -v count="$count" -v required="$required" -v file="$file" '
  function bad(msg) { print file ":" NR ": " msg > "/dev/stderr"; problems++ }
  !/^[ \t]/ { in_log = 0 }
  /^(--- FAIL|FAIL|panic:)/ { bad("failure line: " $0); next }
  /^Benchmark[A-Za-z0-9_]+(-[0-9]+)?[ \t]+[0-9]+[ \t]+[0-9.]+ ns\/op([ \t]+[0-9.eE+-]+ [A-Za-z0-9_\/%-]+)*[ \t]*$/ {
    name = $1
    sub(/^Benchmark/, "", name)
    sub(/-[0-9]+$/, "", name)
    seen[name]++
    next
  }
  /^(goos|goarch|pkg|cpu): / { next }
  /^(PASS|ok[ \t].*)$/ { next }
  /^[ \t]*$/ { next }
  /^--- BENCH: Benchmark[A-Za-z0-9_]+(-[0-9]+)?$/ { in_log = 1; next }
  # b.Logf output: indented lines (a space or a tab; multi-line messages indent deeper)
  # are accepted only inside a --- BENCH: block, so a stray indented line elsewhere
  # still fails.
  in_log && /^[ \t]+[^ \t]/ { next }
  { bad("unexpected line: " substr($0, 1, 120)) }
  END {
    n = split(required, names, " ")
    for (i = 1; i <= n; i++) {
      got = seen[names[i]] + 0
      if (got != count) {
        print file ": " names[i] ": expected " count " result line(s), found " got > "/dev/stderr"
        problems++
      }
    }
    if (problems > 0) { exit 1 }
  }
' "$file"

echo "validate-bench-output: $file ok ($backend, count=$count)"
