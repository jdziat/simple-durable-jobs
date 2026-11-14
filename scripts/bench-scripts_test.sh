#!/usr/bin/env bash
# bench-scripts_test.sh - tests for validate-bench-output.sh and bench-compare.sh.
# Runs in CI (Lint job). The fixtures are real output: clean-sqlite.txt is a clean
# local run; corrupted-postgres-2026-11-10.txt is the CI artifact whose slog lines
# split benchmark records.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIX="$ROOT/scripts/testdata/bench"
VALIDATE="$ROOT/scripts/validate-bench-output.sh"
COMPARE="$ROOT/scripts/bench-compare.sh"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

failures=0
# A setup command that fails would leave an assertion running against a missing or
# stale fixture and pass for the wrong reason; make it stop the run instead.
must() { "$@" || { echo "SETUP FAILED: $*" >&2; exit 1; }; }
pass() { echo "ok   - $1"; }
fail() { echo "FAIL - $1" >&2; failures=$((failures + 1)); }

# expect_status NAME WANT_STATUS OUTPUT_PATTERN -- COMMAND...
expect() {
  local name="$1" want="$2" pat="$3"
  shift 4
  local out status
  out="$("$@" 2>&1)"
  status=$?
  if [ "$status" -ne "$want" ]; then
    fail "$name: exit $status, want $want: $out"
  elif [ -n "$pat" ] && ! grep -q -- "$pat" <<<"$out"; then
    fail "$name: output lacks '$pat': $out"
  else
    pass "$name"
  fi
}

clean="$FIX/clean-sqlite.txt"

# ---- validate-bench-output.sh ----
expect "clean sqlite run passes" 0 "ok (sqlite" -- "$VALIDATE" "$clean" sqlite 3
expect "wrong count fails" 1 "expected 6 result line" -- "$VALIDATE" "$clean" sqlite 6
expect "the corrupted CI artifact fails" 1 "unexpected line" -- "$VALIDATE" "$FIX/corrupted-postgres-2026-11-10.txt" postgres 3
expect "unknown backend is a usage error" 2 "unknown backend" -- "$VALIDATE" "$clean" oracle 3
expect "missing file fails" 1 "not found" -- "$VALIDATE" "$tmp/nope.txt" sqlite 3

must cp "$clean" "$tmp/fail-line.txt"
printf -- '--- FAIL: BenchmarkEndToEnd\n    bench_test.go:221: boom\n' >>"$tmp/fail-line.txt"
expect "a FAIL line fails" 1 "failure line" -- "$VALIDATE" "$tmp/fail-line.txt" sqlite 3

must grep -v '^BenchmarkDispatchLatency' "$clean" >"$tmp/no-dispatch.txt"
expect "a missing benchmark fails" 1 "DispatchLatency: expected 3" -- "$VALIDATE" "$tmp/no-dispatch.txt" sqlite 3

must cp "$clean" "$tmp/stray.txt"
echo "2026/11/14 11:00:00 INFO retention GC enabled" >>"$tmp/stray.txt"
expect "a stray log line fails" 1 "unexpected line" -- "$VALIDATE" "$tmp/stray.txt" sqlite 3

must sed 's/-192 /-4 /' "$clean" >"$tmp/four-core.txt"
expect "a different GOMAXPROCS suffix still validates" 0 "ok (sqlite" -- "$VALIDATE" "$tmp/four-core.txt" sqlite 3

expect "postgres requires the churn benchmarks" 1 "SaturatedRateLimitChurn: expected 3" -- "$VALIDATE" "$clean" postgres 3
must cp "$clean" "$tmp/pg.txt"
for name in SaturatedRateLimitChurn SaturatedKeyedRateLimitChurn; do
  for _ in 1 2 3; do
    printf 'Benchmark%s-192  \t       1\t2000807457 ns/op\t        15.00 completed/s\t 8213576 B/op\t   93591 allocs/op\n' "$name" >>"$tmp/pg.txt"
  done
done
printf -- '--- BENCH: BenchmarkSaturatedRateLimitChurn-192\n    ratelimit_churn_bench_test.go:104: cto-F2 churn soak [unkeyed]: completed=30\n' >>"$tmp/pg.txt"
expect "postgres with churn results and a b.Logf block passes" 0 "ok (postgres" -- "$VALIDATE" "$tmp/pg.txt" postgres 3
expect "mysql is validated like postgres" 0 "ok (mysql" -- "$VALIDATE" "$tmp/pg.txt" mysql 3
expect "mysql requires the churn benchmarks too" 1 "SaturatedKeyedRateLimitChurn: expected 3" -- "$VALIDATE" "$clean" mysql 3

# b.Logf blocks: tabs and deeper indents inside a --- BENCH: block are accepted; an
# indented stray line outside any block is not.
must cp "$clean" "$tmp/logblock.txt"
printf -- '--- BENCH: BenchmarkEndToEnd-192\n\tbench_test.go:12: tab-indented\n        wrapped continuation line\n' >>"$tmp/logblock.txt"
expect "an indented b.Logf block of any indent validates" 0 "ok (sqlite" -- "$VALIDATE" "$tmp/logblock.txt" sqlite 3
must cp "$clean" "$tmp/stray-indent.txt"
printf '    bench_test.go:12: not inside any block\n' >>"$tmp/stray-indent.txt"
expect "an indented line outside a --- BENCH: block fails" 1 "unexpected line" -- "$VALIDATE" "$tmp/stray-indent.txt" sqlite 3

# ---- bench.sh exit status ----
# run_backend is called from an `if`, where set -e is off: a failed `go test` whose
# output still validates must NOT be reported as a pass.
mkdir -p "$tmp/sandbox" "$tmp/shim"
must cp -r "$ROOT/scripts" "$tmp/sandbox/scripts"
cat >"$tmp/shim/go" <<'G'
#!/usr/bin/env bash
cat "$FAKE_GO_OUTPUT"
exit "${FAKE_GO_STATUS:-0}"
G
chmod +x "$tmp/shim/go"
run_bench_sh() { PATH="$tmp/shim:$PATH" FAKE_GO_OUTPUT="$clean" BENCH_COUNT=3 "$tmp/sandbox/scripts/bench.sh" sqlite; }
expect "bench.sh passes a clean run" 0 "sqlite: PASS" -- run_bench_sh
run_bench_sh_go_fails() { FAKE_GO_STATUS=1 run_bench_sh; }
expect "bench.sh fails when go test fails even though the output validates" 1 "sqlite: FAIL" -- run_bench_sh_go_fails

# A skipped backend (go test of the probe fails) must not leave an old output file
# reported as a pass.
mkdir -p "$tmp/sandbox/bench-results"
echo "stale output from an earlier run" >"$tmp/sandbox/bench-results/postgres.txt"
run_bench_sh_postgres_unreachable() { PATH="$tmp/shim:$PATH" FAKE_GO_OUTPUT="$clean" FAKE_GO_STATUS=1 BENCH_COUNT=3 "$tmp/sandbox/scripts/bench.sh" postgres; }
expect "bench.sh reports an unreachable backend as skipped, not PASS" 0 "postgres: skipped" -- run_bench_sh_postgres_unreachable
if [ -e "$tmp/sandbox/bench-results/postgres.txt" ]; then fail "a stale output file survived a skipped backend"; else pass "the stale output file was removed"; fi

# BENCH_COUNT is validated before any benchmark runs.
run_bench_sh_bad_count() { PATH="$tmp/shim:$PATH" FAKE_GO_OUTPUT="$clean" BENCH_COUNT=0 "$tmp/sandbox/scripts/bench.sh" sqlite; }
expect "bench.sh rejects BENCH_COUNT=0 before running anything" 2 "BENCH_COUNT must be a positive integer" -- run_bench_sh_bad_count
for bad in abc -1 1.5; do
  run_bench_sh_bad_count_value() { PATH="$tmp/shim:$PATH" FAKE_GO_OUTPUT="$clean" BENCH_COUNT="$bad" "$tmp/sandbox/scripts/bench.sh" sqlite; }
  expect "bench.sh rejects BENCH_COUNT=$bad" 2 "BENCH_COUNT must be a positive integer" -- run_bench_sh_bad_count_value
done
run_bench_sh_with_timeout() { PATH="$tmp/shim:$PATH" FAKE_GO_OUTPUT="$clean" BENCH_COUNT=3 BENCH_TIMEOUT="$1" "$tmp/sandbox/scripts/bench.sh" sqlite; }
expect "bench.sh rejects BENCH_TIMEOUT without a unit" 2 "BENCH_TIMEOUT must be a Go duration" -- run_bench_sh_with_timeout 25
expect "bench.sh treats an empty BENCH_TIMEOUT as unset and uses the default" 0 "timeout=25m" -- run_bench_sh_with_timeout ""
expect "bench.sh accepts BENCH_TIMEOUT=90s" 0 "sqlite: PASS" -- run_bench_sh_with_timeout 90s
expect "bench.sh accepts a compound BENCH_TIMEOUT=1h30m" 0 "timeout=1h30m" -- run_bench_sh_with_timeout 1h30m
expect "bench.sh rejects a duration with trailing text" 2 "BENCH_TIMEOUT must be a Go duration" -- run_bench_sh_with_timeout 25mins

# ---- bench-compare.sh ----
cat >"$tmp/fake-benchstat" <<'S'
#!/usr/bin/env bash
echo "FAKE BENCHSTAT $(basename "$1") $(basename "$2")"
grep -h '^BenchmarkEnqueue[ -]' "$1" "$2" | head -2 | awk '{ print "NAME=" $1 }'
[ -z "${FAKE_BENCHSTAT_FAIL:-}" ] || exit "$FAKE_BENCHSTAT_FAIL"
S
chmod +x "$tmp/fake-benchstat"
export BENCHSTAT="$tmp/fake-benchstat"

expect "no baseline is a loud skip" 0 "no baseline" -- "$COMPARE" "$tmp/missing-baseline.txt" "$clean"

must sed 's/^cpu: .*/cpu: Some Other CPU/' "$clean" >"$tmp/other-cpu.txt"
expect "a baseline from another cpu is a loud skip" 0 "different cpu" -- "$COMPARE" "$tmp/other-cpu.txt" "$clean"
must grep -v '^cpu:' "$clean" >"$tmp/no-cpu.txt"
expect "files with no cpu line are a loud skip, not a match" 0 "different cpu" -- "$COMPARE" "$tmp/no-cpu.txt" "$tmp/no-cpu.txt"

expect "same cpu runs benchstat on the stripped files" 0 "NAME=BenchmarkEnqueue$" -- "$COMPARE" "$tmp/four-core.txt" "$clean"
out="$("$COMPARE" "$tmp/four-core.txt" "$clean" 2>&1)"
names="$(grep -c '^NAME=BenchmarkEnqueue$' <<<"$out" || true)"
if [ "$names" -ge 2 ] && ! grep -q 'NAME=BenchmarkEnqueue-' <<<"$out"; then
  pass "suffix stripped on both sides (padded names included)"
else
  fail "suffix was not stripped on both sides: $out"
fi

compare_with_failing_benchstat() { FAKE_BENCHSTAT_FAIL=3 "$COMPARE" "$tmp/four-core.txt" "$clean"; }
expect "a benchstat failure propagates" 3 "FAKE BENCHSTAT" -- compare_with_failing_benchstat
unset BENCHSTAT
expect "usage error" 2 "usage" -- "$COMPARE"

if [ "$failures" -ne 0 ]; then
  echo "bench-scripts_test: $failures failure(s)" >&2
  exit 1
fi
echo "bench-scripts_test: all passed"
