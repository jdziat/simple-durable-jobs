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

cp "$clean" "$tmp/fail-line.txt"
printf -- '--- FAIL: BenchmarkEndToEnd\n    bench_test.go:221: boom\n' >>"$tmp/fail-line.txt"
expect "a FAIL line fails" 1 "failure line" -- "$VALIDATE" "$tmp/fail-line.txt" sqlite 3

grep -v '^BenchmarkDispatchLatency' "$clean" >"$tmp/no-dispatch.txt"
expect "a missing benchmark fails" 1 "DispatchLatency: expected 3" -- "$VALIDATE" "$tmp/no-dispatch.txt" sqlite 3

cp "$clean" "$tmp/stray.txt"
echo "2026/11/14 11:00:00 INFO retention GC enabled" >>"$tmp/stray.txt"
expect "a stray log line fails" 1 "unexpected line" -- "$VALIDATE" "$tmp/stray.txt" sqlite 3

sed 's/-192 /-4 /' "$clean" >"$tmp/four-core.txt"
expect "a different GOMAXPROCS suffix still validates" 0 "ok (sqlite" -- "$VALIDATE" "$tmp/four-core.txt" sqlite 3

expect "postgres requires the churn benchmarks" 1 "SaturatedRateLimitChurn: expected 3" -- "$VALIDATE" "$clean" postgres 3
cp "$clean" "$tmp/pg.txt"
for name in SaturatedRateLimitChurn SaturatedKeyedRateLimitChurn; do
  for _ in 1 2 3; do
    printf 'Benchmark%s-192  \t       1\t2000807457 ns/op\t        15.00 completed/s\t 8213576 B/op\t   93591 allocs/op\n' "$name" >>"$tmp/pg.txt"
  done
done
printf -- '--- BENCH: BenchmarkSaturatedRateLimitChurn-192\n    ratelimit_churn_bench_test.go:104: cto-F2 churn soak [unkeyed]: completed=30\n' >>"$tmp/pg.txt"
expect "postgres with churn results and a b.Logf block passes" 0 "ok (postgres" -- "$VALIDATE" "$tmp/pg.txt" postgres 3

# ---- bench-compare.sh ----
cat >"$tmp/fake-benchstat" <<'S'
#!/usr/bin/env bash
echo "FAKE BENCHSTAT $(basename "$1") $(basename "$2")"
grep -h '^BenchmarkEnqueue ' "$1" "$2" | head -2 | cut -c1-30
[ -z "${FAKE_BENCHSTAT_FAIL:-}" ] || exit "$FAKE_BENCHSTAT_FAIL"
S
chmod +x "$tmp/fake-benchstat"
export BENCHSTAT="$tmp/fake-benchstat"

expect "no baseline is a loud skip" 0 "no baseline" -- "$COMPARE" "$tmp/missing-baseline.txt" "$clean"

sed 's/^cpu: .*/cpu: Some Other CPU/' "$clean" >"$tmp/other-cpu.txt"
expect "a baseline from another cpu is a loud skip" 0 "different cpu" -- "$COMPARE" "$tmp/other-cpu.txt" "$clean"

expect "same cpu runs benchstat with suffixes stripped" 0 "BenchmarkEnqueue " -- "$COMPARE" "$tmp/four-core.txt" "$clean"
out="$("$COMPARE" "$tmp/four-core.txt" "$clean" 2>&1)"
if grep -q 'BenchmarkEnqueue-' <<<"$out"; then fail "suffix was not stripped: $out"; else pass "suffix stripped on both sides"; fi

FAKE_BENCHSTAT_FAIL=3 expect "a benchstat failure propagates" 3 "FAKE BENCHSTAT" -- "$COMPARE" "$tmp/four-core.txt" "$clean"
unset BENCHSTAT
expect "usage error" 2 "usage" -- "$COMPARE"

if [ "$failures" -ne 0 ]; then
  echo "bench-scripts_test: $failures failure(s)" >&2
  exit 1
fi
echo "bench-scripts_test: all passed"
