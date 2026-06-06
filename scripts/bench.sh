#!/usr/bin/env bash
# bench.sh - run reproducible benchmarks against supported storage backends.
#
# Usage:
#   scripts/bench.sh
#   scripts/bench.sh sqlite postgres
#
# Env vars:
#   BENCH_COUNT=3
#   TEST_DATABASE_URL=postgres://jobs:jobs@localhost:15433/jobs_test?sslmode=disable
#   TEST_MYSQL_URL=root:jobs@tcp(localhost:13307)/jobs_test?parseTime=true

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ $# -eq 0 ]; then
  BACKENDS=(sqlite postgres mysql)
else
  BACKENDS=("$@")
fi

# EndToEnd benchmarks run about 50s/sample; 3 samples balances stability and
# runtime, benchstat handles n=3, and CI benchmark comparison is warn-only.
: "${BENCH_COUNT:=3}"
: "${TEST_DATABASE_URL:=postgres://jobs:jobs@localhost:15433/jobs_test?sslmode=disable}"
: "${TEST_MYSQL_URL:=root:jobs@tcp(localhost:13307)/jobs_test?parseTime=true}"
mkdir -p bench-results

color() { printf '\033[%sm%s\033[0m\n' "$1" "$2"; }
info()  { color '1;36' "==> $*"; }
warn()  { color '1;33' "!!  $*"; }
err()   { color '1;31' "!!  $*"; }

can_postgres() {
  # shellcheck disable=SC1007 # the empty assignment intentionally blanks the sibling DSN for backend isolation
  TEST_MYSQL_URL= TEST_DATABASE_URL="$TEST_DATABASE_URL" go test ./benchmarks/ -run '^$' -bench '^BenchmarkEnqueue$' -benchtime=1x -count=1 >/dev/null 2>&1
}

can_mysql() {
  # shellcheck disable=SC1007 # the empty assignment intentionally blanks the sibling DSN for backend isolation
  TEST_DATABASE_URL= TEST_MYSQL_URL="$TEST_MYSQL_URL" go test ./benchmarks/ -run '^$' -bench '^BenchmarkEnqueue$' -benchtime=1x -count=1 >/dev/null 2>&1
}

run_backend() {
  local backend="$1"
  local output="bench-results/${backend}.txt"
  local -a env_args=()

  case "$backend" in
    sqlite)
      env_args=("TEST_DATABASE_URL=" "TEST_MYSQL_URL=")
      ;;
    postgres)
      if ! can_postgres; then
        warn "[postgres] not reachable at TEST_DATABASE_URL; skipping"
        return 0
      fi
      env_args=("TEST_MYSQL_URL=" "TEST_DATABASE_URL=$TEST_DATABASE_URL")
      ;;
    mysql)
      if ! can_mysql; then
        warn "[mysql] not reachable at TEST_MYSQL_URL; skipping"
        return 0
      fi
      env_args=("TEST_DATABASE_URL=" "TEST_MYSQL_URL=$TEST_MYSQL_URL")
      ;;
    *)
      err "unknown backend: $backend (expected: sqlite|postgres|mysql)"
      return 2
      ;;
  esac

  info "[$backend] go test -bench=. -benchmem -count=${BENCH_COUNT} ./benchmarks/"
  env "${env_args[@]}" go test -run '^$' -bench=. -benchmem -count="$BENCH_COUNT" ./benchmarks/ | tee "$output"
}

declare -A results
for backend in "${BACKENDS[@]}"; do
  if run_backend "$backend"; then
    results[$backend]=pass
  else
    results[$backend]=fail
  fi
done

echo
info "Summary"
exit_code=0
for backend in "${BACKENDS[@]}"; do
  status="${results[$backend]:-skipped}"
  case "$status" in
    pass)
      if [ -f "bench-results/${backend}.txt" ]; then
        color '1;32' "  $backend: PASS -> bench-results/${backend}.txt"
        awk '/^Benchmark/ {print "    " $1, $3, $4, $5, $6}' "bench-results/${backend}.txt" | tail -n 6
      else
        color '1;33' "  $backend: skipped"
      fi
      ;;
    fail) color '1;31' "  $backend: FAIL"; exit_code=1 ;;
    *)    color '1;33' "  $backend: $status" ;;
  esac
done
exit "$exit_code"
