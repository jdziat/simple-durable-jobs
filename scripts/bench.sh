#!/usr/bin/env bash
# bench.sh - run reproducible benchmarks against supported storage backends.
#
# Usage:
#   scripts/bench.sh
#   scripts/bench.sh sqlite postgres
#
# Env vars:
#   BENCH_COUNT=6
#   BENCH_TIMEOUT=25m
#   TEST_DATABASE_URL=postgres://jobs:jobs@localhost:15433/jobs_test?sslmode=disable
#   TEST_MYSQL_URL=root:jobs@tcp(localhost:13307)/jobs_test?parseTime=true

set -euo pipefail

# run_backend relies on pipefail (set above) to carry go test's status through tee, so
# the script must run in its own shell; sourcing it into one without pipefail would
# let tee's exit 0 hide a failed run.
if [ "${BASH_SOURCE[0]}" != "$0" ]; then
  echo "bench.sh must be executed, not sourced" >&2
  # shellcheck disable=SC2317 # return works when sourced, exit when executed via a wrapper
  return 2 2>/dev/null || exit 2
fi

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ $# -eq 0 ]; then
  BACKENDS=(sqlite postgres mysql)
else
  BACKENDS=("$@")
fi

# Six samples give benchstat enough for a confidence interval (it prints "± ∞" below
# six). The whole SQLite suite takes about 23s per sample on a 192-core host; the CI
# runner is slower, so BENCH_TIMEOUT bounds the run explicitly instead of relying on
# go test's 10m per-binary default. Measure the CI wall time before changing either.
: "${BENCH_COUNT:=6}"
: "${BENCH_TIMEOUT:=25m}"
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

  # The `|| return 1` below depends on pipefail to carry go test's status through tee.
  # It is set at the top of the script; setting it here keeps the dependency local.
  set -o pipefail

  # A file left by an earlier run must not be mistaken for this one: a backend that
  # is skipped below writes nothing, and the summary reports PASS for any file found.
  rm -f "$output"

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

  info "[$backend] go test -bench=. -benchmem -count=${BENCH_COUNT} -timeout=${BENCH_TIMEOUT} ./benchmarks/"
  # `|| return 1` matters: run_backend is called from an `if`, where `set -e` is off,
  # so without it the status of the last command in the function would replace a
  # failed `go test` (pipefail makes the pipeline carry go test's status).
  env "${env_args[@]}" go test -run '^$' -bench=. -benchmem -count="$BENCH_COUNT" -timeout="$BENCH_TIMEOUT" ./benchmarks/ | tee "$output" || return 1
  # A run can exit 0 and still be unusable (a partial or corrupted file); check the
  # file itself before it is uploaded or compared.
  "$ROOT/scripts/validate-bench-output.sh" "$output" "$backend" "$BENCH_COUNT" || return 1
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
