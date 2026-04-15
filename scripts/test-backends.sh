#!/usr/bin/env bash
# test-backends.sh — run the Go test suite against each supported storage
# backend. Postgres and MySQL run in docker-compose-managed containers;
# SQLite is in-process.
#
# Usage:
#   scripts/test-backends.sh                       # all three
#   scripts/test-backends.sh sqlite                # just SQLite
#   scripts/test-backends.sh postgres mysql        # only the external DBs
#
# Env vars:
#   KEEP_CONTAINERS=1   leave containers running after the run
#   GO_TEST_FLAGS=...   override the flags passed to `go test`
#                       (default: -race -p=1)
#   PKGS=...            override the package list
#                       (default: all except examples/, ui/cmd/, ui/gen/)

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [ $# -eq 0 ]; then
  BACKENDS=(sqlite postgres mysql)
else
  BACKENDS=("$@")
fi

: "${GO_TEST_FLAGS:=-race -p=1}"
if [ -z "${PKGS:-}" ]; then
  PKGS=$(go list ./... | grep -v /examples/ | grep -v /ui/cmd/ | grep -v /ui/gen/)
fi

COMPOSE_PROJECT="sdj-test"
: "${POSTGRES_HOST_PORT:=15432}"
: "${MYSQL_HOST_PORT:=13306}"
export POSTGRES_HOST_PORT MYSQL_HOST_PORT
POSTGRES_DSN="postgres://jobs:jobs@localhost:${POSTGRES_HOST_PORT}/jobs_test?sslmode=disable"
MYSQL_DSN="root:jobs@tcp(localhost:${MYSQL_HOST_PORT})/jobs_test?parseTime=true"

color() { printf '\033[%sm%s\033[0m\n' "$1" "$2"; }
info()  { color '1;36' "==> $*"; }
err()   { color '1;31' "!!  $*"; }

# Which external backends are in this run?
need_compose=()
for b in "${BACKENDS[@]}"; do
  case "$b" in
    postgres) need_compose+=(postgres) ;;
    mysql)    need_compose+=(mysql) ;;
    sqlite)   ;;
    *) err "unknown backend: $b (expected: sqlite|postgres|mysql)"; exit 2 ;;
  esac
done

# shellcheck disable=SC2329  # invoked via trap
cleanup() {
  if [ ${#need_compose[@]} -gt 0 ] && [ -z "${KEEP_CONTAINERS:-}" ]; then
    info "Tearing down containers"
    docker compose -p "$COMPOSE_PROJECT" down -v >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [ ${#need_compose[@]} -gt 0 ]; then
  if ! command -v docker >/dev/null; then
    err "docker is required for postgres/mysql backends"
    exit 2
  fi
  info "Starting containers: ${need_compose[*]}"
  docker compose -p "$COMPOSE_PROJECT" up -d --wait "${need_compose[@]}"
fi

run_suite() {
  local name="$1"; shift
  info "[$name] go test ${GO_TEST_FLAGS}"
  # shellcheck disable=SC2086
  env "$@" go test $GO_TEST_FLAGS $PKGS
}

declare -A results
for b in "${BACKENDS[@]}"; do
  case "$b" in
    sqlite)
      if run_suite sqlite; then
        results[$b]=pass
      else
        results[$b]=fail
      fi
      ;;
    postgres)
      if run_suite postgres "TEST_DATABASE_URL=$POSTGRES_DSN"; then
        results[$b]=pass
      else
        results[$b]=fail
      fi
      ;;
    mysql)
      if run_suite mysql "TEST_MYSQL_URL=$MYSQL_DSN"; then
        results[$b]=pass
      else
        results[$b]=fail
      fi
      ;;
  esac
done

echo
info "Summary"
exit_code=0
for b in "${BACKENDS[@]}"; do
  status="${results[$b]:-skipped}"
  case "$status" in
    pass) color '1;32' "  $b: PASS" ;;
    fail) color '1;31' "  $b: FAIL"; exit_code=1 ;;
    *)    color '1;33' "  $b: $status" ;;
  esac
done
exit "$exit_code"
