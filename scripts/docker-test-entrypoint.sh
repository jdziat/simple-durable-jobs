#!/usr/bin/env bash
# docker-test-entrypoint.sh — run the Go test suite inside the test container
# against one or more storage backends.
#
# Usage (as the container command):
#   sqlite | postgres | mysql | all   (default: sqlite)
#
# Postgres/MySQL DSNs target the docker-compose service names on the shared
# network (postgres:5432, mysql:3306) — NOT the host-published ports. The DB
# containers are managed by the outer `docker compose`, not by this script.
#
# Env:
#   GO_TEST_FLAGS   flags passed to `go test` (default: -short -race -p=1)
#   PKGS            package list (default: all except examples/, ui/cmd/, ui/gen/)
set -uo pipefail

: "${GO_TEST_FLAGS:=-short -race -p=1}"
if [ -z "${PKGS:-}" ]; then
  PKGS=$(go list ./... | grep -v /examples/ | grep -v /ui/cmd/ | grep -v /ui/gen/)
fi

mkdir -p coverage

requested=("$@")
[ ${#requested[@]} -eq 0 ] && requested=(sqlite)
[ "${requested[0]}" = "all" ] && requested=(sqlite postgres mysql)

rc=0
for backend in "${requested[@]}"; do
  cover="coverage/coverage-${backend}.out"
  case "$backend" in
    sqlite)
      unset TEST_DATABASE_URL TEST_MYSQL_URL
      ;;
    postgres)
      export TEST_DATABASE_URL="postgres://jobs:jobs@postgres:5432/jobs_test?sslmode=disable"
      unset TEST_MYSQL_URL
      ;;
    mysql)
      export TEST_MYSQL_URL="root:jobs@tcp(mysql:3306)/jobs_test?parseTime=true"
      unset TEST_DATABASE_URL
      ;;
    *)
      echo "unknown backend: $backend (expected sqlite|postgres|mysql|all)" >&2
      exit 2
      ;;
  esac

  echo "==> [$backend] go test ${GO_TEST_FLAGS} (coverage: ${cover})"
  # shellcheck disable=SC2086
  if ! go test ${GO_TEST_FLAGS} -coverprofile="${cover}" ${PKGS}; then
    echo "!!  [$backend] FAILED" >&2
    rc=1
  fi
done

exit "$rc"
