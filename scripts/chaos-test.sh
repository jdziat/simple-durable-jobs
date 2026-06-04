#!/usr/bin/env bash
# chaos-test.sh — run the multi-worker crash/recovery chaos harness against a
# real database, killing workers at random while a workload drains, then assert
# the durability invariants (exactly-once effects, no wedged jobs, fan-out count
# integrity, unique-key dedup, single-scheduler firing).
#
# Usage:
#   scripts/chaos-test.sh [postgres|mysql]   (default: postgres)
set -uo pipefail

backend="${1:-postgres}"
case "$backend" in
  postgres)
    overlay="docker-compose.chaos.yml"
    db="postgres"
    ;;
  mysql)
    overlay="docker-compose.chaos-mysql.yml"
    db="mysql"
    ;;
  *)
    echo "usage: chaos-test.sh [postgres|mysql]" >&2
    exit 2
    ;;
esac

project="${COMPOSE_PROJECT_NAME:-sdj-chaos-$backend}"
compose=(docker compose -p "$project" -f docker-compose.yml -f "$overlay")

cleanup() {
  "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT

echo "==> [$backend] building chaostest image"
if ! "${compose[@]}" build worker; then
  echo "build failed"
  exit 2
fi

echo "==> [$backend] starting $db"
if ! "${compose[@]}" up -d --wait "$db"; then
  echo "$db failed to become healthy"
  exit 2
fi

echo "==> [$backend] starting 4 workers"
if ! "${compose[@]}" up -d --scale worker=4 worker; then
  echo "worker startup failed"
  exit 2
fi

echo "==> [$backend] seeding workload"
if ! "${compose[@]}" run --rm worker seed; then
  echo "seed failed"
  exit 2
fi

echo "==> [$backend] running chaos loop (${CHAOS_DURATION:-60}s)"
end=$((SECONDS + ${CHAOS_DURATION:-60}))
while (( SECONDS < end )); do
  mapfile -t workers < <("${compose[@]}" ps -q worker 2>/dev/null || true)
  if ((${#workers[@]} > 0)); then
    victim="${workers[$((RANDOM % ${#workers[@]}))]}"
    echo "killing worker $victim"
    docker kill -s KILL "$victim" >/dev/null 2>&1 || true
  fi
  "${compose[@]}" up -d --scale worker=4 worker >/dev/null 2>&1 || true
  sleep 4
done

echo "==> [$backend] checking invariants"
"${compose[@]}" run --rm worker check
check_code=$?

echo "==> [$backend] tearing down"
cleanup
trap - EXIT
exit "$check_code"
