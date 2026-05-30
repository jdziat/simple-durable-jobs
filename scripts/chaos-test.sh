#!/usr/bin/env bash
set -uo pipefail

project="${COMPOSE_PROJECT_NAME:-sdj-chaos}"
compose=(docker compose -p "$project" -f docker-compose.yml -f docker-compose.chaos.yml)

cleanup() {
  "${compose[@]}" down -v --remove-orphans
}
trap cleanup EXIT

echo "==> building chaostest image"
if ! "${compose[@]}" build worker; then
  echo "build failed"
  exit 2
fi

echo "==> starting postgres"
if ! "${compose[@]}" up -d --wait postgres; then
  echo "postgres failed to become healthy"
  exit 2
fi

echo "==> starting 4 workers"
if ! "${compose[@]}" up -d --scale worker=4 worker; then
  echo "worker startup failed"
  exit 2
fi

echo "==> seeding workload"
if ! "${compose[@]}" run --rm worker seed; then
  echo "seed failed"
  exit 2
fi

echo "==> running chaos loop"
end=$((SECONDS + 60))
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

echo "==> checking invariants"
"${compose[@]}" run --rm worker check
check_code=$?

echo "==> tearing down"
cleanup
trap - EXIT
exit "$check_code"
