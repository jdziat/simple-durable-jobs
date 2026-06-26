#!/usr/bin/env bash
# torture-test.sh — drive THOUSANDS of complex durable jobs locally via docker
# compose and assert the durability invariants. Reuses the chaostest harness
# (cmd/chaostest) — its complex job types (Call/phase pipelines, nested fan-out,
# signals, durable timers, scheduled ticks, the chaos.megaflow mega-workflow),
# its exactly-once `chaos_effects` ledger, and its 12 invariant checks.
#
# Two modes:
#   clean (default) — drain a large workload with a stable worker fleet, then
#                     assert invariants and report throughput.
#   --chaos         — additionally SIGKILL workers at random while the workload
#                     drains (durability under crash + load).
#
# Usage:
#   scripts/torture-test.sh [postgres|mysql] [--chaos]
#
# Env knobs (all optional; defaults size a heavy ~20-minute run):
#   CHAOS_SCALE           seed multiplier over the baseline counts   (default 50)
#   CHAOS_FANOUT_WIDTH    sub-jobs per chaos.megaflow fan-out         (default 5)
#   TORTURE_WORKERS       number of worker containers                 (default 8)
#   TORTURE_DURATION      --chaos kill-loop seconds                   (default 1200)
#   CHAOS_DRAIN_TIMEOUT   max drain wait in the check phase           (default 1800s)
#   CHAOS_SEED_*          per-type seed overrides (UNIT, PIPELINE, FANOUT,
#                         MEGAFLOW, SLOW, TIMER, SIGNAL_WAITERS, …); win over CHAOS_SCALE
#   KEEP_CONTAINERS=1     leave containers running after the run
#
# Fast smoke (correctness, ~1 min):
#   CHAOS_SCALE=0.1 TORTURE_WORKERS=4 CHAOS_DRAIN_TIMEOUT=120s scripts/torture-test.sh postgres
set -uo pipefail

backend="postgres"
chaos=0
for arg in "$@"; do
  case "$arg" in
    postgres|mysql) backend="$arg" ;;
    --chaos)        chaos=1 ;;
    -h|--help)      sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "usage: torture-test.sh [postgres|mysql] [--chaos]" >&2; exit 2 ;;
  esac
done

case "$backend" in
  postgres) overlay="docker-compose.chaos.yml";       db="postgres" ;;
  mysql)    overlay="docker-compose.chaos-mysql.yml"; db="mysql" ;;
esac

# Torture defaults. Exported so docker-compose interpolation propagates them to
# both the long-running `worker` containers and the one-shot seed/check runs.
export CHAOS_SCALE="${CHAOS_SCALE:-50}"
export CHAOS_FANOUT_WIDTH="${CHAOS_FANOUT_WIDTH:-5}"
export CHAOS_DRAIN_TIMEOUT="${CHAOS_DRAIN_TIMEOUT:-1800s}"
workers="${TORTURE_WORKERS:-8}"
duration="${TORTURE_DURATION:-1200}"

project="${COMPOSE_PROJECT_NAME:-sdj-torture-$backend}"
compose=(docker compose -p "$project" -f docker-compose.yml -f "$overlay")

cleanup() {
  if [ -z "${KEEP_CONTAINERS:-}" ]; then
    "${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

mode="clean"; ((chaos)) && mode="chaos"
echo "==> [$backend] torture mode=$mode scale=$CHAOS_SCALE workers=$workers fanout_width=$CHAOS_FANOUT_WIDTH drain_timeout=$CHAOS_DRAIN_TIMEOUT"

echo "==> [$backend] building chaostest image"
"${compose[@]}" build worker || { echo "build failed"; exit 2; }

echo "==> [$backend] starting $db"
"${compose[@]}" up -d --wait "$db" || { echo "$db failed to become healthy"; exit 2; }

echo "==> [$backend] starting $workers workers"
"${compose[@]}" up -d --scale worker="$workers" worker || { echo "worker startup failed"; exit 2; }

echo "==> [$backend] seeding workload (scale=$CHAOS_SCALE)"
start=$SECONDS
seed_out="$("${compose[@]}" run --rm worker seed)" || { echo "seed failed"; echo "$seed_out"; exit 2; }
echo "$seed_out"
roots="$(grep -oE 'seeded_roots=[0-9]+' <<<"$seed_out" | head -1 | cut -d= -f2)"
roots="${roots:-0}"

if ((chaos)); then
  echo "==> [$backend] chaos kill-loop for ${duration}s while draining"
  end=$((SECONDS + duration))
  while ((SECONDS < end)); do
    mapfile -t running < <("${compose[@]}" ps -q worker 2>/dev/null || true)
    if ((${#running[@]} > 0)); then
      victim="${running[$((RANDOM % ${#running[@]}))]}"
      docker kill -s KILL "$victim" >/dev/null 2>&1 || true
    fi
    "${compose[@]}" up -d --scale worker="$workers" worker >/dev/null 2>&1 || true
    sleep 4
  done
fi

echo "==> [$backend] waiting for drain + checking invariants"
"${compose[@]}" run --rm worker check
check_code=$?
elapsed=$((SECONDS - start))

echo "==> [$backend] torture summary"
echo "    mode=$mode backend=$backend scale=$CHAOS_SCALE workers=$workers"
echo "    seeded_roots=$roots wall_clock=${elapsed}s"
if ((roots > 0 && elapsed > 0)); then
  awk -v r="$roots" -v e="$elapsed" 'BEGIN{printf "    throughput=%.1f root-jobs/sec (fan-out sub-jobs not counted; true execution count is higher)\n", r/e}'
fi

cleanup
trap - EXIT
exit "$check_code"
