#!/usr/bin/env bash
# Cross-version upgrade matrix for the scheduled-fire cursor.
#
# WHY THIS EXISTS: two consecutive review rounds caught fixes that were correct
# against a FRESH database and silently broke deployments whose rows an older
# binary had already written. Every ordinary test starts from an empty database,
# so none of them could see it. This builds a real released binary, lets it
# populate a SQLite file, then points the working tree's binary at that same file
# and asks the only question that matters: do the schedules still fire?
#
# It matters because existing databases hold a MIXTURE of clock faces —
# Daily/Weekly pin UTC, while Every and Cron carry the host's local offset (they
# derive from time.Now() and their Next() preserves that location). Forcing either
# face at write time stalls the other family; the shipped fix instead compares
# INSTANTS. Run under a positive-offset zone, a negative one, and UTC.
#
# Usage: scripts/upgrade-matrix.sh [baseline-tag]   (default: the latest v4 tag)
set -euo pipefail

BASE_TAG="${1:-$(git tag --list 'v4.*' --sort=-v:refname | head -1)}"
if [ -z "$BASE_TAG" ]; then
  # A default GitHub checkout is shallow and carries NO tags, so this would
  # otherwise fail deep inside git-archive with something unrecognisable. The
  # workflow sets fetch-depth: 0 for exactly this reason.
  echo "no v4.* tag found — this needs full history (fetch-depth: 0), or pass a tag explicitly" >&2
  exit 1
fi
ROOT="$(git rev-parse --show-toplevel)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "baseline: $BASE_TAG    working tree: $(git -C "$ROOT" rev-parse --short HEAD)"

mkdir -p "$WORK/base"
git -C "$ROOT" archive "$BASE_TAG" | tar -x -C "$WORK/base"
# The probe is copied INTO the baseline tree so both binaries run identical probe
# code against their own library version; HEAD builds it from the working tree.
cp -r "$ROOT/scripts/upgradematrix" "$WORK/base/upgradematrix"

( cd "$WORK/base" && go build -o "$WORK/seed" ./upgradematrix/ )
( cd "$ROOT" && go build -o "$WORK/head" ./scripts/upgradematrix/ )

fail=0
for tz in UTC Asia/Tokyo Europe/Berlin America/Los_Angeles; do
  db="$WORK/$(echo "$tz" | tr / _).db"
  echo "######## TZ=$tz ########"
  TZ="$tz" "$WORK/seed" "$db" seed | sed 's/^/  [baseline] /'
  if ! TZ="$tz" "$WORK/head" "$db" check | sed 's/^/  [HEAD]     /'; then
    fail=1
  fi
done

if [ "$fail" -ne 0 ]; then
  echo "UPGRADE MATRIX FAILED: at least one schedule stalled after the upgrade"
  exit 1
fi
echo "upgrade matrix OK: every schedule family claimed its next boundary in every zone"
