#!/usr/bin/env bash
# check-go-pins.sh - fail when a workflow pins a Go version that differs from the
# `toolchain` directive in go.mod.
#
# CI pins Go exactly so setup-go installs it directly (no on-the-fly toolchain
# download) and every job runs the same patched standard library. A pin left
# behind after a go.mod bump silently tests an older stdlib; this catches it.
#
# Usage: scripts/check-go-pins.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

want="$(awk '$1 == "toolchain" { sub(/^go/, "", $2); print $2 }' go.mod)"
if [ -z "$want" ]; then
  echo "check-go-pins: no toolchain directive in go.mod" >&2
  exit 2
fi
# A value that is not a plain version would match no pin and fail every workflow
# with a misleading message; say what is wrong instead.
if ! [[ "$want" =~ ^[0-9]+\.[0-9]+(\.[0-9]+)?$ ]]; then
  echo "check-go-pins: cannot parse the go.mod toolchain version: '$want'" >&2
  exit 2
fi

bad=0
seen=0
# The file name comes from the outer loop, never from splitting grep's output, so a
# colon anywhere in a path cannot shift the fields. Anchored on the key so
# `hugo-version:` and `go-version-file:` do not match.
while IFS= read -r -d '' file; do
  while IFS=: read -r line text; do
    seen=$((seen + 1))
    # -n with the p flag prints only a line the pattern matched; a pin of any other
    # form (an expression, a floating version) yields nothing and is reported as
    # unparseable.
    got="$(sed -nE "s/^[[:space:]]*(-[[:space:]]*)?go-version:[[:space:]]*['\"]?([0-9][0-9.]*)['\"]?[[:space:]]*(#.*)?$/\2/p" <<<"$text")"
    if [ -z "$got" ]; then
      echo "check-go-pins: $file:$line has a go-version this script cannot read: $text" >&2
      bad=1
    elif [ "$got" != "$want" ]; then
      echo "check-go-pins: $file:$line pins Go $got, go.mod toolchain is $want" >&2
      bad=1
    fi
  done < <(grep -nE '^[[:space:]]*(-[[:space:]]*)?go-version:' "$file" || true)
done < <(find .github -type f \( -name '*.yml' -o -name '*.yaml' \) -print0)

if [ "$seen" -eq 0 ]; then
  echo "check-go-pins: found no go-version pins to check" >&2
  exit 2
fi
if [ "$bad" -ne 0 ]; then
  exit 1
fi
echo "check-go-pins: $seen pin(s), all Go $want"
