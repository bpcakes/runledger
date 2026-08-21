#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTRACT_PATH="$ROOT_DIR/runledger-admin-web/openapi.json"
TEMP_DIR="$(mktemp -d)"
GENERATED_PATH="$TEMP_DIR/openapi.json"

cleanup() {
  rm -rf -- "$TEMP_DIR"
}

trap cleanup EXIT

"$ROOT_DIR/scripts/generate-admin-openapi.sh" "$GENERATED_PATH"

file_mode() {
  if stat -c '%a' "$1" >/dev/null 2>&1; then
    stat -c '%a' "$1"
  else
    stat -f '%Lp' "$1"
  fi
}

for path in "$CONTRACT_PATH" "$GENERATED_PATH"; do
  mode="$(file_mode "$path")"
  if [[ "$mode" != "644" ]]; then
    echo "error: ${path} must have mode 0644, found ${mode}" >&2
    exit 1
  fi
done

if ! cmp -s "$CONTRACT_PATH" "$GENERATED_PATH"; then
  diff -u "$CONTRACT_PATH" "$GENERATED_PATH" || true
  echo "error: runledger-admin-web/openapi.json is stale; run ./scripts/generate-admin-openapi.sh" >&2
  exit 1
fi
