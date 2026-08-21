#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONTRACT_REPO_PATH="runledger-admin-web/openapi.json"
CONTRACT_PATH="$ROOT_DIR/$CONTRACT_REPO_PATH"
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

contract_index_mode="$(git -C "$ROOT_DIR" ls-files --stage -- "$CONTRACT_REPO_PATH" | awk 'NR == 1 { print $1 }')"
if [[ "$contract_index_mode" != "100644" ]]; then
  echo "error: ${CONTRACT_REPO_PATH} must be a non-executable regular file in Git, found ${contract_index_mode:-untracked}" >&2
  exit 1
fi

generated_mode="$(file_mode "$GENERATED_PATH")"
if [[ "$generated_mode" != "644" ]]; then
  echo "error: ${GENERATED_PATH} must have mode 0644, found ${generated_mode}" >&2
  exit 1
fi

if ! cmp -s "$CONTRACT_PATH" "$GENERATED_PATH"; then
  diff -u "$CONTRACT_PATH" "$GENERATED_PATH" || true
  echo "error: runledger-admin-web/openapi.json is stale; run ./scripts/generate-admin-openapi.sh" >&2
  exit 1
fi
