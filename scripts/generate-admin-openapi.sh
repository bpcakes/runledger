#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUTPUT_OVERRIDDEN=false
if [[ $# -gt 1 ]]; then
  echo "usage: $0 [output-path]" >&2
  exit 2
fi
if [[ $# -eq 1 ]]; then
  OUTPUT_OVERRIDDEN=true
fi
OUTPUT_PATH="${1:-$ROOT_DIR/runledger-admin-web/openapi.json}"
OUTPUT_DIR="$(dirname "$OUTPUT_PATH")"
TEMP_FILE="$(mktemp "$OUTPUT_DIR/.runledger-openapi.XXXXXX")"

cleanup() {
  rm -f -- "$TEMP_FILE"
}

trap cleanup EXIT

cargo run \
  --manifest-path "$ROOT_DIR/Cargo.toml" \
  --locked \
  --quiet \
  -p runledger-admin \
  --example export_openapi \
  >"$TEMP_FILE"

chmod 0644 "$TEMP_FILE"
mv -- "$TEMP_FILE" "$OUTPUT_PATH"

if [[ "$OUTPUT_OVERRIDDEN" == false ]]; then
  npm run generate --prefix "$ROOT_DIR/runledger-admin-web"
fi
