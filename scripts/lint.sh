#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMOKE_MANIFEST="$ROOT_DIR/smoke/external-consumer/Cargo.toml"

cd "$ROOT_DIR"

cargo fmt --all -- --check
cargo fmt --manifest-path "$SMOKE_MANIFEST" -- --check

./scripts/check-admin-openapi.sh

cargo clippy \
  --workspace \
  --all-targets \
  --all-features \
  --locked \
  -- \
  -D warnings

cargo clippy \
  --manifest-path "$SMOKE_MANIFEST" \
  --all-targets \
  --locked \
  -- \
  -D warnings

RUSTDOCFLAGS="-D warnings" \
  cargo doc --workspace --all-features --no-deps --locked
