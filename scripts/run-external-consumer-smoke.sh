#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMOKE_MANIFEST="$ROOT_DIR/smoke/external-consumer/Cargo.toml"
VENDOR_DIR="$ROOT_DIR/target/external-consumer-smoke/vendor"
TARGET_DIR="$ROOT_DIR/target/external-consumer-smoke/target"
TMP_CONFIG="$(mktemp)"

trap 'rm -f "$TMP_CONFIG"' EXIT

crate_version() {
  cargo pkgid -p "$1" | sed 's/.*#//'
}

package_crate() {
  local crate="$1"
  cargo package \
    --allow-dirty \
    --no-verify \
    -p "$crate" \
    --config "patch.crates-io.runledger-core.path=\"${ROOT_DIR}/runledger-core\"" \
    --config "patch.crates-io.runledger-postgres.path=\"${ROOT_DIR}/runledger-postgres\"" \
    --config "patch.crates-io.runledger-runtime.path=\"${ROOT_DIR}/runledger-runtime\"" \
    --config "patch.crates-io.runledger-test-support.path=\"${ROOT_DIR}/runledger-test-support\"" \
    --quiet
}

extract_crate() {
  local crate="$1"
  local version="$2"
  local archive="$ROOT_DIR/target/package/${crate}-${version}.crate"
  tar -xzf "$archive" -C "$VENDOR_DIR"
}

cd "$ROOT_DIR"

rm -rf "$VENDOR_DIR" "$TARGET_DIR"
mkdir -p "$VENDOR_DIR" "$TARGET_DIR"

for crate in runledger-core runledger-postgres runledger-runtime; do
  package_crate "$crate"
done

CORE_VERSION="$(crate_version runledger-core)"
POSTGRES_VERSION="$(crate_version runledger-postgres)"
RUNTIME_VERSION="$(crate_version runledger-runtime)"

extract_crate runledger-core "$CORE_VERSION"
extract_crate runledger-postgres "$POSTGRES_VERSION"
extract_crate runledger-runtime "$RUNTIME_VERSION"

cat > "$TMP_CONFIG" <<EOF
[patch.crates-io]
runledger-core = { path = "${VENDOR_DIR}/runledger-core-${CORE_VERSION}" }
runledger-postgres = { path = "${VENDOR_DIR}/runledger-postgres-${POSTGRES_VERSION}" }
runledger-runtime = { path = "${VENDOR_DIR}/runledger-runtime-${RUNTIME_VERSION}" }
EOF

CARGO_TARGET_DIR="$TARGET_DIR" cargo test \
  --manifest-path "$SMOKE_MANIFEST" \
  --test smoke \
  --config "$TMP_CONFIG"
