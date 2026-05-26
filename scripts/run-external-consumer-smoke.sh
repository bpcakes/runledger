#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SMOKE_SOURCE_DIR="$ROOT_DIR/smoke/external-consumer"
WORK_DIR="$ROOT_DIR/target/external-consumer-smoke/work"
SMOKE_MANIFEST="$WORK_DIR/Cargo.toml"
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

rm -rf "$VENDOR_DIR" "$TARGET_DIR" "$WORK_DIR"
mkdir -p "$VENDOR_DIR" "$TARGET_DIR"
cp -R "$SMOKE_SOURCE_DIR" "$WORK_DIR"

for crate in runledger-core runledger-test-support runledger-postgres runledger-runtime; do
  package_crate "$crate"
done

CORE_VERSION="$(crate_version runledger-core)"
TEST_SUPPORT_VERSION="$(crate_version runledger-test-support)"
POSTGRES_VERSION="$(crate_version runledger-postgres)"
RUNTIME_VERSION="$(crate_version runledger-runtime)"

extract_crate runledger-core "$CORE_VERSION"
extract_crate runledger-test-support "$TEST_SUPPORT_VERSION"
extract_crate runledger-postgres "$POSTGRES_VERSION"
extract_crate runledger-runtime "$RUNTIME_VERSION"

RELEASE_CORE_VERSION="$CORE_VERSION" \
RELEASE_TEST_SUPPORT_VERSION="$TEST_SUPPORT_VERSION" \
RELEASE_POSTGRES_VERSION="$POSTGRES_VERSION" \
RELEASE_RUNTIME_VERSION="$RUNTIME_VERSION" \
perl -0pi -e '
  s/^runledger-core = "[^"]+"/runledger-core = "$ENV{RELEASE_CORE_VERSION}"/m
    or die "failed to pin runledger-core in $ARGV\n";
  s/^runledger-test-support = "[^"]+"/runledger-test-support = "$ENV{RELEASE_TEST_SUPPORT_VERSION}"/m
    if /^runledger-test-support = /m;
  s/^runledger-postgres = "[^"]+"/runledger-postgres = "$ENV{RELEASE_POSTGRES_VERSION}"/m
    or die "failed to pin runledger-postgres in $ARGV\n";
  s/^runledger-runtime = "[^"]+"/runledger-runtime = "$ENV{RELEASE_RUNTIME_VERSION}"/m
    or die "failed to pin runledger-runtime in $ARGV\n";
' "$SMOKE_MANIFEST"

{
  printf '[patch.crates-io]\n'
  printf 'runledger-core = { path = "%s/runledger-core-%s" }\n' "$VENDOR_DIR" "$CORE_VERSION"
  printf 'runledger-test-support = { path = "%s/runledger-test-support-%s" }\n' "$VENDOR_DIR" "$TEST_SUPPORT_VERSION"
  printf 'runledger-postgres = { path = "%s/runledger-postgres-%s" }\n' "$VENDOR_DIR" "$POSTGRES_VERSION"
  printf 'runledger-runtime = { path = "%s/runledger-runtime-%s" }\n' "$VENDOR_DIR" "$RUNTIME_VERSION"
} > "$TMP_CONFIG"

CARGO_TARGET_DIR="$TARGET_DIR" cargo test \
  --manifest-path "$SMOKE_MANIFEST" \
  --test smoke \
  --config "$TMP_CONFIG"
