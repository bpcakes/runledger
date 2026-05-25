#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PUBLISHABLE_SQLX_CRATES=(
  "runledger-postgres"
  "runledger-runtime"
)
VENDORED_MIGRATION_CRATES=(
  "runledger-postgres"
  "runledger-test-support"
)

cd "$ROOT_DIR"

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required" >&2
  exit 1
fi

if ! cargo sqlx --help >/dev/null 2>&1; then
  echo "error: cargo-sqlx is required. Install it with 'cargo install sqlx-cli --no-default-features --features rustls,postgres'." >&2
  exit 1
fi

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "error: DATABASE_URL must point at a PostgreSQL database with the current Runledger migrations applied." >&2
  exit 1
fi

echo "Refreshing workspace SQLx cache..."
cargo sqlx prepare --workspace -- --all-targets

for crate_dir in "${PUBLISHABLE_SQLX_CRATES[@]}"; do
  echo "Syncing SQLx cache into ${crate_dir}/.sqlx..."
  rm -rf "${crate_dir}/.sqlx"
  mkdir -p "${crate_dir}/.sqlx"
  cp -R .sqlx/. "${crate_dir}/.sqlx/"
done

for crate_dir in "${VENDORED_MIGRATION_CRATES[@]}"; do
  echo "Syncing workspace migrations into ${crate_dir}/migrations..."
  rm -rf "${crate_dir}/migrations"
  mkdir -p "${crate_dir}/migrations"
  cp -R migrations/. "${crate_dir}/migrations/"
done

echo "Verifying workspace build with refreshed offline metadata..."
cargo check --workspace

echo "Verifying that publishable crate tarballs include per-crate SQLx cache..."
for crate_dir in "${PUBLISHABLE_SQLX_CRATES[@]}"; do
  cargo package --allow-dirty -p "${crate_dir}" --quiet --list >/dev/null
done

echo "SQLx cache refresh complete."
