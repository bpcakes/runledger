#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PUBLISHABLE_CRATES=(
  "runledger-core"
  "runledger-test-support"
  "runledger-postgres"
  "runledger-runtime"
  "runledger-tui"
)

die() {
  echo "error: $*" >&2
  exit 1
}

command -v cargo >/dev/null 2>&1 || die "cargo is required"

cd "$ROOT_DIR"

for crate in "${PUBLISHABLE_CRATES[@]}"; do
  cmp -s "$ROOT_DIR/LICENSE" "$ROOT_DIR/${crate}/LICENSE" \
    || die "${crate}/LICENSE differs from the repository LICENSE"

  package_files="$(
    cargo package \
      --allow-dirty \
      --list \
      -p "$crate" \
      --config "patch.crates-io.runledger-core.path=\"${ROOT_DIR}/runledger-core\"" \
      --config "patch.crates-io.runledger-test-support.path=\"${ROOT_DIR}/runledger-test-support\"" \
      --config "patch.crates-io.runledger-postgres.path=\"${ROOT_DIR}/runledger-postgres\"" \
      --config "patch.crates-io.runledger-runtime.path=\"${ROOT_DIR}/runledger-runtime\""
  )"

  grep -Fxq "LICENSE" <<<"$package_files" \
    || die "${crate} package does not contain LICENSE"
  echo "Verified packaged license: ${crate}"
done
