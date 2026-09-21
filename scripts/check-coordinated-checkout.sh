#!/usr/bin/env bash
# CI runs this from a fresh Runledger checkout with no pre-existing sibling.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"
if [[ -e ../batter ]]; then
  echo "This regression requires a clean checkout without a sibling Batter directory." >&2
  exit 1
fi
bash scripts/bootstrap-batter.sh
bash scripts/test-coordinated-pin.sh
cargo check --workspace --all-targets --locked

# This is intentionally NOT publication evidence. With no source patches the
# unpublished graph must not masquerade as a verified registry package.
package_log="$(mktemp)"
trap 'rm -f "$package_log"' EXIT
if cargo package -p runledger-postgres --locked >"$package_log" 2>&1; then
  echo "Unexpected unpatched package success: reassess the unpublished graph contract." >&2
  exit 1
fi
if ! grep -Eq 'no matching package named .batter-sqlx.' "$package_log"; then
  cat "$package_log" >&2
  echo "Unpatched packaging failed for an unrelated reason." >&2
  exit 1
fi
echo "Unpatched package rejection confirmed; coordinated archives are source-only evidence."
