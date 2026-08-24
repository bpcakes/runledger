#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "$ROOT_DIR/scripts/lib/sqlx-migration-info.sh"

assert_current() {
  local fixture="$1"

  if ! sqlx_migration_info_is_current <<<"$fixture"; then
    echo "error: expected SQLx migration info to be current" >&2
    printf '%s\n' "$fixture" >&2
    exit 1
  fi
}

assert_not_current() {
  local fixture="$1"

  if sqlx_migration_info_is_current <<<"$fixture"; then
    echo "error: expected SQLx migration info to be rejected" >&2
    printf '%s\n' "$fixture" >&2
    exit 1
  fi
}

assert_current $'202608240001/installed expand workflow step job link\n202608240002/installed record pending work items'
assert_not_current $'202608240001/installed expand workflow step job link\n202608240002/pending contract workflow step job link'
assert_not_current $'202608240001/installed (different checksum) expand workflow step job link\napplied migration had checksum abc123\nlocal migration has checksum def456'
assert_not_current $'202608240001/unknown expand workflow step job link'
assert_not_current ''

echo "SQLx migration-info parser checks passed."
