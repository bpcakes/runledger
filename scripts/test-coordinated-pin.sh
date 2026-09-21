#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fixture="$(mktemp -d -t runledger-source-pin.XXXXXXXX)"
foundation="${RUNLEDGER_BATTER_SOURCE:-$repo_root/../batter}"
# Keep diagnostic fixture artifacts available on failure; never alter the real
# sibling. This tiny local shared clone needs no network access.
git clone --quiet --shared "$foundation" "$fixture/batter"
rustc --edition=2024 "$repo_root/runledger-postgres/build.rs" -o "$fixture/guard"
export CARGO_MANIFEST_DIR="$repo_root/runledger-postgres"
export RUNLEDGER_BATTER_SOURCE="$fixture/batter"
"$fixture/guard"
printf '\n# unreviewed fixture mutation\n' >> "$fixture/batter/Cargo.toml"
if "$fixture/guard" >"$fixture/rejection.log" 2>&1; then
  echo "Source pin accepted unreviewed foundation input." >&2
  exit 1
fi
grep -Fq 'differs from the coordinated pin' "$fixture/rejection.log"
echo "Local Cargo source guard accepts companion-only changes and rejects foundation drift. Fixture: $fixture"
