#!/usr/bin/env bash
# Reproduce the explicitly paired, unpublished development dependency.
set -euo pipefail
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
foundation="${RUNLEDGER_BATTER_SOURCE:-$repo_root/../batter}"
revision="$(tr -d '\n' < "$repo_root/runledger-postgres/batter-revision")"
if [[ ! -e "$foundation" ]]; then
  git clone --filter=blob:none --no-checkout https://github.com/bpcakes/batter.git "$foundation"
  git -C "$foundation" checkout --detach "$revision"
fi
# Never switch, overwrite or clean an existing sibling worktree.
if ! git -C "$foundation" diff --quiet "$revision" -- crates/batter-core crates/batter-sqlx; then
  echo "Existing Batter foundation differs from the reviewed pin; no files were changed." >&2
  exit 1
fi
if [[ -n "$(git -C "$foundation" ls-files --others -- crates/batter-core crates/batter-sqlx)" ]]; then
  echo "Untracked foundation sources: commit/update the coordinated pin before building." >&2
  exit 1
fi
echo "Foundation files match $revision; Cargo builds also verify actual sources and inherited manifest inputs."
