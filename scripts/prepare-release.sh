#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PUBLISHABLE_CRATES=(
  "runledger-core"
  "runledger-postgres"
  "runledger-runtime"
)

usage() {
  echo "usage: $0 <version>" >&2
  echo "example: $0 0.1.1" >&2
}

die() {
  echo "error: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

require_clean_worktree() {
  if [[ -n "$(git status --porcelain)" ]]; then
    die "working tree must be clean before preparing a release"
  fi
}

validate_version() {
  local version="$1"
  if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
    die "version must look like a Cargo semver version, got '$version'"
  fi
}

version_exists_on_crates_io() {
  local crate="$1"
  local version="$2"
  local http_code

  http_code="$(curl -sSL \
    -A "runledger-release-script" \
    -o /dev/null \
    -w "%{http_code}" \
    "https://crates.io/api/v1/crates/${crate}/${version}" \
  )" || die "failed to query crates.io for ${crate} ${version}"

  case "$http_code" in
    200) return 0 ;;
    404) return 1 ;;
    *) die "unexpected crates.io response for ${crate} ${version}: HTTP ${http_code}" ;;
  esac
}

bump_crate_manifest() {
  local manifest="$1"
  RELEASE_VERSION="$VERSION" perl -0pi -e '
    my $version = $ENV{"RELEASE_VERSION"};
    s/^(version\s*=\s*")[^"]+(")/$1$version$2/m
      or die "failed to update package version in $ARGV\n";
  ' "$manifest"
}

bump_workspace_dependency() {
  local crate="$1"
  RELEASE_VERSION="$VERSION" RELEASE_CRATE="$crate" perl -0pi -e '
    my $version = $ENV{"RELEASE_VERSION"};
    my $crate = $ENV{"RELEASE_CRATE"};
    my $quoted = quotemeta($crate);
    s/^($quoted\s*=\s*\{\s*version\s*=\s*")[^"]+("\s*,\s*path\s*=\s*"$quoted"\s*\})/$1$version$2/m
      or die "failed to update workspace dependency for $crate in $ARGV\n";
  ' "$ROOT_DIR/Cargo.toml"
}

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

VERSION="$1"

cd "$ROOT_DIR"

require_command cargo
require_command curl
require_command git
require_clean_worktree
validate_version "$VERSION"

for crate in "${PUBLISHABLE_CRATES[@]}"; do
  if version_exists_on_crates_io "$crate" "$VERSION"; then
    die "${crate} ${VERSION} already exists on crates.io"
  fi
done

for crate in "${PUBLISHABLE_CRATES[@]}"; do
  bump_crate_manifest "$ROOT_DIR/${crate}/Cargo.toml"
  bump_workspace_dependency "$crate"
done

cargo update -w

./scripts/refresh-sqlx-cache.sh
cargo test --workspace
./scripts/run-external-consumer-smoke.sh

for crate in "${PUBLISHABLE_CRATES[@]}"; do
  cargo publish --dry-run -p "$crate"
done

echo "Release ${VERSION} is prepared."
echo "Review the diff, commit it, then run: ./scripts/publish-release.sh ${VERSION}"
