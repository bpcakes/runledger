#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PUBLISHABLE_CRATES=(
  "runledger-core"
  "runledger-test-support"
  "runledger-postgres"
  "runledger-runtime"
)
PUBLISH_REMOTE="${PUBLISH_REMOTE:-origin}"

usage() {
  echo "usage: $0 <version>" >&2
  echo "example: $0 0.1.1" >&2
  echo "set PUBLISH_REMOTE to override the git remote used for the final push" >&2
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
    die "working tree must be clean before publishing"
  fi
}

validate_version() {
  local version="$1"
  if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
    die "version must look like a Cargo semver version, got '$version'"
  fi
}

crate_manifest_version() {
  local crate="$1"
  cargo pkgid -p "$crate" | sed 's/.*#//'
}

require_manifest_versions() {
  local version="$1"

  for crate in "${PUBLISHABLE_CRATES[@]}"; do
    local manifest_version
    manifest_version="$(crate_manifest_version "$crate")"
    if [[ "$manifest_version" != "$version" ]]; then
      die "${crate} manifest is ${manifest_version}, expected ${version}"
    fi

    grep -Eq "^${crate}[[:space:]]*=[[:space:]]*\\{[[:space:]]*version[[:space:]]*=[[:space:]]*\"${version}\"" "$ROOT_DIR/Cargo.toml" \
      || die "workspace dependency for ${crate} is not pinned to ${version}"
  done
}

wait_for_crates_io_index() {
  local crate="$1"
  local version="$2"
  local timeout_seconds="${CRATES_IO_INDEX_TIMEOUT_SECONDS:-600}"
  local start
  start="$(date +%s)"

  echo "Waiting for crates.io to index ${crate} ${version}..."

  while true; do
    if curl -fsSL \
      -A "runledger-release-script" \
      "https://crates.io/api/v1/crates/${crate}/${version}" \
      >/dev/null 2>&1; then
      echo "Indexed: ${crate} ${version}"
      return 0
    fi

    local now
    now="$(date +%s)"
    if (( now - start >= timeout_seconds )); then
      die "timed out waiting for crates.io to index ${crate} ${version}"
    fi

    sleep 10
  done
}

require_tag_absent() {
  local tag="$1"
  if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
    die "tag ${tag} already exists"
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

if [[ $# -ne 1 ]]; then
  usage
  exit 2
fi

VERSION="$1"
TAG="v${VERSION}"

cd "$ROOT_DIR"

require_command cargo
require_command curl
require_command git
require_clean_worktree
validate_version "$VERSION"
require_manifest_versions "$VERSION"
require_tag_absent "$TAG"

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" == "HEAD" ]]; then
  die "cannot publish from detached HEAD"
fi

git remote get-url "$PUBLISH_REMOTE" >/dev/null \
  || die "git remote '${PUBLISH_REMOTE}' does not exist"

for crate in "${PUBLISHABLE_CRATES[@]}"; do
  if version_exists_on_crates_io "$crate" "$VERSION"; then
    echo "${crate} ${VERSION} already exists on crates.io; assuming a previous publish completed."
  else
    cargo publish --dry-run -p "$crate"
    cargo publish -p "$crate"
  fi

  wait_for_crates_io_index "$crate" "$VERSION"
done

git tag "$TAG"

git push "$PUBLISH_REMOTE" "$current_branch"
git push "$PUBLISH_REMOTE" "$TAG"

echo "Published ${VERSION} and pushed ${TAG}."
