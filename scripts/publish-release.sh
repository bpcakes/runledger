#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PUBLISHABLE_CRATES=(
  "runledger-core"
  "runledger-test-support"
  "runledger-postgres"
  "runledger-admin"
  "runledger-runtime"
  "runledger-tui"
)
WORKSPACE_DEPENDENCY_CRATES=(
  "runledger-admin"
  "runledger-core"
  "runledger-test-support"
  "runledger-postgres"
  "runledger-runtime"
)
PUBLISH_REMOTE="${PUBLISH_REMOTE:-origin}"
NPM_PACK_DIR=""
NPM_PACKAGE_ARCHIVE=""

cleanup() {
  if [[ -n "$NPM_PACK_DIR" ]]; then
    rm -rf -- "$NPM_PACK_DIR"
  fi
}

trap cleanup EXIT

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
  done

  for crate in "${WORKSPACE_DEPENDENCY_CRATES[@]}"; do
    grep -Fq \
      "${crate} = { version = \"${version}\", path = \"${crate}\" }" \
      "$ROOT_DIR/Cargo.toml" \
      || die "workspace dependency for ${crate} is not pinned to ${version}"
  done

  local npm_version
  npm_version="$(node -p "require('./runledger-admin-web/package.json').version")" \
    || die "could not read @runledger/admin package version"
  if [[ "$npm_version" != "$version" ]]; then
    die "@runledger/admin package is ${npm_version}, expected ${version}"
  fi
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

require_remote_tag_absent() {
  local remote="$1"
  local tag="$2"
  local remote_tags

  remote_tags="$(
    git ls-remote \
      --tags \
      "$remote" \
      "refs/tags/${tag}" \
      "refs/tags/${tag}^{}"
  )" || die "failed to inspect tag ${tag} on remote '${remote}'"

  if [[ -n "$remote_tags" ]]; then
    die "tag ${tag} already exists on remote '${remote}'"
  fi
}

require_dry_run_push() {
  local remote="$1"
  local branch="$2"
  local tag="$3"

  echo "Checking branch and tag push permissions on remote '${remote}'..."
  git push \
    --atomic \
    --dry-run \
    --porcelain \
    "$remote" \
    "HEAD:refs/heads/${branch}" \
    "HEAD:refs/tags/${tag}" \
    || die "dry-run push of ${branch} and ${tag} to remote '${remote}' failed"
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

npm_version_exists() {
  local version="$1"
  local output
  if output="$(npm view "@runledger/admin@${version}" version --json 2>&1)"; then
    return 0
  fi
  if [[ "$output" == *E404* ]]; then
    return 1
  fi
  echo "npm lookup failed: ${output}" >&2
  return 2
}

prepare_npm_package() {
  NPM_PACK_DIR="$(mktemp -d)"

  npm ci --prefix "$ROOT_DIR/runledger-admin-web"

  local archive_name
  archive_name="$(
    npm pack \
      "$ROOT_DIR/runledger-admin-web" \
      --pack-destination "$NPM_PACK_DIR" \
      --silent
  )"
  NPM_PACKAGE_ARCHIVE="$NPM_PACK_DIR/$archive_name"
  [[ -f "$NPM_PACKAGE_ARCHIVE" ]] \
    || die "npm pack did not create the expected archive: ${NPM_PACKAGE_ARCHIVE}"

  local required_path
  for required_path in \
    package/dist/index.js \
    package/dist/index.d.ts \
    package/dist/client.js \
    package/dist/client.d.ts \
    package/dist/generated/schema.d.ts \
    package/dist/react.js \
    package/dist/react.d.ts \
    package/dist/styles.css \
    package/openapi.json; do
    tar -tzf "$NPM_PACKAGE_ARCHIVE" "$required_path" >/dev/null \
      || die "npm package is missing ${required_path}"
  done

  local openapi_mode
  read -r openapi_mode _ < <(tar -tvzf "$NPM_PACKAGE_ARCHIVE" package/openapi.json)
  [[ "$openapi_mode" == "-rw-r--r--" ]] \
    || die "npm package openapi.json must have mode 0644, found ${openapi_mode}"

  echo "Prepared npm package: ${NPM_PACKAGE_ARCHIVE}"
}

wait_for_npm_index() {
  local version="$1"
  local timeout_seconds="${NPM_INDEX_TIMEOUT_SECONDS:-600}"
  local start
  start="$(date +%s)"

  echo "Waiting for npm to index @runledger/admin ${version}..."
  while true; do
    local npm_status=0
    npm_version_exists "$version" || npm_status=$?
    case "$npm_status" in
      0) break ;;
      1)
        if (( $(date +%s) - start >= timeout_seconds )); then
          die "timed out waiting for npm to index @runledger/admin ${version}"
        fi
        sleep 10
        ;;
      *) die "could not query npm while waiting for @runledger/admin ${version}" ;;
    esac
  done
  echo "Indexed: @runledger/admin ${version}"
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
require_command gh
require_command git
require_command node
require_command npm
require_command tar
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

require_remote_tag_absent "$PUBLISH_REMOTE" "$TAG"
./scripts/verify-release-ci.sh "$PUBLISH_REMOTE" "$current_branch"
require_dry_run_push "$PUBLISH_REMOTE" "$current_branch" "$TAG"

npm_already_published=false
npm_status=0
npm_version_exists "$VERSION" || npm_status=$?
case "$npm_status" in
  0) npm_already_published=true ;;
  1) prepare_npm_package ;;
  *) die "could not determine whether @runledger/admin ${VERSION} exists on npm" ;;
esac

for crate in "${PUBLISHABLE_CRATES[@]}"; do
  if version_exists_on_crates_io "$crate" "$VERSION"; then
    echo "${crate} ${VERSION} already exists on crates.io; assuming a previous publish completed."
  else
    cargo publish --dry-run -p "$crate"
    cargo publish -p "$crate"
  fi

  wait_for_crates_io_index "$crate" "$VERSION"
done

if [[ "$npm_already_published" == true ]]; then
  echo "@runledger/admin ${VERSION} already exists on npm; assuming a previous publish completed."
else
  npm publish "$NPM_PACKAGE_ARCHIVE" --access public
fi
wait_for_npm_index "$VERSION"

git tag "$TAG"

git push \
  --atomic \
  "$PUBLISH_REMOTE" \
  "HEAD:refs/heads/${current_branch}" \
  "refs/tags/${TAG}:refs/tags/${TAG}"

echo "Published ${VERSION} and pushed ${TAG}."
