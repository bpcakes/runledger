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
RELEASE_ARTIFACT_DIR=""
NPM_PACKAGE_ARCHIVE=""
CARGO_PACKAGE_DIR=""

cleanup() {
  if [[ -n "$RELEASE_ARTIFACT_DIR" ]]; then
    rm -rf -- "$RELEASE_ARTIFACT_DIR"
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

require_npm_authentication() {
  local npm_user
  npm_user="$(npm whoami 2>/dev/null)" \
    || die "npm authentication is required before publishing; run 'npm login' and retry"
  [[ -n "$npm_user" ]] \
    || die "npm authentication check returned an empty username"
  echo "Authenticated to npm as ${npm_user}."
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

require_publish_order() {
  local publish_order
  publish_order="${PUBLISHABLE_CRATES[*]}"

  RUNLEDGER_PUBLISH_ORDER="$publish_order" node -e '
    let input = "";
    process.stdin.setEncoding("utf8");
    process.stdin.on("data", (chunk) => { input += chunk; });
    process.stdin.on("end", () => {
      const metadata = JSON.parse(input);
      const order = process.env.RUNLEDGER_PUBLISH_ORDER.split(/\s+/);
      const positions = new Map(order.map((name, position) => [name, position]));
      const packages = new Map(metadata.packages.map((pkg) => [pkg.name, pkg]));

      for (const [crate, position] of positions) {
        const pkg = packages.get(crate);
        if (pkg === undefined) throw new Error(`publishable crate ${crate} is not in the workspace`);
        for (const dependency of pkg.dependencies) {
          const dependencyPosition = positions.get(dependency.name);
          if (dependencyPosition !== undefined && dependencyPosition >= position) {
            throw new Error(
              `${crate} must be published after workspace dependency ${dependency.name}`,
            );
          }
        }
      }
    });
  ' < <(cargo metadata --format-version 1 --no-deps) \
    || die "publishable crates are not in dependency order"
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

file_digest() {
  local algorithm="$1"
  local path="$2"

  node -e '
    const { createHash } = require("node:crypto");
    const { readFileSync } = require("node:fs");
    const [algorithm, path] = process.argv.slice(1);
    process.stdout.write(createHash(algorithm).update(readFileSync(path)).digest("hex"));
  ' "$algorithm" "$path"
}

file_sha512_integrity() {
  local path="$1"

  node -e '
    const { createHash } = require("node:crypto");
    const { readFileSync } = require("node:fs");
    const digest = createHash("sha512").update(readFileSync(process.argv[1])).digest("base64");
    process.stdout.write(`sha512-${digest}`);
  ' "$path"
}

crate_package_archive() {
  local crate="$1"
  local version="$2"
  echo "${CARGO_PACKAGE_DIR}/${crate}-${version}.crate"
}

prepare_release_artifacts() {
  local version="$1"
  RELEASE_ARTIFACT_DIR="$(mktemp -d)"
  CARGO_PACKAGE_DIR="$(
    cargo metadata --format-version 1 --no-deps \
      | node -e '
          let input = "";
          process.stdin.setEncoding("utf8");
          process.stdin.on("data", (chunk) => { input += chunk; });
          process.stdin.on("end", () => {
            process.stdout.write(`${JSON.parse(input).target_directory}/package`);
          });
        '
  )"

  npm ci --prefix "$ROOT_DIR/runledger-admin-web"

  local archive_name
  archive_name="$(
    npm pack \
      "$ROOT_DIR/runledger-admin-web" \
      --pack-destination "$RELEASE_ARTIFACT_DIR" \
      --silent
  )"
  NPM_PACKAGE_ARCHIVE="$RELEASE_ARTIFACT_DIR/$archive_name"
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

  echo "Prepared release artifacts from commit $(git rev-parse HEAD)."
}

prepare_crate_artifact() {
  local crate="$1"
  local version="$2"

  # Package in publication order. Cargo normalizes path dependencies to
  # registry dependencies in the archive's Cargo.toml and Cargo.lock, so a
  # dependent crate cannot be packaged canonically until its same-version
  # Runledger dependencies have been indexed by crates.io.
  cargo package --locked --no-verify -p "$crate" >/dev/null

  local archive
  archive="$(crate_package_archive "$crate" "$version")"
  [[ -f "$archive" ]] \
    || die "cargo package did not create the expected archive: ${archive}"
}

verify_existing_crate_artifact() {
  local crate="$1"
  local version="$2"
  local local_archive
  local_archive="$(crate_package_archive "$crate" "$version")"
  local remote_archive="$RELEASE_ARTIFACT_DIR/${crate}-${version}.published.crate"

  curl -fsSL \
    -A "runledger-release-script" \
    "https://crates.io/api/v1/crates/${crate}/${version}/download" \
    -o "$remote_archive" \
    || die "failed to download published ${crate} ${version} for identity verification"

  local local_digest
  local remote_digest
  local_digest="$(file_digest sha256 "$local_archive")"
  remote_digest="$(file_digest sha256 "$remote_archive")"
  if [[ "$local_digest" != "$remote_digest" ]]; then
    die "published ${crate} ${version} does not match the artifact built from commit $(git rev-parse HEAD)"
  fi

  echo "Verified identical published artifact: ${crate} ${version} (${local_digest})"
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

verify_existing_npm_artifact() {
  local version="$1"
  local published_integrity
  published_integrity="$(npm view "@runledger/admin@${version}" dist.integrity)" \
    || die "failed to read npm integrity for @runledger/admin ${version}"
  [[ -n "$published_integrity" ]] \
    || die "npm did not report integrity for @runledger/admin ${version}"

  local local_integrity
  local_integrity="$(file_sha512_integrity "$NPM_PACKAGE_ARCHIVE")"
  if [[ "$local_integrity" != "$published_integrity" ]]; then
    die "published @runledger/admin ${version} does not match the artifact built from commit $(git rev-parse HEAD)"
  fi

  echo "Verified identical published artifact: @runledger/admin ${version} (${local_integrity})"
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
require_publish_order

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$current_branch" == "HEAD" ]]; then
  die "cannot publish from detached HEAD"
fi

git remote get-url "$PUBLISH_REMOTE" >/dev/null \
  || die "git remote '${PUBLISH_REMOTE}' does not exist"

require_remote_tag_absent "$PUBLISH_REMOTE" "$TAG"
./scripts/verify-release-ci.sh "$PUBLISH_REMOTE" "$current_branch"
require_dry_run_push "$PUBLISH_REMOTE" "$current_branch" "$TAG"
prepare_release_artifacts "$VERSION"

npm_already_published=false
npm_status=0
npm_version_exists "$VERSION" || npm_status=$?
case "$npm_status" in
  0)
    verify_existing_npm_artifact "$VERSION"
    npm_already_published=true
    ;;
  1) ;;
  *) die "could not determine whether @runledger/admin ${VERSION} exists on npm" ;;
esac

if [[ "$npm_already_published" == false ]]; then
  require_npm_authentication
fi

for crate in "${PUBLISHABLE_CRATES[@]}"; do
  prepare_crate_artifact "$crate" "$VERSION"

  crate_already_published=false
  if version_exists_on_crates_io "$crate" "$VERSION"; then
    verify_existing_crate_artifact "$crate" "$VERSION"
    crate_already_published=true
  fi

  if [[ "$crate_already_published" == true ]]; then
    echo "${crate} ${VERSION} already exists on crates.io and matches this release commit."
  else
    cargo publish --locked --dry-run -p "$crate"
    cargo publish --locked -p "$crate"
  fi

  wait_for_crates_io_index "$crate" "$VERSION"
  if [[ "$crate_already_published" == false ]]; then
    verify_existing_crate_artifact "$crate" "$VERSION"
  fi
done

if [[ "$npm_already_published" == true ]]; then
  echo "@runledger/admin ${VERSION} already exists on npm and matches this release commit."
else
  npm publish "$NPM_PACKAGE_ARCHIVE" --access public
fi
wait_for_npm_index "$VERSION"
verify_existing_npm_artifact "$VERSION"

git push \
  --atomic \
  "$PUBLISH_REMOTE" \
  "HEAD:refs/heads/${current_branch}" \
  "HEAD:refs/tags/${TAG}"

# The remote tag is the publication record. Reconcile the optional local
# lightweight tag only after the atomic remote update succeeds so a failed push
# cannot leave local-only state that blocks a retry.
git tag --force "$TAG" HEAD

echo "Published ${VERSION} and pushed ${TAG}."
