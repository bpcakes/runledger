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
WORKSPACE_DEPENDENCY_CRATES=(
  "runledger-core"
  "runledger-test-support"
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

release_generated_path() {
  local path="$1"

  case "$path" in
    Cargo.toml | \
      Cargo.lock | \
      smoke/external-consumer/Cargo.lock | \
      runledger-core/Cargo.toml | \
      runledger-test-support/Cargo.toml | \
      runledger-postgres/Cargo.toml | \
      runledger-runtime/Cargo.toml | \
      runledger-tui/Cargo.toml | \
      .sqlx/* | \
      runledger-postgres/.sqlx/* | \
      runledger-runtime/.sqlx/* | \
      runledger-postgres/migrations/* | \
      runledger-test-support/migrations/*)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

crate_manifest_version() {
  local manifest="$1"

  awk '
    /^\[package\][[:space:]]*$/ {
      in_package = 1
      next
    }
    in_package && /^\[/ {
      exit
    }
    in_package && /^[[:space:]]*version[[:space:]]*=/ {
      value = $0
      sub(/^[^"]*"/, "", value)
      sub(/".*$/, "", value)
      print value
      found = 1
      exit
    }
    END {
      if (!found) {
        exit 1
      }
    }
  ' "$manifest"
}

require_manifest_versions() {
  local version="$1"
  local crate

  for crate in "${PUBLISHABLE_CRATES[@]}"; do
    local manifest_version
    manifest_version="$(crate_manifest_version "$ROOT_DIR/${crate}/Cargo.toml")" \
      || die "could not read ${crate} package version"
    if [[ "$manifest_version" != "$version" ]]; then
      die "cannot resume: ${crate} manifest is ${manifest_version}, expected ${version}"
    fi
  done

  for crate in "${WORKSPACE_DEPENDENCY_CRATES[@]}"; do
    grep -Fq \
      "${crate} = { version = \"${version}\", path = \"${crate}\" }" \
      "$ROOT_DIR/Cargo.toml" \
      || die "cannot resume: workspace dependency for ${crate} is not pinned to ${version}"
  done
}

require_clean_or_resumable_worktree() {
  local version="$1"
  local dirty=false
  local unexpected_paths=()
  local path

  while IFS= read -r -d '' path; do
    dirty=true
    if ! release_generated_path "$path"; then
      unexpected_paths+=("$path")
    fi
  done < <(
    git diff --name-only -z
    git diff --cached --name-only -z
    git ls-files --others --exclude-standard -z
  )

  if [[ "${#unexpected_paths[@]}" -gt 0 ]]; then
    echo "error: working tree contains changes outside the resumable release output:" >&2
    printf '  %s\n' "${unexpected_paths[@]}" >&2
    exit 1
  fi

  if [[ "$dirty" == true ]]; then
    require_manifest_versions "$version"
    echo "Resuming release preparation for ${version} from the existing generated diff."
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

package_with_workspace_patches() {
  local crate="$1"
  shift

  cargo package \
    --allow-dirty \
    -p "$crate" \
    "$@" \
    --config "patch.crates-io.runledger-core.path=\"${ROOT_DIR}/runledger-core\"" \
    --config "patch.crates-io.runledger-test-support.path=\"${ROOT_DIR}/runledger-test-support\"" \
    --config "patch.crates-io.runledger-postgres.path=\"${ROOT_DIR}/runledger-postgres\"" \
    --config "patch.crates-io.runledger-runtime.path=\"${ROOT_DIR}/runledger-runtime\"" \
    --quiet
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
validate_version "$VERSION"
require_clean_or_resumable_worktree "$VERSION"

for crate in "${PUBLISHABLE_CRATES[@]}"; do
  if version_exists_on_crates_io "$crate" "$VERSION"; then
    die "${crate} ${VERSION} already exists on crates.io"
  fi
done

for crate in "${PUBLISHABLE_CRATES[@]}"; do
  bump_crate_manifest "$ROOT_DIR/${crate}/Cargo.toml"
done

for crate in "${WORKSPACE_DEPENDENCY_CRATES[@]}"; do
  bump_workspace_dependency "$crate"
done

cargo update -w
cargo update \
  --manifest-path "$ROOT_DIR/smoke/external-consumer/Cargo.toml" \
  -p runledger-core \
  -p runledger-test-support \
  -p runledger-postgres \
  -p runledger-runtime

./scripts/refresh-sqlx-cache.sh
cargo test --workspace
./scripts/run-external-consumer-smoke.sh
./scripts/check-package-licenses.sh

cargo publish --allow-dirty --dry-run -p runledger-core

for crate in runledger-test-support runledger-postgres runledger-runtime; do
  package_with_workspace_patches "$crate" --no-verify >/dev/null
done

# Keep verification enabled for the distributable binary so Cargo builds the
# extracted runledger-tui package before any irreversible publication begins.
package_with_workspace_patches runledger-tui >/dev/null

echo "Release ${VERSION} is prepared."
echo "Review the diff, commit it, then run: ./scripts/publish-release.sh ${VERSION}"
