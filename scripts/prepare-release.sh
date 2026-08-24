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
      runledger-admin-web/package.json | \
      runledger-admin-web/package-lock.json | \
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

workspace_package_version() {
  awk '
    /^\[workspace\.package\][[:space:]]*$/ {
      in_workspace_package = 1
      next
    }
    in_workspace_package && /^\[/ {
      exit
    }
    in_workspace_package && /^[[:space:]]*version[[:space:]]*=/ {
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
  ' "$ROOT_DIR/Cargo.toml"
}

manifest_inherits_workspace_version() {
  local manifest="$1"

  awk '
    /^\[package\][[:space:]]*$/ {
      in_package = 1
      next
    }
    in_package && /^\[/ {
      exit
    }
    in_package && /^[[:space:]]*version\.workspace[[:space:]]*=[[:space:]]*true[[:space:]]*$/ {
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
  local manifest_version

  manifest_version="$(workspace_package_version)" \
    || die "could not read workspace package version"
  if [[ "$manifest_version" != "$version" ]]; then
    die "cannot resume: workspace package version is ${manifest_version}, expected ${version}"
  fi

  for crate in "${PUBLISHABLE_CRATES[@]}"; do
    manifest_inherits_workspace_version "$ROOT_DIR/${crate}/Cargo.toml" \
      || die "cannot resume: ${crate} does not inherit the workspace package version"
  done

  for crate in "${WORKSPACE_DEPENDENCY_CRATES[@]}"; do
    grep -Fq \
      "${crate} = { version = \"${version}\", path = \"${crate}\" }" \
      "$ROOT_DIR/Cargo.toml" \
      || die "cannot resume: workspace dependency for ${crate} is not pinned to ${version}"
  done

  local npm_version
  npm_version="$(node -p "require('./runledger-admin-web/package.json').version")" \
    || die "could not read @runledger/admin package version"
  if [[ "$npm_version" != "$version" ]]; then
    die "cannot resume: @runledger/admin package is ${npm_version}, expected ${version}"
  fi
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

bump_workspace_package_version() {
  RELEASE_VERSION="$VERSION" perl -0pi -e '
    my $version = $ENV{"RELEASE_VERSION"};
    s/(\[workspace\.package\]\s*\n\s*version\s*=\s*")[^"]+(")/$1$version$2/
      or die "failed to update workspace package version in $ARGV\n";
  ' "$ROOT_DIR/Cargo.toml"
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
    --config "patch.crates-io.runledger-admin.path=\"${ROOT_DIR}/runledger-admin\"" \
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
require_command node
require_command npm
validate_version "$VERSION"
require_clean_or_resumable_worktree "$VERSION"

for crate in "${PUBLISHABLE_CRATES[@]}"; do
  if version_exists_on_crates_io "$crate" "$VERSION"; then
    die "${crate} ${VERSION} already exists on crates.io"
  fi
done

npm_status=0
npm_version_exists "$VERSION" || npm_status=$?
case "$npm_status" in
  0) die "@runledger/admin ${VERSION} already exists on npm" ;;
  1) ;;
  *) die "could not determine whether @runledger/admin ${VERSION} exists on npm" ;;
esac

bump_workspace_package_version

for crate in "${WORKSPACE_DEPENDENCY_CRATES[@]}"; do
  bump_workspace_dependency "$crate"
done

npm version "$VERSION" --allow-same-version --no-git-tag-version --prefix "$ROOT_DIR/runledger-admin-web" >/dev/null
require_manifest_versions "$VERSION"

cargo update -w
cargo update \
  --manifest-path "$ROOT_DIR/smoke/external-consumer/Cargo.toml" \
  -p runledger-core \
  -p runledger-test-support \
  -p runledger-postgres \
  -p runledger-admin \
  -p runledger-runtime

./scripts/refresh-sqlx-cache.sh
./scripts/check-admin-openapi.sh
cargo test --workspace
./scripts/run-external-consumer-smoke.sh
./scripts/check-package-licenses.sh

npm ci --prefix "$ROOT_DIR/runledger-admin-web"
npm test --prefix "$ROOT_DIR/runledger-admin-web"
npm pack "$ROOT_DIR/runledger-admin-web" --dry-run >/dev/null

cargo publish --allow-dirty --dry-run -p runledger-core

for crate in runledger-test-support runledger-postgres runledger-admin runledger-runtime; do
  package_with_workspace_patches "$crate" --no-verify >/dev/null
done

# Keep verification enabled for the distributable binary so Cargo builds the
# extracted runledger-tui package before any irreversible publication begins.
package_with_workspace_patches runledger-tui >/dev/null

echo "Release ${VERSION} is prepared."
echo "Review the diff, commit it, then run: ./scripts/publish-release.sh ${VERSION}"
