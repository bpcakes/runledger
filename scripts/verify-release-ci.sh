#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REMOTE="${1:-origin}"
BRANCH="${2:-$(git -C "$ROOT_DIR" rev-parse --abbrev-ref HEAD)}"
WORKFLOW_NAME="CI"

die() {
  echo "error: $*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || die "$1 is required"
}

require_command gh
require_command git

cd "$ROOT_DIR"

if [[ "$BRANCH" == "HEAD" ]]; then
  die "cannot verify release CI from detached HEAD"
fi

remote_url="$(git remote get-url "$REMOTE")" \
  || die "git remote '${REMOTE}' does not exist"
repository="$(gh repo view "$remote_url" --json nameWithOwner --jq '.nameWithOwner')" \
  || die "failed to resolve GitHub repository for remote '${REMOTE}'"

remote_ref="refs/heads/${BRANCH}"
git fetch --quiet --no-tags "$REMOTE" "$remote_ref" \
  || die "failed to fetch ${remote_ref} from remote '${REMOTE}'"

head_sha="$(git rev-parse HEAD)"
remote_head="$(git rev-parse FETCH_HEAD)"
if [[ "$remote_head" != "$head_sha" ]]; then
  die "remote branch ${REMOTE}/${BRANCH} is at ${remote_head}, expected exact release commit ${head_sha}"
fi

run_record="$(
  gh run list \
    --repo "$repository" \
    --workflow "$WORKFLOW_NAME" \
    --branch "$BRANCH" \
    --commit "$head_sha" \
    --event push \
    --limit 10 \
    --json databaseId,headSha,status,conclusion,url \
    --jq 'if length == 0 then empty else .[0] | [.databaseId, .headSha, .status, .conclusion, .url] | @tsv end'
)" || die "failed to inspect GitHub Actions runs for ${head_sha}"

if [[ -z "$run_record" ]]; then
  die "no ${WORKFLOW_NAME} push run found for exact release commit ${head_sha}"
fi

IFS=$'\t' read -r run_id run_sha run_status run_conclusion run_url <<<"$run_record"
if [[ "$run_sha" != "$head_sha" ]]; then
  die "latest matching CI run targets ${run_sha}, expected ${head_sha}"
fi
if [[ "$run_status" != "completed" || "$run_conclusion" != "success" ]]; then
  die "CI run ${run_url} is ${run_status}/${run_conclusion}, expected completed/success"
fi

job_record="$(
  gh run view "$run_id" \
    --repo "$repository" \
    --json jobs \
    --jq '[(.jobs | length), ([.jobs[] | select(.status != "completed" or .conclusion != "success")] | length)] | @tsv'
)" || die "failed to inspect jobs for CI run ${run_id}"
IFS=$'\t' read -r job_count failing_job_count <<<"$job_record"

if [[ "$job_count" -eq 0 ]]; then
  die "CI run ${run_url} contains no jobs"
fi
if [[ "$failing_job_count" -ne 0 ]]; then
  die "CI run ${run_url} contains ${failing_job_count} incomplete or unsuccessful job(s)"
fi

echo "Verified remote release commit and green CI: ${run_url}"
