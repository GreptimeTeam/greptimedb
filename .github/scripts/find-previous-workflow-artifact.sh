#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Find the most recent previous successful workflow run that has a non-expired artifact.

Usage:
  find-previous-workflow-artifact.sh --workflow-path PATH --artifact-name NAME [options]

Options:
  --repo OWNER/REPO          GitHub repository. Defaults to GITHUB_REPOSITORY.
  --current-run-id ID        Current workflow run id to exclude. Defaults to GITHUB_RUN_ID.
  --workflow-path PATH       Workflow path, for example .github/workflows/nightly-jsonbench.yaml.
  --artifact-name NAME       Artifact name to find.
  --status STATUS            Workflow run status filter. Defaults to success.
  --per-page N               GitHub API page size. Defaults to 100.
  --run-id-only              Print only the run id. This is the default.
  --artifact-id-only         Print only the artifact id.
  --json                     Print a JSON object with run_id and artifact_id.
  --debug                    Print GitHub API requests and responses to stderr.
  -h, --help                 Show this help.

The script uses gh CLI and jq. Provide GH_TOKEN or authenticate gh before running it.
EOF
}

repo="${GITHUB_REPOSITORY:-}"
current_run_id="${GITHUB_RUN_ID:-}"
workflow_path=""
artifact_name=""
status="success"
per_page="100"
output_format="run_id"
debug="false"

debug_log() {
  if [[ "${debug}" == "true" ]]; then
    printf '[debug] %s\n' "$*" >&2
  fi
}

log_stderr_file() {
  if [[ "${debug}" != "true" || ! -s "${err_file}" ]]; then
    return
  fi

  while read -r line; do
    debug_log "stderr: ${line}"
  done < "${err_file}"
  : > "${err_file}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="$2"
      shift 2
      ;;
    --current-run-id)
      current_run_id="$2"
      shift 2
      ;;
    --workflow-path)
      workflow_path="$2"
      shift 2
      ;;
    --artifact-name)
      artifact_name="$2"
      shift 2
      ;;
    --status)
      status="$2"
      shift 2
      ;;
    --per-page)
      per_page="$2"
      shift 2
      ;;
    --run-id-only)
      output_format="run_id"
      shift
      ;;
    --artifact-id-only)
      output_format="artifact_id"
      shift
      ;;
    --json)
      output_format="json"
      shift
      ;;
    --debug)
      debug="true"
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "${repo}" ]]; then
  echo "--repo is required when GITHUB_REPOSITORY is not set." >&2
  exit 2
fi

if [[ -z "${workflow_path}" ]]; then
  echo "--workflow-path is required." >&2
  exit 2
fi

if [[ -z "${artifact_name}" ]]; then
  echo "--artifact-name is required." >&2
  exit 2
fi

err_file=$(mktemp)
trap 'rm -f "${err_file}"' EXIT

debug_log "request: gh api --method GET repos/${repo}/actions/runs -f status=${status} -f per_page=${per_page} --paginate"
candidate_run_ids=$(
  gh api --method GET "repos/${repo}/actions/runs" \
    -f "status=${status}" \
    -f "per_page=${per_page}" \
    --paginate \
    --jq ".workflow_runs[] | select(.path == \"${workflow_path}\") | .id" \
    2> "${err_file}" || true
)
log_stderr_file
debug_log "response run ids: ${candidate_run_ids:-<none>}"

while read -r run_id; do
  if [[ -z "${run_id}" || "${run_id}" == "${current_run_id}" ]]; then
    debug_log "skip run id: ${run_id:-<empty>}"
    continue
  fi

  debug_log "request: gh api repos/${repo}/actions/runs/${run_id}/artifacts"
  artifacts_response=$(
    gh api "repos/${repo}/actions/runs/${run_id}/artifacts" \
      2> "${err_file}" || true
  )
  log_stderr_file
  debug_log "response for run ${run_id}: ${artifacts_response}"

  artifact_id=$(
    printf '%s\n' "${artifacts_response}" \
      | jq -r --arg name "${artifact_name}" '.artifacts[]? | select(.name == $name and (.expired | not)) | .id' \
      | head -n 1 || true
  )
  debug_log "artifact id for run ${run_id}: ${artifact_id:-<none>}"

  if [[ -z "${artifact_id}" ]]; then
    continue
  fi

  case "${output_format}" in
    run_id)
      echo "${run_id}"
      ;;
    artifact_id)
      echo "${artifact_id}"
      ;;
    json)
      printf '{"run_id":"%s","artifact_id":"%s"}\n' "${run_id}" "${artifact_id}"
      ;;
  esac
  exit 0
done <<< "${candidate_run_ids}"

debug_log "no previous workflow run with artifact '${artifact_name}' found"

case "${output_format}" in
  json)
    printf '{"run_id":"","artifact_id":""}\n'
    ;;
esac
