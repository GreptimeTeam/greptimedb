#!/usr/bin/env bash

set -euo pipefail

FUZZ_ARTIFACT_ROOT="${FUZZ_ARTIFACT_ROOT:-/tmp/greptime-fuzz-artifacts}"
FUZZ_ARTIFACT_COLLECTOR="${FUZZ_ARTIFACT_COLLECTOR:-.github/scripts/collect-fuzz-target-artifacts.sh}"
FUZZ_ARTIFACT_COLLECTION_TIMEOUT_SECS="${FUZZ_ARTIFACT_COLLECTION_TIMEOUT_SECS:-180}"

log() {
  printf '[run-fuzz-targets] %s\n' "$*"
}

parse_bool() {
  case "$2" in
    true | false)
      printf -v "$1" '%s' "$2"
      ;;
    *)
      log "$1 must be true or false, got: $2"
      exit 2
      ;;
  esac
}

write_result() {
  local path="$1"
  local target="$2"
  local status="$3"
  local started_at="$4"
  local completed_at="$5"
  local duration_secs="$6"
  local exit_code="$7"
  local after_prior_failure="$8"
  local artifact_collection="$9"

  jq -n \
    --arg target "${target}" \
    --arg group "${FUZZ_GROUP}" \
    --arg git_sha "${GITHUB_SHA:-}" \
    --arg status "${status}" \
    --arg started_at "${started_at}" \
    --arg completed_at "${completed_at}" \
    --arg duration_secs "${duration_secs}" \
    --arg exit_code "${exit_code}" \
    --arg max_total_time "${FUZZ_MAX_TOTAL_TIME}" \
    --argjson after_prior_failure "${after_prior_failure}" \
    --arg artifact_collection "${artifact_collection}" \
    '{
      target: $target,
      group: $group,
      git_sha: $git_sha,
      status: $status,
      started_at: (if $started_at == "" then null else $started_at end),
      completed_at: (if $completed_at == "" then null else $completed_at end),
      duration_secs: (if $duration_secs == "" then null else ($duration_secs | tonumber) end),
      exit_code: (if $exit_code == "" then null else ($exit_code | tonumber) end),
      max_total_time_secs: ($max_total_time | tonumber),
      after_prior_failure: $after_prior_failure,
      artifact_collection: $artifact_collection
    }' >"${path}"
}

write_summary() {
  local manifest="$1"
  local summary="$2"

  {
    printf '## Fuzz target results: `%s`\n\n' "${FUZZ_GROUP}"
    printf '| Target | Status | Duration (s) | Exit code | After prior failure | Artifacts |\n'
    printf '| --- | --- | ---: | ---: | --- | --- |\n'
    jq -r '.[] | "| `\(.target)` | \(.status) | \(.duration_secs // "-") | \(.exit_code // "-") | \(.after_prior_failure) | \(.artifact_collection) |"' "${manifest}"
  } >"${summary}"

  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    cat "${summary}" >>"${GITHUB_STEP_SUMMARY}"
  fi
}

: "${FUZZ_TARGETS:?FUZZ_TARGETS is required}"
: "${FUZZ_GROUP:?FUZZ_GROUP is required}"
: "${FUZZ_MAX_TOTAL_TIME:?FUZZ_MAX_TOTAL_TIME is required}"

if [[ ! "${FUZZ_GROUP}" =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
  log "FUZZ_GROUP must be a lowercase slug, got: ${FUZZ_GROUP}"
  exit 2
fi
if [[ ! "${FUZZ_MAX_TOTAL_TIME}" =~ ^[1-9][0-9]*$ ]]; then
  log "FUZZ_MAX_TOTAL_TIME must be a positive integer, got: ${FUZZ_MAX_TOTAL_TIME}"
  exit 2
fi
if [[ ! "${FUZZ_ARTIFACT_COLLECTION_TIMEOUT_SECS}" =~ ^[1-9][0-9]*$ ]]; then
  log "FUZZ_ARTIFACT_COLLECTION_TIMEOUT_SECS must be a positive integer"
  exit 2
fi

parse_bool fuzz_unstable "${FUZZ_UNSTABLE:-false}"
parse_bool fuzz_fail_fast "${FUZZ_FAIL_FAST:-true}"
parse_bool fuzz_collect_cluster_artifacts "${FUZZ_COLLECT_CLUSTER_ARTIFACTS:-false}"

targets=()
while IFS= read -r target; do
  target="${target%$'\r'}"
  [[ -z "${target}" ]] && continue
  if [[ ! "${target}" =~ ^[a-zA-Z0-9_]+$ ]]; then
    log "invalid fuzz target: ${target}"
    exit 2
  fi
  targets+=("${target}")
done <<<"${FUZZ_TARGETS}"

if [[ "${#targets[@]}" -eq 0 ]]; then
  log "FUZZ_TARGETS contains no targets"
  exit 2
fi

mkdir -p "${FUZZ_ARTIFACT_ROOT}/targets"
result_paths=()
failed_targets=()
prior_failure=false

for ((index = 0; index < ${#targets[@]}; index++)); do
  target="${targets[index]}"
  target_dir="${FUZZ_ARTIFACT_ROOT}/targets/${target}"
  result_path="${target_dir}/result.json"
  mkdir -p "${target_dir}/csv" "${target_dir}/libfuzzer"
  result_paths+=("${result_path}")

  if [[ "${prior_failure}" == true && "${fuzz_fail_fast}" == true ]]; then
    write_result \
      "${result_path}" "${target}" "skipped_after_failure" \
      "" "" "" "" true "not_requested"
    continue
  fi

  started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  started_epoch="$(date +%s)"
  cargo_args=(fuzz run "${target}" --fuzz-dir tests-fuzz -D -s none)
  if [[ "${fuzz_unstable}" == true ]]; then
    cargo_args+=(--features=unstable)
  fi
  cargo_args+=(-- "-max_total_time=${FUZZ_MAX_TOTAL_TIME}" "-artifact_prefix=${target_dir}/libfuzzer/")

  echo "::group::Fuzz target: ${target}"
  log "run target ${target}"
  if GT_FUZZ_DUMP_DIR="${target_dir}/csv" \
    cargo "${cargo_args[@]}" 2>&1 | tee "${target_dir}/fuzz.log"; then
    exit_code=0
    status="success"
    artifact_collection="not_requested"
  else
    exit_code=$?
    status="failure"
    failed_targets+=("${target}")
    artifact_collection="success"

    log "target ${target} failed with exit code ${exit_code}; collect diagnostics"
    if timeout --signal=TERM --kill-after=10s \
      "${FUZZ_ARTIFACT_COLLECTION_TIMEOUT_SECS}s" \
      env \
        FUZZ_COLLECT_CLUSTER_ARTIFACTS="${fuzz_collect_cluster_artifacts}" \
        "${FUZZ_ARTIFACT_COLLECTOR}" "${target}" "${target_dir}"; then
      :
    else
      collector_exit_code=$?
      if [[ "${collector_exit_code}" -eq 124 ]]; then
        artifact_collection="timeout"
      else
        artifact_collection="failed"
      fi
      echo "::warning title=Fuzz artifact collection failed::target=${target}, status=${artifact_collection}, exit_code=${collector_exit_code}"
    fi
    echo "::error title=Fuzz target failed::${target} exited with code ${exit_code}"
  fi
  echo "::endgroup::"

  completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  completed_epoch="$(date +%s)"
  duration_secs=$((completed_epoch - started_epoch))
  write_result \
    "${result_path}" "${target}" "${status}" \
    "${started_at}" "${completed_at}" "${duration_secs}" "${exit_code}" \
    "${prior_failure}" "${artifact_collection}"

  if [[ "${status}" == failure ]]; then
    prior_failure=true
  fi
done

manifest="${FUZZ_ARTIFACT_ROOT}/manifest.json"
summary="${FUZZ_ARTIFACT_ROOT}/summary.md"
jq -s '.' "${result_paths[@]}" >"${manifest}"
write_summary "${manifest}" "${summary}"

if [[ "${#failed_targets[@]}" -gt 0 ]]; then
  log "failed targets: ${failed_targets[*]}"
  exit 1
fi

log "all targets passed: ${targets[*]}"
