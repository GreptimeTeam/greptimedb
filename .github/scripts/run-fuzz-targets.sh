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
  local elapsed_secs="$3"
  local total_count="$4"
  local success_count="$5"
  local failure_count="$6"
  local skipped_count="$7"
  local policy
  local target reproduce_command
  local -a reproduce_args

  if [[ "${fuzz_fail_fast}" == true ]]; then
    policy="fail-fast"
  else
    policy="continue-after-failure"
  fi

  {
    printf '## Fuzz target results: `%s`\n\n' "${FUZZ_GROUP}"
    printf -- '- Policy: `%s`\n' "${policy}"
    printf -- '- Results: **%s passed**, **%s failed**, **%s skipped** / %s total\n' \
      "${success_count}" "${failure_count}" "${skipped_count}" "${total_count}"
    printf -- '- Group elapsed time: **%ss**\n' "${elapsed_secs}"
    if [[ -n "${GITHUB_SHA:-}" ]]; then
      printf -- '- Commit: `%s`\n' "${GITHUB_SHA}"
    fi
    printf '\n'
    printf '| Target | Status | Duration (s) | Exit code | After prior failure | Artifacts |\n'
    printf '| --- | --- | ---: | ---: | --- | --- |\n'
    jq -r '.[] | "| `\(.target)` | \(.status) | \(.duration_secs // "-") | \(.exit_code // "-") | \(.after_prior_failure) | \(.artifact_collection) |"' "${manifest}"

    if [[ "${failure_count}" -gt 0 ]]; then
      printf '\n### Reproduce failed targets\n\n'
      while IFS= read -r target; do
        reproduce_args=(cargo fuzz run "${target}" --fuzz-dir tests-fuzz -D -s none)
        if [[ "${fuzz_unstable}" == true ]]; then
          reproduce_args+=(--features=unstable)
        fi
        reproduce_args+=(-- "-max_total_time=${FUZZ_MAX_TOTAL_TIME}")
        printf -v reproduce_command '%q ' "${reproduce_args[@]}"
        printf -- '- `%s`\n' "${reproduce_command% }"
      done < <(jq -r '.[] | select(.status == "failure") | .target' "${manifest}")
    fi
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

if [[ -n "${FUZZ_BIN_DIR:-}" && ! -d "${FUZZ_BIN_DIR}" ]]; then
  log "FUZZ_BIN_DIR does not exist: ${FUZZ_BIN_DIR}"
  exit 2
fi

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
group_started_epoch="$(date +%s)"

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
    echo "::notice title=Fuzz target skipped::target=${target}, group=${FUZZ_GROUP}, reason=prior target failure"
    continue
  fi

  started_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  started_epoch="$(date +%s)"
  echo "::group::Fuzz target: ${target}"
  log "run target ${target}"
  if [[ -n "${FUZZ_BIN_DIR:-}" ]]; then
    fuzz_binary="${FUZZ_BIN_DIR}/${target}"
    if [[ ! -x "${fuzz_binary}" ]]; then
      log "prebuilt fuzz binary does not exist or is not executable: ${fuzz_binary}"
      exit 2
    fi
    run_args=("${fuzz_binary}" "-max_total_time=${FUZZ_MAX_TOTAL_TIME}" "-artifact_prefix=${target_dir}/libfuzzer/")
  else
    run_args=(cargo fuzz run "${target}" --fuzz-dir tests-fuzz -D -s none)
    if [[ "${fuzz_unstable}" == true ]]; then
      run_args+=(--features=unstable)
    fi
    run_args+=(-- "-max_total_time=${FUZZ_MAX_TOTAL_TIME}" "-artifact_prefix=${target_dir}/libfuzzer/")
  fi
  if GT_FUZZ_DUMP_DIR="${target_dir}/csv" \
    "${run_args[@]}" 2>&1 | tee "${target_dir}/fuzz.log"; then
    exit_code=0
    status="success"
    artifact_collection="not_requested"
  else
    exit_code=$?
    status="failure"
    artifact_collection="success"
  fi

  completed_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  completed_epoch="$(date +%s)"
  duration_secs=$((completed_epoch - started_epoch))

  if [[ "${status}" == failure ]]; then
    failed_targets+=("${target}")
    echo "::error title=Fuzz target failed::target=${target}, group=${FUZZ_GROUP}, exit_code=${exit_code}, duration_secs=${duration_secs}; collecting diagnostics"
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
  fi
  echo "::endgroup::"

  write_result \
    "${result_path}" "${target}" "${status}" \
    "${started_at}" "${completed_at}" "${duration_secs}" "${exit_code}" \
    "${prior_failure}" "${artifact_collection}"

  if [[ "${status}" == failure ]]; then
    prior_failure=true
  else
    echo "::notice title=Fuzz target completed::target=${target}, group=${FUZZ_GROUP}, status=success, duration_secs=${duration_secs}, after_prior_failure=${prior_failure}"
  fi
done

manifest="${FUZZ_ARTIFACT_ROOT}/manifest.json"
summary="${FUZZ_ARTIFACT_ROOT}/summary.md"
jq -s '.' "${result_paths[@]}" >"${manifest}"
group_completed_epoch="$(date +%s)"
group_elapsed_secs=$((group_completed_epoch - group_started_epoch))
total_count="$(jq 'length' "${manifest}")"
success_count="$(jq '[.[] | select(.status == "success")] | length' "${manifest}")"
failure_count="$(jq '[.[] | select(.status == "failure")] | length' "${manifest}")"
skipped_count="$(jq '[.[] | select(.status == "skipped_after_failure")] | length' "${manifest}")"
write_summary \
  "${manifest}" "${summary}" "${group_elapsed_secs}" \
  "${total_count}" "${success_count}" "${failure_count}" "${skipped_count}"
echo "::notice title=Fuzz group completed::group=${FUZZ_GROUP}, passed=${success_count}, failed=${failure_count}, skipped=${skipped_count}, elapsed_secs=${group_elapsed_secs}"

if [[ "${#failed_targets[@]}" -gt 0 ]]; then
  log "failed targets: ${failed_targets[*]}"
  exit 1
fi

log "all targets passed: ${targets[*]}"
