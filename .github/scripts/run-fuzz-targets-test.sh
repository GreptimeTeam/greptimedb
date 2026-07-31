#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
RUNNER="${SCRIPT_DIR}/run-fuzz-targets.sh"
COLLECTOR="${SCRIPT_DIR}/collect-fuzz-target-artifacts.sh"

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

assert_eq() {
  local expected="$1"
  local actual="$2"
  local message="$3"
  [[ "${expected}" == "${actual}" ]] || \
    fail "${message}: expected=${expected@Q}, actual=${actual@Q}"
}

assert_file() {
  [[ -f "$1" ]] || fail "expected file: $1"
}

new_fixture() {
  fixture="$(mktemp -d -t fuzz-runner-test.XXXXXX)"
  mkdir -p "${fixture}/bin"

  cat >"${fixture}/bin/cargo" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\t%s\t%s\n' "${GT_FUZZ_DUMP_DIR}" "$*" "${MOCK_CARGO_MARKER:-}" >>"${MOCK_CARGO_LOG}"
target="$3"
case " ${MOCK_FAIL_TARGETS:-} " in
  *" ${target} "*) exit 17 ;;
esac
EOF

  cat >"${fixture}/collector" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\t%s\t%s\n' "$1" "$2" "${FUZZ_COLLECT_CLUSTER_ARTIFACTS}" >>"${MOCK_COLLECTOR_LOG}"
mkdir -p "$2/mock-artifacts"
printf 'collected\n' >"$2/mock-artifacts/state.txt"
EOF

  chmod +x "${fixture}/bin/cargo" "${fixture}/collector"
}

cleanup_fixture() {
  rm -rf "${fixture}"
}

run_fixture() {
  local targets="$1"
  local fail_fast="$2"
  local unstable="$3"
  local fail_targets="$4"
  local artifact_root="${fixture}/artifacts"
  local -a runner_env=(
    "PATH=${fixture}/bin:${PATH}"
    "MOCK_CARGO_LOG=${fixture}/cargo.log"
    "MOCK_COLLECTOR_LOG=${fixture}/collector.log"
    "MOCK_FAIL_TARGETS=${fail_targets}"
    "MOCK_CARGO_MARKER=fixture"
    "FUZZ_TARGETS=${targets}"
    "FUZZ_GROUP=test-group"
    "FUZZ_MAX_TOTAL_TIME=120"
    "FUZZ_UNSTABLE=${unstable}"
    "FUZZ_COLLECT_CLUSTER_ARTIFACTS=true"
    "FUZZ_ARTIFACT_ROOT=${artifact_root}"
    "FUZZ_ARTIFACT_COLLECTOR=${fixture}/collector"
    "GITHUB_SHA=deadbeef"
    "GITHUB_STEP_SUMMARY=${fixture}/github-step-summary.md"
  )
  if [[ "${fail_fast}" != unset ]]; then
    runner_env+=("FUZZ_FAIL_FAST=${fail_fast}")
  fi

  set +e
  env -u FUZZ_FAIL_FAST "${runner_env[@]}" \
    "${RUNNER}" >"${fixture}/stdout.log" 2>&1
  fixture_status=$?
  set -e
}

test_successful_targets_run_in_order() {
  new_fixture
  run_fixture $'fuzz_create_table\nfuzz_insert' false true ""

  assert_eq 0 "${fixture_status}" "successful run status"
  assert_eq 2 "$(wc -l <"${fixture}/cargo.log" | tr -d ' ')" "cargo invocation count"
  assert_eq \
    $'fuzz_create_table\nfuzz_insert' \
    "$(awk -F '\t' '{print $2}' "${fixture}/cargo.log" | sed -E 's/^fuzz run ([^ ]+).*/\1/')" \
    "target order"
  grep -q -- '--features=unstable' "${fixture}/cargo.log" || fail "unstable feature missing"
  grep -q -- '-max_total_time=120' "${fixture}/cargo.log" || fail "fuzz time missing"
  grep -q -- '-artifact_prefix=.*/targets/fuzz_create_table/libfuzzer/' "${fixture}/cargo.log" || \
    fail "target-scoped libFuzzer prefix missing"
  assert_eq success "$(jq -r '.[0].status' "${fixture}/artifacts/manifest.json")" "first status"
  assert_eq success "$(jq -r '.[1].status' "${fixture}/artifacts/manifest.json")" "second status"
  assert_file "${fixture}/artifacts/summary.md"
  grep -q 'Policy: `continue-after-failure`' "${fixture}/artifacts/summary.md" || \
    fail "group policy missing from summary"
  grep -q 'Results: \*\*2 passed\*\*, \*\*0 failed\*\*, \*\*0 skipped\*\* / 2 total' \
    "${fixture}/artifacts/summary.md" || fail "group counts missing from summary"
  grep -q 'title=Fuzz target completed' "${fixture}/stdout.log" || \
    fail "target completion notice missing"
  grep -q 'title=Fuzz group completed' "${fixture}/stdout.log" || \
    fail "group completion notice missing"
  assert_file "${fixture}/github-step-summary.md"
  cleanup_fixture
}

test_fail_fast_stops_after_first_failure() {
  new_fixture
  run_fixture $'fuzz_create_table\nfuzz_insert\nfuzz_alter_table' true false "fuzz_insert"

  assert_eq 1 "${fixture_status}" "fail-fast run status"
  assert_eq 2 "$(wc -l <"${fixture}/cargo.log" | tr -d ' ')" "fail-fast cargo count"
  assert_eq 1 "$(wc -l <"${fixture}/collector.log" | tr -d ' ')" "collector count"
  assert_eq failure "$(jq -r '.[1].status' "${fixture}/artifacts/manifest.json")" "failed status"
  assert_eq skipped_after_failure "$(jq -r '.[2].status' "${fixture}/artifacts/manifest.json")" "skipped status"
  assert_eq true "$(jq -r '.[2].after_prior_failure' "${fixture}/artifacts/manifest.json")" "skipped provenance"
  assert_file "${fixture}/artifacts/targets/fuzz_insert/mock-artifacts/state.txt"
  grep -q 'title=Fuzz target failed.*target=fuzz_insert' "${fixture}/stdout.log" || \
    fail "target failure annotation missing"
  grep -q 'title=Fuzz target skipped.*target=fuzz_alter_table' "${fixture}/stdout.log" || \
    fail "skipped target annotation missing"
  grep -q '### Reproduce failed targets' "${fixture}/artifacts/summary.md" || \
    fail "reproduction section missing"
  grep -q 'cargo fuzz run fuzz_insert' "${fixture}/artifacts/summary.md" || \
    fail "reproduction command missing"
  cleanup_fixture
}

test_fail_fast_defaults_to_true() {
  new_fixture
  run_fixture $'fuzz_create_table\nfuzz_insert\nfuzz_alter_table' unset true "fuzz_insert"

  assert_eq 1 "${fixture_status}" "default fail-fast run status"
  assert_eq 2 "$(wc -l <"${fixture}/cargo.log" | tr -d ' ')" "default fail-fast cargo count"
  assert_eq skipped_after_failure \
    "$(jq -r '.[2].status' "${fixture}/artifacts/manifest.json")" \
    "default fail-fast skipped status"
  grep -q -- '--features=unstable' "${fixture}/artifacts/summary.md" || \
    fail "unstable reproduction argument missing"
  cleanup_fixture
}

test_non_fail_fast_marks_later_targets() {
  new_fixture
  run_fixture $'fuzz_create_table\nfuzz_insert' false false "fuzz_create_table"

  assert_eq 1 "${fixture_status}" "non-fail-fast run status"
  assert_eq 2 "$(wc -l <"${fixture}/cargo.log" | tr -d ' ')" "non-fail-fast cargo count"
  assert_eq true "$(jq -r '.[1].after_prior_failure' "${fixture}/artifacts/manifest.json")" "later target provenance"
  assert_eq success "$(jq -r '.[1].status' "${fixture}/artifacts/manifest.json")" "later target status"
  cleanup_fixture
}

test_invalid_configuration_fails_before_cargo() {
  new_fixture
  run_fixture 'fuzz target' true false ""

  assert_eq 2 "${fixture_status}" "invalid target status"
  [[ ! -e "${fixture}/cargo.log" ]] || fail "cargo ran for invalid target"
  cleanup_fixture

  new_fixture
  run_fixture 'fuzz_create_table' sometimes false ""

  assert_eq 2 "${fixture_status}" "invalid fail-fast status"
  [[ ! -e "${fixture}/cargo.log" ]] || fail "cargo ran for invalid fail-fast"
  cleanup_fixture
}

test_collector_keeps_target_scopes_separate() {
  new_fixture
  service_log="${fixture}/greptime.log"
  printf 'service output\n' >"${service_log}"

  FUZZ_COLLECT_CLUSTER_ARTIFACTS=false \
    FUZZ_SERVICE_LOG="${service_log}" \
    "${COLLECTOR}" fuzz_insert "${fixture}/target-a"
  FUZZ_COLLECT_CLUSTER_ARTIFACTS=false \
    FUZZ_SERVICE_LOG="${service_log}" \
    "${COLLECTOR}" fuzz_alter_table "${fixture}/target-b"

  assert_file "${fixture}/target-a/service/greptime.log"
  assert_file "${fixture}/target-b/service/greptime.log"
  assert_file "${fixture}/target-a/artifact-collection.log"
  assert_file "${fixture}/target-b/artifact-collection.log"
  cleanup_fixture
}

test_cluster_collector_honors_target_scope_and_namespace() {
  new_fixture

  cat >"${fixture}/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'kubectl %s\n' "$*" >>"${MOCK_COMMAND_LOG}"
printf 'mock kubectl output\n'
EOF
  cat >"${fixture}/bin/kind" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf 'kind %s\n' "$*" >>"${MOCK_COMMAND_LOG}"
EOF
  cat >"${fixture}/monitor-collector" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\t%s\t%s\t%s\n' \
  "${GT_FUZZ_NS}" "${GT_FUZZ_CLUSTER}" "${GT_MONITOR_ARTIFACT_DIR}" \
  "${GT_MONITOR_SERVER_EXPORT_DIR}" \
  >>"${MOCK_MONITOR_LOG}"
printf 'monitor output\n' >"${GT_MONITOR_ARTIFACT_DIR}/monitor.txt"
EOF
  chmod +x "${fixture}/bin/kubectl" "${fixture}/bin/kind"

  PATH="${fixture}/bin:${PATH}" \
    MOCK_COMMAND_LOG="${fixture}/commands.log" \
    MOCK_MONITOR_LOG="${fixture}/monitor.log" \
    FUZZ_COLLECT_CLUSTER_ARTIFACTS=true \
    FUZZ_MONITOR_COLLECTOR="${fixture}/monitor-collector" \
    GT_FUZZ_NS=test-namespace \
    GT_FUZZ_CLUSTER=test-cluster \
    "${COLLECTOR}" fuzz_insert "${fixture}/target"

  grep -q 'kubectl describe pod -n test-namespace' "${fixture}/commands.log" || \
    fail "collector ignored configured namespace"
  grep -q "kind export logs ${fixture}/target/kind" "${fixture}/commands.log" || \
    fail "kind logs are not target-scoped"
  assert_eq \
    "test-namespace"$'\t'"test-cluster"$'\t'"${fixture}/target/monitor"$'\t'"/tmp/gt-monitor-dump/fuzz_insert" \
    "$(cat "${fixture}/monitor.log")" \
    "monitor collector scope"
  assert_file "${fixture}/target/kubernetes/nodes.txt"
  assert_file "${fixture}/target/kubernetes/pods.txt"
  assert_file "${fixture}/target/kubernetes/events.txt"
  assert_file "${fixture}/target/monitor/monitor.txt"
  cleanup_fixture
}

test_setup_failure_writes_artifact_contract() {
  new_fixture
  artifact_root="${fixture}/artifacts"
  setup_summary="${fixture}/setup-step-summary.md"

  FUZZ_COLLECT_CLUSTER_ARTIFACTS=false \
    FUZZ_GROUP=setup-group \
    GITHUB_SHA=deadbeef \
    GITHUB_STEP_SUMMARY="${setup_summary}" \
    "${COLLECTOR}" setup "${artifact_root}/targets/setup"

  assert_file "${artifact_root}/targets/setup/result.json"
  assert_file "${artifact_root}/manifest.json"
  assert_file "${artifact_root}/summary.md"
  assert_file "${setup_summary}"
  assert_eq setup-group "$(jq -r '.[0].group' "${artifact_root}/manifest.json")" \
    "setup manifest group"
  assert_eq setup "$(jq -r '.[0].target' "${artifact_root}/manifest.json")" \
    "setup manifest target"
  assert_eq failure "$(jq -r '.[0].status' "${artifact_root}/manifest.json")" \
    "setup manifest status"
  assert_eq setup "$(jq -r '.[0].phase' "${artifact_root}/manifest.json")" \
    "setup manifest phase"
  grep -q 'Failure phase: `setup`' "${artifact_root}/summary.md" || \
    fail "setup failure phase missing from summary"
  cmp -s "${artifact_root}/summary.md" "${setup_summary}" || \
    fail "setup summary was not appended to the job summary"
  cleanup_fixture
}

test_setup_failure_keeps_manifest_when_collection_fails() {
  new_fixture
  artifact_root="${fixture}/artifacts"

  cat >"${fixture}/bin/kubectl" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
  cat >"${fixture}/bin/kind" <<'EOF'
#!/usr/bin/env bash
exit 1
EOF
  chmod +x "${fixture}/bin/kubectl" "${fixture}/bin/kind"

  set +e
  PATH="${fixture}/bin:${PATH}" \
    FUZZ_COLLECT_CLUSTER_ARTIFACTS=true \
    FUZZ_GROUP=setup-group \
    FUZZ_MONITOR_COLLECTOR="${fixture}/missing-monitor-collector" \
    GITHUB_STEP_SUMMARY="${fixture}/setup-step-summary.md" \
    "${COLLECTOR}" setup "${artifact_root}/targets/setup" \
    >"${fixture}/collector-stdout.log" 2>&1
  collector_status=$?
  set -e

  assert_eq 1 "${collector_status}" "failed setup collection status"
  assert_file "${artifact_root}/manifest.json"
  assert_eq failed "$(jq -r '.[0].artifact_collection' "${artifact_root}/manifest.json")" \
    "failed setup collection marker"
  cleanup_fixture
}

test_successful_targets_run_in_order
test_fail_fast_stops_after_first_failure
test_fail_fast_defaults_to_true
test_non_fail_fast_marks_later_targets
test_invalid_configuration_fails_before_cargo
test_collector_keeps_target_scopes_separate
test_cluster_collector_honors_target_scope_and_namespace
test_setup_failure_writes_artifact_contract
test_setup_failure_keeps_manifest_when_collection_fails

printf 'All fuzz orchestration script tests passed.\n'
