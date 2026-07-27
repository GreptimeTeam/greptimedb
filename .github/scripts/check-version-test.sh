#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHECK_VERSION_SCRIPT="${SCRIPT_DIR}/check-version.sh"
TEST_DIR="$(mktemp -d)"
MOCK_BIN="${TEST_DIR}/bin"

cleanup() {
  rm -rf "${TEST_DIR}"
}
trap cleanup EXIT

mkdir -p "${MOCK_BIN}"
cat > "${MOCK_BIN}/curl" <<'EOF'
#!/bin/bash
printf '%s\n' "${MOCK_CURL_RESPONSE}"
EOF
chmod +x "${MOCK_BIN}/curl"

cat > "${MOCK_BIN}/jq" <<'EOF'
#!/bin/bash
input=$(< /dev/stdin)
case "$*" in
  *.message*)
    printf 'null\n'
    ;;
  *.tag_name*)
    tag_name=${input#*\"tag_name\":\"}
    tag_name=${tag_name%%\"*}
    printf '%s\n' "${tag_name}"
    ;;
esac
EOF
chmod +x "${MOCK_BIN}/jq"

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

assert_output() {
  local test_name=$1
  local output_file=$2
  local expected_stable=$3
  local expected_latest=$4

  if ! grep -Fxq "is-current-version-stable=${expected_stable}" "${output_file}"; then
    fail "${test_name}: expected is-current-version-stable=${expected_stable}; output was: $(tr '\n' ' ' < "${output_file}")"
  fi

  if ! grep -Fxq "is-current-version-latest=${expected_latest}" "${output_file}"; then
    fail "${test_name}: expected is-current-version-latest=${expected_latest}; output was: $(tr '\n' ' ' < "${output_file}")"
  fi
}

run_case() {
  local test_name=$1
  local current_version=$2
  local latest_version=$3
  local expected_stable=$4
  local expected_latest=$5
  local output_file="${TEST_DIR}/${test_name}.output"

  : > "${output_file}"
  MOCK_CURL_RESPONSE="{\"tag_name\":\"${latest_version}\"}" \
    GITHUB_OUTPUT="${output_file}" PATH="${MOCK_BIN}:${PATH}" \
    "${CHECK_VERSION_SCRIPT}" "${current_version}" >/dev/null
  assert_output "${test_name}" "${output_file}" "${expected_stable}" "${expected_latest}"
}

run_case stable-newer v1.2.4 v1.2.3 true true
run_case beta-newer-base v1.2.4-beta.1 v1.2.3 false true
run_case beta-against-same-stable v1.2.3-beta.1 v1.2.3 false false
run_case rc v1.2.4-rc.1 v1.2.3 false true
run_case nightly-suffix v1.2.4-nightly-20250101 v1.2.3 false true
run_case build-suffix v1.2.4-build.1 v1.2.3 false true
run_case invalid-version v1.2 v1.2.3 false false

empty_output="${TEST_DIR}/empty-input.output"
: > "${empty_output}"
if MOCK_CURL_RESPONSE='{"tag_name":"v1.2.3"}' \
  GITHUB_OUTPUT="${empty_output}" PATH="${MOCK_BIN}:${PATH}" \
  "${CHECK_VERSION_SCRIPT}" "" >/dev/null 2>&1; then
  fail "empty-input: expected check-version.sh to fail"
fi
if [ -s "${empty_output}" ]; then
  fail "empty-input: expected no output; output was: $(tr '\n' ' ' < "${empty_output}")"
fi

printf 'check-version tests passed\n'
