#!/usr/bin/env bash

set -euo pipefail

GT_FUZZ_NS="${GT_FUZZ_NS:-my-greptimedb}"
GT_FUZZ_CLUSTER="${GT_FUZZ_CLUSTER:-my-greptimedb}"
GT_MONITOR_HTTP_LOCAL_PORT="${GT_MONITOR_HTTP_LOCAL_PORT:-14000}"
GT_MONITOR_ARTIFACT_DIR="${GT_MONITOR_ARTIFACT_DIR:-/tmp/fuzz-monitor-dumps}"
GT_MONITOR_SERVER_EXPORT_DIR="${GT_MONITOR_SERVER_EXPORT_DIR:-/tmp/gt-monitor-dump}"

MONITOR_SERVICE="${GT_FUZZ_CLUSTER}-monitor-standalone"
MONITOR_POD="${GT_FUZZ_CLUSTER}-monitor-standalone-0"

PORT_FORWARD_LOG="${GT_MONITOR_ARTIFACT_DIR}/port-forward.log"
SQL_LOG="${GT_MONITOR_ARTIFACT_DIR}/sql.log"
COPY_LOG="${GT_MONITOR_ARTIFACT_DIR}/copy.log"
STATE_LOG="${GT_MONITOR_ARTIFACT_DIR}/state.log"

log() {
  printf '[collect-fuzz-monitor-artifacts] %s\n' "$*" | tee -a "${STATE_LOG}"
}

cleanup() {
  if [ -n "${PORT_FORWARD_PID:-}" ]; then
    kill "${PORT_FORWARD_PID}" >/dev/null 2>&1 || true
  fi
}

exec_sql() {
  local db="$1"
  local sql="$2"
  local output

  log "execute sql on db=${db}: ${sql}"

  output="$(curl -sS -G "http://127.0.0.1:${GT_MONITOR_HTTP_LOCAL_PORT}/v1/sql" \
    --data-urlencode "db=${db}" \
    --data-urlencode "sql=${sql}")"

  printf '%s\n' "${output}" >>"${SQL_LOG}"

  if printf '%s' "${output}" | grep -q '"error"'; then
    log "sql failed: ${output}"
    return 1
  fi
}

export_show_create_table() {
  local table="$1"
  local output_file="${GT_MONITOR_ARTIFACT_DIR}/${table}.show_create_table.sql"
  local output

  log "export SHOW CREATE TABLE for ${table}"
  output="$(curl -sS -G "http://127.0.0.1:${GT_MONITOR_HTTP_LOCAL_PORT}/v1/sql" \
    --data-urlencode "db=public" \
    --data-urlencode "sql=SHOW CREATE TABLE ${table};")"

  printf '%s\n' "${output}" >>"${SQL_LOG}"

  if printf '%s' "${output}" | grep -q '"error"'; then
    log "show create table failed for ${table}: ${output}"
    return 1
  fi

  printf '%s' "${output}" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d["output"][0]["records"]["rows"][0][1])' >"${output_file}"
}

mkdir -p "${GT_MONITOR_ARTIFACT_DIR}"
rm -rf "${GT_MONITOR_ARTIFACT_DIR:?}/"*

{
  echo "namespace=${GT_FUZZ_NS}"
  echo "cluster=${GT_FUZZ_CLUSTER}"
  echo "service=${MONITOR_SERVICE}"
  echo "pod=${MONITOR_POD}"
  echo "http_port=${GT_MONITOR_HTTP_LOCAL_PORT}"
} >"${STATE_LOG}"

log "start port-forward service/${MONITOR_SERVICE} in namespace ${GT_FUZZ_NS}"
kubectl port-forward "service/${MONITOR_SERVICE}" "${GT_MONITOR_HTTP_LOCAL_PORT}:4000" -n "${GT_FUZZ_NS}" >"${PORT_FORWARD_LOG}" 2>&1 &
PORT_FORWARD_PID="$!"
trap cleanup EXIT
log "port-forward pid=${PORT_FORWARD_PID}"

log "wait for port-forward bootstrap"
for i in {1..30}; do
  if curl -s --fail "http://127.0.0.1:${GT_MONITOR_HTTP_LOCAL_PORT}/health" &> /dev/null; then
    log "port-forward is ready"
    break
  fi
  if [ "$i" -eq 30 ]; then
    log "Timed out waiting for port-forward to be ready."
    exit 1
  fi
  sleep 1
done

log "ensure export dir exists in pod ${MONITOR_POD}"
kubectl exec -n "${GT_FUZZ_NS}" "${MONITOR_POD}" -- mkdir -p "${GT_MONITOR_SERVER_EXPORT_DIR}" >>"${COPY_LOG}" 2>&1

export_show_create_table "_gt_logs"
export_show_create_table "opentelemetry_traces"

exec_sql "public" "COPY _gt_logs TO '${GT_MONITOR_SERVER_EXPORT_DIR}/_gt_logs.parquet' WITH (FORMAT='parquet');"
exec_sql "public" "COPY opentelemetry_traces TO '${GT_MONITOR_SERVER_EXPORT_DIR}/opentelemetry_traces.parquet' WITH (FORMAT='parquet');"

log "copy _gt_logs.parquet from pod"
kubectl cp "${GT_FUZZ_NS}/${MONITOR_POD}:${GT_MONITOR_SERVER_EXPORT_DIR}/_gt_logs.parquet" "${GT_MONITOR_ARTIFACT_DIR}/_gt_logs.parquet" >>"${COPY_LOG}" 2>&1
log "copy opentelemetry_traces.parquet from pod"
kubectl cp "${GT_FUZZ_NS}/${MONITOR_POD}:${GT_MONITOR_SERVER_EXPORT_DIR}/opentelemetry_traces.parquet" "${GT_MONITOR_ARTIFACT_DIR}/opentelemetry_traces.parquet" >>"${COPY_LOG}" 2>&1

ls -la "${GT_MONITOR_ARTIFACT_DIR}" >>"${STATE_LOG}" 2>&1

log "artifacts collected under ${GT_MONITOR_ARTIFACT_DIR}"
