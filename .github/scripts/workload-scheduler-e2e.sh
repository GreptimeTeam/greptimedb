#!/usr/bin/env bash
# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

BIN="${GREPTIMEDB_BIN:-${1:-./bins/greptime}}"
RUN_DIR="${WORKLOAD_SCHEDULER_RUN_DIR:-${TMPDIR:-/tmp}/sqlness-workload-scheduler-e2e}"
TIMEOUT="${WORKLOAD_SCHEDULER_READINESS_TIMEOUT:-60}"
PIDS=()
rm -rf -- "${RUN_DIR}"
mkdir -p "${RUN_DIR}"
SQL_LOG="${RUN_DIR}/sql.log"
STATE_LOG="${RUN_DIR}/scheduler-state.log"
: >"${SQL_LOG}"; : >"${STATE_LOG}"

cleanup() {
  set +e
  for pid in "${PIDS[@]}"; do kill -TERM -- "-${pid}" 2>/dev/null || true; done
  sleep 1
  for pid in "${PIDS[@]}"; do kill -KILL -- "-${pid}" 2>/dev/null || true; wait "${pid}" 2>/dev/null || true; done
}
trap cleanup EXIT
[[ -x "${BIN}" ]] || { echo "greptime binary is not executable: ${BIN}" >&2; exit 1; }

# Hold all sockets while selecting them; selections are distinct, but a close-to-bind race remains.
mapfile -t PORTS < <(python3 - <<'PY'
import socket
s=[]
try:
 for _ in range(8):
  x=socket.socket(); x.bind(("127.0.0.1",0)); s.append(x)
 print("\n".join(str(x.getsockname()[1]) for x in s))
finally:
 for x in s: x.close()
PY
)
(( ${#PORTS[@]} == 8 )) || { echo "failed to allocate loopback ports" >&2; exit 1; }
META_RPC="127.0.0.1:${PORTS[0]}"; META_HTTP="http://127.0.0.1:${PORTS[1]}"
DATA_RPC="127.0.0.1:${PORTS[2]}"; DATA_HTTP="http://127.0.0.1:${PORTS[3]}"
FRONT_RPC="127.0.0.1:${PORTS[4]}"; FRONT_HTTP="http://127.0.0.1:${PORTS[5]}"
MYSQL="127.0.0.1:${PORTS[6]}"; POSTGRES="127.0.0.1:${PORTS[7]}"

start() {
  local name="$1"; shift; local dir="${RUN_DIR}/${name}"; mkdir -p "${dir}"
  echo "starting ${name}; logs retained in ${dir}" >&2
  setsid "${BIN}" "$@" >"${dir}/stdout.log" 2>"${dir}/stderr.log" & PIDS+=("$!")
}
wait_health() {
  local name="$1" url="$2" deadline=$((SECONDS + TIMEOUT))
  until curl -fsS --max-time 2 "${url}/health" >/dev/null; do
    (( SECONDS < deadline )) || { echo "timed out waiting for ${name} health" >&2; return 1; }
    sleep 1
  done
}
wait_lease() {
  local deadline=$((SECONDS + TIMEOUT))
  until curl -fsS --max-time 2 "${META_HTTP}/admin/node-lease" | jq -e 'type == "array" and length > 0' >/dev/null; do
    (( SECONDS < deadline )) || { echo "timed out waiting for datanode lease" >&2; return 1; }
    sleep 1
  done
}
state() {
  local out; out="$(curl -fsS --max-time 10 "${DATA_HTTP}/debug/workload_scheduler")"
  printf '%s\n' "${out}" >>"${STATE_LOG}"; printf '%s' "${out}"
}
sql() {
  local statement="$1" out
  out="$(curl -fsS --max-time 30 --data-urlencode "sql=${statement}" --data-urlencode 'db=public' "${FRONT_HTTP}/v1/sql")"
  printf '%s\n%s\n' "${statement}" "${out}" >>"${SQL_LOG}"
  jq -e '(.error // null) == null' <<<"${out}" >/dev/null || { echo "SQL failed: ${statement}" >&2; return 1; }
  printf '%s' "${out}"
}
assert_status() {
  local status="$1" enabled="$2" query_weight="$3" write_weight="$4"
  jq -e --argjson enabled "${enabled}" --argjson query_weight "${query_weight}" \
    --argjson write_weight "${write_weight}" \
    '.enabled == $enabled and .query.weight == $query_weight and .write.weight == $write_weight' \
    <<<"${status}" >/dev/null || { echo "unexpected scheduler status: ${status}" >&2; return 1; }
}
assert_metrics() {
  local metrics
  metrics="$(curl -fsS --max-time 10 "${DATA_HTTP}/metrics")"
  grep -Eq '^greptime_workload_scheduler_enabled 1$' <<<"${metrics}"
  for class in query write; do
    expected_weight=3
    [[ "${class}" == write ]] && expected_weight=7
    grep -Eq "^greptime_workload_scheduler_weight\\{workload=\\\"${class}\\\"\\} ${expected_weight}$" <<<"${metrics}"
    grep -Eq "^greptime_workload_scheduler_polls_total\\{workload=\\\"${class}\\\"\\} [0-9.e+-]+$" <<<"${metrics}"
  done
}
metric_polls() {
  local metrics
  metrics="$(curl -fsS --max-time 10 "${DATA_HTTP}/metrics")"
  awk '
    $1 == "greptime_workload_scheduler_polls_total{workload=\"query\"}" {
      workload = "query"
      count[workload]++
      value[workload] = $2
      if (NF != 2 || $2 !~ /^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$/) {
        invalid[workload] = $2
      }
    }
    $1 == "greptime_workload_scheduler_polls_total{workload=\"write\"}" {
      workload = "write"
      count[workload]++
      value[workload] = $2
      if (NF != 2 || $2 !~ /^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?$/) {
        invalid[workload] = $2
      }
    }
    END {
      failed = 0
      for (i = 1; i <= 2; i++) {
        workload = (i == 1 ? "query" : "write")
        if (count[workload] == 0) {
          printf "metric_polls: missing %s Prometheus sample\n", workload > "/dev/stderr"
          failed = 1
        } else if (count[workload] != 1) {
          printf "metric_polls: expected exactly one %s Prometheus sample, found %d\n", workload, count[workload] > "/dev/stderr"
          failed = 1
        }
        if (workload in invalid) {
          printf "metric_polls: invalid %s Prometheus numeric token: %s\n", workload, invalid[workload] > "/dev/stderr"
          failed = 1
        }
      }
      if (failed) exit 1
      print value["query"]
      print value["write"]
    }
  ' <<<"${metrics}"
}
assert_metric_polls() {
  local before="$1" after="$2" phase="$3" comparison="$4"
  local -a before_values after_values
  mapfile -t before_values <<<"${before}"
  mapfile -t after_values <<<"${after}"
  for i in 0 1; do
    local class=query
    [[ "${i}" == 1 ]] && class=write
    awk -v before="${before_values[${i}]}" -v after="${after_values[${i}]}" \
      "BEGIN { exit !(after ${comparison} before) }" || {
        echo "${phase}: ${class} Prometheus polls did not satisfy ${comparison}" >&2; return 1;
      }
  done
}
assert_values() {
  local response="$1" expected="$2"
  jq -e --argjson expected "${expected}" '[.output[0].records.rows[][0]] == $expected' <<<"${response}" >/dev/null
}

mkdir -p "${RUN_DIR}/datanode"
cat >"${RUN_DIR}/datanode/datanode.toml" <<EOF
[runtime.experimental_workload_scheduler]
enable = true
query_weight = 1
write_weight = 1
[wal]
provider = "raft_engine"
dir = "${RUN_DIR}/datanode/wal"
[storage]
data_home = "${RUN_DIR}/datanode/data"
EOF
start metasrv metasrv start --grpc-bind-addr "${META_RPC}" --grpc-server-addr "${META_RPC}" \
  --http-addr "${META_HTTP#http://}" --backend memory-store --enable-region-failover false \
  --data-home "${RUN_DIR}/metasrv" --log-dir "${RUN_DIR}/metasrv/logs"
wait_health metasrv "${META_HTTP}"
start datanode datanode start --config-file "${RUN_DIR}/datanode/datanode.toml" --node-id 1 \
  --grpc-bind-addr "${DATA_RPC}" --grpc-server-addr "${DATA_RPC}" --http-addr "${DATA_HTTP#http://}" \
  --metasrv-addrs "${META_RPC}" --data-home "${RUN_DIR}/datanode" --log-dir "${RUN_DIR}/datanode/logs"
wait_health datanode "${DATA_HTTP}"; wait_lease
start frontend frontend start --metasrv-addrs "${META_RPC}" --http-addr "${FRONT_HTTP#http://}" \
  --mysql-addr "${MYSQL}" --postgres-addr "${POSTGRES}" --grpc-bind-addr "${FRONT_RPC}" \
  --grpc-server-addr "${FRONT_RPC}" --log-dir "${RUN_DIR}/frontend/logs"
wait_health frontend "${FRONT_HTTP}"

assert_status "$(state)" true 1 1
curl -fsS --max-time 10 -X POST -H 'Content-Type: application/json' \
  --data '{"query":3,"write":7}' "${DATA_HTTP}/debug/workload_scheduler/weights" >/dev/null
assert_status "$(state)" true 3 7
assert_metrics
sql 'CREATE TABLE scheduler_e2e (ts TIMESTAMP TIME INDEX, v INT)' >/dev/null
before="$(metric_polls)"; sql 'INSERT INTO scheduler_e2e VALUES (1000, 10), (2000, 20)' >/dev/null
assert_values "$(sql 'SELECT v FROM scheduler_e2e ORDER BY v')" '[10,20]'; after="$(metric_polls)"
assert_status "$(state)" true 3 7; assert_metric_polls "${before}" "${after}" 'enabled phase' '>'

curl -fsS --max-time 10 -X POST -H 'Content-Type: application/json' --data false "${DATA_HTTP}/debug/workload_scheduler/enabled" >/dev/null
before="$(metric_polls)"; assert_status "$(state)" false 3 7
sql 'INSERT INTO scheduler_e2e VALUES (3000, 30)' >/dev/null
assert_values "$(sql 'SELECT v FROM scheduler_e2e ORDER BY v')" '[10,20,30]'; after="$(metric_polls)"
assert_status "$(state)" false 3 7; assert_metric_polls "${before}" "${after}" 'disabled phase' '=='

echo "distributed workload scheduler E2E passed; logs retained in ${RUN_DIR}"
