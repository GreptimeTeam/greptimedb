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

# Runs the Prometheus conformance comparison against a local GreptimeDB.
#
# The server is started and stopped inside this script on purpose: a compatibility run must leave no
# server behind, and CI executes the whole comparison as one step anyway.
#
# Usage: narthex-prometheus-compat.sh <greptime-binary> <oracle-binary> <cases-dir> <work-dir> [--native]
set -euo pipefail

GREPTIME_BIN="${1:?usage: narthex-prometheus-compat.sh <greptime> <oracle> <cases-dir> <work-dir> [--native]}"
ORACLE_BIN="${2:?missing oracle}"
CASES_DIR="${3:?missing cases dir}"
WORK_DIR="${4:?missing work dir}"
MODE="${5:-}"

HTTP_PORT="${NARTHEX_COMPAT_HTTP_PORT:-14000}"
GRPC_PORT="${NARTHEX_COMPAT_GRPC_PORT:-14001}"

mkdir -p "$WORK_DIR"
SERVER_LOG="$WORK_DIR/greptime.log"
CONFIG="$WORK_DIR/config.toml"

cat >"$CONFIG" <<TOML
mode = 'standalone'
[query]
experimental_native_promql = true
[storage]
type = 'File'
data_home = '$WORK_DIR/data'
[http]
addr = '127.0.0.1:$HTTP_PORT'
bind_addr = '127.0.0.1:$HTTP_PORT'
[grpc]
addr = '127.0.0.1:$GRPC_PORT'
bind_addr = '127.0.0.1:$GRPC_PORT'
TOML

rm -rf "$WORK_DIR/data"
"$GREPTIME_BIN" --log-level=warn standalone start -c "$CONFIG" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
stop_server() {
  if kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
}
trap stop_server EXIT

for _ in $(seq 1 60); do
  if curl -sS -m 2 -o /dev/null "http://127.0.0.1:$HTTP_PORT/health" 2>/dev/null; then
    break
  fi
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    echo "error: GreptimeDB exited during startup" >&2
    tail -40 "$SERVER_LOG" >&2
    exit 1
  fi
  sleep 1
done
if ! curl -sS -m 2 -o /dev/null "http://127.0.0.1:$HTTP_PORT/health" 2>/dev/null; then
  echo "error: GreptimeDB did not become healthy" >&2
  tail -40 "$SERVER_LOG" >&2
  exit 1
fi

ARGS=(
  --cases "$CASES_DIR"
  --oracle "$ORACLE_BIN"
  --sql-url "http://127.0.0.1:$HTTP_PORT/v1/sql"
  --prom-url "http://127.0.0.1:$HTTP_PORT"
  --report "$WORK_DIR/report.json"
)
if [[ "$MODE" == "--native" ]]; then
  ARGS+=(--native)
fi

set +e
python3 "$(dirname "$0")/narthex-prometheus-compat.py" "${ARGS[@]}" 2>&1 | tee "$WORK_DIR/compat.log"
STATUS="${PIPESTATUS[0]}"
set -e

stop_server
trap - EXIT
exit "$STATUS"
