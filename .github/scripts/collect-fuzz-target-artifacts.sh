#!/usr/bin/env bash

set -uo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "Usage: $0 <target> <target-artifact-dir>" >&2
  exit 2
fi

target="$1"
target_dir="$2"
namespace="${GT_FUZZ_NS:-my-greptimedb}"
collection_log="${target_dir}/artifact-collection.log"
monitor_collector="${FUZZ_MONITOR_COLLECTOR:-.github/scripts/collect-fuzz-monitor-artifacts.sh}"
failed=false

mkdir -p "${target_dir}"
exec > >(tee -a "${collection_log}") 2>&1

log() {
  printf '[collect-fuzz-target-artifacts] %s\n' "$*"
}

collect() {
  local name="$1"
  shift

  log "collect ${name}"
  if "$@"; then
    return
  fi

  log "failed to collect ${name}"
  failed=true
}

log "collect artifacts for target ${target} under ${target_dir}"

if [[ -n "${FUZZ_SERVICE_LOG:-}" && -f "${FUZZ_SERVICE_LOG}" ]]; then
  mkdir -p "${target_dir}/service"
  collect "service log" cp "${FUZZ_SERVICE_LOG}" "${target_dir}/service/greptime.log"
fi

case "${FUZZ_COLLECT_CLUSTER_ARTIFACTS:-false}" in
  false)
    ;;
  true)
    mkdir -p "${target_dir}/kind" "${target_dir}/kubernetes" "${target_dir}/monitor"
    collect "Kubernetes nodes" \
      bash -c 'kubectl describe nodes >"$1" 2>&1' _ \
      "${target_dir}/kubernetes/nodes.txt"
    collect "Kubernetes pods" \
      bash -c 'kubectl get pods -A -o wide >"$1" 2>&1 && kubectl describe pod -n "$2" >>"$1" 2>&1' _ \
      "${target_dir}/kubernetes/pods.txt" "${namespace}"
    collect "Kubernetes events" \
      bash -c 'kubectl get events -A --sort-by=.lastTimestamp >"$1" 2>&1' _ \
      "${target_dir}/kubernetes/events.txt"
    collect "Kind logs" kind export logs "${target_dir}/kind"
    collect "monitor dumps" \
      env \
        GT_FUZZ_NS="${namespace}" \
        GT_FUZZ_CLUSTER="${GT_FUZZ_CLUSTER:-my-greptimedb}" \
        GT_MONITOR_HTTP_LOCAL_PORT="${GT_MONITOR_HTTP_LOCAL_PORT:-14000}" \
        GT_MONITOR_ARTIFACT_DIR="${target_dir}/monitor" \
        GT_MONITOR_SERVER_EXPORT_DIR="${GT_MONITOR_SERVER_EXPORT_DIR:-/tmp/gt-monitor-dump/${target}}" \
        bash "${monitor_collector}"
    ;;
  *)
    log "FUZZ_COLLECT_CLUSTER_ARTIFACTS must be true or false"
    exit 2
    ;;
esac

if [[ "${failed}" == true ]]; then
  exit 1
fi

log "artifact collection completed"
