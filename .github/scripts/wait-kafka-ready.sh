#!/usr/bin/env bash

set -euo pipefail

GT_KAFKA_NAMESPACE="${GT_KAFKA_NAMESPACE:-kafka-cluster}"
GT_KAFKA_CLIENT_NAMESPACE="${GT_KAFKA_CLIENT_NAMESPACE:-my-greptimedb}"
GT_KAFKA_BOOTSTRAP_SERVERS="${GT_KAFKA_BOOTSTRAP_SERVERS:-kafka.kafka-cluster.svc.cluster.local:9092}"
GT_KAFKA_IMAGE="${GT_KAFKA_IMAGE:-docker.io/greptime/kafka:3.9.0-debian-12-r1}"
GT_KAFKA_READY_TIMEOUT_SECS="${GT_KAFKA_READY_TIMEOUT_SECS:-120}"
GT_KAFKA_READY_INTERVAL_SECS="${GT_KAFKA_READY_INTERVAL_SECS:-5}"
GT_KAFKA_POD_LABEL="${GT_KAFKA_POD_LABEL:-app.kubernetes.io/instance=kafka}"

log() {
  printf '[wait-kafka-ready] %s\n' "$*"
}

run_client_probe() {
  local pod_name="kafka-readiness-${RANDOM}-${RANDOM}"

  kubectl run "${pod_name}" \
    --namespace "${GT_KAFKA_CLIENT_NAMESPACE}" \
    --image "${GT_KAFKA_IMAGE}" \
    --restart=Never \
    --rm \
    --attach \
    --quiet \
    --env "GT_KAFKA_BOOTSTRAP_SERVERS=${GT_KAFKA_BOOTSTRAP_SERVERS}" \
    --command -- bash -euo pipefail -c '
      if command -v kafka-broker-api-versions.sh >/dev/null 2>&1; then
        kafka_tool="kafka-broker-api-versions.sh"
      elif [ -x /opt/bitnami/kafka/bin/kafka-broker-api-versions.sh ]; then
        kafka_tool="/opt/bitnami/kafka/bin/kafka-broker-api-versions.sh"
      else
        echo "kafka-broker-api-versions.sh not found" >&2
        exit 1
      fi

      "${kafka_tool}" --bootstrap-server "${GT_KAFKA_BOOTSTRAP_SERVERS}" >/tmp/kafka-ready.out
      cat /tmp/kafka-ready.out
    '
}

log "wait for Kafka pods in namespace ${GT_KAFKA_NAMESPACE}"
kubectl wait \
  --for=condition=Ready \
  pod -l "${GT_KAFKA_POD_LABEL}" \
  --timeout="${GT_KAFKA_READY_TIMEOUT_SECS}s" \
  -n "${GT_KAFKA_NAMESPACE}"

deadline=$((SECONDS + GT_KAFKA_READY_TIMEOUT_SECS))
attempt=1

while [ "${SECONDS}" -le "${deadline}" ]; do
  log "probe Kafka from namespace ${GT_KAFKA_CLIENT_NAMESPACE}, attempt ${attempt}"
  if run_client_probe; then
    log "Kafka is ready at ${GT_KAFKA_BOOTSTRAP_SERVERS}"
    exit 0
  fi

  log "Kafka probe failed; retrying in ${GT_KAFKA_READY_INTERVAL_SECS}s"
  sleep "${GT_KAFKA_READY_INTERVAL_SECS}"
  attempt=$((attempt + 1))
done

log "timed out waiting for Kafka bootstrap ${GT_KAFKA_BOOTSTRAP_SERVERS}"
kubectl get pods -o wide -n "${GT_KAFKA_NAMESPACE}" || true
kubectl get pods -o wide -n "${GT_KAFKA_CLIENT_NAMESPACE}" || true
exit 1
