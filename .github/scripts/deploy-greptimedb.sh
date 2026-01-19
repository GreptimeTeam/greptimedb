#!/usr/bin/env bash

set -e
set -o pipefail

KUBERNETES_VERSION="${KUBERNETES_VERSION:-v1.32.0}"
ENABLE_STANDALONE_MODE="${ENABLE_STANDALONE_MODE:-true}"
DEFAULT_INSTALL_NAMESPACE=${DEFAULT_INSTALL_NAMESPACE:-default}
GREPTIMEDB_IMAGE_TAG=${GREPTIMEDB_IMAGE_TAG:-latest}
GREPTIMEDB_OPERATOR_IMAGE_TAG=${GREPTIMEDB_OPERATOR_IMAGE_TAG:-v0.5.1}
GREPTIMEDB_INITIALIZER_IMAGE_TAG="${GREPTIMEDB_OPERATOR_IMAGE_TAG}"
GREPTIME_CHART="https://greptimeteam.github.io/helm-charts/"
ETCD_CHART="oci://registry-1.docker.io/bitnamicharts/etcd"
ETCD_CHART_VERSION="${ETCD_CHART_VERSION:-12.0.8}"
ETCD_IMAGE_TAG="${ETCD_IMAGE_TAG:-3.6.1-debian-12-r3}"

# Create a cluster with 1 control-plane node and 5 workers.
function create_kind_cluster() {
  cat <<EOF | kind create cluster --name "${CLUSTER}" --image kindest/node:"$KUBERNETES_VERSION" --config=-
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
- role: worker
- role: worker
- role: worker
- role: worker
EOF
}

# Add greptime Helm chart repo.
function add_greptime_chart() {
  helm repo add greptime "$GREPTIME_CHART"
  helm repo update
}

# Deploy a etcd cluster with 3 members.
function deploy_etcd_cluster() {
  local namespace="$1"

  helm upgrade --install etcd "$ETCD_CHART" \
    --version "$ETCD_CHART_VERSION" \
    --create-namespace \
    --set replicaCount=3 \
    --set auth.rbac.create=false \
    --set auth.rbac.token.enabled=false \
    --set global.security.allowInsecureImages=true \
    --set image.registry=docker.io \
    --set image.repository=greptime/etcd \
    --set image.tag="$ETCD_IMAGE_TAG" \
    -n "$namespace"

  # Wait for etcd cluster to be ready.
  kubectl rollout status statefulset/etcd -n "$namespace"
}

# Deploy greptimedb-operator.
function deploy_greptimedb_operator() {
  # Use the latest chart and image.
  helm upgrade --install greptimedb-operator greptime/greptimedb-operator \
    --create-namespace \
    -n "$DEFAULT_INSTALL_NAMESPACE" \
    --wait \
    --wait-for-jobs
}

# Deploy greptimedb cluster by using local storage.
# It will expose cluster service ports as '14000', '14001', '14002', '14003' to local access.
function deploy_greptimedb_cluster() {
  local cluster_name=$1
  local install_namespace=$2

  kubectl create ns "$install_namespace"

  deploy_etcd_cluster "$install_namespace"

  helm upgrade --install "$cluster_name" greptime/greptimedb-cluster \
    --create-namespace \
    --set image.tag="$GREPTIMEDB_IMAGE_TAG" \
    --set initializer.tag="$GREPTIMEDB_INITIALIZER_IMAGE_TAG" \
    --set "meta.backendStorage.etcd.endpoints[0]=etcd.$install_namespace.svc.cluster.local:2379" \
    --set meta.backendStorage.etcd.storeKeyPrefix="$cluster_name" \
    -n "$install_namespace"

  # Wait for greptimedb cluster to be ready.
  while true; do
    PHASE=$(kubectl -n "$install_namespace" get gtc "$cluster_name" -o jsonpath='{.status.clusterPhase}')
    if [ "$PHASE" == "Running" ]; then
      echo "Cluster is ready"
      break
    else
      echo "Cluster is not ready yet: Current phase: $PHASE"
      sleep 5 # wait for 5 seconds before check again.
    fi
  done

  # Expose greptimedb cluster to local access.
  # Expose greptimedb cluster to local access and check if port-forward is available.
  kubectl -n "$install_namespace" port-forward svc/"$cluster_name"-frontend \
    14000:4000 \
    14001:4001 \
    14002:4002 \
    14003:4003 > /tmp/connections.out 2>&1 &
  PORT_FORWARD_PID=$!
  
  # Wait for the port forward to be ready (checks up to 30 seconds)
  for i in {1..30}; do
    if nc -z localhost 14000 && nc -z localhost 14001 && nc -z localhost 14002 && nc -z localhost 14003; then
      echo "Port forward is available."
      break
    fi
    if ! kill -0 $PORT_FORWARD_PID 2>/dev/null; then
      echo "Port forward process exited unexpectedly."
      exit 1
    fi
    sleep 1
    if [ "$i" -eq 30 ]; then
      echo "Port forward did not become available in time."
      exit 1
    fi
  done
}

# Deploy greptimedb cluster by using S3.
# It will expose cluster service ports as '24000', '24001', '24002', '24003' to local access.
function deploy_greptimedb_cluster_with_s3_storage() {
  local cluster_name=$1
  local install_namespace=$2

  kubectl create ns "$install_namespace"

  deploy_etcd_cluster "$install_namespace"

  helm upgrade --install "$cluster_name" greptime/greptimedb-cluster -n "$install_namespace" \
    --create-namespace \
    --set image.tag="$GREPTIMEDB_IMAGE_TAG" \
    --set initializer.tag="$GREPTIMEDB_INITIALIZER_IMAGE_TAG" \
    --set "meta.backendStorage.etcd.endpoints[0]=etcd.$install_namespace.svc.cluster.local:2379" \
    --set meta.backendStorage.etcd.storeKeyPrefix="$cluster_name" \
    --set objectStorage.s3.bucket="$AWS_CI_TEST_BUCKET" \
    --set objectStorage.s3.region="$AWS_REGION" \
    --set objectStorage.s3.root="$DATA_ROOT" \
    --set objectStorage.credentials.secretName=s3-credentials \
    --set objectStorage.credentials.accessKeyId="$AWS_ACCESS_KEY_ID" \
    --set objectStorage.credentials.secretAccessKey="$AWS_SECRET_ACCESS_KEY"

  # Wait for greptimedb cluster to be ready.
  while true; do
    PHASE=$(kubectl -n "$install_namespace" get gtc "$cluster_name" -o jsonpath='{.status.clusterPhase}')
    if [ "$PHASE" == "Running" ]; then
      echo "Cluster is ready"
      break
    else
      echo "Cluster is not ready yet: Current phase: $PHASE"
      sleep 5 # wait for 5 seconds before check again.
    fi
  done

  # Expose greptimedb cluster to local access.
  kubectl -n "$install_namespace" port-forward svc/"$cluster_name"-frontend \
    24000:4000 \
    24001:4001 \
    24002:4002 \
    24003:4003 > /tmp/connections.out &
}

# Deploy standalone greptimedb.
# It will expose cluster service ports as '34000', '34001', '34002', '34003' to local access.
function deploy_standalone_greptimedb() {
  helm upgrade --install greptimedb-standalone greptime/greptimedb-standalone \
    --create-namespace \
    --set image.tag="$GREPTIMEDB_IMAGE_TAG" \
    -n "$DEFAULT_INSTALL_NAMESPACE"

  # Wait for etcd cluster to be ready.
  kubectl rollout status statefulset/greptimedb-standalone -n "$DEFAULT_INSTALL_NAMESPACE"

  # Expose greptimedb to local access.
  kubectl -n "$DEFAULT_INSTALL_NAMESPACE" port-forward svc/greptimedb-standalone \
    34000:4000 \
    34001:4001 \
    34002:4002 \
    34003:4003 > /tmp/connections.out &
}

# Entrypoint of the script.
function main() {
  create_kind_cluster
  add_greptime_chart

  # Deploy standalone greptimedb in the same K8s.
  if [ "$ENABLE_STANDALONE_MODE" == "true" ]; then
    deploy_standalone_greptimedb
  fi

  deploy_greptimedb_operator
  deploy_greptimedb_cluster testcluster testcluster
  deploy_greptimedb_cluster_with_s3_storage testcluster-s3 testcluster-s3
}

# Usages:
# - Deploy greptimedb cluster: ./deploy-greptimedb.sh
main
