#! /usr/bin/env bash

CLUSTER_DASHBOARD_DIR=${1:-grafana/dashboards/metrics/cluster}
STANDALONE_DASHBOARD_DIR=${2:-grafana/dashboards/metrics/standalone}
DAC_IMAGE=ghcr.io/zyy17/dac:20250423-522bd35

remove_instance_filters() {
  # Remove the instance filters for the standalone dashboards.
  sed 's/instance=~\\"$datanode\\",//; s/instance=~\\"$datanode\\"//; s/instance=~\\"$frontend\\",//; s/instance=~\\"$frontend\\"//; s/instance=~\\"$metasrv\\",//; s/instance=~\\"$metasrv\\"//; s/instance=~\\"$flownode\\",//; s/instance=~\\"$flownode\\"//;' $CLUSTER_DASHBOARD_DIR/dashboard.json > $STANDALONE_DASHBOARD_DIR/dashboard.json
}

generate_intermediate_dashboards_and_docs() {
  docker run -v ${PWD}:/greptimedb --rm ${DAC_IMAGE} \
    -i /greptimedb/$CLUSTER_DASHBOARD_DIR/dashboard.json \
    -o /greptimedb/$CLUSTER_DASHBOARD_DIR/dashboard.yaml \
    -m /greptimedb/$CLUSTER_DASHBOARD_DIR/dashboard.md

  docker run -v ${PWD}:/greptimedb --rm ${DAC_IMAGE} \
    -i /greptimedb/$STANDALONE_DASHBOARD_DIR/dashboard.json \
    -o /greptimedb/$STANDALONE_DASHBOARD_DIR/dashboard.yaml \
    -m /greptimedb/$STANDALONE_DASHBOARD_DIR/dashboard.md
}

remove_instance_filters
generate_intermediate_dashboards_and_docs
