#! /usr/bin/env bash

CLUSTER_DASHBOARD_DIR=${1:-grafana/dashboards/cluster}
STANDALONE_DASHBOARD_DIR=${2:-grafana/dashboards/standalone}
DAC_IMAGE=ghcr.io/zyy17/dac:20250421-6df0ad7

remove_instance_filters() {
  # Remove the instance filters for the standalone dashboards.
  sed 's/instance=~\\"$datanode\\",//; s/instance=~\\"$datanode\\"//; s/instance=~\\"$frontend\\",//; s/instance=~\\"$frontend\\"//; s/instance=~\\"$metasrv\\",//; s/instance=~\\"$metasrv\\"//; s/instance=~\\"$flownode\\",//; s/instance=~\\"$flownode\\"//;' $CLUSTER_DASHBOARD_DIR/dashboard.json > $STANDALONE_DASHBOARD_DIR/dashboard.json
}

generate_intermediate_dashboards() {
  docker run -v ${PWD}:/greptimedb --rm ${DAC_IMAGE}  -i /greptimedb/$CLUSTER_DASHBOARD_DIR/dashboard.json > $CLUSTER_DASHBOARD_DIR/dashboard.yaml
  docker run -v ${PWD}:/greptimedb --rm ${DAC_IMAGE}  -i /greptimedb/$STANDALONE_DASHBOARD_DIR/dashboard.json > $STANDALONE_DASHBOARD_DIR/dashboard.yaml
}

remove_instance_filters
generate_intermediate_dashboards
