#! /usr/bin/env bash

CLUSTER_DASHBOARD_DIR=${1:-grafana/dashboards/cluster}
STANDALONE_DASHBOARD_DIR=${2:-grafana/dashboards/standalone}

remove_instance_filters() {
  # Remove the instance filters for the standalone dashboards.
  for file in $CLUSTER_DASHBOARD_DIR/*.json; do
    sed 's/instance=~\\"$datanode\\",//; s/instance=~\\"$datanode\\"//; s/instance=~\\"$frontend\\",//; s/instance=~\\"$frontend\\"//; s/instance=~\\"$metasrv\\",//; s/instance=~\\"$metasrv\\"//; s/instance=~\\"$flownode\\",//; s/instance=~\\"$flownode\\"//;' $file > $STANDALONE_DASHBOARD_DIR/${file##*/}
  done
}

remove_instance_filters
