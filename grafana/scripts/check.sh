#!/usr/bin/env bash

DASHBOARD_DIR=${1:-grafana/dashboards}

check_dashboard_description() {
  for dashboard in $(find $DASHBOARD_DIR -name "*.json"); do
    echo "Checking $dashboard description"

    # Use jq to check for panels with empty or missing descriptions
    invalid_panels=$(cat $dashboard | jq -r '
      .panels[]
    | select((.type == "stats" or .type == "timeseries") and (.description == "" or .description == null))')

    # Check if any invalid panels were found
    if [[ -n "$invalid_panels" ]]; then
      echo "Error: The following panels have empty or missing descriptions:"
      echo "$invalid_panels"
      exit 1
    else
      echo "All panels with type 'stats' or 'timeseries' have valid descriptions."
    fi
  done
}

check_standalone_dashboards() {
  # Execute the gen-standalone.sh script
  ./grafana/scripts/gen-standalone.sh

  if [[ -n "$(git diff --name-only grafana/dashboards/standalone)" ]]; then
    echo "Error: The standalone dashboards are not generated correctly. You should execute the `make dashboards` command."
    exit 1
  fi
}

check_datasource() {
  for dashboard in $(find $DASHBOARD_DIR -name "*.json"); do
    echo "Checking $dashboard datasource"
    jq -r '.panels[] | select(.type != "row") | .targets[] | [.datasource.type, .datasource.uid] | @tsv' $dashboard | while read -r type uid; do
    # if the datasource is prometheus, check if the uid is ${metrics}
    if [[ "$type" == "prometheus" && "$uid" != "\${metrics}" ]]; then
      echo "Error: The datasource uid of $dashboard is not valid. It should be \${metrics}, got $uid"
      exit 1
    fi
    # if the datasource is mysql, check if the uid is ${information_schema}
    if [[ "$type" == "mysql" && "$uid" != "\${information_schema}" ]]; then
      echo "Error: The datasource uid of $dashboard is not valid. It should be \${information_schema}, got $uid"
      exit 1
    fi
    done
  done
}

check_standalone_dashboards
check_dashboard_description
check_datasource
