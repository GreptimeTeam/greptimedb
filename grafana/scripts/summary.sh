#!/usr/bin/env bash

DASHBOARD_DIR=${1:-grafana/dashboards}

summary() {
  for dashboard in $(find $DASHBOARD_DIR -name "*.json"); do
    echo '# `'$(echo $dashboard | sed 's|^grafana/dashboards/||')'`'
    echo '| Title | Description | Expressions |'
    echo '|---|---|---|'
    cat $dashboard | jq -r '
          .panels |
          map(select(.type == "stat" or .type == "timeseries")) |
          .[] | "| \(.title) | \(.description | gsub("\n"; "<br>")) | \(.targets | map(.expr // .rawSql | "`\(.|gsub("\n"; "<br>"))`")  | join("<br>")) |"'
  done
}

summary
