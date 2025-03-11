#!/usr/bin/env bash

BASEDIR=$(dirname "$0")
echo '| Title | Description | Expressions |
|---|---|---|'

cat $BASEDIR/greptimedb-cluster.json | jq -r '
  .panels |
  map(select(.type == "stat" or .type == "timeseries")) |
  .[] | "| \(.title) | \(.description | gsub("\n"; "<br>")) | \(.targets | map(.expr // .rawSql | "`\(.|gsub("\n"; "<br>"))`")  | join("<br>")) |"
'
