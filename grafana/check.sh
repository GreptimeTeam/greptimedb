#!/usr/bin/env bash

BASEDIR=$(dirname "$0")

# Use jq to check for panels with empty or missing descriptions
invalid_panels=$(cat $BASEDIR/greptimedb-cluster.json | jq -r '
  .panels[]
  | select((.type == "stats" or .type == "timeseries") and (.description == "" or .description == null))
')

# Check if any invalid panels were found
if [[ -n "$invalid_panels" ]]; then
  echo "Error: The following panels have empty or missing descriptions:"
  echo "$invalid_panels"
  exit 1
else
  echo "All panels with type 'stats' or 'timeseries' have valid descriptions."
  exit 0
fi
