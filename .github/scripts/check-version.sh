#!/bin/bash

# Get current version
CURRENT_VERSION=$1
if [ -z "$CURRENT_VERSION" ]; then
  echo "Error: Failed to get current version"
  exit 1
fi

# Get the latest version from GitHub Releases
API_RESPONSE=$(curl -s "https://api.github.com/repos/GreptimeTeam/greptimedb/releases/latest")

if [ -z "$API_RESPONSE" ] || [ "$(echo "$API_RESPONSE" | jq -r '.message')" = "Not Found" ]; then
  echo "Error: Failed to fetch latest version from GitHub"
  exit 1
fi

# Get the latest version
LATEST_VERSION=$(echo "$API_RESPONSE" | jq -r '.tag_name')

if [ -z "$LATEST_VERSION" ] || [ "$LATEST_VERSION" = "null" ]; then
  echo "Error: No valid version found in GitHub releases"
  exit 1
fi

# Cleaned up version number format (removed possible 'v' prefix and -nightly suffix)
CLEAN_CURRENT=$(echo "$CURRENT_VERSION" | sed 's/^v//' | sed 's/-nightly-.*//')
CLEAN_LATEST=$(echo "$LATEST_VERSION" | sed 's/^v//' | sed 's/-nightly-.*//')

echo "Current version: $CLEAN_CURRENT"
echo "Latest release version: $CLEAN_LATEST"

# Use sort -V to compare versions
HIGHER_VERSION=$(printf "%s\n%s" "$CLEAN_CURRENT" "$CLEAN_LATEST" | sort -V | tail -n1)

if [ "$HIGHER_VERSION" = "$CLEAN_CURRENT" ]; then
  echo "Current version ($CLEAN_CURRENT) is NEWER than or EQUAL to latest ($CLEAN_LATEST)"
  echo "is-current-version-latest=true" >> $GITHUB_OUTPUT
else
  echo "Current version ($CLEAN_CURRENT) is OLDER than latest ($CLEAN_LATEST)"
  echo "is-current-version-latest=false" >> $GITHUB_OUTPUT
fi
