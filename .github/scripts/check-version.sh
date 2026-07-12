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

# Function to extract base version (without pre-release suffix)
get_base_version() {
  echo "$1" | sed -E 's/-(alpha|beta|rc|pre).*//'
}

# Function to check if a version is pre-release
is_prerelease() {
  [[ "$1" =~ -(alpha|beta|rc|pre) ]]
}

# Compare versions properly considering pre-release
compare_versions() {
  local current=$1
  local latest=$2

  # Extract base versions
  local current_base=$(get_base_version "$current")
  local latest_base=$(get_base_version "$latest")

  # Compare base versions first
  HIGHER_BASE=$(printf "%s\n%s" "$current_base" "$latest_base" | sort -V | tail -n1)

  if [ "$HIGHER_BASE" = "$latest_base" ] && [ "$current_base" != "$latest_base" ]; then
    # Latest has higher base version
    echo "current_older"
    return
  elif [ "$HIGHER_BASE" = "$current_base" ] && [ "$current_base" != "$latest_base" ]; then
    # Current has higher base version
    echo "current_newer"
    return
  fi

  # Base versions are equal, compare pre-release status
  if [ "$current_base" = "$latest_base" ]; then
    # If current is pre-release and latest is not, current is older
    if is_prerelease "$current" && ! is_prerelease "$latest"; then
      echo "current_older"
      return
    fi

    # If latest is pre-release and current is not, current is newer
    if ! is_prerelease "$current" && is_prerelease "$latest"; then
      echo "current_newer"
      return
    fi
  fi

  # Both are same type or different base versions already handled, use sort -V
  HIGHER_VERSION=$(printf "%s\n%s" "$current" "$latest" | sort -V | tail -n1)
  if [ "$HIGHER_VERSION" = "$current" ]; then
    echo "current_newer_or_equal"
  else
    echo "current_older"
  fi
}

RESULT=$(compare_versions "$CLEAN_CURRENT" "$CLEAN_LATEST")

if [ "$RESULT" = "current_newer" ] || [ "$RESULT" = "current_newer_or_equal" ]; then
  echo "Current version ($CLEAN_CURRENT) is NEWER than or EQUAL to latest ($CLEAN_LATEST)"
  if [ -n "$GITHUB_OUTPUT" ]; then
    echo "is-current-version-latest=true" >> $GITHUB_OUTPUT
  fi
else
  echo "Current version ($CLEAN_CURRENT) is OLDER than latest ($CLEAN_LATEST)"
  if [ -n "$GITHUB_OUTPUT" ]; then
    echo "is-current-version-latest=false" >> $GITHUB_OUTPUT
  fi
fi
