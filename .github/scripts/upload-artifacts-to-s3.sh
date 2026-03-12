#!/usr/bin/env bash

set -e
set -o pipefail

ARTIFACTS_DIR=$1
VERSION=$2
RELEASE_DIRS="releases/greptimedb"
GREPTIMEDB_REPO="GreptimeTeam/greptimedb"

# Check if necessary variables are set.
function check_vars() {
  for var in VERSION ARTIFACTS_DIR; do
    if [ -z "${!var}" ]; then
      echo "$var is not set or empty."
      echo "Usage: $0 <artifacts-dir> <version> <aws-s3-bucket>"
      exit 1
    fi
  done
}

# Uploads artifacts to AWS S3 bucket.
function upload_artifacts() {
  # The bucket layout will be:
  # releases/greptimedb
  # ├── latest-version.txt
  # ├── latest-nightly-version.txt
  # ├── v0.1.0
  # │   ├── greptime-darwin-amd64-v0.1.0.sha256sum
  # │   └── greptime-darwin-amd64-v0.1.0.tar.gz
  # └── v0.2.0
  #    ├── greptime-darwin-amd64-v0.2.0.sha256sum
  #    └── greptime-darwin-amd64-v0.2.0.tar.gz
  find "$ARTIFACTS_DIR" -type f \( -name "*.tar.gz" -o -name "*.sha256sum" \) | while IFS= read -r file; do
    filename=$(basename "$file")
    TARGET_URL="$PROXY_URL/$RELEASE_DIRS/$VERSION/$filename"

    curl -X PUT \
      -u "$PROXY_USERNAME:$PROXY_PASSWORD" \
      -F "file=@$file" \
      "$TARGET_URL"
  done
}

# Updates the latest version information in AWS S3 if UPDATE_VERSION_INFO is true.
function update_version_info() {
  if [ "$UPDATE_VERSION_INFO" == "true" ]; then
    # If it's the official release(like v1.0.0, v1.0.1, v1.0.2, etc.), update latest-version.txt.
    if [[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      echo "Updating latest-version.txt"
      echo "$VERSION" > latest-version.txt
      TARGET_URL="$PROXY_URL/$RELEASE_DIRS/latest-version.txt"

      curl -X PUT \
        -u "$PROXY_USERNAME:$PROXY_PASSWORD" \
        -F "file=@latest-version.txt" \
        "$TARGET_URL"
    fi

    # If it's the nightly release, update latest-nightly-version.txt.
    if [[ "$VERSION" == *"nightly"* ]]; then
      echo "Updating latest-nightly-version.txt"
      echo "$VERSION" > latest-nightly-version.txt

      TARGET_URL="$PROXY_URL/$RELEASE_DIRS/latest-nightly-version.txt"
      curl -X PUT \
        -u "$PROXY_USERNAME:$PROXY_PASSWORD" \
        -F "file=@latest-nightly-version.txt" \
        "$TARGET_URL"
    fi
  fi
}

# Downloads artifacts from Github if DOWNLOAD_ARTIFACTS_FROM_GITHUB is true.
function download_artifacts_from_github() {
  if [ "$DOWNLOAD_ARTIFACTS_FROM_GITHUB" == "true" ]; then
    # Check if jq is installed.
    if ! command -v jq &> /dev/null; then
      echo "jq is not installed. Please install jq to continue."
      exit 1
    fi

    # Get the latest release API response.
    RELEASES_API_RESPONSE=$(curl -s -H "Accept: application/vnd.github.v3+json" "https://api.github.com/repos/$GREPTIMEDB_REPO/releases/latest")

    # Extract download URLs for the artifacts.
    # Exclude source code archives which are typically named as 'greptimedb-<version>.zip' or 'greptimedb-<version>.tar.gz'.
    ASSET_URLS=$(echo "$RELEASES_API_RESPONSE" | jq -r '.assets[] | select(.name | test("greptimedb-.*\\.(zip|tar\\.gz)$") | not) | .browser_download_url')

    # Download each asset.
    while IFS= read -r url; do
      if [ -n "$url" ]; then
        curl -LJO "$url"
        echo "Downloaded: $url"
      fi
    done <<< "$ASSET_URLS"
  fi
}

function main() {
  check_vars
  download_artifacts_from_github
  upload_artifacts
  update_version_info
}

# Usage example:
#   PROXY_URL=<proxy_url> \
#   PROXY_USERNAME=<proxy_username> \
#   PROXY_PASSWORD=<proxy_password> \
#   UPDATE_VERSION_INFO=true \
#   DOWNLOAD_ARTIFACTS_FROM_GITHUB=false \
#     ./upload-artifacts-to-s3.sh <artifacts-dir> <version>
main
