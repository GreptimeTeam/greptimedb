#!/usr/bin/env bash

set -e
set -o pipefail

ARTIFACTS_DIR=$1
VERSION=$2
AWS_S3_BUCKET=$3
RELEASE_DIRS="releases/greptimedb"
GREPTIMEDB_REPO="GreptimeTeam/greptimedb"

# Check if necessary variables are set.
function check_vars() {
  for var in AWS_S3_BUCKET VERSION ARTIFACTS_DIR; do
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
  # │   ├── greptime-darwin-amd64-pyo3-v0.1.0.sha256sum
  # │   └── greptime-darwin-amd64-pyo3-v0.1.0.tar.gz
  # └── v0.2.0
  #    ├── greptime-darwin-amd64-pyo3-v0.2.0.sha256sum
  #    └── greptime-darwin-amd64-pyo3-v0.2.0.tar.gz
  find "$ARTIFACTS_DIR" -type f \( -name "*.tar.gz" -o -name "*.sha256sum" \) | while IFS= read -r file; do
    aws s3 cp \
      "$file" "s3://$AWS_S3_BUCKET/$RELEASE_DIRS/$VERSION/$(basename "$file")"
  done
}

# Updates the latest version information in AWS S3 if UPDATE_VERSION_INFO is true.
function update_version_info() {
  if [ "$UPDATE_VERSION_INFO" == "true" ]; then
    # If it's the officail release(like v1.0.0, v1.0.1, v1.0.2, etc.), update latest-version.txt.
    if [[ "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
      echo "Updating latest-version.txt"
      echo "$VERSION" > latest-version.txt
      aws s3 cp \
        latest-version.txt "s3://$AWS_S3_BUCKET/$RELEASE_DIRS/latest-version.txt"
    fi

    # If it's the nightly release, update latest-nightly-version.txt.
    if [[ "$VERSION" == *"nightly"* ]]; then
      echo "Updating latest-nightly-version.txt"
      echo "$VERSION" > latest-nightly-version.txt
      aws s3 cp \
        latest-nightly-version.txt "s3://$AWS_S3_BUCKET/$RELEASE_DIRS/latest-nightly-version.txt"
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
#   AWS_ACCESS_KEY_ID=<your_access_key_id> \
#   AWS_SECRET_ACCESS_KEY=<your_secret_access_key> \
#   AWS_DEFAULT_REGION=<your_region> \
#   UPDATE_VERSION_INFO=true \
#   DOWNLOAD_ARTIFACTS_FROM_GITHUB=false \
#     ./upload-artifacts-to-s3.sh <artifacts-dir> <version> <aws-s3-bucket>
main
