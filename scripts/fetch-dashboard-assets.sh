#!/usr/bin/env bash

# This script is used to download built dashboard assets from the "GreptimeTeam/dashboard" repository.
set -e

declare -r SCRIPT_DIR=$(cd $(dirname ${0}) >/dev/null 2>&1 && pwd)
declare -r ROOT_DIR=$(dirname ${SCRIPT_DIR})
declare -r STATIC_DIR="$ROOT_DIR/src/servers/dashboard"
OUT_DIR="${1:-$SCRIPT_DIR}"

DASHBOARD_REPOSITORY="${DASHBOARD_REPOSITORY:-GreptimeTeam/dashboard}"
DASHBOARD_ASSET="${DASHBOARD_ASSET:-build.tar.gz}"
DASHBOARD_GITHUB_TOKEN="${DASHBOARD_GITHUB_TOKEN:-${GH_TOKEN:-${GITHUB_TOKEN:-}}}"
RELEASE_VERSION="${DASHBOARD_RELEASE_VERSION:-$(cat "$STATIC_DIR/VERSION" | tr -d '\t\r\n ')}"

echo "Downloading assets to dir: $OUT_DIR"
cd $OUT_DIR

if [[ -z "$GITHUB_PROXY_URL" ]]; then
  GITHUB_URL="https://github.com"
else
  GITHUB_URL="${GITHUB_PROXY_URL%/}"
fi

function retry_fetch() {
    local url=$1
    local filename=$2
    local auth_args=()

    if [[ -n "$DASHBOARD_GITHUB_TOKEN" ]]; then
        auth_args=(
            -H "Authorization: Bearer ${DASHBOARD_GITHUB_TOKEN}"
            -H "Accept: application/octet-stream"
        )
    fi

    curl --connect-timeout 10 --retry 3 -fsSL "${auth_args[@]}" "$url" --output "$filename" || {
        echo "Failed to download $url"
        echo "You may try to set http_proxy and https_proxy environment variables."
        if [[ -z "$GITHUB_PROXY_URL" ]]; then
          echo "You may try to set GITHUB_PROXY_URL=http://mirror.ghproxy.com/https://github.com/"
        fi
        exit 1
     }
}

# Download the SHA256 checksum attached to the release. To verify the integrity
# of the download, this checksum will be used to check the download tar file
# containing the built dashboard assets.
retry_fetch "${GITHUB_URL}/${DASHBOARD_REPOSITORY}/releases/download/${RELEASE_VERSION}/sha256.txt" sha256.txt

# Download the tar file containing the built dashboard assets.
retry_fetch "${GITHUB_URL}/${DASHBOARD_REPOSITORY}/releases/download/${RELEASE_VERSION}/${DASHBOARD_ASSET}" "$DASHBOARD_ASSET"

# Verify the checksums match; exit if they don't.
case "$(uname -s)" in
    FreeBSD | Darwin)
        echo "$(cat sha256.txt)" | shasum --algorithm 256 --check \
            || { echo "Checksums did not match for downloaded dashboard assets!"; exit 1; } ;;
    Linux)
        echo "$(cat sha256.txt)" | sha256sum --check -- \
            || { echo "Checksums did not match for downloaded dashboard assets!"; exit 1; } ;;
    *)
        echo "The '$(uname -s)' operating system is not supported as a build host for the dashboard" >&2
        exit 1
esac

# Extract the assets and clean up.
tar -xzf "$DASHBOARD_ASSET" -C "$STATIC_DIR"
rm sha256.txt
rm "$DASHBOARD_ASSET"

echo "Successfully download dashboard assets to $STATIC_DIR"
