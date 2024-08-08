#!/usr/bin/env bash

set -ue

OS_TYPE=
ARCH_TYPE=

# Set the GitHub token to avoid GitHub API rate limit.
# You can run with `GITHUB_TOKEN`:
#  GITHUB_TOKEN=<your_token> ./scripts/install.sh
GITHUB_TOKEN=${GITHUB_TOKEN:-}

VERSION=${1:-latest}
GITHUB_ORG=GreptimeTeam
GITHUB_REPO=greptimedb
BIN=greptime

function get_os_type() {
  os_type="$(uname -s)"

  case "$os_type" in
    Darwin)
      OS_TYPE=darwin
      ;;
    Linux)
      OS_TYPE=linux
      ;;
    *)
      echo "Error: Unknown OS type: $os_type"
      exit 1
  esac
}

function get_arch_type() {
  arch_type="$(uname -m)"

  case "$arch_type" in
    arm64)
      ARCH_TYPE=arm64
      ;;
    aarch64)
      ARCH_TYPE=arm64
      ;;
    x86_64)
      ARCH_TYPE=amd64
      ;;
    amd64)
      ARCH_TYPE=amd64
      ;;
    *)
      echo "Error: Unknown CPU type: $arch_type"
      exit 1
  esac
}

function download_artifact() {
  if [ -n "${OS_TYPE}" ] && [ -n "${ARCH_TYPE}" ]; then
    # Use the latest stable released version.
    # GitHub API reference: https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#get-the-latest-release.
    if [ "${VERSION}" = "latest" ]; then
      # To avoid other tools dependency, we choose to use `curl` to get the version metadata and parsed by `sed`.
      VERSION=$(curl -sL \
        -H "Accept: application/vnd.github+json" \
        -H "X-GitHub-Api-Version: 2022-11-28" \
        ${GITHUB_TOKEN:+-H "Authorization: Bearer $GITHUB_TOKEN"} \
        "https://api.github.com/repos/${GITHUB_ORG}/${GITHUB_REPO}/releases/latest" | sed -n 's/.*"tag_name": "\([^"]*\)".*/\1/p')
      if [ -z "${VERSION}" ]; then
        echo "Failed to get the latest stable released version."
        exit 1
        fi
    fi

    echo "Downloading ${BIN}, OS: ${OS_TYPE}, Arch: ${ARCH_TYPE}, Version: ${VERSION}"
    PACKAGE_NAME="${BIN}-${OS_TYPE}-${ARCH_TYPE}-${VERSION}.tar.gz"

    if [ -n "${PACKAGE_NAME}" ]; then
      wget "https://github.com/${GITHUB_ORG}/${GITHUB_REPO}/releases/download/${VERSION}/${PACKAGE_NAME}"

      # Extract the binary and clean the rest.
      tar xvf "${PACKAGE_NAME}" && \
      mv "${PACKAGE_NAME%.tar.gz}/${BIN}" "${PWD}" && \
      rm -r "${PACKAGE_NAME}" && \
      rm -r "${PACKAGE_NAME%.tar.gz}" && \
      echo "Run './${BIN} --help' to get started"
    fi
  fi
}

get_os_type
get_arch_type
download_artifact
