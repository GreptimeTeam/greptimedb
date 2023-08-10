#!/usr/bin/env bash

set -e

# - If it's a tag push release, the version is the tag name(${{ github.ref_name }});
# - If it's a scheduled release, the version is '${{ env.NEXT_RELEASE_VERSION }}-nightly-$buildTime', like 'v0.2.0-nightly-20230313';
# - If it's a manual release, the version is '${{ env.NEXT_RELEASE_VERSION }}-$(git rev-parse --short HEAD)-YYYYMMDDSS', like 'v0.2.0-e5b243c-2023071245';
# - If it's a nightly build, the version is 'nightly-YYYYMMDD-$(git rev-parse --short HEAD)', like 'nightly-20230712-e5b243c'.
# create_version ${GIHUB_EVENT_NAME} ${NEXT_RELEASE_VERSION} ${NIGHTLY_RELEASE_PREFIX}
function create_version() {
  # Read from envrionment variables.
  if [ -z "$GITHUB_EVENT_NAME" ]; then
      echo "GITHUB_EVENT_NAME is empty"
      exit 1
  fi

  if [ -z "$NEXT_RELEASE_VERSION" ]; then
      echo "NEXT_RELEASE_VERSION is empty"
      exit 1
  fi

  if [ -z "$NIGHTLY_RELEASE_PREFIX" ]; then
      echo "NIGHTLY_RELEASE_PREFIX is empty"
      exit 1
  fi

  # Reuse $NEXT_RELEASE_VERSION to identify whether it's a nightly build.
  # It will be like 'nigtly-20230808-7d0d8dc6'.
  if [ "$NEXT_RELEASE_VERSION" = nightly ]; then
    echo "$NIGHTLY_RELEASE_PREFIX-$(date "+%Y%m%d")-$(git rev-parse --short HEAD)"
    exit 0
  fi

  # Reuse $NEXT_RELEASE_VERSION to identify whether it's a dev build.
  # It will be like 'dev-2023080819-f0e7216c'.
  if [ "$NEXT_RELEASE_VERSION" = dev ]; then
    if [ -z "$COMMIT_SHA" ]; then
      echo "COMMIT_SHA is empty in dev build"
      exit 1
    fi
    echo "dev-$(date "+%Y%m%d-%s")-$(echo "$COMMIT_SHA" | cut -c1-8)"
    exit 0
  fi

  # Note: Only output 'version=xxx' to stdout when everything is ok, so that it can be used in GitHub Actions Outputs.
  if [ "$GITHUB_EVENT_NAME" = push ]; then
    if [ -z "$GITHUB_REF_NAME" ]; then
      echo "GITHUB_REF_NAME is empty in push event"
      exit 1
    fi
    echo "$GITHUB_REF_NAME"
  elif [ "$GITHUB_EVENT_NAME" = workflow_dispatch ]; then
    echo "$NEXT_RELEASE_VERSION-$(git rev-parse --short HEAD)-$(date "+%Y%m%d-%s")"
  elif [ "$GITHUB_EVENT_NAME" = schedule ]; then
    echo "$NEXT_RELEASE_VERSION-$NIGHTLY_RELEASE_PREFIX-$(date "+%Y%m%d")"
  else
    echo "Unsupported GITHUB_EVENT_NAME: $GITHUB_EVENT_NAME"
    exit 1
  fi
}

# You can run as following examples:
#  GITHUB_EVENT_NAME=push NEXT_RELEASE_VERSION=v0.4.0 NIGHTLY_RELEASE_PREFIX=nigtly GITHUB_REF_NAME=v0.3.0 ./create-version.sh
#  GITHUB_EVENT_NAME=workflow_dispatch NEXT_RELEASE_VERSION=v0.4.0 NIGHTLY_RELEASE_PREFIX=nigtly ./create-version.sh
#  GITHUB_EVENT_NAME=schedule NEXT_RELEASE_VERSION=v0.4.0 NIGHTLY_RELEASE_PREFIX=nigtly ./create-version.sh
#  GITHUB_EVENT_NAME=schedule NEXT_RELEASE_VERSION=nightly NIGHTLY_RELEASE_PREFIX=nigtly ./create-version.sh
#  GITHUB_EVENT_NAME=workflow_dispatch COMMIT_SHA=f0e7216c4bb6acce9b29a21ec2d683be2e3f984a NEXT_RELEASE_VERSION=dev NIGHTLY_RELEASE_PREFIX=nigtly ./create-version.sh
create_version
