#!/usr/bin/env bash

set -e
set -o pipefail

SRC_IMAGE=$1
DST_REGISTRY=$2
SKOPEO_STABLE_IMAGE="quay.io/skopeo/stable:latest"

# Check if necessary variables are set.
function check_vars() {
  for var in DST_REGISTRY_USERNAME DST_REGISTRY_PASSWORD DST_REGISTRY SRC_IMAGE; do
    if [ -z "${!var}" ]; then
      echo "$var is not set or empty."
      echo "Usage: DST_REGISTRY_USERNAME=<your-dst-registry-username> DST_REGISTRY_PASSWORD=<your-dst-registry-password> $0 <dst-registry> <src-image>"
      exit 1
    fi
  done
}

# Copies images from DockerHub to the destination registry.
function copy_images_from_dockerhub() {
  # Check if docker is installed.
  if ! command -v docker &> /dev/null; then
    echo "docker is not installed. Please install docker to continue."
    exit 1
  fi

  # Extract the name and tag of the source image.
  IMAGE_NAME=$(echo "$SRC_IMAGE" | sed "s/.*\///")

  echo "Copying $SRC_IMAGE to $DST_REGISTRY/$IMAGE_NAME"

  docker run "$SKOPEO_STABLE_IMAGE" copy -a docker://"$SRC_IMAGE" \
    --dest-creds "$DST_REGISTRY_USERNAME":"$DST_REGISTRY_PASSWORD" \
    docker://"$DST_REGISTRY/$IMAGE_NAME"
}

function main() {
  check_vars
  copy_images_from_dockerhub
}

# Usage example:
# DST_REGISTRY_USERNAME=123 DST_REGISTRY_PASSWORD=456 \
#   ./copy-image.sh greptime/greptimedb:v0.4.0 greptime-registry.cn-hangzhou.cr.aliyuncs.com
main
