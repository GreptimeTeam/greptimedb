#!/usr/bin/env bash

set -e

RUST_TOOLCHAIN_VERSION_FILE="rust-toolchain.toml"
DEV_BUILDER_UBUNTU_REGISTRY="docker.io"
DEV_BUILDER_UBUNTU_NAMESPACE="greptime"
DEV_BUILDER_UBUNTU_NAME="dev-builder-ubuntu"

function check_rust_toolchain_version() {
  DEV_BUILDER_IMAGE_TAG=$(grep "DEV_BUILDER_IMAGE_TAG ?= " Makefile | cut -d= -f2 | sed 's/^[ \t]*//')
  if [ -z "$DEV_BUILDER_IMAGE_TAG" ]; then
    echo "Error: No DEV_BUILDER_IMAGE_TAG found in Makefile"
    exit 1
  fi

  DEV_BUILDER_UBUNTU_IMAGE="$DEV_BUILDER_UBUNTU_REGISTRY/$DEV_BUILDER_UBUNTU_NAMESPACE/$DEV_BUILDER_UBUNTU_NAME:$DEV_BUILDER_IMAGE_TAG"

  CURRENT_VERSION=$(grep -Eo '[0-9]{4}-[0-9]{2}-[0-9]{2}' "$RUST_TOOLCHAIN_VERSION_FILE")
  if [ -z "$CURRENT_VERSION" ]; then
    echo "Error: No rust toolchain version found in $RUST_TOOLCHAIN_VERSION_FILE"
    exit 1
  fi

  RUST_TOOLCHAIN_VERSION_IN_BUILDER=$(docker run "$DEV_BUILDER_UBUNTU_IMAGE" rustc --version | grep  -Eo '[0-9]{4}-[0-9]{2}-[0-9]{2}')
  if [ -z "$RUST_TOOLCHAIN_VERSION_IN_BUILDER" ]; then
    echo "Error: No rustc version found in $DEV_BUILDER_UBUNTU_IMAGE"
    exit 1
  fi

  # Compare the version and the difference should be less than 1 day.
  current_rust_toolchain_seconds=$(date -d "$CURRENT_VERSION" +%s)
  rust_toolchain_in_dev_builder_ubuntu_seconds=$(date -d "$RUST_TOOLCHAIN_VERSION_IN_BUILDER" +%s)
  date_diff=$(( (current_rust_toolchain_seconds - rust_toolchain_in_dev_builder_ubuntu_seconds) / 86400 ))

  if [ $date_diff -gt 1 ]; then
    echo "Error: The rust toolchain '$RUST_TOOLCHAIN_VERSION_IN_BUILDER' in builder '$DEV_BUILDER_UBUNTU_IMAGE' maybe outdated, please update it to '$CURRENT_VERSION'"
    exit 1
  fi
}

check_rust_toolchain_version
