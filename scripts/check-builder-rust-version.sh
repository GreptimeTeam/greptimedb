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

  # The pinned channel, e.g. "1.96.1" or "nightly-2026-03-21".
  RUST_TOOLCHAIN=$(grep -E '^channel' "$RUST_TOOLCHAIN_VERSION_FILE" | cut -d'"' -f2)
  if [ -z "$RUST_TOOLCHAIN" ]; then
    echo "Error: No rust toolchain channel found in $RUST_TOOLCHAIN_VERSION_FILE"
    exit 1
  fi

  RUSTC_VERSION_OUTPUT=$(docker run "$DEV_BUILDER_UBUNTU_IMAGE" rustc --version)
  RUSTC_VERSION=$(echo "$RUSTC_VERSION_OUTPUT" | awk '{print $2}')
  if [ -z "$RUSTC_VERSION" ]; then
    echo "Error: No rustc version found in $DEV_BUILDER_UBUNTU_IMAGE"
    exit 1
  fi

  if [[ "$RUST_TOOLCHAIN" =~ ^[0-9]+\.[0-9]+ ]]; then
    # Stable channel: the builder image must ship exactly the pinned version.
    if [ "$RUSTC_VERSION" != "$RUST_TOOLCHAIN" ]; then
      echo "Error: The rust toolchain '$RUSTC_VERSION' in builder '$DEV_BUILDER_UBUNTU_IMAGE' doesn't match the pinned '$RUST_TOOLCHAIN', please rebuild the dev-builder image"
      exit 1
    fi
  else
    # Nightly channel: keep the legacy date-based check. The difference
    # between the pinned nightly date and the rustc build date shipped in
    # the builder should be less than 1 day.
    CURRENT_VERSION=$(echo "$RUST_TOOLCHAIN" | grep -Eo '[0-9]{4}-[0-9]{2}-[0-9]{2}')
    if [ -z "$CURRENT_VERSION" ]; then
      echo "Error: Unsupported toolchain channel '$RUST_TOOLCHAIN' in $RUST_TOOLCHAIN_VERSION_FILE"
      exit 1
    fi

    RUST_TOOLCHAIN_VERSION_IN_BUILDER=$(echo "$RUSTC_VERSION_OUTPUT" | grep -Eo '[0-9]{4}-[0-9]{2}-[0-9]{2}')
    if [ -z "$RUST_TOOLCHAIN_VERSION_IN_BUILDER" ]; then
      echo "Error: No rustc version found in $DEV_BUILDER_UBUNTU_IMAGE"
      exit 1
    fi

    current_rust_toolchain_seconds=$(date -d "$CURRENT_VERSION" +%s)
    rust_toolchain_in_dev_builder_ubuntu_seconds=$(date -d "$RUST_TOOLCHAIN_VERSION_IN_BUILDER" +%s)
    date_diff=$(( (current_rust_toolchain_seconds - rust_toolchain_in_dev_builder_ubuntu_seconds) / 86400 ))

    if [ $date_diff -gt 1 ]; then
      echo "Error: The rust toolchain '$RUST_TOOLCHAIN_VERSION_IN_BUILDER' in builder '$DEV_BUILDER_UBUNTU_IMAGE' maybe outdated, please update it to '$CURRENT_VERSION'"
      exit 1
    fi
  fi
}

check_rust_toolchain_version
