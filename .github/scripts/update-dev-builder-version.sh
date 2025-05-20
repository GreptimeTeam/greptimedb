#!/bin/bash

DEV_BUILDER_IMAGE_TAG=$1

update_dev_builder_version() {
  if [ -z "$DEV_BUILDER_IMAGE_TAG" ]; then
    echo "Error: Should specify the dev-builder image tag"
    exit 1
  fi

  # Configure Git configs.
  git config --global user.email greptimedb-ci@greptime.com
  git config --global user.name greptimedb-ci

  # Checkout a new branch.
  BRANCH_NAME="ci/update-dev-builder-$(date +%Y%m%d%H%M%S)"
  git checkout -b $BRANCH_NAME

  # Update the dev-builder image tag in the Makefile.
  sed -i "s/DEV_BUILDER_IMAGE_TAG ?=.*/DEV_BUILDER_IMAGE_TAG ?= ${DEV_BUILDER_IMAGE_TAG}/g" Makefile

  # Commit the changes.
  git add Makefile
  git commit -m "ci: update dev-builder image tag"
  git push origin $BRANCH_NAME

  # Create a Pull Request.
  gh pr create \
      --title "ci: update dev-builder image tag" \
      --body "This PR updates the dev-builder image tag" \
      --base main \
      --head $BRANCH_NAME \
      --reviewer zyy17 \
      --reviewer daviderli614
}

update_dev_builder_version
