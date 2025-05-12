#!/bin/bash

set -e

VERSION=${VERSION}
GITHUB_TOKEN=${GITHUB_TOKEN}

update_homebrew_greptime_version() {
  # Configure Git configs.
  git config --global user.email update-greptime-version@greptime.com
  git config --global user.name update-greptime-version

  # Clone helm-charts repository.
  git clone "https://x-access-token:${GITHUB_TOKEN}@github.com/GreptimeTeam/homebrew-greptime.git"
  cd homebrew-greptime

  # Set default remote for gh CLI
  gh repo set-default GreptimeTeam/homebrew-greptime

  # Checkout a new branch.
  BRANCH_NAME="chore/greptimedb-${VERSION}"
  git checkout -b $BRANCH_NAME

  # Update version.
  make update-greptime-version VERSION=${VERSION}

  # Commit the changes.
  git add .
  git commit -m "chore: Update GreptimeDB version to ${VERSION}"
  git push origin $BRANCH_NAME

  # Create a Pull Request.
  gh pr create \
    --title "chore: Update GreptimeDB version to ${VERSION}" \
    --body "This PR updates the GreptimeDB version." \
    --base main \
    --head $BRANCH_NAME \
    --reviewer zyy17 \
    --reviewer daviderli614
}

update_homebrew_greptime_version
