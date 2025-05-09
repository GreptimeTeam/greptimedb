#!/bin/bash

set -e

VERSION=${VERSION}
GITHUB_TOKEN=${GITHUB_TOKEN}

update_helm_charts_version() {
  # Configure Git configs.
  git config --global user.email update-helm-charts-version@greptime.com
  git config --global user.name update-helm-charts-version

  # Clone helm-charts repository.
  git clone "https://x-access-token:${GITHUB_TOKEN}@github.com/GreptimeTeam/helm-charts.git"
  cd helm-charts

  # Set default remote for gh CLI
  gh repo set-default GreptimeTeam/helm-charts

  # Checkout a new branch.
  BRANCH_NAME="chore/greptimedb-${VERSION}"
  git checkout -b $BRANCH_NAME

  # Update version.
  make update-version CHART=greptimedb-cluster VERSION=${VERSION}
  make update-version CHART=greptimedb-standalone VERSION=${VERSION}

  # Update docs.
  make docs

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

update_helm_charts_version
