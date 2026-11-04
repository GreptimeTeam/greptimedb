#!/usr/bin/env bash
set -euo pipefail
: "${HELPERS_IMAGE:?Verified image digest is required}"
: "${GITHUB_REPOSITORY:?Repository is required}"
repository=greptime-registry.cn-hangzhou.cr.aliyuncs.com/tools/query-regression-helpers
[[ "$HELPERS_IMAGE" == "$repository@sha256:"* ]]
[[ "${HELPERS_IMAGE#"$repository@"}" =~ ^sha256:[0-9a-f]{64}$ ]]
# Like QUERY_REGRESSION_ECS_IMAGE_ID, this is CI configuration, not a source
# change. Create on first publication, then update only after image verification.
gh variable set QUERY_REGRESSION_HELPERS_IMAGE \
  --repo "$GITHUB_REPOSITORY" --body "$HELPERS_IMAGE"
gh api "repos/$GITHUB_REPOSITORY/actions/variables/QUERY_REGRESSION_HELPERS_IMAGE" \
  | jq -e --arg expected "$HELPERS_IMAGE" '.value == $expected' >/dev/null
printf 'QUERY_REGRESSION_HELPERS_IMAGE updated to `%s`\n' "$HELPERS_IMAGE"
