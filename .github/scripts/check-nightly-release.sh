#!/usr/bin/env bash

set -euo pipefail

# Returns whether the current release run should proceed, 'true' or 'false'.
#
# The nightly schedule runs every weekday, but a scheduled release only proceeds
# when the latest published nightly release is older than
# NIGHTLY_RELEASE_MAX_AGE_DAYS days. This way, a failed nightly run is retried
# by the next weekday schedule, instead of leaving a whole week without nightly
# builds. Non-scheduled runs (tag pushes and manual dispatches) always proceed.
#
# Only the result is written to stdout, so that it can be used in GitHub Actions
# outputs; all diagnostics go to stderr.
#
# You can run as following examples:
#   GITHUB_EVENT_NAME=push ./check-nightly-release.sh
#   GITHUB_EVENT_NAME=schedule GITHUB_REPOSITORY=GreptimeTeam/greptimedb GH_TOKEN=ghp_xxx ./check-nightly-release.sh
#   GITHUB_EVENT_NAME=schedule NIGHTLY_RELEASE_MAX_AGE_DAYS=5 GITHUB_REPOSITORY=GreptimeTeam/greptimedb GH_TOKEN=ghp_xxx ./check-nightly-release.sh
if [ -z "$GITHUB_EVENT_NAME" ]; then
    echo "GITHUB_EVENT_NAME is empty" >&2
    exit 1
fi

if [ "$GITHUB_EVENT_NAME" != "schedule" ]; then
    echo "Not a scheduled run, the nightly check is skipped." >&2
    echo "true"
    exit 0
fi

MAX_AGE_DAYS="${NIGHTLY_RELEASE_MAX_AGE_DAYS:-5}"
if ! [[ "$MAX_AGE_DAYS" =~ ^[0-9]+$ ]]; then
    echo "Invalid NIGHTLY_RELEASE_MAX_AGE_DAYS: ${MAX_AGE_DAYS}" >&2
    exit 1
fi

SECONDS_PER_DAY=86400

# The publish time of the latest published nightly release, like the tag
# 'v0.2.0-nightly-20230313'. Fail open on GitHub API errors: proceeding with
# the release is safer than skipping it.
if ! latest_nightly_published_at="$(gh api "repos/${GITHUB_REPOSITORY}/releases?per_page=100" \
    --jq '[.[] | select(.draft == false and (.tag_name | contains("nightly")))][0].published_at // empty')"; then
    echo "::warning::Failed to query the latest nightly release from the GitHub API, will proceed with the nightly release." >&2
    echo "true"
    exit 0
fi

if [ -z "$latest_nightly_published_at" ]; then
    echo "No published nightly release found, will proceed with the nightly release." >&2
    echo "true"
    exit 0
fi

published_at_epoch="$(date --date="$latest_nightly_published_at" +%s)"
now_epoch="$(date +%s)"
age_seconds=$(( now_epoch - published_at_epoch ))
age_days=$(( age_seconds / SECONDS_PER_DAY ))

echo "The latest nightly release was published at ${latest_nightly_published_at} (${age_days} days ago)." >&2

if [ "$age_seconds" -gt $(( MAX_AGE_DAYS * SECONDS_PER_DAY )) ]; then
    echo "The latest nightly release is older than ${MAX_AGE_DAYS} days, will proceed with the nightly release." >&2
    echo "true"
else
    echo "The latest nightly release is not older than ${MAX_AGE_DAYS} days, will skip the nightly release." >&2
    echo "false"
fi
