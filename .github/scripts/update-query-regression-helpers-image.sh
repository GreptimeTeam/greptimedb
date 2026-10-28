#!/usr/bin/env bash
set -euo pipefail
: "${HELPERS_IMAGE:?Published image digest is required}"
repository=greptime-registry.cn-hangzhou.cr.aliyuncs.com/tools/query-regression-helpers
[[ "$HELPERS_IMAGE" =~ ^${repository}@sha256:[0-9a-f]{64}$ ]]
branch="ci/update-query-regression-helpers-${GITHUB_RUN_ID}-${GITHUB_RUN_ATTEMPT}"
git config user.name greptimedb-ci
git config user.email greptimedb-ci@greptime.com
git fetch origin main
git checkout -b "$branch" origin/main
pin=.github/query-regression-helpers-image.txt
printf '%s\n' "$HELPERS_IMAGE" > "$pin"
git add "$pin"
if git diff --cached --quiet; then
  echo 'Helper image pin is already current.'
  exit 0
fi
git commit -s -m 'ci(query-regression): update precompiled helpers image'
git push origin "HEAD:refs/heads/$branch"
body="$(mktemp)"
trap 'rm -f -- "$body"' EXIT
cat > "$body" <<EOF
I hereby agree to the terms of the [GreptimeDB CLA](https://github.com/GreptimeTeam/.github/blob/main/CLA.md).

## Refer to a related PR or issue link (optional)

${GITHUB_SERVER_URL}/${GITHUB_REPOSITORY}/actions/runs/${GITHUB_RUN_ID}

## What's changed and what's your intention?

Update the query-regression helper image to \`$HELPERS_IMAGE\`, built from \`$GITHUB_SHA\`.
The ECS build smoke-tested both packaged binaries and compared fixture plans. The publishing job extracted both binaries from the published digest and verified their SHA-256 checksums against those build outputs.
Runs opting into \`helpers_image: pinned\` use this version after merge. Source-build defaults are unchanged.

## PR Checklist

- [ ] I have written the necessary rustdoc comments.
- [x] I have added the necessary unit tests and integration tests.
- [ ] This PR requires documentation updates.
- [x] API changes are backward compatible.
- [x] Schema or data changes are backward compatible.
- [ ] This PR needs to be backported to release branches, and I have added the matching labels.
EOF
gh pr create --draft --base main --head "$branch" \
  --title 'ci(query-regression): update precompiled helpers image' --body-file "$body"
