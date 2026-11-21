#!/usr/bin/env bash

# Verifies that the pinned DataFusion forks in `[patch.crates-io]` point at a
# commit that is an ancestor of one of the allowed fork branches.
#
# A pin to a commit that only exists on an unmerged PR branch silently drops
# whatever that branch carried as soon as somebody bumps to the branch head.
# This check makes that impossible: every pin must be reachable from a branch
# named in DATAFUSION_ALLOWED_BRANCHES.
#
# Usage:
#   DATAFUSION_ALLOWED_BRANCHES="greptimedb-55.1-thin-fork" scripts/check-datafusion-pin.sh
#   GITHUB_TOKEN=... scripts/check-datafusion-pin.sh Cargo.toml
#
# Requires either `gh` (locally) or `curl` plus GITHUB_TOKEN (in CI).

set -euo pipefail

cargo_toml="${1:-Cargo.toml}"
allowed_branches="${DATAFUSION_ALLOWED_BRANCHES:-greptimedb-55.1-thin-fork}"
repo="${DATAFUSION_REPO:-GreptimeTeam/datafusion}"
fork_url="https://github.com/${repo}.git"

if [[ ! -f "$cargo_toml" ]]; then
    echo "ERROR: $cargo_toml not found" >&2
    exit 1
fi

# Collect `rev = "..."` from the lines that pin the DataFusion fork.
mapfile -t revs < <(
    python3 - "$cargo_toml" "$fork_url" <<'PYEOF'
import re
import sys

path, url = sys.argv[1], sys.argv[2]
pattern = re.compile(r'rev\s*=\s*"([0-9a-f]+)"')
revs = {
    match.group(1)
    for line in open(path, encoding="utf-8")
    if url in line
    for match in [pattern.search(line)]
    if match
}
for rev in sorted(revs):
    print(rev)
PYEOF
)

if [[ ${#revs[@]} -eq 0 ]]; then
    echo "No ${repo} pins found in ${cargo_toml}; nothing to check."
    exit 0
fi

if [[ ${#revs[@]} -ne 1 ]]; then
    echo "ERROR: ${cargo_toml} pins ${repo} to ${#revs[@]} different revisions:" >&2
    printf '  %s\n' "${revs[@]}" >&2
    echo "All DataFusion pins must move together." >&2
    exit 1
fi

rev="${revs[0]}"
echo "DataFusion pin: ${rev}"
echo "Allowed branches: ${allowed_branches}"

# Ask GitHub whether `rev` is an ancestor of `branch`. `compare/rev...branch`
# reports "ahead" when the branch is ahead of rev, i.e. rev is an ancestor,
# and "diverged"/"behind" when it is not.
compare_status() {
    local branch="$1"
    local response
    if command -v gh >/dev/null 2>&1 && [[ -z "${GITHUB_TOKEN:-}" ]]; then
        response=$(gh api "repos/${repo}/compare/${rev}...${branch}" 2>/dev/null) || return 2
    else
        : "${GITHUB_TOKEN:?set GITHUB_TOKEN or install gh}"
        response=$(curl -sS -H "Authorization: Bearer ${GITHUB_TOKEN}" \
            -H "Accept: application/vnd.github+json" \
            "https://api.github.com/repos/${repo}/compare/${rev}...${branch}") || return 2
    fi
    printf '%s' "$response" | python3 -c '
import json, sys
data = json.load(sys.stdin)
if "status" not in data:
    print("error:" + str(data.get("message", "no status in response")))
else:
    print(data["status"])
'
}

for branch in ${allowed_branches//,/ }; do
    echo "Checking ${rev} against ${repo}@${branch}"
    status=$(compare_status "$branch") || status="error:request-failed"
    case "$status" in
        ahead | identical)
            echo "PASS: ${rev} is on ${branch}"
            exit 0
            ;;
        behind)
            echo "  ${branch} does not contain ${rev} yet (branch is behind the pin)"
            ;;
        diverged)
            echo "  ${branch} and ${rev} have diverged"
            ;;
        *)
            echo "  ${status}"
            ;;
    esac
done

echo "ERROR: DataFusion pin ${rev} is not on any of: ${allowed_branches}" >&2
echo "Pin a commit that is merged into one of those branches. A pin to an" >&2
echo "unmerged PR branch head silently drops its content on the next bump." >&2
exit 1
