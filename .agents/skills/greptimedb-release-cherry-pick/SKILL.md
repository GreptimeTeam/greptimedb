---
name: greptimedb-release-cherry-pick
description: Plan and apply GreptimeDB release-branch cherry-picks by resolving tracked PRs and commit dependencies, ordering picks, reconciling conflicts, and verifying parity with the original PRs. Use when preparing a patch release such as 1.1.x or reviewing a release-tracking issue for commits to backport.
---

# GreptimeDB Release Cherry-Pick

## Goal

Prepare a release branch for a patch release without losing the intent of the
original pull requests. Determine which commits to backport, their prerequisite
dependencies and safe application order, then verify that every resulting change
matches its source PR after conflict resolution.

Use this skill for patch releases on `release/v<MAJOR>.<MINOR>` (for example,
`release/v1.1`). It complements `greptimedb-release`, which creates the release
after the branch is prepared.

## Inputs

Collect or infer the following before changing the repository:

| Input | Rule |
| --- | --- |
| Target version and branch | Infer `release/vX.Y` from `X.Y.Z`; confirm it with the user. |
| Release-tracking issue | Prefer the issue URL or number that lists possible backports, such as `https://github.com/GreptimeTeam/greptimedb/issues/8530`. |
| Source repository | Always use `GreptimeTeam/greptimedb` for GitHub queries, even when the checkout has fork remotes. |
| Starting point | Use the current tip of the remote target branch, or a user-specified base commit. |

If the version, branch, or issue is unclear, stop and ask. Do not infer that
every issue comment, linked PR, or labelled PR should be picked.

## Safety Rules

- Resolve the git remote that points at `GreptimeTeam/greptimedb`; never assume
  it is named `origin`.
- Treat issue entries as candidates, not authorization. A candidate may already
  be present on the release branch, be superseded, or require changes that are
  unsuitable for a patch release.
- Keep investigation read-only until the user confirms the final commit list and
  order.
- Before applying picks, show the complete plan: target branch/base, source PRs,
  exact commit SHAs, prerequisites, order, and expected conflict areas. Require
  explicit approval of that list.
- Do not push, open a PR, alter an existing branch, force-push, or delete a
  branch/worktree unless the user explicitly requests it.
- Never resolve a conflict by silently dropping behavior. If the source intent
  cannot be preserved, stop, explain the discrepancy, and ask the user how to
  proceed.
- Do not hand-edit generated files. Follow `.agents/generated-files.md` if a
  picked change affects generated artifacts.

## 1. Inspect the Target Branch

Resolve the upstream remote and fetch the exact branch tip before assessing
candidates. Prefer `FETCH_HEAD` immediately after fetching, since a
remote-tracking ref can be absent or stale:

```bash
git remote -v
git fetch <remote> release/vX.Y
git rev-parse FETCH_HEAD
git log --oneline --decorate -n 30 FETCH_HEAD
```

Record the target base SHA in the plan. Inspect it rather than assuming the
current local checkout is on the intended branch.

## 2. Build the Candidate Set

Read the release-tracking issue, its comments, and referenced PRs. Query GitHub
explicitly:

```bash
gh issue view <issue-number> --repo GreptimeTeam/greptimedb --comments
gh pr view <pr-number> --repo GreptimeTeam/greptimedb \
  --json number,title,url,state,mergedAt,mergeCommit,baseRefName,headRefName,body,commits,files
```

For each candidate, report:

- PR number, title, state, merged time, and source URL;
- the actual merged commit or commits, including whether the PR was squash,
  merge, or rebase merged;
- for a merge commit, its parent SHAs and the verified mainline parent to use
  when applying it, or the selected non-merge commits to apply instead;
- whether it is already reachable from the target branch;
- why it is a release candidate and any reason to exclude or defer it;
- files and behavior likely to conflict with the target branch.

Do not use a PR head SHA after merge unless it is proven to be the commit that
should be picked. Prefer the merged commit(s) present on the canonical branch.

Detect already-applied work by checking ancestry, patch identity, and release
branch history. A different SHA alone does not mean the change is absent:

```bash
git merge-base --is-ancestor <source-commit> <target-base>
git log --oneline <target-base> --grep='#<pr-number>'
git log --cherry-pick --right-only --no-merges <target-base>...<source-commit>
```

When evidence is ambiguous, mark the candidate as unresolved rather than adding
it to the final pick list.

### 2.1 Freeze the Candidate Ledger

Before requesting approval, record the tracking issue's body, comments,
strike-throughs, and `updatedAt` time. Maintain one ledger row for every
referenced PR with: mention source, candidate status (`selected`, `excluded`,
`already backported`, `superseded`, or `unresolved`), existing release PR/commit
evidence, and decision owner.

Refresh the issue immediately before applying the first commit. A candidate
addition/removal, a newly discovered prerequisite, or a changed issue decision
invalidates the approved plan: regenerate the complete table and obtain fresh
approval. Do not silently extend a previously approved pick list.

## 3. Resolve Prerequisites and Pick Order

For every remaining candidate, identify dependencies from:

1. explicit issue or PR references;
2. commit and PR descriptions;
3. changed APIs, configuration, schema, feature flags, generated artifacts, and
   tests; and
4. the source branch history.

Build an explicit dependency DAG, not just a prose list. For every source merge
commit, inspect its parent snapshot and the history of changed symbols to find
APIs, semantics, tests, and revisions supplied earlier on the source branch.
Useful evidence includes:

```bash
git log --ancestry-path <source-base>..<source-commit>
git blame <source-commit>^ -- <changed-path>
git log -S '<symbol-or-api>' <source-commit>^ -- <changed-path>
```

Record each edge and its evidence. No backport may consume a production API,
dependency revision, or behavior that is supplied only by a later backport.
After any rebase or reorder, re-check the DAG against the final commit order.

Classify every dependency:

- **Already present** on the release branch: document the evidence; do not pick it.
- **Must pick first**: include its exact commit and place it before its dependent.
- **Optional / follow-up**: explain why the candidate can safely proceed without it.
- **Unavailable or unsuitable**: stop and ask whether to exclude, adapt, or defer
  the dependent change.

Order independent commits chronologically as they landed on the source branch.
For dependent commits, order topologically: prerequisites first, then their
dependents. Do not compress unrelated commits into a synthetic squash merely to
make the release history shorter.

Pay particular attention to persisted or wire-format changes. Backports must
preserve compatibility and include the applicable compatibility coverage; do not
pick only the implementation while omitting its required format or migration
work.

### 3.1 Audit External Revisions

For every Cargo git revision or external repository dependency, record the target
branch revision, requested source revision, canonical external merge commit, and
final selected revision. Compare the external ranges and enumerate included
commits. A newer timestamp or different SHA does not prove one revision contains
another; prove supersession through ancestry, PR-head-to-merge mapping, or patch
equivalence.

For protobuf changes, inventory all new fields, enum variants, and changed
struct literals across the full revision range—not only the field used by the
candidate PR. Evaluate old/new client-server combinations, including proto3
default values for absent scalar fields. Reject or defer a revision that changes
rolling-upgrade behavior ambiguously or imports unrelated API surface without a
specific compatibility plan.

## 4. Confirm the Final Pick List

Before any `git cherry-pick`, present a table like this and wait for an explicit
user confirmation:

| Order | Source PR | Commit | Prerequisites | Status / reason |
| --- | --- | --- | --- | --- |
| 1 | #1234 | `abc1234` | already present: `def5678` | include |
| 2 | #1250 | `fedcba9` | #1234 | include |
| — | #1260 | `0123456` | unavailable #1240 | exclude / decision needed |

Also state the target branch and base SHA, excluded candidates, known conflict
areas, and checks to run afterwards. Do not proceed until the user approves the
exact included commits and order.

## 5. Apply the Approved Commits

Apply only the confirmed commits in the approved order, preferably in an
isolated worktree or a newly created local branch based on the recorded target
base. Preserve each source commit's message unless a conflict-resolution commit
is explicitly agreed with the user:

```bash
# Non-merge commit
git cherry-pick -x <commit-sha>

# Merge commit: inspect its parents, then verify that parent 1 is the
# base-branch parent before using it as the mainline.
git show --no-patch --format=raw <merge-commit-sha>
git cherry-pick -m 1 -x <merge-commit-sha>
```

Do not cherry-pick a merge commit without `-m`: Git requires its mainline
parent to determine which change to replay. For GitHub merge commits, parent 1
is normally the branch into which the PR was merged and is usually the correct
mainline, but verify the parent order before applying it. Alternatively, select
the PR's individual non-merge commits when that preserves the approved change
set. `-x` records the source SHA and makes later auditing easier. Do not start a
new pick while the previous one remains conflicted. After every successful
cherry-pick (including after resolving and continuing a conflict), run:

```bash
make clippy
```

This is the mandatory local compile gate. Stop immediately if it fails; diagnose
the failure before applying another commit. This catches omitted prerequisites,
target-branch API drift, and dependency-resolution errors while the responsible
pick is still isolated.

## 6. Reconcile Conflicts Without Changing Intent

For each conflict:

1. Read the original PR diff, review discussion, tests, and source commit.
2. Compare the source parent, source commit, target-base version, and tentative
   result to identify the semantic difference rather than choosing one side by
   line position.
3. Implement the source PR's intended behavior using target-branch APIs and
   conventions.
4. Retain target-branch-only fixes unless the original PR deliberately replaces
   them.
5. Add or adapt tests when the source PR's test no longer applies verbatim.
6. Review the resolved diff against the original PR before continuing:

```bash
git diff <source-commit>^ <source-commit> -- <paths>
git diff HEAD^ HEAD -- <paths>
```

If a conflict requires a behavioral decision, a new dependency, a schema or wire
format judgment, or a deviation from the source PR, stop and obtain user
approval. Record the decision in the final report.

Before adapting a production conflict, determine whether the source-side method,
field, or behavior was introduced by an earlier source commit. A missing
production symbol is a **missing-prerequisite alarm**, not routine target API
drift: update the DAG, obtain approval, and replay from the prerequisite. Adapt
only when the target intentionally exposes an equivalent API. Record source
intent, prerequisite evidence, target adaptation, behavioral deviation, and
clippy result for every resolved conflict.

## 7. Verify PR Equivalence

After all approved picks succeed, verify every backport individually and as a
set. The release commit SHA will differ from the source, but its intended effect
must not.

For each source PR:

1. Compare changed files and behavior with the original PR diff.
2. Review the release-side patch, including conflict resolutions, for missing or
   unintended hunks.
3. Confirm source tests were backported, replaced by equivalent target-branch
   tests, or explicitly excluded with a reason.
4. Confirm generated files, configuration snapshots, and compatibility tests are
   handled through their source generators where applicable.

Use range and patch comparisons as evidence, but interpret them with review:

```bash
git log --oneline <target-base>..HEAD
git diff --stat <target-base>..HEAD
git range-diff <source-base>..<source-tip> <target-base>..HEAD
```

Local validation is intentionally limited to `make clippy` after every pick.
Do not run the full test suite locally by default. Open or update the release PR
after the final branch rewrite and rely on its CI for tests. Record the final tip
SHA and CI URLs/statuses in the final report; CI evidence from an earlier tip is
invalid after a rebase, replay, dependency update, or conflict-resolution
amendment. For wire-format or persisted-format changes, ensure the PR CI includes
the relevant compatibility coverage, or state the missing coverage explicitly.

Any rebase, reset/replay, candidate removal, dependency change, or amended
conflict resolution invalidates earlier source-to-release SHA mappings, reviews,
clippy evidence, and CI evidence. Regenerate the mapping and rerun the required
checks at the final tip before reporting completion.

## Output

Report results in this format:

```markdown
## Cherry-pick plan or result
- Target: `release/vX.Y` from `<base-sha>`
- Tracking issue: #NNNN

## Included commits
| Order | PR | Source commit | Release commit | Dependency evidence |
| --- | --- | --- | --- | --- |

## Excluded or unresolved candidates
- #NNNN: <reason and required decision>

## Conflict resolutions
- `<path>`: <source intent, target adaptation, and verification evidence>

## Equivalence verification
- #NNNN: <original behavior/tests compared and result>

## Validation
- Per-pick `make clippy`: passed | failed
- Release PR CI at `<final-tip-sha>`: pending | passed | failed | not run

## Next action
- <await confirmation, open a PR, push, or hand off to greptimedb-release>
```

State assumptions and unresolved questions plainly. The release owner, not the
skill, decides the final commit set.
