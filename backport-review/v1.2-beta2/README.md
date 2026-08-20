# v1.2.0-beta.2 backport review index

## Review target

- Release base: `8eeff8b4417e6fb675a66cecbdba40060322785c`
- Aggregate branch: `work/release-v1.2-beta2-complete`
- Aggregate head: `b5c38fbfc15053f42d4c1d843204bbdd52cb4a00`
- Aggregate commits: 40
- CI: https://github.com/GreptimeTeam/greptimedb/actions/runs/32341712112
- `release/v1.2` was not updated by this work.

## How to read the artifacts

- `range-diffs/pr-<number>.txt`: final one-commit `git range-diff --creation-factor=100` from the upstream merged PR commit to the final aggregate carrier commit.
- `patch-id-verdict.tsv`: stable patch IDs and `exact`/`adapted` verdicts. Patch-ID equality means patch equivalence; it does not claim the cherry-pick had no conflict.
- `upstream-pr-map.tsv`: PR number, merged commit, state, URL, and title.
- `aggregate-commit-provenance.tsv`: final aggregate commits, cherry-pick trailers, patch IDs, and subjects.
- `aggregate-commits.tsv`: final ordered aggregate history.
- `aggregate.diff`: complete release-base-to-aggregate raw diff.
- `manual-deltas.md`: human explanation of every non-trivial adaptation and aggregate conflict.

## Scope

The issue lists 31 PRs. #8672 and #8699 were already present in the release base. The other 29 were backported. The dependency closure also includes full #8734 and a release-compatible slice of #8392.

## Mapping classes

- **exact patch-id**: stable patch ID equals the upstream merged PR commit.
- **adapted**: the final aggregate patch differs from upstream because of release APIs, dependency/lock state, conflict resolution, or removal of unrelated main-only behavior.
- **folded**: the requested behavior is carried inside another reconstructed commit rather than a standalone commit; #8818 is folded into the reconstructed #8579 carrier.
- **slice**: only the release-compatible behavior was selected; #8392 keeps the beta2 two-field persisted/wire `FlowStateValue` contract.
- **dependency**: not listed in the original 31 but required by the selected event chain; #8734.
- **release-extra**: compatibility, lock, regression-test, formatting, or generated-result correction that is not represented as an upstream PR.

## Accepted breaking-format exception

PR #8824 is included by explicit release-owner decision. It changes native-histogram persisted Struct children in place (`UInt32/UInt64` fields to `Int32/Int64`, including `count_u64`/`zero_count_u64` becoming `count_i64`/`zero_count_i64`) without a versioned migration. This is an accepted release risk based on the feature being believed unused, not a compatibility guarantee.
