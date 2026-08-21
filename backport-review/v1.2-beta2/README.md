# v1.2.0-beta.2 backport review index

## Review target

- Release base: `2f5e97850e55d86c0ed9eff1719994c88b4450a0`
- Aggregate branch: `work/release-v1.2-beta2-complete`
- Aggregate head: `40e02ff6b62ad3445f7035e437bd9e322c8af31b`
- Aggregate commits: 46 (the prior 43-commit aggregate ancestry, two JSON2 layout commits, and the #8902 carrier)
- CI: Historical [GitHub Actions run 32341712112](https://github.com/GreptimeTeam/greptimedb/actions/runs/32341712112), whose actual head SHA was `b5c38fbfc15053f42d4c1d843204bbdd52cb4a00`, completed with conclusion `failure` on the superseded-base aggregate. Failed jobs: Clippy, Check Unused Dependencies, and Check (ubuntu-latest). SQLness, Compatibility Test, binary builds, Rustfmt, license, and fuzz jobs passed; `test` was skipped downstream because Clippy and Check Unused Dependencies failed, while `coverage` was skipped independently because this was a `workflow_dispatch` run and coverage runs only for `merge_group`, not because of failed prerequisites. No CI run or result is recorded for the final range `2f5e97850e55d86c0ed9eff1719994c88b4450a0..40e02ff6b62ad3445f7035e437bd9e322c8af31b`.
- `release/v1.2` was not updated by this work.

## How to read the artifacts

- **[Start here: canonical manual decision index](decisions/README.md)**: priority-ordered semantic concerns, exact differences, exclusions, compatibility impact, and reviewer file pointers.
- PR #8825 mapping: upstream head `bf947141753a9ea554aa1ca31637fdd3ad1429f3`, merge `60c7bbcaf30a0e65ad7fa2b2b8ad0c1199a662b1`; carrier `d18430b874f9c8054747dbf688585cd7d2988e3e`; final head `40e02ff6b62ad3445f7035e437bd9e322c8af31b`. Requested by WenyXu in [Issue #8892 comment 5353700059](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5353700059). See [#8825 raw evidence](range-diffs/pr-8825.txt) and [event-context compatibility](decisions/event-context-compatibility.md).
- `range-diffs/pr-<number>.txt`: normally a final one-commit `git range-diff --creation-factor=100` from the upstream merged PR commit to the final aggregate carrier commit. These raw `.txt` files are **unabridged differences-only evidence**, not the complete shipping patch; `pr-8825.txt` is explicitly raw metadata/command evidence because its upstream PR-head object is unavailable locally.
- `patch-id-verdict.tsv`: stable patch IDs and `exact`/`adapted` verdicts. Patch-ID equality means patch equivalence; it does not claim the cherry-pick had no conflict.
- `upstream-pr-map.tsv`: PR number, merged commit, state, URL, and title.
- `aggregate-commit-provenance.tsv`: final aggregate commits, cherry-pick trailers, patch IDs, and subjects.
- `aggregate-commits.tsv`: final ordered aggregate history.
- `aggregate.diff`: complete release-base-to-aggregate raw diff.
- `manual-deltas.md`: human explanation of every non-trivial adaptation and aggregate conflict.

## Scope

The review scope has 33 explicit PRs: the prior 32 plus #8902. Official PR #8921 is now in the release base `2f5e97850e55d86c0ed9eff1719994c88b4450a0`, so it is base-present and is not represented as an aggregate carrier. #8672 and #8699 remain base-present as well. The other explicit PRs are represented by backport mappings; #8825 remains carried by `d18430b874f9c8054747dbf688585cd7d2988e3e`, and #8902 is appended at final head `40e02ff6b62ad3445f7035e437bd9e322c8af31b`. The dependency closure also includes full #8734 and a release-compatible slice of #8392.

## Mapping classes

- **exact patch-id**: stable patch ID equals the upstream merged PR commit.
- **adapted**: the final aggregate patch differs from upstream because of release APIs, dependency/lock state, conflict resolution, or removal of unrelated main-only behavior.
- **folded**: the requested behavior is carried inside another reconstructed commit rather than a standalone commit; #8818 is folded into the reconstructed #8579 carrier.
- **slice**: only the release-compatible behavior was selected; #8392 keeps the beta2 two-field persisted/wire `FlowStateValue` contract.
- **dependency**: not listed in the original 31 but required by the selected event chain; #8734.
- **release-extra**: compatibility, lock, regression-test, formatting, or generated-result correction that is not represented as an upstream PR.

## Accepted breaking-format exception

PR #8824 is accepted by explicit release-owner direction in this backport session. Public artifacts do not identify a named approver or date; Issue comment [5352919911](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5352919911) is the public record of the accepted exception, not evidence of independent third-party approval. It changes native-histogram persisted Struct children in place (`UInt32/UInt64` fields to `Int32/Int64`, including `count_u64`/`zero_count_u64` becoming `count_i64`/`zero_count_i64`) without a migration, downgrade, or mixed-version compatibility layer. This is an accepted release risk based on the feature being believed unused, not a compatibility guarantee.

## Final rebased range and appended #8902

The final review range is `2f5e97850e55d86c0ed9eff1719994c88b4450a0..40e02ff6b62ad3445f7035e437bd9e322c8af31b`, a linear 46-commit range. The superseded beta2 base `8eeff8b4417e6fb675a66cecbdba40060322785c` and endpoint `b7fdcb89568dc707c323f8c0a3b9413ec4e889ab` are historical provenance only; all active aggregate carrier fields in the TSV indexes are remapped to the rebased commits. Official #8921 is the release-base commit `2f5e97850e55d86c0ed9eff1719994c88b4450a0`, not a duplicate aggregate carrier.

PR #8902 is appended as final carrier `40e02ff6b62ad3445f7035e437bd9e322c8af31b`; its readable decision is [Dictionary group-by fast path](decisions/dictionary-groupby-fast-path.md), and its raw patch comparison is [#8902](range-diffs/pr-8902.txt).
