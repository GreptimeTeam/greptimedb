# v1.2.0-beta.2 backport review index

## Review target

- Release base: `8eeff8b4417e6fb675a66cecbdba40060322785c`
- Aggregate branch: `work/release-v1.2-beta2-complete`
- Aggregate head: `b7fdcb89568dc707c323f8c0a3b9413ec4e889ab`
- Aggregate commits: 43 (40 previously recorded, plus two narrow test adaptations and the #8825 carrier, with the existing aggregate ancestry retained)
- CI: [completed with conclusion `failure`](https://github.com/GreptimeTeam/greptimedb/actions/runs/32341712112) at the pre-#8825 aggregate HEAD `b5c38fbfc15053f42d4c1d843204bbdd52cb4a00`; failed jobs: Clippy, Check Unused Dependencies, and Check (ubuntu-latest). SQLness, Compatibility Test, binary builds, Rustfmt, license, and fuzz jobs passed; `test` was skipped downstream because Clippy and Check Unused Dependencies failed, while `coverage` was skipped independently because this was a `workflow_dispatch` run and coverage runs only for `merge_group`, not because of failed prerequisites. No new CI result is claimed for #8825.
- `release/v1.2` was not updated by this work.

## How to read the artifacts

- **[Start here: canonical manual decision index](decisions/README.md)**: priority-ordered semantic concerns, exact differences, exclusions, compatibility impact, and reviewer file pointers.
- PR #8825 mapping: upstream head `bf947141753a9ea554aa1ca31637fdd3ad1429f3`, merge `60c7bbcaf30a0e65ad7fa2b2b8ad0c1199a662b1`; carrier/final head `b7fdcb89568dc707c323f8c0a3b9413ec4e889ab`. Requested by WenyXu in [Issue #8892 comment 5353700059](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5353700059). See [#8825 raw evidence](range-diffs/pr-8825.txt) and [event-context compatibility](decisions/event-context-compatibility.md).
- `range-diffs/pr-<number>.txt`: normally a final one-commit `git range-diff --creation-factor=100` from the upstream merged PR commit to the final aggregate carrier commit. These raw `.txt` files are **unabridged differences-only evidence**, not the complete shipping patch; `pr-8825.txt` is explicitly raw metadata/command evidence because its upstream PR-head object is unavailable locally.
- `patch-id-verdict.tsv`: stable patch IDs and `exact`/`adapted` verdicts. Patch-ID equality means patch equivalence; it does not claim the cherry-pick had no conflict.
- `upstream-pr-map.tsv`: PR number, merged commit, state, URL, and title.
- `aggregate-commit-provenance.tsv`: final aggregate commits, cherry-pick trailers, patch IDs, and subjects.
- `aggregate-commits.tsv`: final ordered aggregate history.
- `aggregate.diff`: complete release-base-to-aggregate raw diff.
- `manual-deltas.md`: human explanation of every non-trivial adaptation and aggregate conflict.

## Scope

The review scope now has 32 explicit PRs: the original 31 plus #8825. #8672 and #8699 remain base-present in the release base; the other 30 explicit PRs are represented by backport mappings, with #8825 carried by `b7fdcb89568dc707c323f8c0a3b9413ec4e889ab`. The dependency closure also includes full #8734 and a release-compatible slice of #8392. #8825 is classified as adapted, not exact: its shared Channel mapping is replayed on the release chain while the event-context compatibility decisions and narrow test adaptations are retained.

## Mapping classes

- **exact patch-id**: stable patch ID equals the upstream merged PR commit.
- **adapted**: the final aggregate patch differs from upstream because of release APIs, dependency/lock state, conflict resolution, or removal of unrelated main-only behavior.
- **folded**: the requested behavior is carried inside another reconstructed commit rather than a standalone commit; #8818 is folded into the reconstructed #8579 carrier.
- **slice**: only the release-compatible behavior was selected; #8392 keeps the beta2 two-field persisted/wire `FlowStateValue` contract.
- **dependency**: not listed in the original 31 but required by the selected event chain; #8734.
- **release-extra**: compatibility, lock, regression-test, formatting, or generated-result correction that is not represented as an upstream PR.

## Accepted breaking-format exception

PR #8824 is accepted by explicit release-owner direction in this backport session. Public artifacts do not identify a named approver or date; Issue comment [5352919911](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5352919911) is the public record of the accepted exception, not evidence of independent third-party approval. It changes native-histogram persisted Struct children in place (`UInt32/UInt64` fields to `Int32/Int64`, including `count_u64`/`zero_count_u64` becoming `count_i64`/`zero_count_i64`) without a migration, downgrade, or mixed-version compatibility layer. This is an accepted release risk based on the feature being believed unused, not a compatibility guarantee.
