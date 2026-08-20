# v1.2.0-beta.2 backport review index

## Review target

- Release base: `8eeff8b4417e6fb675a66cecbdba40060322785c`
- Aggregate branch: `work/release-v1.2-beta2-complete`
- Aggregate head: `b5c38fbfc15053f42d4c1d843204bbdd52cb4a00`
- Aggregate commits: 40
- CI: [completed with conclusion `failure`](https://github.com/GreptimeTeam/greptimedb/actions/runs/32341712112) at aggregate HEAD `b5c38fbfc15053f42d4c1d843204bbdd52cb4a00`; failed jobs: Clippy, Check Unused Dependencies, and Check (ubuntu-latest). SQLness, Compatibility Test, binary builds, Rustfmt, license, and fuzz jobs passed; `test` was skipped downstream because Clippy and Check Unused Dependencies failed, while `coverage` was skipped independently because this was a `workflow_dispatch` run and coverage runs only for `merge_group`, not because of failed prerequisites.
- `release/v1.2` was not updated by this work.

## How to read the artifacts

- **[Start here: canonical manual decision index](decisions/README.md)**: priority-ordered semantic concerns, exact differences, exclusions, compatibility impact, and reviewer file pointers.
- `range-diffs/pr-<number>.txt`: final one-commit `git range-diff --creation-factor=100` from the upstream merged PR commit to the final aggregate carrier commit. These raw `.txt` files are **unabridged differences-only evidence**, not the complete shipping patch.
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

PR #8824 is accepted by explicit release-owner direction in this backport session. Public artifacts do not identify a named approver or date; Issue comment [5352919911](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5352919911) is the public record of the accepted exception, not evidence of independent third-party approval. It changes native-histogram persisted Struct children in place (`UInt32/UInt64` fields to `Int32/Int64`, including `count_u64`/`zero_count_u64` becoming `count_i64`/`zero_count_i64`) without a migration, downgrade, or mixed-version compatibility layer. This is an accepted release risk based on the feature being believed unused, not a compatibility guarantee.
