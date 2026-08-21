# v1.2.0-beta.2 backport review index

## Review target

- Release base: `2f5e97850e55d86c0ed9eff1719994c88b4450a0`
- Aggregate branch: `work/release-v1.2-beta2-complete`
- Aggregate head: `cb52adcbf798d3d2f009874f46dc5d476073bd8c`
- Aggregate commits: 49 (the prior 48-commit aggregate history plus the #8752 secure_fs test-portability follow-up)
- CI evidence: Historical [GitHub Actions run 32341712112](https://github.com/GreptimeTeam/greptimedb/actions/runs/32341712112), whose actual head SHA was `b5c38fbfc15053f42d4c1d843204bbdd52cb4a00`, failed on the superseded-base aggregate: Clippy, Check Unused Dependencies, and Check (ubuntu-latest) failed; SQLness, Compatibility Test, binary builds, Rustfmt, license, and fuzz passed; `test` was skipped downstream because Clippy and Check Unused Dependencies failed, while `coverage` was skipped independently because this was a `workflow_dispatch` run and coverage runs only for `merge_group`. The later [Release run 32462587383](https://github.com/GreptimeTeam/greptimedb/actions/runs/32462587383) was triggered at the pre-#8751 head `81fd630955f16b9c45811539aa8434b47d5a875b`: its Windows job `96713326607` failed in the composite Windows-artifact step, while the overall run was subsequently cancelled and dependent jobs were cancelled. The refreshed Release run [32466550985](https://github.com/GreptimeTeam/greptimedb/actions/runs/32466550985) Windows job `96725255397` also failed. These Windows results are evidence of job outcomes, not a diagnosis and not final CI success for the refreshed head. Final-head tag-push [Release run 32470573285](https://github.com/GreptimeTeam/greptimedb/actions/runs/32470573285) at `cb52adcbf798d3d2f009874f46dc5d476073bd8c` completed all platform artifacts and Compatibility Test successfully, then was cancelled because the query regression was queued on unavailable `perf-regression-8-cores`. Same-tag [workflow_dispatch recovery run 32487995883](https://github.com/GreptimeTeam/greptimedb/actions/runs/32487995883) completed successfully with `release_validation=skip-all`: query regression and Compatibility Test were skipped, so neither skipped validation is claimed as passed; platform artifacts, DockerHub images, and GitHub release publication succeeded. GitHub release `v1.2.0-beta.2` is `draft=false`, `prerelease=true`, published at `2026-08-21T14:51:51Z`, with 14 assets.
- `release/v1.2` was fast-forwarded to the aggregate head `cb52adcbf798d3d2f009874f46dc5d476073bd8c`; the range remains linear with zero merge commits.

## How to read the artifacts

- **[Start here: canonical manual decision index](decisions/README.md)**: priority-ordered semantic concerns, exact differences, exclusions, compatibility impact, and reviewer file pointers.
- PR #8895 mapping: upstream merge `d7f1233f775d54380318d6ad9ff62504a7cbcff1`, head `6b8645bf584a04b45828c7e8d0fb19418db70d2e`; carrier `fed5c80d7e94bd2c8ab1f7ddac8ab205d993156d`; see [#8895 raw evidence](range-diffs/pr-8895.txt) and [JSON2 decision coverage](decisions/json2-mergescan.md).
- PR #8901 mapping: upstream merge `b96ea86a621e7283e77e4e37dd4e3ec8dfc44f73`, head `b450abd46e473a07cb1b42a9c7c62d0b0d551e17`; carrier `5a642091cd2b6185fbe6a08d6f76d8295955152f`; see [#8901 raw evidence](range-diffs/pr-8901.txt) and [JSON2 decision coverage](decisions/json2-mergescan.md).
- PR #8825 mapping: upstream head `bf947141753a9ea554aa1ca31637fdd3ad1429f3`, merge `60c7bbcaf30a0e65ad7fa2b2b8ad0c1199a662b1`; carrier `d18430b874f9c8054747dbf688585cd7d2988e3e`; final pre-version carrier `40e02ff6b62ad3445f7035e437bd9e322c8af31b`. Requested by WenyXu in [Issue #8892 comment 5353700059](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5353700059). See [#8825 raw evidence](range-diffs/pr-8825.txt) and [event-context compatibility](decisions/event-context-compatibility.md).
- `range-diffs/pr-<number>.txt`: normally a final one-commit `git range-diff --creation-factor=100` from the upstream merged PR commit to the final aggregate carrier commit. These raw `.txt` files are **unabridged differences-only evidence**, not the complete shipping patch; `pr-8825.txt` is explicitly raw metadata/command evidence because its upstream PR-head object is unavailable locally.
- `patch-id-verdict.tsv`: stable patch IDs and `exact`/`adapted` verdicts. Patch-ID equality means patch equivalence; it does not claim the cherry-pick had no conflict.
- `upstream-pr-map.tsv`: PR number, merged commit, state, URL, and title.
- `aggregate-commit-provenance.tsv`: final aggregate commits, cherry-pick trailers, patch IDs, and subjects; non-PR release-extra commits use an empty upstream field and are classified in this README.
- `aggregate-commits.tsv`: final ordered aggregate history.
- `aggregate.diff`: complete release-base-to-aggregate raw diff.
- `manual-deltas.md`: human explanation of every non-trivial adaptation and aggregate conflict.

## Scope

The review scope has 36 public Issue-requested PR relationships: the original 31 plus #8825, #8921, #8895, #8901, and #8902. The current Issue body contains the original 31 requests, while the public comments add #8825, #8921, #8895, and #8901; [comment 5366438434](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5366438434), posted by `discord9` at `2026-08-21T07:12:57Z`, publicly requests #8902. Official PR #8921 is now in the release base `2f5e97850e55d86c0ed9eff1719994c88b4450a0`, so it is base-present and is not represented as an aggregate carrier. #8672 and #8699 remain base-present as well. The other public Issue-requested PRs are represented by backport mappings; #8825 remains carried by `d18430b874f9c8054747dbf688585cd7d2988e3e`, and #8902 is carried by the final pre-version carrier `40e02ff6b62ad3445f7035e437bd9e322c8af31b`, followed by the non-PR version identity commit `81fd630955f16b9c45811539aa8434b47d5a875b`. The dependency closure/follow-up adds exactly four provenance relationships: full #8734, a release-compatible slice of #8392, #8751, and test-only #8752 (upstream merge 4e43c279ff29e9071394f85b90b874906115d29f), for 40 total provenance relationships. The patch-ID verdict contains 14 exact stable patch-ID relationships—#8777, #8832, #8787, #8849, #8859, #8889, #8833, #8600, #8808, #8709, #8522, #8739, #8772, and #8901—and 22 adapted relationships, including the production-only partial #8751 carrier and the test-only #8752 follow-up.

## Mapping classes

- **exact patch-id**: stable patch ID equals the upstream merged PR commit.
- **adapted**: the final aggregate patch differs from upstream because of release APIs, dependency/lock state, conflict resolution, or removal of unrelated main-only behavior.
- **folded**: the requested behavior is carried inside another reconstructed commit rather than a standalone commit; #8818 is folded into the reconstructed #8579 carrier.
- **slice**: only the release-compatible behavior was selected; #8392 keeps the beta2 two-field persisted/wire `FlowStateValue` contract.
- **dependency**: not listed in the original 31 but required by the selected event chain; #8734.
- **release-extra**: compatibility, lock, regression-test, formatting, generated-result, or non-PR release-version correction that is not represented as an upstream PR. The final version identity commit is classified here.

## Dependency closure: upstream #8751 required by #8734

Upstream PR [#8751](https://github.com/GreptimeTeam/greptimedb/pull/8751), merge `b70daafc77c87806afd00521c368545a60e5f574`, is recorded as a dependency-closure relationship required by upstream [#8734](https://github.com/GreptimeTeam/greptimedb/pull/8734). The aggregate carrier is `cb52adcbf798d3d2f009874f46dc5d476073bd8c`, and the release-base parent remains `2f5e97850e55d86c0ed9eff1719994c88b4450a0`.

The upstream PR has two hunks: a production cleanup in `src/common/meta/src/ddl/drop_table.rs` and a rollback regression-test hunk in `src/common/meta/src/ddl/tests/drop_table.rs`. The regression test was already present through the existing #8734 backport sequence, so the refreshed aggregate carrier contains only the production hunk. This is a production-only partial upstream carrier; its stable patch ID is therefore not equal to the full upstream patch, and `patch-id-verdict.tsv` classifies #8751 as **adapted**, not exact. The raw comparison is [`range-diffs/pr-8751.txt`](range-diffs/pr-8751.txt).

### Verification evidence

- Before #8751: local Main verification recorded a 4/4 failure.
- After #8751: Main recorded a 20/20 no-retry pass, log SHA `fa9ae951fa78dce66c7b0aedc81cd172c9fc898445712162c616c99aeb7000d8`.
- After #8751: common-meta recorded a 546/546 no-retry pass, log SHA `84b54cdff1767a2c0e0683e59a955472eacabc769871c5d893940f07d932a79a`.

These are local before/after verification records; final release evidence is recorded in the CI evidence above.

## Accepted breaking-format exception

PR #8824 is accepted by explicit release-owner direction in this backport session. Public artifacts do not identify a named approver or date; Issue comment [5352919911](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5352919911) is the public record of the accepted exception, not evidence of independent third-party approval. It changes native-histogram persisted Struct children in place (`UInt32/UInt64` fields to `Int32/Int64`, including `count_u64`/`zero_count_u64` becoming `count_i64`/`zero_count_i64`) without a migration, downgrade, or mixed-version compatibility layer. This is an accepted release risk based on the feature being believed unused, not a compatibility guarantee.

## Final rebased range, appended #8902, dependency closure #8751, and test-only #8752 follow-up

The final review range is `2f5e97850e55d86c0ed9eff1719994c88b4450a0..cb52adcbf798d3d2f009874f46dc5d476073bd8c`, a linear 49-commit range. The superseded beta2 base `8eeff8b4417e6fb675a66cecbdba40060322785c` and endpoint `b7fdcb89568dc707c323f8c0a3b9413ec4e889ab` are historical provenance only; all active aggregate carrier fields in the TSV indexes are remapped to the rebased commits. Official #8921 is the release-base commit `2f5e97850e55d86c0ed9eff1719994c88b4450a0`, not a duplicate aggregate carrier.

The three selected PRs #8672, #8699, and #8921 are base-present; the other selected PRs have aggregate provenance. Thus the machine-readable indexes contain exactly 36 public Issue-requested PR rows plus exactly the four closure/follow-up rows (#8734, #8392, #8751, and #8752): 40 unique provenance relationships.

PR #8902 is carried by pre-version commit `40e02ff6b62ad3445f7035e437bd9e322c8af31b`; the final version identity is recorded by non-PR release-extra commit `cb52adcbf798d3d2f009874f46dc5d476073bd8c`; its readable decision is [Dictionary group-by fast path](decisions/dictionary-groupby-fast-path.md), and its raw patch comparison is [#8902](range-diffs/pr-8902.txt).

## Test-only follow-up: upstream #8752 and secure_fs portability

Upstream PR [#8752](https://github.com/GreptimeTeam/greptimedb/pull/8752), merge `4e43c279ff29e9071394f85b90b874906115d29f`, is appended as a test-only dependency-closure/follow-up relationship for the #8735/#8708 `secure_fs` behavior chain. The target parent lacked the #8735 context, so this carrier is adapted to the beta2 parent; the resulting test body matches final main. Carrier `cb52adcbf798d3d2f009874f46dc5d476073bd8c` changes only `src/object-store/src/secure_fs.rs`, preserving the portable call/path checks while gating the strong `is_none()` assertion to non-Windows. See [`range-diffs/pr-8752.txt`](range-diffs/pr-8752.txt).

Oracle review: **APPROVE**. Focused Main verification passed 20/20 without retry, log SHA `fc831f2b2d51c79e8b1fbc6267e8fe8bac90d06c746e1136067e9bcbcbec40df`; fixer focused verification and full object-store verification each passed 20/20 without retry. Earlier Release run [32466550985](https://github.com/GreptimeTeam/greptimedb/actions/runs/32466550985) Windows job `96725255397` failed; final release evidence is recorded in the CI evidence above.
