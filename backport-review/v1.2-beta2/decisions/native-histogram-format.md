# Native histogram format (#8824)

**Status:** accepted breaking-format exception. It was accepted by explicit release-owner direction in this backport session. Public artifacts do not identify a named approver or date; [Issue comment 5352919911](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5352919911) is the public record of the accepted exception, not evidence of independent third-party approval. The feature is only believed unused; this is not a compatibility-safe migration.

## Original PR patch — intended upstream behavior

The immutable upstream commit is [#8824](https://github.com/GreptimeTeam/greptimedb/commit/3510ef7d4c13f9afa3ad8fdafa8842f98dfd5cb3): change native-histogram persisted Struct children from unsigned to signed integer types, including `count_u64`/`zero_count_u64` to `count_i64`/`zero_count_i64`.

## Resulting backport patch — what ships

The immutable aggregate carrier is [b0b6a98](https://github.com/GreptimeTeam/greptimedb/commit/b0b6a9821d9160d598d0a1e3a6cab1b5cfe59a1d). It applies the same format change to the beta2 baseline. There is no migration or downgrade layer.

## Relative to original PR — exact differences

| Aspect | Original PR | Backport | Why |
| --- | --- | --- | --- |
| Struct child types/names | Unsigned children become signed; `*_u64` names change to `*_i64` | Same intended change | Release baseline requires replaying the format patch |
| Existing persisted data | Upstream context | No conversion supplied | Beta2 has no migration/downgrade layer |
| Release decision | Mainline change | Explicitly accepted breaking exception | Feature is only believed unused |

## Intentionally excluded/not provided

No migration, dual reader, downgrade path, or claim of compatibility safety is provided. Do not treat absence of observed use as proof that old data cannot exist.

## Compatibility and rollback impact

Old persisted Struct data may be unreadable under the new schema, and rollback cannot be assumed safe without a data migration. Rollback consequence: reverting binaries does not itself restore or convert data written with the changed children. Mixed-version operation should be treated as unsupported for this format.

## Files reviewers should inspect

- `src/common/query/src/native_histogram.rs` and `src/promql/src/functions/native_histogram.rs`: inspect child names and integer types in the model/serialization paths.
- `src/mito2/`: inspect persisted schema consumers and whether any migration is present.
- The release aggregate's native histogram tests: confirm the intended signed representation, without treating tests as migration coverage.

## Verification evidence already recorded

Recorded CI status is documented in the [canonical CI status](README.md#canonical-ci-status): run 32341712112 at aggregate HEAD completed with **failure**. The aggregate diff is also recorded evidence; no new test result is claimed by this page.

## Raw audit evidence — unabridged differences only

[`range-diffs/pr-8824.txt`](../range-diffs/pr-8824.txt) is the unabridged range-diff. It shows differences between the upstream and backport patches, not the full shipping patch.
