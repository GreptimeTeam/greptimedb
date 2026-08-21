# Remote schema validation (#8579 + folded #8818)

**Status:** reconstructed/folded semantic concern.

## Original PR patch — intended upstream behavior

Immutable upstream commits: [#8579](https://github.com/GreptimeTeam/greptimedb/commit/bbc91ea637516c4657a8cdb9f40289f126b9ef61) validates MergeScan remote schemas; [#8818](https://github.com/GreptimeTeam/greptimedb/commit/cd6efa0abf6ec99737ad8bce36376c4946023edc) narrows metadata comparison by ignoring field metadata.

## Resulting backport patch — what ships

The reconstructed carrier is [9d8894b](https://github.com/GreptimeTeam/greptimedb/commit/9d8894b7f8b3e3391e4347f1a7a9ce3d1c5182fa). It folds #8818 metadata behavior into #8579 and separately preserves JSON extension compatibility. No separate shipping carrier is claimed for #8818.

## Relative to original PR — exact differences

| Aspect | Original PR | Backport | Why |
| --- | --- | --- | --- |
| Schema identity | Validate remote MergeScan schema | Same validation on release MergeScan model | Release execution APIs differ |
| Metadata | #8818 ignores ordinary field metadata | Name/type/nullability remain significant; ordinary metadata does not | Fold #8818 semantics into reconstructed carrier |
| Lifecycle APIs | Mainline context may include #8615 APIs | #8615 `select_target`/dynamic-filter lifecycle APIs excluded | Not part of this release concern |

## Intentionally excluded/not provided

Unrelated #8615 APIs are not imported. Ordinary metadata differences are not treated as schema identity differences; this does not mean name, type, or nullability differences are accepted.

## Compatibility and rollback impact

Remote readers and writers must agree on schema identity rules. A rollback can alter which metadata differences reject a scan; review mixed-version behavior before reverting only one side.

## Files reviewers should inspect

- `src/query/src/dist_plan/merge_scan.rs`: inspect MergeScan implementation and remote schema identity rules.
- `src/mito2/src/sst/parquet/json_align/stream.rs`: inspect parquet stream schema handling.
- `src/datatypes/src/schema/ext.rs` and `src/datatypes/src/extension/json.rs`: inspect folded extension/schema compatibility behavior.
- Query and parquet schema tests in the aggregate diff: inspect name/type/nullability versus ordinary metadata cases.

## Verification evidence already recorded

Recorded CI status is documented in the [canonical CI status](README.md#canonical-ci-status): run 32341712112 at aggregate HEAD completed with **failure**. No new test result is claimed.

## Raw audit evidence — unabridged differences only

- [`range-diffs/pr-8579.txt`](../range-diffs/pr-8579.txt)
- [`range-diffs/pr-8818.txt`](../range-diffs/pr-8818.txt)

The range-diffs are differences between patches, not the complete shipping patch.
