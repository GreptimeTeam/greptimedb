# JSON2 MergeScan (#8745)

**Status:** adapted semantic change with release helper and focused test extras.

## Original PR patch — intended upstream behavior

The immutable upstream patch is [#8745](https://github.com/GreptimeTeam/greptimedb/commit/95d9d92e42bfaa3338cf460bdb031803e29368bf), separating the JSON2 extension type in MergeScan/parquet handling.

## Resulting backport patch — what ships

The primary carrier is [ed18426](https://github.com/GreptimeTeam/greptimedb/commit/ed184264fabf2076fe65a7bf6a30b0c6ce205dba). Release extras [MergeScan helper wiring](https://github.com/GreptimeTeam/greptimedb/commit/e897a65e16e1a5ebd1fa667a6c6ea6718e91a12d) and [legacy identity coverage](https://github.com/GreptimeTeam/greptimedb/commit/234fea812fd2b0406cf148806aa8566a74a3fcd6) complete the behavior on the beta2 model.

## Relative to original PR — exact differences

| Aspect | Original PR | Backport | Why |
| --- | --- | --- | --- |
| Extension split | Mainline JSON2 extension type separation | Same semantic split on release MergeScan | Release MergeScan APIs differ |
| Helper wiring | Upstream helper locations | Explicit release helper commit | Complete adaptation without importing unrelated API changes |
| Regression coverage | Upstream tests | Focused legacy JSON2 identity test extra | Make release behavior observable |

## Intentionally excluded/not provided

No unrelated query lifecycle or dynamic-filter API is included. The helper/test commits are release completion work, not claims that the upstream patch was incomplete.

## Compatibility and rollback impact

Remote parquet readers and MergeScan schema identity must agree on JSON2 extension representation. A partial rollback can reintroduce extension identity mismatches; revert the semantic carrier and its helper wiring together.

## Files reviewers should inspect

- `src/query/src/dist_plan/merge_scan.rs` and `src/query/src/datafusion/json_expr_planner.rs`: inspect MergeScan extension-type selection and JSON2 hints.
- `src/mito2/src/sst/parquet/json_align/`: inspect parquet read integration.
- Focused JSON2 tests in `src/query/` and `src/mito2/`: inspect legacy identity expectations.

## Verification evidence already recorded

Recorded CI status is documented in the [canonical CI status](README.md#canonical-ci-status): run 32341712112 at aggregate HEAD completed with **failure**. No new test result is claimed.

## Raw audit evidence — unabridged differences only

[`range-diffs/pr-8745.txt`](../range-diffs/pr-8745.txt) is an unabridged patch-to-patch range-diff, not the full shipping patch.
