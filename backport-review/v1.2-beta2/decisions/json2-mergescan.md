# JSON2 MergeScan (#8745)

**Status:** adapted semantic change with release helper and focused test extras.

## Original PR patch — intended upstream behavior

The immutable upstream patch is [#8745](https://github.com/GreptimeTeam/greptimedb/commit/95d9d92e42bfaa3338cf460bdb031803e29368bf), separating the JSON2 extension type in MergeScan/parquet handling.

## Resulting backport patch — what ships

The primary carrier is [05c67e4](https://github.com/GreptimeTeam/greptimedb/commit/05c67e4d5f7bdadc9fe4393e95b7b5776b5e4bbf). Release extras [MergeScan helper wiring](https://github.com/GreptimeTeam/greptimedb/commit/c9c7f0958a45fd1189072947e0df4d1d6de6be5b) and [legacy identity coverage](https://github.com/GreptimeTeam/greptimedb/commit/3dc37d9f9e955bc98ad7ef86b25ad19553a34136) complete the behavior on the beta2 model.

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

## Additional JSON2 layout provenance

The explicit Issue-requested JSON2 layout PRs are also represented in the machine-readable indexes and have complete raw range evidence:

- **#8895**: upstream merge `d7f1233f775d54380318d6ad9ff62504a7cbcff1`, head `6b8645bf584a04b45828c7e8d0fb19418db70d2e`, complete upstream range `0a6a22033e3ebb4d6c7243874d840558a9eb7216^..6b8645bf584a04b45828c7e8d0fb19418db70d2e`, aggregate carrier `fed5c80d7e94bd2c8ab1f7ddac8ab205d993156d`. The carrier folds the two upstream commits, retains JSON2 DDL/layout settings, and omits the upstream-only >=v1.3.0 compatibility case; it is `adapted`.
- **#8901**: upstream merge `b96ea86a621e7283e77e4e37dd4e3ec8dfc44f73`, head `b450abd46e473a07cb1b42a9c7c62d0b0d551e17`, complete upstream range `6e637fbe58f36643fea40f38d5ff352fe00254f9^..b450abd46e473a07cb1b42a9c7c62d0b0d551e17`, aggregate carrier `5a642091cd2b6185fbe6a08d6f76d8295955152f`. The carrier folds the three upstream commits and is `exact` by stable patch ID.

These are part of the 36 explicit Issue-requested PR relationships. Together with base-present #8672/#8699/#8921 and the two closure relationships (#8734 full, #8392 release-compatible slice), the review contains 38 provenance relationships.
