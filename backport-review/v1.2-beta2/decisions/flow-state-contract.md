# Flow state contract (#8392 slice + #8729)

**Status:** release-compatible slice with an explicit persisted/wire constraint.

## Original PR patch — intended upstream behavior

The immutable upstream commits are [#8392](https://github.com/GreptimeTeam/greptimedb/commit/cabc2f6cc667291d7bc3dcbb8f9cc82e27bd93a0) and [#8729](https://github.com/GreptimeTeam/greptimedb/commit/70470bafbe38df2d3fd21e70e1e7097ffd7317c6). Together they add Flow statistics/status and correct aggregation and SQL quoting in the upstream model.

## Resulting backport patch — what ships

The slice is [f913538](https://github.com/GreptimeTeam/greptimedb/commit/f9135380b5591b82c9ec5bbf1706459672bd72db), followed by [#8729 carrier](https://github.com/GreptimeTeam/greptimedb/commit/c4baa56b23888fa2a2692b39ae01500d3f295efa) and [contract correction](https://github.com/GreptimeTeam/greptimedb/commit/be9106be721f8794f83694272a24b90360dc8cbe). The selected statistics/status behavior ships without importing the newer state layout.

## Relative to original PR — exact differences

| Aspect | Original PR | Backport | Why |
| --- | --- | --- | --- |
| `FlowStateValue` | Upstream three-field evolution | Remains `state_size + last_exec_time_map` | Preserve beta2 persisted/wire contract |
| Distributed fields | `start_time`/`uptime_seconds` behavior available upstream | Unsupported/NULL | Release-compatible slice does not add those fields |
| Local statistics | Flow statistics/status behavior | Selected statistics/status behavior and quoting fixes | Keep behavior that fits beta2 |
| Merge semantics | Upstream aggregation model | Per-node merge semantics retained | #8729 was applied after the slice |

## Intentionally excluded/not provided

The distributed `start_time` and `uptime_seconds` fields are not provided and remain NULL/unsupported. The upstream three-field persisted/wire `FlowStateValue` contract is intentionally excluded.

## Compatibility and rollback impact

The two-field state layout is the compatibility boundary for beta2 readers/writers. Adding the upstream field layout later requires a separately reviewed migration and mixed-version plan; this page does not provide one.

## Files reviewers should inspect

- `src/flow/`: inspect Flow statistics, status output, merge behavior, and the two-field state representation.
- `src/frontend/`: inspect `SHOW FLOW STATUS` request/result integration.
- `src/common/meta/src/key/flow/flow_state.rs`: inspect inline state-contract tests and persisted/wire fields.
- `tests/cases/standalone/common/flow/flow_status.sql` and `tests/cases/standalone/common/flow/flow_status.result`: inspect recorded Flow status cases.

## Verification evidence already recorded

Recorded CI status is documented in the [canonical CI status](README.md#canonical-ci-status): run 32341712112 at aggregate HEAD completed with **failure**. No new test result is claimed.

## Raw audit evidence — unabridged differences only

- [`range-diffs/pr-8392.txt`](../range-diffs/pr-8392.txt)
- [`range-diffs/pr-8729.txt`](../range-diffs/pr-8729.txt)

These unabridged range-diffs compare patches; they are not the full shipping patch.
