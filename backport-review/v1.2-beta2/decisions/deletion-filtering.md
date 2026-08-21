# Deletion filtering (#8872)

**Status:** adapted storage semantics; overlap safety is retained with explicit exclusions.

## Original PR patch — intended upstream behavior

The immutable upstream patch is [#8872](https://github.com/GreptimeTeam/greptimedb/commit/3c80df043a46f0511bd81a64a072ca18436efac3), keeping deletion markers when compacting part of a window.

## Resulting backport patch — what ships

The aggregate carrier is [ec84c0f](https://github.com/GreptimeTeam/greptimedb/commit/ec84c0f7177d2f14b1a148cee56d818c41701638). It retains the deletion-marker overlap recheck on the beta2 compaction model.

## Relative to original PR — exact differences

| Aspect | Original PR | Backport | Why |
| --- | --- | --- | --- |
| Marker retention | Keep markers during partial-window compaction | Same intended retention | Release compaction model supports the replay |
| Overlap check | Upstream recheck | Recheck retained | Preserves safety against overlapping deletion markers |
| Newer-window priority | Not the #8872 concern | Explicitly excluded (#8714) | Avoid importing unrelated behavior and test |
| `MixedRange` | Not required by #8872 | Explicitly excluded (#8784) | Release API/semantic scope |

## Intentionally excluded/not provided

#8714 newer-window priority and its copied test are excluded. #8784 `MixedRange` is excluded. This page does not imply either behavior is present in beta2.

## Compatibility and rollback impact

Compaction output and deletion visibility can change if the overlap recheck is removed. Roll back the carrier as a unit with any dependent compaction code; no new on-disk format is asserted here.

## Files reviewers should inspect

- `src/mito2/`: partial-window compaction and deletion-marker filtering.
- Compaction tests adjacent to the carrier: inspect overlap recheck and marker retention.
- Do not infer #8714 or #8784 behavior from this carrier; inspect their absence in the aggregate diff if needed.

## Verification evidence already recorded

Recorded CI status is documented in the [canonical CI status](README.md#canonical-ci-status): run 32341712112 at aggregate HEAD completed with **failure**. No new test result is claimed.

## Raw audit evidence — unabridged differences only

[`range-diffs/pr-8872.txt`](../range-diffs/pr-8872.txt) shows differences between the upstream and backport patches only; it is not the full shipping patch.
