# Proto, lock, and Unflushed contract (#8726 → #8768 → #8856)

**Status:** adapted persisted/wire and dependency chain. The final aggregate retains the implemented Unflushed path.

## Original PR patch — intended upstream behavior

Immutable upstream commits: [#8726](https://github.com/GreptimeTeam/greptimedb/commit/9f724aa5f6466a3e90c4f6407f2f6e00211ed925), [#8768](https://github.com/GreptimeTeam/greptimedb/commit/78084a9d4485ee61ff03e071f6428fc57c906332), and [#8856](https://github.com/GreptimeTeam/greptimedb/commit/1af4c335242e7e5e06ee91bc428a71bb631d0798). They extend heartbeat/lifecycle state, add Unflushed truncation, and separate procedure submission context.

## Resulting backport patch — what ships

The carriers are [#8726](https://github.com/GreptimeTeam/greptimedb/commit/45309752df03f6a5fdda03d741727637de28ed01), [#8768](https://github.com/GreptimeTeam/greptimedb/commit/cf69ee5df93bb8a6ab43337c4941e103a12c6f2e), and [#8856](https://github.com/GreptimeTeam/greptimedb/commit/a451a16cd06d1c1936baaf011991f2c3e3a0cc7e). Release extra [0e477aa](https://github.com/GreptimeTeam/greptimedb/commit/0e477aa945d03dd131376cfff1f5cec0c3b47a69) restores selected beta2 lock dependency edges/versions and documents the distributed Flow NULL-field contract; [94a4bb7](https://github.com/GreptimeTeam/greptimedb/commit/94a4bb78b3bb8dab0a8ae773c57dc39b3598172f) performs event submission-context conversion and is not proto/Unflushed behavior; [b5c38fb](https://github.com/GreptimeTeam/greptimedb/commit/b5c38fbfc15053f42d4c1d843204bbdd52cb4a00) performs final proto #335 lock alignment before the #8825 carrier. Proto revisions are #333, #334, and #335 respectively.

## Relative to original PR — exact differences

| Aspect | Original PR | Backport | Why |
| --- | --- | --- | --- |
| Dependency graph | Mainline graph | Release graph re-resolved from beta2 lock | Avoid unrelated dependency drift |
| Proto sequence | Upstream revisions | #333 → #334 → #335 staged across carriers | Preserve generated wire compatibility at each dependency boundary |
| Unflushed truncate | Upstream implementation | `RegionRequest::Truncate(Unflushed)` remains implemented | Flow already supports it; no temporary rejection imported |
| Context consumer | Upstream API context | Release frontend consumer signature | Match by-value `ExecutorContext` API |

## Intentionally excluded/not provided

Unrelated main dependency upgrades, SQLness generated-result content beyond recorded terminator extras, and the event lane's temporary unsupported Unflushed rejection are not provided.

## Compatibility and rollback impact

Proto and lock revisions are a mixed-version boundary. Review generated contracts and lock package identities together; rolling back only one stage can leave incompatible generated types or dependency resolution. The final aggregate is aligned to proto #335.

## Files reviewers should inspect

- `Cargo.toml` and `Cargo.lock`: inspect release graph, selected greptime-proto revision, and package alignment.
- External `greptime-proto` revision #333/#334/#335: inspect the revision-dependent generated fields and request variants.
- `src/common/grpc/`: inspect local generated-proto consumers and request variants.
- `src/flow/` and `src/mito2/`: inspect Unflushed truncate implementation and consumers.
- `src/frontend/`: inspect the final `ExecutorContext` consumer signature.

## Verification evidence already recorded

Recorded CI status is documented in the [canonical CI status](README.md#canonical-ci-status): run 32341712112 at aggregate HEAD completed with **failure**. No new test result is claimed.

## Raw audit evidence — unabridged differences only

- [`range-diffs/pr-8726.txt`](../range-diffs/pr-8726.txt)
- [`range-diffs/pr-8768.txt`](../range-diffs/pr-8768.txt)
- [`range-diffs/pr-8856.txt`](../range-diffs/pr-8856.txt)

These files show patch-to-patch differences only, not the full shipping patch.
