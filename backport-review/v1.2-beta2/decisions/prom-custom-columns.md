# Prometheus custom columns (#8659)

**Status:** adapted release API behavior.

## Original PR patch — intended upstream behavior

The immutable upstream patch is [#8659](https://github.com/GreptimeTeam/greptimedb/commit/d90cca4b752b34d7ccdb54fe8445ce3bf832e5e0), enabling Prometheus remote reads with custom timestamp and value columns.

## Resulting backport patch — what ships

The aggregate carrier is [8cc4cab](https://github.com/GreptimeTeam/greptimedb/commit/8cc4cab224dee0991b0c96f7b88df7d8221572ea). It adapts custom timestamp/value-column remote reads to beta2 frontend and server APIs; final paths include `src/frontend/src/instance/prom_store.rs` and `src/servers/src/prom_store.rs`.

## Relative to original PR — exact differences

| Aspect | Original PR | Backport | Why |
| --- | --- | --- | --- |
| Remote read plumbing | Upstream frontend/server APIs | Beta2 frontend/server APIs | Release protocol surface differs |
| Custom columns | Timestamp and value columns supported | Same intended custom-column behavior | Adaptation is contextual, not a stated semantic reduction |
| Final locations | Upstream file layout | `src/frontend/src/instance/prom_store.rs` and `src/servers/src/prom_store.rs` | Match release layout |

## Intentionally excluded/not provided

No unrelated PromQL protocol changes are included. This page does not claim compatibility for APIs outside the custom-column read path.

## Compatibility and rollback impact

Clients using custom timestamp/value columns depend on both frontend request handling and server read conversion. Roll back the carrier consistently across those paths to avoid an API mismatch.

## Files reviewers should inspect

- `src/frontend/src/instance/prom_store.rs`: inspect remote-read request conversion and custom columns.
- `src/servers/src/prom_store.rs`: inspect protocol/server handling of the adapted request.
- Prometheus remote-read tests under these crates: inspect existing recorded coverage.

## Verification evidence already recorded

Recorded CI status is documented in the [canonical CI status](README.md#canonical-ci-status): run 32341712112 at aggregate HEAD completed with **failure**. No new test result is claimed.

## Raw audit evidence — unabridged differences only

[`range-diffs/pr-8659.txt`](../range-diffs/pr-8659.txt) is patch-to-patch evidence only, not the full shipping patch.
