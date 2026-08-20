# Event context compatibility (#8734, #8834, #8835, #8852)

**Status:** adapted mixed-version event-context chain. Rolling compatibility extras are part of what ships.

## Original PR patch — intended upstream behavior

Immutable upstream patches: [#8734](https://github.com/GreptimeTeam/greptimedb/commit/1f1c9270a806658d2c3d9700c8cd1445d687f89f) adds event context; [#8834](https://github.com/GreptimeTeam/greptimedb/commit/72f6cf09bf7f7d78c84514163554ad2a29fc7f05) centralizes conversion; [#8835](https://github.com/GreptimeTeam/greptimedb/commit/943eee852f292bfffd7240867dc27248216c353f) records admin executions; [#8852](https://github.com/GreptimeTeam/greptimedb/commit/154f90b3652bf9b1b3e7eccc5f277f856a2f3079) hardens permissions and visibility.

## Resulting backport patch — what ships

Carriers are [#8734](https://github.com/GreptimeTeam/greptimedb/commit/408929ffd8bad5aa75547274ab62e667f90a449c), [#8834](https://github.com/GreptimeTeam/greptimedb/commit/e712162bd37f3e4aca3d339991c453153a3639fa), [#8835](https://github.com/GreptimeTeam/greptimedb/commit/0d5585ba332fe63374ae72e5745643465bb78fc4), and [#8852](https://github.com/GreptimeTeam/greptimedb/commit/4cb869ace4f9ac05c3cd6a470c6dad19cdd82491). Rolling extras [legacy fallback](https://github.com/GreptimeTeam/greptimedb/commit/587f001adfee05e4446fbb1b14c51afad4647bc1) and [single conversion](https://github.com/GreptimeTeam/greptimedb/commit/94a4bb78b3bb8dab0a8ae773c57dc39b3598172f) complete compatibility. The aggregate head is [b5c38fb](https://github.com/GreptimeTeam/greptimedb/commit/b5c38fbfc15053f42d4c1d843204bbdd52cb4a00).

## Relative to original PR — exact differences

| Aspect | Original PR | Backport | Why |
| --- | --- | --- | --- |
| Channel handling (#8834) | Upstream Channel enum handling | Release `common_session::channel_protocol(u8)` and legacy QueryContext conversion | Match beta2 protocol/context APIs; visible mixed-version item |
| Builder context (#8835) | Upstream builder imports/API | Release `CatalogManagerRef` plus `EventRecorderRef` context | Match release builder surface |
| Recording layer | Admin execution recording | `AdminFunctionRecordingLayer` remains installed | Preserve intended event recording |
| #8852 conflict | Upstream file context | Both Flow imports and `api::v1::greptime_request::Request` retained | Resolve aggregate conflict without dropping either behavior |
| Rolling input | New protobuf event context | Legacy QueryContext/extensions fallback, then one persistent conversion | Support old frontends that omit `event_context` |

## Intentionally excluded/not provided

Upstream Channel-enum handling is not used in the release path. No claim is made that old frontends supply protobuf `event_context`; the legacy fallback is the compatibility path. Unrelated event changes are excluded.

## Compatibility and rollback impact

This is a mixed-version boundary: old frontend messages may omit `event_context`, so fallback conversion must remain available. Rolling back only centralization or only the fallback can lose actor/channel context or duplicate conversion. Review the chain together.

## Files reviewers should inspect

- `src/common/session/src/lib.rs`: inspect `channel_protocol(u8)` and legacy channel mapping.
- `src/meta-srv/src/service/procedure.rs`: inspect actor/submission handling and legacy fallback.
- `src/operator/` and event/procedure builders: inspect `CatalogManagerRef`, `EventRecorderRef`, and one-time conversion.
- `src/frontend/src/instance.rs`: inspect the #8852 import conflict resolution and Flow/request imports.

## Verification evidence already recorded

Recorded CI status is documented in the [canonical CI status](README.md#canonical-ci-status): run 32341712112 at aggregate HEAD completed with **failure**. No new test result is claimed.

## Raw audit evidence — unabridged differences only

- [`range-diffs/pr-8734.txt`](../range-diffs/pr-8734.txt)
- [`range-diffs/pr-8834.txt`](../range-diffs/pr-8834.txt)
- [`range-diffs/pr-8835.txt`](../range-diffs/pr-8835.txt)
- [`range-diffs/pr-8852.txt`](../range-diffs/pr-8852.txt)

Each range-diff shows differences between patches, not the full shipping patch.
