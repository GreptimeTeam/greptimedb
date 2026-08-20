# Event context compatibility (#8734, #8834, #8835, #8852, #8825)

**Status:** adapted mixed-version event-context chain. Rolling compatibility extras and the shared Channel carrier are part of what ships.

## Original PR patch — intended upstream behavior

Immutable upstream patches: [#8734](https://github.com/GreptimeTeam/greptimedb/commit/1f1c9270a806658d2c3d9700c8cd1445d687f89f) adds event context; [#8834](https://github.com/GreptimeTeam/greptimedb/commit/72f6cf09bf7f7d78c84514163554ad2a29fc7f05) centralizes conversion; [#8835](https://github.com/GreptimeTeam/greptimedb/commit/943eee852f292bfffd7240867dc27248216c353f) records admin executions; [#8852](https://github.com/GreptimeTeam/greptimedb/commit/154f90b3652bf9b1b3e7eccc5f277f856a2f3079) hardens permissions and visibility. PR #8825 upstream has head `bf947141753a9ea554aa1ca31637fdd3ad1429f3` and merge commit [60c7bbcaf30a0e65ad7fa2b2b8ad0c1199a662b1](https://github.com/GreptimeTeam/greptimedb/commit/60c7bbcaf30a0e65ad7fa2b2b8ad0c1199a662b1), requested by WenyXu in [Issue #8892 comment 5353700059](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5353700059).

## Resulting backport patch — what ships

Carriers are [#8734](https://github.com/GreptimeTeam/greptimedb/commit/408929ffd8bad5aa75547274ab62e667f90a449c), [#8834](https://github.com/GreptimeTeam/greptimedb/commit/e712162bd37f3e4aca3d339991c453153a3639fa), [#8835](https://github.com/GreptimeTeam/greptimedb/commit/0d5585ba332fe63374ae72e5745643465bb78fc4), and [#8852](https://github.com/GreptimeTeam/greptimedb/commit/4cb869ace4f9ac05c3cd6a470c6dad19cdd82491). The #8825 carrier is [b7fdcb89568dc707c323f8c0a3b9413ec4e889ab](https://github.com/GreptimeTeam/greptimedb/commit/b7fdcb89568dc707c323f8c0a3b9413ec4e889ab), which is also the aggregate final head. Rolling extras [legacy fallback](https://github.com/GreptimeTeam/greptimedb/commit/587f001adfee05e4446fbb1b14c51afad4647bc1) and [single conversion](https://github.com/GreptimeTeam/greptimedb/commit/94a4bb78b3bb8dab0a8ae773c57dc39b3598172f) complete compatibility. The narrow test adaptations [6c1424a8](https://github.com/GreptimeTeam/greptimedb/commit/6c1424a8eef66dc2c3abf9a7be25a69f2ab69f33) and [b75c72c18](https://github.com/GreptimeTeam/greptimedb/commit/b75c72c18d39453cc693a594c64b68d31beb62c1) are retained and not reverted.

## PR #8825 mapping and exact conflict resolution

#8825 introduces the shared `common_base::protocol::Channel` and re-exports it as `session::context::Channel`. Its stable wire values and names are unchanged (`Unknown = 0`, `Mysql = 1` through `Splunk = 14`), and out-of-range values still map to `Unknown`. Session SQL-dialect mapping is retained through the `dialect_for_channel` helper. `common-meta` now derives protocol names from the shared `Channel`.

The semantic conflict resolution is deliberate: the shared Channel mapping replaces `common_session::channel_protocol`, but it does **not** replace the existing mixed-version event-context compatibility. The aggregate preserves the old-frontend fallback when protobuf `event_context` is omitted and preserves exactly-once protobuf → `PersistentEventContext` → `ProcedureEventInput` conversion. Actor/event JSON and protobuf schemas are unchanged. Therefore #8825 is classified as **adapted**, not exact: the shared mapping is integrated while the release compatibility behavior remains.

## Relative to original PR — exact differences

| Aspect | Upstream | Backport / reason |
| --- | --- | --- |
| Channel definition | Channel is shared by the upstream common/session consumers | `common_base::protocol::Channel` is shared; `session::context::Channel` re-exports it |
| Mapping | Stable wire values/names and Unknown behavior | Unchanged; common-meta uses shared Channel instead of `common_session::channel_protocol` |
| Session dialect | Channel-owned dialect behavior in upstream context | Release `dialect_for_channel` helper retained |
| Old frontends | Newer event-context path | Existing fallback for omitted protobuf `event_context` retained |
| Conversion | Centralized event context conversion | Exactly-once protobuf → `PersistentEventContext` → `ProcedureEventInput` retained |
| Schemas | Existing actor/event JSON and protobuf schema | Unchanged |
| Tests | Upstream test context | Narrow release adaptations `6c1424a8` and `b75c72c18` retained |

## Intentionally excluded/not provided

No schema change is introduced by #8825. No claim is made that old frontends supply protobuf `event_context`; the legacy fallback remains the compatibility path. Unrelated event changes are excluded.

## Compatibility and rollback impact

This is a mixed-version boundary: old frontend messages may omit `event_context`, so fallback conversion must remain available. Shared Channel unifies protocol mapping but does not authorize removing the fallback or the one-time conversion. Rolling back only centralization, only #8825, or only the fallback can lose actor/channel context or duplicate conversion. Review the chain together.

## Files reviewers should inspect

- `src/common/base/src/protocol.rs`: inspect the shared Channel enum, stable values/names, and Unknown mapping.
- `src/session/src/context.rs`: inspect the re-export and `dialect_for_channel` helper.
- `src/common/meta/src/rpc/ddl.rs`: inspect protocol derivation from shared Channel.
- `src/meta-srv/src/service/procedure.rs`: inspect actor/submission handling and legacy fallback.
- `src/operator/` and event/procedure builders: inspect `CatalogManagerRef`, `EventRecorderRef`, and one-time conversion.
- `src/frontend/src/instance.rs`: inspect the #8852 import conflict resolution and retained test adaptation.

## Verification evidence already recorded

Recorded CI status is documented in the [canonical CI status](README.md#canonical-ci-status): the pre-#8825 run 32341712112 completed with **failure**. No new test result is claimed.

## Raw audit evidence — unabridged differences only

- [`range-diffs/pr-8734.txt`](../range-diffs/pr-8734.txt)
- [`range-diffs/pr-8834.txt`](../range-diffs/pr-8834.txt)
- [`range-diffs/pr-8835.txt`](../range-diffs/pr-8835.txt)
- [`range-diffs/pr-8852.txt`](../range-diffs/pr-8852.txt)
- [`range-diffs/pr-8825.txt`](../range-diffs/pr-8825.txt) — raw metadata/command evidence; upstream PR-head object was unavailable, so no range-diff is fabricated.

The existing `.txt` range-diffs are unabridged differences-only evidence. The #8825 evidence file is explicitly labeled because it records exact metadata and failed commands rather than pretending to contain an upstream-to-carrier range-diff.
