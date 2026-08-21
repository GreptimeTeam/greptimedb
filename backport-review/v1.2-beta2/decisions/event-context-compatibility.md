# Event context compatibility (#8734, #8834, #8835, #8852, #8825)

**Status:** adapted mixed-version event-context chain. Rolling compatibility extras and the shared Channel carrier are part of what ships.

## Original PR patch — intended upstream behavior

Immutable upstream patches: [#8734](https://github.com/GreptimeTeam/greptimedb/commit/1f1c9270a806658d2c3d9700c8cd1445d687f89f) adds event context; [#8834](https://github.com/GreptimeTeam/greptimedb/commit/72f6cf09bf7f7d78c84514163554ad2a29fc7f05) centralizes conversion; [#8835](https://github.com/GreptimeTeam/greptimedb/commit/943eee852f292bfffd7240867dc27248216c353f) records admin executions; [#8852](https://github.com/GreptimeTeam/greptimedb/commit/154f90b3652bf9b1b3e7eccc5f277f856a2f3079) hardens permissions and visibility. PR #8825 upstream has head `bf947141753a9ea554aa1ca31637fdd3ad1429f3` and merge commit [60c7bbcaf30a0e65ad7fa2b2b8ad0c1199a662b1](https://github.com/GreptimeTeam/greptimedb/commit/60c7bbcaf30a0e65ad7fa2b2b8ad0c1199a662b1), requested by WenyXu in [Issue #8892 comment 5353700059](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5353700059).

## Resulting backport patch — what ships

Carriers are [#8734](https://github.com/GreptimeTeam/greptimedb/commit/e9ad585975022b206275afb2735ac480869bcd33), [#8834](https://github.com/GreptimeTeam/greptimedb/commit/5d059c84bd2f7ecb724d5a84501196d57f27481a), [#8835](https://github.com/GreptimeTeam/greptimedb/commit/22174d32dadf9ea58073b15c72828db9927969c6), and [#8852](https://github.com/GreptimeTeam/greptimedb/commit/2927fa339ec677fb632a8940bcd0d43eea78881a). The #8825 carrier is [d18430b874f9c8054747dbf688585cd7d2988e3e](https://github.com/GreptimeTeam/greptimedb/commit/d18430b874f9c8054747dbf688585cd7d2988e3e), the final aggregate head is `6f150e01cd4221d4c62c5f7082f6d318b3110e00` (with `40e02ff6b62ad3445f7035e437bd9e322c8af31b` as the pre-version carrier). Rolling extras [legacy fallback](https://github.com/GreptimeTeam/greptimedb/commit/46edabe916a10c2cafe816b8fe01f0d5da8fffc8) and [single conversion](https://github.com/GreptimeTeam/greptimedb/commit/e61a6df6b2b110badd5755f68010b6439d69ac84) complete compatibility. The narrow test adaptations [9bf878f5](https://github.com/GreptimeTeam/greptimedb/commit/9bf878f562518d30a412138ad4244d7d8e7867e6) and [6595b5d7b](https://github.com/GreptimeTeam/greptimedb/commit/6595b5d7bf1f59587bf89676d420db7371cdc459) are retained and not reverted.

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
| Tests | Upstream test context | Narrow release adaptations `9bf878f5` and `6595b5d7b` retained |

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
