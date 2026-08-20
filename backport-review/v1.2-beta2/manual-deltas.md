# Manual backport and aggregate delta ledger

The canonical reviewer index is [`decisions/README.md`](decisions/README.md). This ledger preserves the per-carrier rationale; raw `.txt` range-diffs remain unabridged differences-only audit evidence.

## Flow / Frontend

### #8726 — adapted
- Aggregate: `45309752df03f6a5fdda03d741727637de28ed01`
- Kept the release dependency graph and advanced `greptime-proto` only to heartbeat-extension revision #333 (`032510ded...`) at this boundary.
- Re-resolved `Cargo.lock` from the release lock instead of importing unrelated main dependency drift.

### #8600 — exact patch-id
- Aggregate: `1ad6c2a1cdaa129bf744c4d3d70188916fba223e`.

### #8768 — adapted
- Aggregate: `cf69ee5df93bb8a6ab43337c4941e103a12c6f2e`.
- Advances proto to #334 (`e7be20ff...`) for Unflushed truncate.
- Lock was re-resolved against the release dependency graph.
- SQLness EOF formatting is carried by explicit release-extra commits instead of being hidden in this PR.

### #8392 — release-compatible slice
- Aggregate carrier: `0b6702b7ccc0d0fdbd5f53490743dee6a197f516`.
- Selected Flow statistics and `SHOW FLOW STATUS` behavior.
- Did not import the main-line three-field persisted/wire state contract. `FlowStateValue` remains `state_size + last_exec_time_map`; local `FlowStat` may retain `start_time_map`; distributed `start_time`/`uptime_seconds` stay unsupported/NULL.

### #8729 — adapted
- Aggregate: `239c4e62b177203dc440fb3faabad8cb6fd70d4f`.
- Applied after the #8392 slice, retaining per-node merge semantics and identifier quoting while keeping the release two-field state contract via `84a83d31...`.

### #8860 — adapted
- Aggregate: `0ffca14740493d0c1afa556908345d9e135d7656`.
- Updated `pgwire` to 0.40.7 while retaining the release `windows-sys 0.60.2` graph; lock conflict was resolved with Cargo's offline resolver.

### #8858 — adapted
- Aggregate: `065d0dfaadc2d6252adc5a671ec0658cfd51d4d6`.
- Adapted the auth public-export hunk because the release baseline lacks `SEMANTIC_GRAPH_QUERY`; no behavioral divergence was identified. This is a release API/context adaptation, not evidence that every adapted item has semantic risk.

### #8898 — adapted
- Aggregate: `e2a85ff19b00edba80640123b17166704a0c1117`.
- Updated the release dashboard baseline from v0.13.10 to v0.13.13; upstream starts from v0.13.12.

### Flow release extras
- `84a83d31...`: restore the beta2 two-field heartbeat/persisted contract.
- `0e477aa9...`: restore selected beta2 `Cargo.lock` dependency edges/versions and document the distributed Flow NULL-field contract.
- `8b131f46...`, `2e908d51...`: SQLness result terminator handling; the real runner rewrote these generated EOF separators.

## Query / JSON / Storage

### #8579 + #8818 — reconstructed/folded
- Aggregate carrier: `a7c296cf46cc4f1ce0241231cbb8b8031dfb1bcf`.
- #8579 remote-schema validation is retained without importing #8615 `select_target`/dynamic-filter lifecycle APIs.
- #8818 auxiliary Arrow metadata semantics are folded into this carrier: name/type/nullability differences remain significant, ordinary metadata differences do not; JSON extension compatibility remains separately constrained.

### #8745 — adapted
- Aggregate: `ed184264fabf2076fe65a7bf6a30b0c6ce205dba`.
- JSON2 extension-type split was replayed on the release MergeScan model.
- Final MergeScan helper wiring is isolated in `e897a65e...`; focused legacy JSON2 identity coverage is in `234fea81...`.

### #8872 — adapted
- Aggregate: `bc889665136f50067e5655757ff7f88ab07f177d`.
- Retains deletion-marker overlap recheck.
- Explicitly excludes unrelated #8714 newer-window-priority behavior and its copied test; no #8784 `MixedRange` import.

### #8824 — adapted, accepted breaking-format exception
- Aggregate: `a024250fdb7808562b100b0ef2b4d8e9deaeb9a3`.
- Accepted by explicit release-owner direction in this backport session.
- Public artifacts do not identify a named approver or date; Issue comment [5352919911](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5352919911) is the public record of the accepted exception, not evidence of independent third-party approval.
- Changes native-histogram persisted child names/types in place. There is no migration, downgrade, or mixed-version compatibility layer; the feature is only believed unused. Review as an explicit risk acceptance, not a compatibility-preserving backport.

### Other Query adaptations
- #8659 differs because it is replayed on release protocol/server APIs.
- #8898 is the dashboard version update only, adapted to the release file state.
- #8859, #8889, #8833, #8808, #8709, #8522, #8739, and #8772 have exact stable patch IDs in the final aggregate.

## Platform / Event

### #8825 — adapted
- Upstream PR head: `bf947141753a9ea554aa1ca31637fdd3ad1429f3`; merge: `60c7bbcaf30a0e65ad7fa2b2b8ad0c1199a662b1`.
- Requested by WenyXu in [Issue #8892 comment 5353700059](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5353700059).
- Aggregate carrier and final head: `b7fdcb89568dc707c323f8c0a3b9413ec4e889ab`.
- Adds shared `common_base::protocol::Channel`, re-exports it as `session::context::Channel`, and retains stable wire values/names and `Unknown` behavior. Session dialect mapping remains through the `dialect_for_channel` helper; common-meta derives protocol names from shared `Channel`.
- Semantic conflict resolution is explicit: shared mapping replaces `common_session::channel_protocol`, but the aggregate preserves the old-frontend fallback for omitted protobuf `event_context` and exactly-once protobuf → `PersistentEventContext` → `ProcedureEventInput` conversion. Actor/event JSON and protobuf schemas are unchanged.
- Narrow test adaptations `6c1424a8` and `b75c72c18` are retained and not reverted.
- Raw evidence: [`range-diffs/pr-8825.txt`](range-diffs/pr-8825.txt). The upstream PR-head object is unavailable locally, so no range-diff is fabricated.

### #8852 — adapted
- Aggregate: `4cb869ace4f9ac05c3cd6a470c6dad19cdd82491`.
- Aggregate conflict in `src/frontend/src/instance.rs` was a single test import adjacent to Flow changes. Retained both Flow imports and #8852's `api::v1::greptime_request::Request`.

### #8834 — adapted
- Aggregate: `e712162bd37f3e4aca3d339991c453153a3639fa`.
- The release path materially retains `common_session::channel_protocol(u8)` and legacy `QueryContext` conversion rather than upstream Channel-enum handling. This is a visible mixed-version compatibility item: old frontends can still arrive without the newer event-context representation.

### #8835 — adapted
- Aggregate: `0d5585ba332fe63374ae72e5745643465bb78fc4`.
- Builder imports and API context use the release `CatalogManagerRef` plus `EventRecorderRef` surface. `AdminFunctionRecordingLayer` remains installed, so the adaptation is context/API alignment rather than removal of admin execution recording.

### #8734 — dependency
- Aggregate: `408929ffd8bad5aa75547274ab62e667f90a449c`.
- Full event-context dependency required by the selected lifecycle/submission chain.

### #8856 — adapted
- Aggregate: `a451a16cd06d1c1936baaf011991f2c3e3a0cc7e`.
- Final aggregate advances proto #334 -> #335 (`32f467fa...`).
- `Cargo.toml`/`Cargo.lock` preserve prior Flow/Query dependency state and add the #335-generated contract.
- Flow already implements Unflushed truncate, so aggregate keeps `RegionRequest::Truncate(Unflushed)` rather than Event lane's temporary unsupported rejection.
- Includes the release frontend test consumer signature needed by the by-value `ExecutorContext` API.

### #8849 — exact patch-id
- Aggregate: `926d9bbd0ff038254c218e6e0e67de91a324e3c6`.
- Stable patch ID equals upstream. During one intermediate aggregate replay the same content was already present and appeared empty; the final aggregate contains the exact patch as the commit above.

### Rolling compatibility extras
- `587f001a...`: if an old frontend omits protobuf `event_context`, derive persistent context from legacy `QueryContext.extensions` and typed channel.
- `94a4bb78...`: perform protobuf -> persistent -> `ProcedureEventInput` conversion once at the correct layer.
- Aggregate conflict in `src/meta-srv/src/service/procedure.rs` retained actor/submission logic and the legacy fallback.

## Final aggregate-only changes

- `6735127c...`: rustfmt-only aggregate cleanup.
- `b5c38fbf...`: Cargo offline resolver update to align the final lock package dependency list with proto #335.

## Final proto chain

1. #8726: proto #333 `032510ded...` (heartbeat extensions).
2. #8768 / Flow final: proto #334 `e7be20ff...` (Unflushed truncate).
3. #8856 / aggregate final: proto #335 `32f467fa...` (procedure submission context).
