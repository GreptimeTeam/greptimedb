# Manual backport and aggregate delta ledger

The canonical reviewer index is [`decisions/README.md`](decisions/README.md). This ledger preserves the per-carrier rationale; raw `.txt` range-diffs remain unabridged differences-only audit evidence.

## Flow / Frontend

### #8726 — adapted
- Aggregate: `424e53d8413719e348bc8a694d9d22d3a051ce68`
- Kept the release dependency graph and advanced `greptime-proto` only to heartbeat-extension revision #333 (`032510ded...`) at this boundary.
- Re-resolved `Cargo.lock` from the release lock instead of importing unrelated main dependency drift.

### #8600 — exact patch-id
- Aggregate: `9c683fccfc11cd5db720b14c7adb22b7d734defd`.

### #8768 — adapted
- Aggregate: `6618427fc630545d0ca742ca414ace71c16ebdaa`.
- Advances proto to #334 (`e7be20ff...`) for Unflushed truncate.
- Lock was re-resolved against the release dependency graph.
- SQLness EOF formatting is carried by explicit release-extra commits instead of being hidden in this PR.

### #8392 — release-compatible slice
- Aggregate carrier: `f9135380b5591b82c9ec5bbf1706459672bd72db`.
- Selected Flow statistics and `SHOW FLOW STATUS` behavior.
- Did not import the main-line three-field persisted/wire state contract. `FlowStateValue` remains `state_size + last_exec_time_map`; local `FlowStat` may retain `start_time_map`; distributed `start_time`/`uptime_seconds` stay unsupported/NULL.

### #8729 — adapted
- Aggregate: `c4baa56b23888fa2a2692b39ae01500d3f295efa`.
- Applied after the #8392 slice, retaining per-node merge semantics and identifier quoting while keeping the release two-field state contract via `be9106be...`.

### #8860 — adapted
- Aggregate: `507dda2d88033216f6e12e423bf8c20dd1579ba4`.
- Updated `pgwire` to 0.40.7 while retaining the release `windows-sys 0.60.2` graph; lock conflict was resolved with Cargo's offline resolver.

### #8858 — adapted
- Aggregate: `009f78781b5c52f191d26afea9f30584745b4e19`.
- Adapted the auth public-export hunk because the release baseline lacks `SEMANTIC_GRAPH_QUERY`; no behavioral divergence was identified. This is a release API/context adaptation, not evidence that every adapted item has semantic risk.

### #8898 — adapted
- Aggregate: `1d1ebdcfafc3dc6c4a1d4dfc4b8fc5dd854f34a9`.
- Updated the release dashboard baseline from v0.13.10 to v0.13.13; upstream starts from v0.13.12.

### Flow release extras
- `be9106be...`: restore the beta2 two-field heartbeat/persisted contract.
- `73b54382...`: restore selected beta2 `Cargo.lock` dependency edges/versions and document the distributed Flow NULL-field contract.
- `c96b21ef...`, `556fe09f...`: SQLness result terminator handling; the real runner rewrote these generated EOF separators.

## Query / JSON / Storage

### #8579 + #8818 — reconstructed/folded
- Aggregate carrier: `9d8894b7f8b3e3391e4347f1a7a9ce3d1c5182fa`.
- #8579 remote-schema validation is retained without importing #8615 `select_target`/dynamic-filter lifecycle APIs.
- #8818 auxiliary Arrow metadata semantics are folded into this carrier: name/type/nullability differences remain significant, ordinary metadata differences do not; JSON extension compatibility remains separately constrained.

### #8745 — adapted
- Aggregate: `05c67e4d5f7bdadc9fe4393e95b7b5776b5e4bbf`.
- JSON2 extension-type split was replayed on the release MergeScan model.
- Final MergeScan helper wiring is isolated in `c9c7f095...`; focused legacy JSON2 identity coverage is in `3dc37d9f...`.

### #8872 — adapted
- Aggregate: `ec84c0f7177d2f14b1a148cee56d818c41701638`.
- Retains deletion-marker overlap recheck.
- Explicitly excludes unrelated #8714 newer-window-priority behavior and its copied test; no #8784 `MixedRange` import.

### #8824 — adapted, accepted breaking-format exception
- Aggregate: `b0b6a9821d9160d598d0a1e3a6cab1b5cfe59a1d`.
- Accepted by explicit release-owner direction in this backport session.
- Public artifacts do not identify a named approver or date; Issue comment [5352919911](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5352919911) is the public record of the accepted exception, not evidence of independent third-party approval.
- Changes native-histogram persisted child names/types in place. There is no migration, downgrade, or mixed-version compatibility layer; the feature is only believed unused. Review as an explicit risk acceptance, not a compatibility-preserving backport.

### #8902 — adapted, two-commit upstream range folded into one carrier
- Upstream PR range: `d99b0df374eec0329ac3f430e40b80a1e6c6963c` followed by `1a91c76c615afec71239b84af7787a0505bef914`; PR head: `1a91c76c615afec71239b84af7787a0505bef914`; merge commit: `8d887ddd00d67699d0fb873cda11eb50b5771555`.
- Aggregate carrier: `40e02ff6b62ad3445f7035e437bd9e322c8af31b`. The full range comparison is [`range-diffs/pr-8902.txt`](range-diffs/pr-8902.txt), generated from `d99b0df^..1a91c76` against `40e02ff^..40e02ff`; its range-diff explicitly shows `1a91c76` as folded/absorbed (`< -: ---------`).
- The second upstream pin commit is absorbed because beta2 conflict resolution directly selected final DataFusion pin `4c8a6bf283` in the carrier, rather than replaying the pin commit separately. The carrier therefore accounts for both upstream commits while remaining one aggregate commit.
- Readable provenance is recorded in [`aggregate-commit-provenance.tsv`](aggregate-commit-provenance.tsv), with both upstream IDs on the `40e02ff` row.

### Other Query adaptations
- #8659 differs because it is replayed on release protocol/server APIs.
- #8898 is the dashboard version update only, adapted to the release file state.
- The 14 exact stable patch-ID relationships are #8777, #8832, #8787, #8849, #8859, #8889, #8833, #8600, #8808, #8709, #8522, #8739, #8772, and #8901. The remaining 20 verdicts are adapted.

## Platform / Event

### #8825 — adapted
- Upstream PR head: `bf947141753a9ea554aa1ca31637fdd3ad1429f3`; merge: `60c7bbcaf30a0e65ad7fa2b2b8ad0c1199a662b1`.
- Requested by WenyXu in [Issue #8892 comment 5353700059](https://github.com/GreptimeTeam/greptimedb/issues/8892#issuecomment-5353700059).
- Aggregate carrier: `d18430b874f9c8054747dbf688585cd7d2988e3e`; final head: `40e02ff6b62ad3445f7035e437bd9e322c8af31b`.
- Adds shared `common_base::protocol::Channel`, re-exports it as `session::context::Channel`, and retains stable wire values/names and `Unknown` behavior. Session dialect mapping remains through the `dialect_for_channel` helper; common-meta derives protocol names from shared `Channel`.
- Semantic conflict resolution is explicit: shared mapping replaces `common_session::channel_protocol`, but the aggregate preserves the old-frontend fallback for omitted protobuf `event_context` and exactly-once protobuf → `PersistentEventContext` → `ProcedureEventInput` conversion. Actor/event JSON and protobuf schemas are unchanged.
- Narrow test adaptations `9bf878f5` and `6595b5d7b` are retained and not reverted.
- Raw evidence: [`range-diffs/pr-8825.txt`](range-diffs/pr-8825.txt). The upstream PR-head object is unavailable locally, so no range-diff is fabricated.

### #8852 — adapted
- Aggregate: `2927fa339ec677fb632a8940bcd0d43eea78881a`.
- Aggregate conflict in `src/frontend/src/instance.rs` was a single test import adjacent to Flow changes. Retained both Flow imports and #8852's `api::v1::greptime_request::Request`.

### #8834 — adapted
- Aggregate: `5d059c84bd2f7ecb724d5a84501196d57f27481a`.
- The release path materially retains `common_session::channel_protocol(u8)` and legacy `QueryContext` conversion rather than upstream Channel-enum handling. This is a visible mixed-version compatibility item: old frontends can still arrive without the newer event-context representation.

### #8835 — adapted
- Aggregate: `22174d32dadf9ea58073b15c72828db9927969c6`.
- Builder imports and API context use the release `CatalogManagerRef` plus `EventRecorderRef` surface. `AdminFunctionRecordingLayer` remains installed, so the adaptation is context/API alignment rather than removal of admin execution recording.

### #8734 — dependency
- Aggregate: `e9ad585975022b206275afb2735ac480869bcd33`.
- Full event-context dependency required by the selected lifecycle/submission chain.

### #8856 — adapted
- Aggregate: `cf6c0033593bd2c6cc617718ff5d0a19a7d9dd1c`.
- Final aggregate advances proto #334 -> #335 (`32f467fa...`).
- `Cargo.toml`/`Cargo.lock` preserve prior Flow/Query dependency state and add the #335-generated contract.
- Flow already implements Unflushed truncate, so aggregate keeps `RegionRequest::Truncate(Unflushed)` rather than Event lane's temporary unsupported rejection.
- Includes the release frontend test consumer signature needed by the by-value `ExecutorContext` API.

### #8849 — exact patch-id
- Aggregate: `ab808586525877230b9774f2ce29aca590152642`.
- Stable patch ID equals upstream. During one intermediate aggregate replay the same content was already present and appeared empty; the final aggregate contains the exact patch as the commit above.

### Rolling compatibility extras
- `46edabe9...`: if an old frontend omits protobuf `event_context`, derive persistent context from legacy `QueryContext.extensions` and typed channel.
- `e61a6df6...`: perform protobuf -> persistent -> `ProcedureEventInput` conversion once at the correct layer.
- Aggregate conflict in `src/meta-srv/src/service/procedure.rs` retained actor/submission logic and the legacy fallback.

## Final aggregate-only changes

- `7b74b73d...`: rustfmt-only aggregate cleanup.
- `3e8992f3...`: Cargo offline resolver update to align the final lock package dependency list with proto #335.

## Final proto chain

1. #8726: proto #333 `032510ded...` (heartbeat extensions).
2. #8768 / Flow final: proto #334 `e7be20ff...` (Unflushed truncate).
3. #8856 / aggregate final: proto #335 `32f467fa...` (procedure submission context).

### #8902 — adapted, appended final carrier
- Upstream implementation: `d99b0df374eec0329ac3f430e40b80a1e6c6963c`; final aggregate carrier: `40e02ff6b62ad3445f7035e437bd9e322c8af31b`.
- DataFusion `452cb4b` advances to linear successor `4c8a6bf`, preserving #8842 and adding `DictionaryGroupValuesColumn`.
- GreptimeDB adds `tests-integration/tests/dict_groupby_sst.rs` plus module registration. The test writes 1,200 rows, flushes append-mode flat SST, and exactly verifies six ordered tuples.
- This integration test is not direct fast-path proof: AggregateExec EXPLAIN/metrics do not expose GroupValues identity. Direct `DictionaryGroupValuesColumn` coverage is in the pinned DataFusion fork tests.

### #8895 — adapted, two-commit upstream range folded into one carrier
- Upstream PR range: `0a6a22033e3ebb4d6c7243874d840558a9eb7216` followed by `6b8645bf584a04b45828c7e8d0fb19418db70d2e`; PR head: `6b8645bf584a04b45828c7e8d0fb19418db70d2e`; merge: `d7f1233f775d54380318d6ad9ff62504a7cbcff1`.
- Aggregate carrier: `fed5c80d7e94bd2c8ab1f7ddac8ab205d993156d`.
- The final carrier folds the complete upstream PR range, retains the JSON2 DDL/layout-settings behavior, and omits the upstream-only >=v1.3.0 compatibility-test case. Stable patch IDs therefore differ and the verdict is `adapted`.
- Complete range-diff evidence, including endpoint metadata and the exact generation command, is [`range-diffs/pr-8895.txt`](range-diffs/pr-8895.txt).

### #8901 — exact patch-id, three-commit upstream range folded into one carrier
- Upstream PR range: `6e637fbe58f36643fea40f38d5ff352fe00254f9`, `f2e3d6d6e16c24fdede441cf54ff1c04a3effd91`, and `b450abd46e473a07cb1b42a9c7c62d0b0d551e17`; PR head: `b450abd46e473a07cb1b42a9c7c62d0b0d551e17`; merge: `b96ea86a621e7283e77e4e37dd4e3ec8dfc44f73`.
- Aggregate carrier: `5a642091cd2b6185fbe6a08d6f76d8295955152f`.
- The final carrier folds the complete upstream PR range and retains the JSON2 v2 physical-layout primitives and dependency updates. Its stable patch ID equals the upstream merge patch ID; the changed subject is an aggregate adaptation, so the verdict records `exact` patch equivalence.
- Complete range-diff evidence, including endpoint metadata and the exact generation command, is [`range-diffs/pr-8901.txt`](range-diffs/pr-8901.txt).

## Final provenance scope

The final review records 36 public Issue-requested PR relationships: the original 31 plus #8825, #8921, #8895, #8901, and #8902. Of those, #8672, #8699, and #8921 are base-present. The dependency closure contributes exactly two additional relationships—full #8734 and the release-compatible slice of #8392—for 38 unique provenance relationships total.
