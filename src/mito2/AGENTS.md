# mito2 — Agent & Contributor Guide

Navigation aid for `src/mito2`. Keep it short and point to code; do not duplicate
the code here. Paths are relative to the repo root.

Repo-wide rules that apply here: [`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

## What this crate does

Mito2 is GreptimeDB's primary time-series region storage engine. It owns the
write path (memtable + WAL), flushing memtables to Parquet SST files,
TWCS/windowed compaction, and the read path (multi-level merge + dedup with
snapshot isolation). It implements the `RegionEngine` trait from `store-api`.

## Module map

| Module | Path | Purpose |
| --- | --- | --- |
| `engine` | `src/mito2/src/engine.rs` | `MitoEngine` (the `RegionEngine` impl) and request dispatch |
| `worker` | `src/mito2/src/worker.rs`, `src/mito2/src/worker/` | Per-region worker loop; write/alter/flush handlers |
| `region` | `src/mito2/src/region.rs`, `src/mito2/src/region/version.rs` | `MitoRegion` state and copy-on-write `VersionControl` snapshots |
| `request` | `src/mito2/src/request.rs` | `WriteRequest`/`RegionRequest` types and result channels |
| `wal` | `src/mito2/src/wal.rs` | Write-ahead log wrapper over `log-store` |
| `memtable` | `src/mito2/src/memtable/` | In-memory write buffers (time-series / bulk / partition) |
| `flush` | `src/mito2/src/flush.rs` | `FlushScheduler`, `WriteBufferManager`, memtable → SST |
| `compaction` | `src/mito2/src/compaction/` | Compaction scheduler (`scheduler.rs` + `scheduler/`), TWCS picker, strict-window manual picker, compactor, memory control |
| `compaction/multi_region` | `src/mito2/src/compaction/multi_region.rs` | Enterprise lossless routing by a captured table partition rule; selected SSTs to per-region outputs, without manifest publication |
| `engine/table_compaction` | `src/mito2/src/engine/table_compaction.rs` | Enterprise conditional publication of table-compaction results: shared-input snapshot, verified-output checks, and the manifest-locked swap of shared inputs for L0 outputs |
| `access_layer` | `src/mito2/src/access_layer.rs` | SST read/write over the object store |
| `sst` | `src/mito2/src/sst/` | Parquet format, file metadata, index layout |
| `series_index` | `src/mito2/src/series_index/` | Format interfaces (`format.rs`), index writer/searcher, sample artifacts and source coverage (`artifact.rs`), bucket planning, catalogs, reconciliation (`maintenance.rs`), and deferred purge |
| `read` | `src/mito2/src/read/` | `ScanRegion`, merge, dedup, projection, streaming |
| `manifest` | `src/mito2/src/manifest/` | `RegionManifestManager`, manifest actions/edits |
| `cache` | `src/mito2/src/cache.rs` | Write/file/page caches |
| `gc` | `src/mito2/src/gc.rs`, `src/mito2/src/gc/` | Dropped-file cleanup worker |
| `schedule` | `src/mito2/src/schedule/` | Local/remote background job scheduling |
| `remap_manifest` | `src/mito2/src/remap_manifest.rs` | Manifest path remapping for region copy/migration |
| `config` | `src/mito2/src/config.rs` | `MitoConfig` tuning knobs |
| `tail_replay` | `src/mito2/src/tail_replay.rs`, `src/mito2/src/worker/handle_tail_replay.rs` | Background replay of pending WAL tails into SSTs, published with one manifest edit |
| `temp_file_sweeper` | `src/mito2/src/temp_file_sweeper.rs` | Reclaims temporary files a crashed writer left in a region, triggered when the region becomes a writable leader or a create opens an existing region (under the `hdfs-object-store` feature) |
| `test_util` | `src/mito2/src/test_util.rs` | `TestEnv` and builders (under the `test` feature) |

## Write path

`MitoEngine::handle_request` (`engine.rs`), or `handle_batch_put_requests` for the puts
of one request to several regions (one message per worker) → worker loop
(`worker/handle_write.rs`) → sequence + WAL assembly (`region_write_ctx.rs`) →
`wal.rs` → memtable (`memtable/`) → when buffer pressure trips, `flush.rs`
writes SSTs via `access_layer.rs` and appends a `RegionEdit` to the manifest
(`manifest/manager.rs`).

When reconstructing primary key bytes, follow the writer's ordering. Mito encodes
raw tags in declared primary key order. Metric-engine sparse keys encode table ID
and TSID first, then non-null labels in label-name order.

## Read path

`MitoEngine::handle_query` (`engine.rs`) → `read/scan_region.rs` takes an
immutable `Version` (`region/version.rs`) → scans memtables and Parquet SSTs
(`sst/parquet.rs`) → merges (`read/`) and dedups by sequence → projected,
filtered `RecordBatch` stream.

Compaction SST scans share a read path. Multi-region compaction retains raw
versions and routes each row before merging. Sparse routing temporarily
materializes only the columns used by the partition rule.

Internal series merges use the query runtime pool forwarded through
`QueryScanContext`; reader and output-batch accounting uses the separate scan
memory tracker. Direct scanner callers default to an unbounded operator pool.

Live compaction queries read shared per-region state directly from `MitoRegion`,
without sending a worker request. The scheduler publishes active and pending task
state together; readers clone task Arcs under a short read lock, then filter and
build rows outside the lock. Execution handles provide live background phases.
There is no second task registry or completed-task history. See
`compaction/scheduler/state.rs`. The datanode provider reuses the lazy inspection scan
path and retains residual SQL filters.
Task cancellation uses `RegionRequest::CancelCompaction` through `Region.Handle`;
the owning worker matches the live task ID without entering the DDL queue.
Task-specific cancellation preserves other pending manual work when no DDL is
pending. DDL cancellation retains its existing scheduling fence and priority.

## Public surface

- Entry: `MitoEngine` in `src/mito2/src/engine.rs`, built via `MitoEngineBuilder`.
- Trait: `impl RegionEngine for MitoEngine` (`store-api`'s region engine contract).
- Consumed by `datanode` (sends `RegionRequest`s) and the query layer (scans).

## When you change X, also touch Y

- **Manifest format** (`manifest/action.rs`): affects crash recovery and
  follower replay. Keep it backward compatible. Compaction task state does not
  belong in the manifest; publication uses ordinary file edits after locked input
  checks. Shared SSTs require Enterprise capability. Compatibility fixtures:
  `tests/compatibility/manifest/table_compaction/` and
  `tests/compatibility/manifest/pending_tails/`.
- **SST/Parquet layout** (`sst/`): readers must stay compatible with existing files.
- **Index extensions**: shared contracts and provider validation live in `sst/index/registry.rs`;
  typed sketch options are enterprise-gated. Lifecycle and failure tests live in
  `engine/index_build_test.rs` and `compaction/`. A background rebuild must not replace a file
  that carries extension indexes (`worker/handle_rebuild_index.rs::needs_background_indexes`).
- **Series-index disk limit**: tasks estimate usage from entry sizes in open-region snapshots
  and defer new builds when the shared estimate is full. Cleanup still prunes obsolete
  range entries and expires series indexes to reclaim published usage. Concurrent builds may
  overshoot; closed-region files are not counted. Series handles and the SST purger own file deletion.
- **Series format PoC**: samples are rebuildable local SST sidecars. Optional writer interfaces (`format.rs`) integrate with reconciliation, catalog publication, and deferred purge. Defaults retain the original Parquet writer and disable samples. The opt-in columnar backend (`series_index/columnar.rs`) implements streaming OpenDAL writes and pinned format reads; its wire contract is in `docs/formats/mito-series.md`. Artifact objects record paths and sizes, including the manifest, so reads need no stat. Codecs currently run inline pending scheduling benchmarks. Sample payload reads coalesce nearby selected ranges in bounded, memory-accounted windows; boundary blocks keep timestamp-first pruning. Aligned native payload blocks share the page-cache budget with Parquet, with generation/namespace isolation and no shared lock held across I/O. Columnar stage timings and read counters are exported through Mito metrics. Columnar `SeriesScan` raw reads use scanner-wide preparation in `read/series_scan/prepared.rs`; ordinary record-batch scans retain their SST path. Native reader metadata shares the decoded SST cache reservation; maintenance readers bypass this cache. The paired `experimental_series_index_format` setting selects legacy or columnar artifacts.
- **Series-index coverage** (`series_index/catalog.rs`): `SeriesIndexEntry` stores
  compaction-window width and SST summaries keyed by aligned start in both catalogs and Parquet footers.
- **Local index cache**: startup checks a root layout version before workers start; missing or incompatible versions reset the whole rebuildable cache, including catalogs and range indexes. Samples use one generation directory; source SST identity stays in metadata.
- **Columnar samples**: `series_index/samples.rs` implements opt-in sample artifacts,
  including nullable Float64 fields and preserved sequence/delete rows. Its wire
  contract is `docs/formats/mito-samples.md`; time/ID bounds may overlap across
  leaves and parts. Both columnar backends share cancellation-safe output ownership
  in `series_index/artifact_writer.rs`. Format readers do not apply query visibility; the prepared-source adapter applies visibility and the existing merge/dedup pipeline. Eligible sample merges use identity-only keys and omit tag columns; candidates retain memory-accounted series batch/row labels, and the raw adapter attaches explicit labels with region-qualified identities. Scanner preflight declares that representation so lazy streams and planned frames agree. Non-separable tag/sample residuals retain the full flat schema. Manual reconciliation covers all snapshot SSTs regardless of maintenance eligibility, but retains the soft disk limit. Manual builds validate columnar sample and tag schemas before reuse. Columnar reconciliation consolidates snapshot SSTs into UTC-day sample generations; catalogs record all source IDs. Grouped reads require complete daily coverage and all sources still visible, open each artifact once, and fall back for source-sequence-pruned scans. SST replacement invalidates a whole affected generation; catalog publication and reader handles govern retirement.
- **Request types** (`request.rs`): usually tied to proto definitions consumed by `datanode`.
- **WAL/memtable encoding** (`wal/`, `memtable/`): breaks replay if changed incompatibly.

## Testing

```bash
cargo nextest run -p mito2
```

Tests live next to the code as `*_test.rs` (e.g. `src/mito2/src/engine/flush_test.rs`).
`TestEnv` in `test_util.rs` spins up an engine over an in-process object store.

## Gotchas

- Parquet versus Narthex storage is fixed at region creation. ALTER cannot cross
  that boundary; COPY, remap, and imported edits must reject mismatches before
  publication. Readers and compaction select storage from region options, not
  by inspecting individual files. Parquet flat/primary-key layouts may coexist.

- Native series-index maintenance and scans share fixed-window sequence watermarks over manifest-tracked index files. The coverage contract lives in `docs/formats/mito-series.md`.

- Parquet row groups have uniform row counts except for the final group
  (`sst/parquet/row_selection.rs`). Memory-pressure flushing must not create
  arbitrary partial groups: readers and index row IDs rely on this layout.
- Append-mode Narthex raw-series reads apply LastRow dedup after merging;
  non-append reads use the configured merge mode, and batch reads retain append
  semantics. Physical COPY follows Parquet's
  untrusted source-sequence semantics; repartition still assigns admission sequences.
  The region worker owns COPY locks; task guards hold them through publication and
  installation so retries cannot race failure cleanup, including across region reopen.
  The last active or queued task removes its lock entry, including on cancellation.
- Sequence numbers are strictly increasing per region; dedup and snapshot reads
  depend on this. Do not change assignment lightly.
- Manifest version is monotonic — never reset or skip it. Staging takes no version:
  its actions stay in memory (`manifest/manager.rs`) and are committed once, as the
  next version, when staging ends (`MitoRegion::end_staging_locked`).
- In regions whose writes require a lease and whose WAL is the object store WAL,
  each manifest version has one fixed object name, created conditionally. A late
  create can succeed again once checkpoint cleanup frees the name, so a commit
  checks its result (`ensure_version_not_checkpointed` in `manifest/storage.rs`).
- In those regions a commit after the region's Leader deadline sends nothing, and a version
  conflict fences the region (`ManifestContext::fence_for_reload`): no lease or catchup promotes
  it until the worker swaps in an instance reloaded from the new manifest
  (`worker/handle_reload.rs`). A tail replay round that fails while the storage holds a newer
  manifest fences a writable leader the same way. Never await a worker sender while holding a
  manifest guard. The catchup commit (`ManifestContext::commit_catchup`) is the exception: it
  commits as a Follower without the deadline check, and a conflict fails the catchup retryably
  without fencing the region; a retry reopens it from the newer manifest.
- Pending tails (`RegionManifest::pending_tails`) are WAL ranges a region still has
  to recover; the flushed entry id never prunes them. Opening a region replays its
  WAL after `RegionManifest::wal_replay_start`, and so does a catchup. The WAL releases
  at open, after a flush, on close and in a catchup stay below this node's tails
  (`cap_wal_release`); truncate, drop and offline cleanup release without that cap. Handoff
  floors are dropped once the flushed entry id reaches them, in
  `RegionManifestBuilder::try_build`. While a
  region has pending tails, scans and memtable/SST stats fail with
  `RegionPendingTails`, and so do truncate, entering staging, a repartition flush,
  a close with `flush_on_close`, and every ALTER other than adding nullable columns
  without a default or changing options; compaction pauses. These checks run
  before any region state changes. Full-listing GC skips a region that has pending
  tails or whose manifest `has_update()` reports newer, or fails to check.
- A catchup with `set_writable` and the source's last entry, of a region whose manifest is
  published conditionally and that does not await its Leader lease, hands the region over
  without replaying: see `hand_over` and `HANDOVER_SEQUENCE_GAP` in `region/catchup.rs`. It
  refuses a region with rows in its memtable. During any catchup a manifest sync and a
  follower's option change are refused, as the catchup changes the version outside the worker.
- A leader that is writable, altering or editing replays its pending tails in one
  background task (`tail_replay.rs`), started by the worker when a lease, catchup or
  role change makes the region writable, or when it registers a region created or opened
  as a writable leader. Each tail goes into its own SSTs, since an
  SST can give all its rows its largest sequence. The task commits outside the edit queue
  (`ManifestContext::update_manifest_for_tails`) and always reports its end, which
  lets the worker start the next replay or reload a fenced region. The edit that records
  tails must raise the committed sequence to theirs, or writes accepted during the replay
  lose to replayed rows. This node's later entry ids must be above every tail's `upto`, and
  its unflushed entries at or below one must be in its own tails, or the tail commit's
  flushed entry id skips them. A node bounds its replays' concurrency and SST writes apart
  from flushes, and their chunk memory within the global write buffer (`TailReplayLimits`).
  Replayed SSTs build indexes as flushed ones (`IndexBuildType::Flush`), under
  `create_on_flush`. `RegionRequest::DiscardPendingTails` discards one node's tails
  without replaying them; it commits through `update_manifest_for_tails` too
  (`worker/handle_tail_replay.rs`) and raises the flushed entry id to the largest
  discarded `upto`, so the same entry id rule applies.
- Lock ordering: take the manifest lock before updating `version_control`; the
  reverse deadlocks against concurrent flush/compaction.
- All region I/O runs on tokio workers; never `block_on` inside a worker.
- Loaded regions have resident compaction state, including `Idle` and handed-off
  DDLs. Publish create/open/catchup results through the worker's `register_region`;
  only close/drop unregisters them. See `compaction/scheduler/state.rs`.
- Explicit truncate retires shared references too (`worker/handle_truncate.rs`):
  `All` removes all data; `ByTimeRanges` removes fully contained files. Other
  regions/readers still protect the shared objects. Stale compaction results must
  not reinstate truncated inputs.
- Manifest and runtime file views share immutable `FileMetaRef` values. The engine-local
  metadata interner compares complete metadata before sharing; file identity alone is insufficient.
  Partition expressions are shared by table and complete expression content, so historical
  SST partition rules remain distinct after repartition.
  Interned references reclaim entries on final release, including delayed readers,
  without a maintenance task or new writes. Hash table capacity remains allocated
  until the interner is dropped.
  Manifest consumers receive the interner directly. Scrape-time entry counts use weak
  references and do not add maintenance tasks or retain engine storage.
  Index replacement preserves old readers and updates the lifetime guard to the newest metadata.
- Local compaction units use the snapshot-free `CompactionContext`. Retaining a
  full `CompactionRegion` in a unit pins other units' inputs and delays SST purge.
- Candidate waiting history is owned by the region scheduler (`scheduler/history.rs`).
  Background planning works on a private snapshot; only a fenced result may replace
  resident history, and an accepted plan consumes the fairness turn for each selected
  key. Execution success or failure must not rewrite waiting ages. TWCS discovery
  must not mutate scheduler state.
- Compaction admission is engine-wide (`compaction/scheduler/coordinator.rs`),
  but pick/DDL state remains worker-owned. Repick only after every batch unit's
  `Released`; FIFO submission is not admission and pipeline children share the
  parent unit's slot. Keep service accounting across batches until region unload.
- In flat batches, equal `__primary_key` dictionary keys imply the same series, but
  not the reverse: `wrap_pk_binary_to_dict` gives every row its own key. Compare key
  bytes when grouping rows by series.
- Staging regions, ending staging included, never flush: a flush would stage its SST and
  release the rows' WAL, losing them if staging does not end successfully. Every flush
  trigger in `worker/handle_flush.rs` must skip them: background triggers test `is_writable()`
  and `defers_background_flush()`, a flush request `is_flushable()` and `is_staging()` (`is_writable()` and
  `is_flushable()` are false while ending staging). `handle_close` leaves staging first.
  Tail replays never run for them: entering staging fails while tails are pending.
- Direct SST imports without a staging binding cannot publish into staging.
  `worker/handle_manifest.rs` seals edit admission while EnterStaging drains accepted
  edits. Scheduler-owned DDLs resume through execution, bypassing initial admission.
- On a node whose writes need a lease, a region that a catchup made writable waits for a
  Leader or StagingLeader lease sent after the catchup (`awaits_leader_lease`). Until then
  `ManifestContext::update_locked` refuses its commits unless they are staged or the region is
  downgrading (a close that must flush downgrades first); only a replay of tails
  (`TailsEditor::Replay`) skips the check, a discard does not, and ending staging checks it
  before `commit_staged_locked`. `ManifestContext::commit_catchup` commits an empty edit (a
  handover's edit through `commit_catchup_locked`) as a Follower; fire its hook only after
  every manifest guard is dropped, including the old instance's read guard held by
  `handle_caught_up_again`. No flush or compaction trigger starts work for a
  waiting region except a downgrade's flush. The lease that ends the wait wakes the worker
  (`handle_leader_lease_wait_ended`) to schedule the flushes the wait deferred and, only if the
  region is then a writable leader, start a compaction. The missing indexes, the refused index
  build retries and the backfilled metadata wait in `deferred_lease_work`, which the worker
  sweeps after each batch (`sweep_deferred_lease_work`). A catchup of a waiting
  region catches it up again into an unregistered replacement while the region stays registered,
  demoted unless it is in a transient leader state. The replacement replaces it only if the
  replay succeeds, the region took no write and made no commit meanwhile and, for a catchup that
  makes it writable, the replacement's catchup commit succeeds; otherwise the region keeps its
  memtable for the next Leader lease (`catch_up_again`, `handle_caught_up_again`); the datanode
  upgrade handler sends that catchup unless `RegionEngine::is_confirmed_leader` is true, so an
  engine wrapping mito must implement that method from mito's answers (the metric engine requires
  both regions of the pair). A Follower lease that carries a `candidate_lease_id` leaves a waiting
  region alone (`ManifestContext::demote_candidate`); other Follower leases demote it.
- The latest lease each region received (`GrantedLeases` in `lease.rs`) lives outside the
  region instance, so a reopen during catchup keeps it. Every path that removes a region from
  a worker's region map must also remove its record and its deferred lease work:
  `RegionWorkerLoop::remove_region` (`worker/handle_close.rs`, also used after a close-time
  flush), `drop_region` (`worker/handle_drop.rs`) and `RegionWorkerLoop::clean` (`worker.rs`).

## Maintenance contract

Update this file when you add/rename a top-level module, change the write/read
path entry points, or alter a persisted format (manifest, SST, WAL).
