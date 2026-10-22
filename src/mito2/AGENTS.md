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
| `access_layer` | `src/mito2/src/access_layer.rs` | SST read/write over the object store |
| `sst` | `src/mito2/src/sst/` | Parquet format, file metadata, index layout |
| `series_index` | `src/mito2/src/series_index/` | Format interfaces (`format.rs`), index writer/searcher, per-SST samples artifacts (`artifact.rs`), bucket planning, catalogs, reconciliation (`maintenance.rs`), and deferred purge |
| `read` | `src/mito2/src/read/` | `ScanRegion`, merge, dedup, projection, streaming |
| `manifest` | `src/mito2/src/manifest/` | `RegionManifestManager`, manifest actions/edits |
| `cache` | `src/mito2/src/cache.rs` | Write/file/page caches |
| `gc` | `src/mito2/src/gc.rs`, `src/mito2/src/gc/` | Dropped-file cleanup worker |
| `schedule` | `src/mito2/src/schedule/` | Local/remote background job scheduling |
| `remap_manifest` | `src/mito2/src/remap_manifest.rs` | Manifest path remapping for region copy/migration |
| `config` | `src/mito2/src/config.rs` | `MitoConfig` tuning knobs |
| `test_util` | `src/mito2/src/test_util.rs` | `TestEnv` and builders (under the `test` feature) |

## Write path

`MitoEngine::handle_request` (`engine.rs`) → worker loop
(`worker/handle_write.rs`) → sequence + WAL assembly (`region_write_ctx.rs`) →
`wal.rs` → memtable (`memtable/`) → when buffer pressure trips, `flush.rs`
writes SSTs via `access_layer.rs` and appends a `RegionEdit` to the manifest
(`manifest/manager.rs`).

## Read path

`MitoEngine::handle_query` (`engine.rs`) → `read/scan_region.rs` takes an
immutable `Version` (`region/version.rs`) → scans memtables and Parquet SSTs
(`sst/parquet.rs`) → merges (`read/`) and dedups by sequence → projected,
filtered `RecordBatch` stream.

## Public surface

- Entry: `MitoEngine` in `src/mito2/src/engine.rs`, built via `MitoEngineBuilder`.
- Trait: `impl RegionEngine for MitoEngine` (`store-api`'s region engine contract).
- Consumed by `datanode` (sends `RegionRequest`s) and the query layer (scans).

## When you change X, also touch Y

- **Manifest format** (`manifest/action.rs`): affects crash recovery and
  follower replay. Keep it backward compatible.
- **SST/Parquet layout** (`sst/`): readers must stay compatible with existing files.
- **Series-index disk limit**: tasks estimate usage from entry sizes in open-region snapshots
  and defer new builds when the shared estimate is full. Cleanup still prunes obsolete
  range entries and expires series indexes to reclaim published usage. Concurrent builds may
  overshoot; closed-region files are not counted. Series handles and the SST purger own file deletion.
- **Series format PoC**: samples are rebuildable local SST sidecars. Optional writer interfaces (`format.rs`) integrate with reconciliation, catalog publication, and deferred purge. Defaults retain the original Parquet writer and disable samples. The opt-in columnar backend (`series_index/columnar.rs`) implements streaming OpenDAL writes and pinned format reads; its wire contract is in `docs/formats/mito-series.md`. Artifact objects record paths and sizes, including the manifest, so reads need no stat. Codecs currently run inline pending scheduling benchmarks. Columnar `SeriesScan` raw reads use scanner-wide preparation in `read/series_scan/prepared.rs`; ordinary record-batch scans retain their SST path. The paired `experimental_series_index_format` setting selects legacy or columnar artifacts.
- **Series-index coverage** (`series_index/catalog.rs`): `SeriesIndexEntry` stores
  compaction-window width and SST summaries keyed by aligned start in both catalogs and Parquet footers.
- **Columnar samples**: `series_index/samples.rs` implements opt-in per-SST sidecars,
  including nullable Float64 fields and preserved sequence/delete rows. Its wire
  contract is `docs/formats/mito-samples.md`; time/ID bounds may overlap across
  leaves and parts. Both columnar backends share cancellation-safe output ownership
  in `series_index/artifact_writer.rs`. Format readers do not apply query visibility; the prepared-source adapter applies visibility and the existing merge/dedup pipeline. Manual reconciliation covers all snapshot SSTs regardless of maintenance eligibility, but retains the soft disk limit. Manual builds validate columnar sample and tag schemas before reuse.
- **Request types** (`request.rs`): usually tied to proto definitions consumed by `datanode`.
- **WAL/memtable encoding** (`wal/`, `memtable/`): breaks replay if changed incompatibly.

## Testing

```bash
cargo nextest run -p mito2
```

Tests live next to the code as `*_test.rs` (e.g. `src/mito2/src/engine/flush_test.rs`).
`TestEnv` in `test_util.rs` spins up an engine over an in-process object store.

## Gotchas

- Sequence numbers are strictly increasing per region; dedup and snapshot reads
  depend on this. Do not change assignment lightly.
- Manifest version is monotonic — never reset or skip it.
- Lock ordering: take the manifest lock before updating `version_control`; the
  reverse deadlocks against concurrent flush/compaction.
- All region I/O runs on tokio workers; never `block_on` inside a worker.
- Loaded regions have resident compaction state, including `Idle` and handed-off
  DDLs. Publish create/open/catchup results through the worker's `register_region`;
  only close/drop unregisters them. See `compaction/scheduler/state.rs`.
- Local compaction units use the snapshot-free `CompactionContext`. Retaining a
  full `CompactionRegion` in a unit pins other units' inputs and delays SST purge.

## Maintenance contract

Update this file when you add/rename a top-level module, change the write/read
path entry points, or alter a persisted format (manifest, SST, WAL).
