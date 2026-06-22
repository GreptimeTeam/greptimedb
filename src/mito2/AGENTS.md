# mito2 — Agent & Contributor Guide

Navigation aid for `src/mito2`. Keep it short and point to code; do not duplicate
the code here. Paths are relative to the repo root.

Repo-wide rules that apply here: [`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

## What this crate does

Mito2 is GreptimeDB's primary time-series region storage engine. It owns the
write path (memtable + WAL), flushing memtables to Parquet SST files,
TWCS/level compaction, and the read path (multi-level merge + dedup with
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
| `compaction` | `src/mito2/src/compaction/` | TWCS (`twcs.rs`) and level pickers; `compactor.rs` |
| `access_layer` | `src/mito2/src/access_layer.rs` | SST read/write over the object store |
| `sst` | `src/mito2/src/sst/` | Parquet format, file metadata, index layout |
| `read` | `src/mito2/src/read/` | `ScanRegion`, merge, dedup, projection, streaming |
| `manifest` | `src/mito2/src/manifest/` | `RegionManifestManager`, manifest actions/edits |
| `cache` | `src/mito2/src/cache.rs` | Write/file/page caches |
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

## Maintenance contract

Update this file when you add/rename a top-level module, change the write/read
path entry points, or alter a persisted format (manifest, SST, WAL).
