# metric-engine — Agent & Contributor Guide

Navigation aid for `src/metric-engine`. Keep it short and point to code. Paths
are relative to the repo root.

Repo-wide rules that apply here: [`.agents/architecture-invariants.md`](../../.agents/architecture-invariants.md).

## What this crate does

The Metric Engine is optimized for Prometheus-style workloads with a huge number
of small tables. Many **logical** regions (one per metric table) share a single
**physical** pair of Mito2 regions: a data region and a metadata region. Rows
are multiplexed with metric-engine internal identity: dense primary-key mode
injects `__table_id` and `__tsid`, while the default sparse mode encodes them
into `__primary_key`. Reads still add a logical-table filter before forwarding
to the physical data region. It implements `RegionEngine` and delegates all real
storage to `mito2`.

The architecture is documented at the top of `src/metric-engine/src/lib.rs`.

## Module map

| Module | Path | Purpose |
| --- | --- | --- |
| `engine` | `src/metric-engine/src/engine.rs`, `src/metric-engine/src/engine/` | `MetricEngine` (`RegionEngine` impl) and per-op handlers (`create.rs`, `put.rs`, `read.rs`, `alter.rs`, `drop.rs`, ...) |
| `metadata_region` | `src/metric-engine/src/metadata_region.rs` | K-V over a Mito2 region storing logical table/column metadata, with an LRU cache |
| `data_region` | `src/metric-engine/src/data_region.rs` | Wraps the Mito2 data region; forwards writes and manages physical columns |
| `row_modifier` | `src/metric-engine/src/row_modifier.rs` | Rewrites incoming rows for dense or sparse primary-key encoding |
| `batch_modifier` | `src/metric-engine/src/batch_modifier.rs` | RecordBatch-level TSID computation and sparse primary-key encoding |
| `state` | `src/metric-engine/src/engine/state.rs` | In-memory cache of physical columns and logical column metadata |
| `repeated_task` | `src/metric-engine/src/repeated_task.rs` | Periodic metadata-region flush task |
| `utils` | `src/metric-engine/src/utils.rs` | `RegionId` conversions (data vs metadata group), manifest encoding |
| `config` | `src/metric-engine/src/config.rs` | `EngineConfig` (metadata flush interval, sparse PK) |
| `test_util` | `src/metric-engine/src/test_util.rs` | `TestEnv` building the Mito2 + Metric stack |

## Write path

`MetricEngine::handle_request(Put)` (`engine/put.rs`) rejects direct writes to a
physical region, resolves the physical region for the logical id, loads logical
columns from `metadata_region`, then `row_modifier` rewrites the rows according
to the data region's primary-key encoding and forwards the request to the Mito2
data region.

## Read path

`MetricEngine::handle_query` (`engine/read.rs`): a query against a logical region
is rewritten to add a `__table_id == <logical_id>` filter and forwarded to the
Mito2 data region. Queries against a physical region pass straight through.

## Public surface

- Entry: `MetricEngine` in `src/metric-engine/src/engine.rs`, built via `try_new(mito, config)`.
- Trait: `impl RegionEngine for MetricEngine` (name = `"metric"`).
- Depends on `mito2`, `store-api`, and `mito-codec` (sparse primary key codec).

## When you change X, also touch Y

- **Reserved column ids / names** (`__tsid`, `__table_id`, `__primary_key`): see
  `store-api`'s metric engine consts; keep them in sync with `engine.rs`.
- **Metadata K-V encoding** (`metadata_region.rs`): changes the on-disk metadata layout.
- **RegionId group mapping** (`utils.rs`): data vs metadata region derivation.
- **Physical column rules** (`engine/alter/`): a physical region allows only one field column.

## Testing

```bash
cargo nextest run -p metric-engine
```

`TestEnv` in `test_util.rs` gives you `mito()` and `metric()` handles.

## Gotchas

- Physical vs logical region confusion: physical regions reject direct user
  writes; operate on logical region ids.
- TSID must be stable for the same tag set — it is a hash over sorted tag names
  + values and may be stored in `__tsid` or encoded into `__primary_key`.
- Metadata is cached (LRU with a TTL); after an alter, stale reads are possible
  until invalidation/expiry.
- Always convert ids via `utils::to_data_region_id` / `to_metadata_region_id`.

## Maintenance contract

Update this file when you change the logical/physical region model, the injected
columns, the metadata encoding, or the public engine entry points.
