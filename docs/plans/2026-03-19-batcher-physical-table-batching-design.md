# Physical Table Batching in PendingRowsBatcher

**Date:** 2026-03-19
**Status:** Draft

## Problem

Currently `flush_batch` in `pending_rows_batcher.rs` iterates over each **logical table**
independently (line 869). For each logical table it:

1. Resolves the table by name via `catalog_manager`
2. Fetches partition rules via `partition_manager.find_table_partition_rule()`
3. Splits the record batch by partition rule
4. Sends a `BulkInsertRequest` with the **logical table's region ID** per region
5. The metric engine on the datanode then transforms each batch (adding `__table_id`/`__tsid`)

For a physical table with N logical tables across R regions, this produces:
- N table lookups
- N partition rule lookups
- Up to N×R RPCs to datanodes

## Goal

Reduce both **RPC count** and **partition lookup overhead** by batching all logical tables
under the same physical table into a single `BulkInsertRequest` per physical region.

## Design

### Approach

Move the `__table_id`/`__tsid`/`__primary_key` sparse encoding transformation into the
batcher itself, merge all logical tables' data per physical region, and send a single
`BulkInsertRequest` per physical data region — bypassing the metric engine entirely.

### Architecture

```
Before (per logical table):
  logical_table_A → resolve → partition_rule → split → BulkInsert(logical_region) → metric engine → data region
  logical_table_B → resolve → partition_rule → split → BulkInsert(logical_region) → metric engine → data region
  ...N times...

After (per physical table):
  [logical_table_A, B, ...] → resolve physical table once
    → physical partition_rule once
    → for each logical table batch:
        → resolve logical table to get table_id + tag column info
        → modify_batch_sparse() to produce __primary_key, greptime_timestamp, greptime_value
    → merge all modified batches (shared physical schema)
    → split merged batch by physical partition rule
    → for each physical region:
        → BulkInsert(physical_data_region_id) → mito directly
```

### Scope

- **Only** the `PendingRowsBatcher`'s `flush_batch` path is modified.
- The existing `BulkInsert` operator in `operator/src/bulk_insert.rs` is unchanged.
- **Only sparse encoding** is supported (matching the metric engine's existing bulk insert path).
- Non-metric-engine tables continue to use the existing per-logical-table path.

### Key Steps in flush_batch (metric engine path)

1. **Resolve the physical table once** — lookup the physical table name (from `BatchKey`)
   via `catalog_manager`.
2. **Fetch physical partition rule once** — one call to
   `partition_manager.find_table_partition_rule(&physical_table_info)`.
3. **For each logical table's `TableBatch`:**
   - Resolve the logical table to get its `table_id` and schema.
   - Build `TagColumnInfo` from the logical table's tag columns (name, batch index, physical column_id).
   - Call `modify_batch_sparse(batch, table_id, &sorted_tag_columns, &non_tag_indices)`
     from `metric-engine/src/batch_modifier.rs`.
   - The result has schema: `[__primary_key (Binary), greptime_timestamp, greptime_value]`.
4. **Merge all modified batches** — since all now share the same 3-column physical schema,
   use `concat_batches()` to produce one large record batch.
5. **Split by physical partition rule** — one `split_record_batch()` call.
6. **Send one `BulkInsertRequest` per physical region** using
   `RegionId::new(physical_table_id, region_number)` with data region group
   (via `to_data_region_id()`).

### Dependency on batch_modifier.rs

The `batch_modifier.rs` module in the metric engine crate provides:
- `compute_tsid_array()` — computes `__tsid` hash from sorted tag columns
- `modify_batch_sparse()` — produces the sparse-encoded physical schema

These functions are currently `pub(crate)`. They will need to be made `pub` (or the batcher
will need to depend on the metric-engine crate, or the functions will be extracted to a
shared crate).

### TagColumnInfo Resolution

For each logical table, we need:
- Tag column **names** (from the logical table schema, sorted alphabetically)
- Tag column **indices** in the RecordBatch (from column position matching)
- Tag column **column_ids** in the physical region (requires mapping logical column names
  to physical region column IDs)

The physical region's column ID mapping may need to be cached or fetched once per
physical table.

### Data Region ID

The physical data region ID is computed using `to_data_region_id()` from
`metric-engine/src/utils.rs`, which sets the region group to `METRIC_DATA_REGION_GROUP`.

### Fallback Path

When `prom_store_with_metric_engine` is `false` (or the physical table is the default
`greptime_physical_table` placeholder), the batcher falls back to the existing
per-logical-table processing.

## Trade-offs

| Aspect | Pro | Con |
|--------|-----|-----|
| RPC count | N×R → R | — |
| Partition lookups | N → 1 | — |
| Complexity | — | Batcher gains metric-engine-specific knowledge |
| Coupling | — | Encoding changes in metric engine require batcher updates |
| Error granularity | — | Failure affects all logical tables in the batch |
| Crate dependency | — | Either metric-engine becomes a dependency or code is extracted |

## Open Questions

1. **Column ID mapping**: How to efficiently map logical table tag column names to physical
   region column IDs? Options: cache in metadata, derive from physical table schema, or
   fetch from region metadata.
2. **Crate boundary**: Should `batch_modifier.rs` functions be extracted to a shared crate
   (e.g., `metric-codec`), or should the servers crate depend on `metric-engine`?
3. **Error handling**: If one logical table's batch fails transformation, should we skip
   just that table or fail the entire physical batch?
