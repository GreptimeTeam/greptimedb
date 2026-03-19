# Physical Table Batching Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Modify `flush_batch` in `PendingRowsBatcher` to merge all logical tables' data per physical table, perform sparse encoding (`__primary_key`), and send one `BulkInsertRequest` per physical region — bypassing the metric engine.

**Architecture:** When `prom_store_with_metric_engine` is true, `flush_batch` resolves the physical table once, computes `__primary_key` (encoding `__table_id` + `__tsid` + tags) for each logical table's batch using `modify_batch_sparse`, merges them, splits by the physical table's partition rule, and sends to physical data regions. Falls back to the existing per-logical-table path otherwise.

**Tech Stack:** Rust, Arrow RecordBatch, `fxhash`, `mito-codec::SparsePrimaryKeyCodec`, `store-api::metric_engine_consts`

---

## Task 1: Extract batch_modifier to a shared crate

The functions `modify_batch_sparse`, `compute_tsid_array`, `TagColumnInfo`, and `build_tag_arrays` in `src/metric-engine/src/batch_modifier.rs` are `pub(crate)`. The `servers` crate cannot depend on `metric-engine` (it would create a circular or too-heavy dependency). We need to make these functions accessible.

**Files:**
- Modify: `src/metric-engine/src/batch_modifier.rs` (change visibility from `pub(crate)` to `pub`)
- Modify: `src/metric-engine/src/lib.rs` (re-export `batch_modifier` module as `pub`)
- Modify: `src/servers/Cargo.toml` (add `metric-engine` dependency)

**Step 1: Check if metric-engine can be a dependency of servers**

Run: `cargo metadata --format-version 1 | python3 -c "import sys,json; d=json.load(sys.stdin); pkgs={p['name']:p for p in d['packages']}; me=pkgs.get('metric-engine',{}); deps=[d['name'] for d in me.get('dependencies',[])]; print('servers' in deps, deps[:20])" 2>/dev/null || echo "check manually"`

Check that `metric-engine` does NOT depend on `servers` (which would create a cycle). If it does, we need to extract to a new crate like `metric-codec`.

**Step 2: Make batch_modifier public**

In `src/metric-engine/src/batch_modifier.rs`, change:
```rust
// Before:
pub(crate) struct TagColumnInfo {
pub(crate) fn compute_tsid_array(
pub(crate) fn modify_batch_sparse(

// After:
pub struct TagColumnInfo {
pub fn compute_tsid_array(
pub fn modify_batch_sparse(
```

Also make `build_tag_arrays` pub:
```rust
// Before (line 97):
fn build_tag_arrays<'a>(
// After:
pub fn build_tag_arrays<'a>(
```

Remove `#[allow(dead_code)]` from `TagColumnInfo` (line 30) and `compute_tsid_array` (line 41).

**Step 3: Make batch_modifier module public**

In `src/metric-engine/src/lib.rs`, find the `mod batch_modifier;` line and change it:
```rust
// Before:
mod batch_modifier;
// After:
pub mod batch_modifier;
```

**Step 4: Add metric-engine dependency to servers**

In `src/servers/Cargo.toml`, add after line 46 (`common-meta.workspace = true`):
```toml
metric-engine.workspace = true
```

If this creates a circular dependency, the fallback is to create a new `metric-codec` crate (documented in the design doc). Verify with:

Run: `cargo check -p servers 2>&1 | head -30`
Expected: No circular dependency error

**Step 5: Verify compilation**

Run: `cargo check -p metric-engine -p servers 2>&1 | tail -20`
Expected: Both crates compile successfully

**Step 6: Commit**

```bash
git add src/metric-engine/src/batch_modifier.rs src/metric-engine/src/lib.rs src/servers/Cargo.toml
git commit -m "feat: make batch_modifier public for use by PendingRowsBatcher"
```

---

## Task 2: Add prom_store_with_metric_engine to flush_batch

Currently `flush_batch` doesn't know whether the metric engine is in use. We need to thread this flag through.

**Files:**
- Modify: `src/servers/src/pending_rows_batcher.rs`

**Step 1: Add field to FlushBatch**

At `pending_rows_batcher.rs:115`, add a new field:
```rust
struct FlushBatch {
    table_batches: Vec<TableBatch>,
    total_row_count: usize,
    ctx: QueryContextRef,
    waiters: Vec<FlushWaiter>,
    prom_store_with_metric_engine: bool,  // NEW
}
```

**Step 2: Thread the flag through start_worker**

At `pending_rows_batcher.rs:626`, add parameter:
```rust
fn start_worker(
    mut rx: mpsc::Receiver<WorkerCommand>,
    shutdown: broadcast::Sender<()>,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
    flush_interval: Duration,
    max_batch_rows: usize,
    flush_semaphore: Arc<Semaphore>,
    prom_store_with_metric_engine: bool,  // NEW
) {
```

**Step 3: Set the flag in drain_batch**

Modify `drain_batch` to accept the flag and include it in `FlushBatch`. Since `drain_batch` is a function that takes `&mut PendingBatch`, add the flag as a parameter:
```rust
fn drain_batch(batch: &mut PendingBatch, prom_store_with_metric_engine: bool) -> Option<FlushBatch> {
    // ... existing code ...
    Some(FlushBatch {
        table_batches,
        total_row_count,
        ctx,
        waiters,
        prom_store_with_metric_engine,
    })
}
```

Update all call sites of `drain_batch` within `start_worker` to pass the flag.

**Step 4: Update flush_batch signature**

At `pending_rows_batcher.rs:841`:
```rust
async fn flush_batch(
    flush: FlushBatch,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
    catalog_manager: CatalogManagerRef,
) {
    let FlushBatch {
        table_batches,
        total_row_count,
        ctx,
        waiters,
        prom_store_with_metric_engine,  // NEW destructure
    } = flush;
```

**Step 5: Pass flag from PendingRowsBatcher to start_worker**

At `pending_rows_batcher.rs:411`, add the flag:
```rust
start_worker(
    rx,
    self.shutdown.clone(),
    self.partition_manager.clone(),
    self.node_manager.clone(),
    self.catalog_manager.clone(),
    self.flush_interval,
    self.max_batch_rows,
    self.flush_semaphore.clone(),
    self.prom_store_with_metric_engine,  // NEW
);
```

**Step 6: Verify compilation**

Run: `cargo check -p servers 2>&1 | tail -20`
Expected: Compiles (no behavior change yet)

**Step 7: Commit**

```bash
git add src/servers/src/pending_rows_batcher.rs
git commit -m "refactor: thread prom_store_with_metric_engine through flush_batch"
```

---

## Task 3: Implement physical table batching in flush_batch

This is the core change. When `prom_store_with_metric_engine` is true, `flush_batch` will:
1. Resolve the physical table once
2. Get its partition rule once
3. For each logical table batch, resolve the table and call `modify_batch_sparse`
4. Merge all modified batches
5. Split by physical partition rule
6. Send one BulkInsertRequest per physical region

**Files:**
- Modify: `src/servers/src/pending_rows_batcher.rs`

**Step 1: Add necessary imports**

At the top of `pending_rows_batcher.rs`, add:
```rust
use metric_engine::batch_modifier::{TagColumnInfo, modify_batch_sparse};
use store_api::metric_engine_consts::METRIC_DATA_REGION_GROUP;
```

**Step 2: Implement flush_batch_physical helper function**

Add a new function `flush_batch_physical` that handles the physical-table-batched path. This should be placed near `flush_batch` (around line 1080):

```rust
/// Flushes a batch by merging all logical tables into one physical table batch.
///
/// For each logical table:
/// 1. Resolves the logical table to get table_id and tag column info
/// 2. Calls modify_batch_sparse to produce __primary_key + greptime_timestamp + greptime_value
///
/// Then merges all modified batches, splits by the physical table's partition rule,
/// and sends one BulkInsertRequest per physical data region.
async fn flush_batch_physical(
    table_batches: Vec<TableBatch>,
    catalog: &str,
    schema: &str,
    physical_table_name: &str,
    partition_manager: &PartitionRuleManagerRef,
    node_manager: &NodeManagerRef,
    catalog_manager: &CatalogManagerRef,
    ctx: &QueryContextRef,
) -> std::result::Result<Vec<FlushWriteResult>, String> {
    // 1. Resolve physical table
    let physical_table = catalog_manager
        .table(catalog, schema, physical_table_name, Some(ctx.as_ref()))
        .await
        .map_err(|e| format!("Failed to resolve physical table {}: {:?}", physical_table_name, e))?
        .ok_or_else(|| format!("Physical table not found: {}", physical_table_name))?;
    let physical_table_info = physical_table.table_info();
    let physical_name_to_ids = physical_table_info
        .name_to_ids()
        .ok_or_else(|| format!("Physical table {} missing column ID mapping", physical_table_name))?;

    // 2. Get physical partition rule once
    let partition_rule = partition_manager
        .find_table_partition_rule(&physical_table_info)
        .await
        .map_err(|e| format!("Failed to get partition rule for physical table {}: {:?}", physical_table_name, e))?;

    // 3. Transform each logical table's batch
    let mut modified_batches: Vec<RecordBatch> = Vec::new();
    let mut physical_schema: Option<Arc<ArrowSchema>> = None;

    for table_batch in &table_batches {
        let Some(first) = table_batch.batches.first() else {
            continue;
        };

        // Concat this logical table's batches
        let concatenated = concat_batches(&first.schema(), &table_batch.batches)
            .map_err(|e| format!("Failed to concat batches for {}: {:?}", table_batch.table_name, e))?;

        // Resolve logical table to get table_id
        let logical_table = catalog_manager
            .table(catalog, schema, &table_batch.table_name, Some(ctx.as_ref()))
            .await
            .map_err(|e| format!("Failed to resolve table {}: {:?}", table_batch.table_name, e))?
            .ok_or_else(|| format!("Table not found: {}", table_batch.table_name))?;
        let logical_table_info = logical_table.table_info();
        let table_id = logical_table_info.table_id();

        // Build TagColumnInfo: tag columns are everything except greptime_timestamp and greptime_value
        let batch_schema = concatenated.schema();
        let mut tag_columns = Vec::new();
        let mut non_tag_indices = Vec::new();

        for (index, field) in batch_schema.fields().iter().enumerate() {
            let name = field.name();
            if name == greptime_timestamp() || name == greptime_value() {
                non_tag_indices.push(index);
            } else {
                // Look up column_id from physical table
                let column_id = *physical_name_to_ids.get(name.as_str())
                    .ok_or_else(|| format!(
                        "Column '{}' from logical table '{}' not found in physical table '{}'",
                        name, table_batch.table_name, physical_table_name
                    ))?;
                tag_columns.push(TagColumnInfo {
                    name: name.clone(),
                    index,
                    column_id,
                });
            }
        }
        tag_columns.sort_by(|a, b| a.name.cmp(&b.name));

        // Call modify_batch_sparse
        let modified = modify_batch_sparse(concatenated, table_id, &tag_columns, &non_tag_indices)
            .map_err(|e| format!("Failed to encode batch for {}: {:?}", table_batch.table_name, e))?;

        if physical_schema.is_none() {
            physical_schema = Some(modified.schema().clone());
        }
        modified_batches.push(modified);
    }

    if modified_batches.is_empty() {
        return Ok(vec![]);
    }

    let physical_schema = physical_schema.unwrap();

    // 4. Merge all modified batches
    let merged = concat_batches(&physical_schema, &modified_batches)
        .map_err(|e| format!("Failed to merge physical batches: {:?}", e))?;

    // 5. Split by physical partition rule
    let region_masks = partition_rule.0.split_record_batch(&merged)
        .map_err(|e| format!("Failed to split by physical partition rule: {:?}", e))?;

    // 6. Build and send BulkInsertRequests per physical region
    let physical_table_id = physical_table_info.table_id();
    let mut region_writes = Vec::new();

    for (region_number, mask) in region_masks {
        if mask.select_none() {
            continue;
        }

        let region_batch = if mask.select_all() {
            merged.clone()
        } else {
            filter_record_batch(&merged, mask.array())
                .map_err(|e| format!("Failed to filter for region {}: {:?}", region_number, e))?
        };

        let row_count = region_batch.num_rows();
        if row_count == 0 {
            continue;
        }

        // Compute data region ID: physical table ID + region number + data region group
        let physical_region_id = RegionId::new(physical_table_id, region_number);
        let data_region_id = RegionId::with_group_and_seq(
            physical_table_id,
            METRIC_DATA_REGION_GROUP,
            physical_region_id.region_sequence(),
        );

        let datanode = partition_manager
            .find_region_leader(physical_region_id)
            .await
            .map_err(|e| format!("Failed to find leader for region {}: {:?}", physical_region_id, e))?;

        let (schema_bytes, data_header, payload) = record_batch_to_ipc(region_batch)
            .map_err(|e| format!("Failed to encode IPC for region {}: {:?}", data_region_id, e))?;

        let request = RegionRequest {
            header: Some(RegionRequestHeader {
                tracing_context: TracingContext::from_current_span().to_w3c(),
                ..Default::default()
            }),
            body: Some(region_request::Body::BulkInsert(BulkInsertRequest {
                region_id: data_region_id.as_u64(),
                partition_expr_version: None,
                body: Some(bulk_insert_request::Body::ArrowIpc(ArrowIpc {
                    schema: schema_bytes,
                    data_header,
                    payload,
                })),
            })),
        };

        region_writes.push(FlushRegionWrite {
            region_id: data_region_id,
            row_count,
            datanode,
            request,
        });
    }

    Ok(flush_region_writes_concurrently(node_manager.clone(), region_writes).await)
}
```

**Step 3: Modify flush_batch to dispatch to physical path**

In `flush_batch` (line 868, before the `for table_batch in table_batches` loop), add a conditional branch:

```rust
    if prom_store_with_metric_engine {
        let physical_table_name = ctx
            .extension(PHYSICAL_TABLE_KEY)
            .unwrap_or(GREPTIME_PHYSICAL_TABLE)
            .to_string();

        match flush_batch_physical(
            table_batches,
            &catalog,
            &schema,
            &physical_table_name,
            &partition_manager,
            &node_manager,
            &catalog_manager,
            &ctx,
        )
        .await
        {
            Ok(results) => {
                for result in results {
                    match result {
                        FlushWriteResult::Success { row_count } => {
                            FLUSH_TOTAL.inc();
                            FLUSH_ROWS.observe(row_count as f64);
                        }
                        FlushWriteResult::Failed { row_count, message } => {
                            record_failure!(row_count, message);
                        }
                    }
                }
            }
            Err(msg) => {
                record_failure!(total_row_count, msg);
            }
        }
    } else {
        // existing for loop over table_batches
        for table_batch in table_batches {
            // ... existing code unchanged ...
        }
    }
```

Note: Need to add `use common_query::prelude::PHYSICAL_TABLE_KEY;` or use the existing constant access pattern.

**Step 4: Add PHYSICAL_TABLE_KEY import**

Check if `PHYSICAL_TABLE_KEY` is already available. The existing code at line 141 uses `PHYSICAL_TABLE_KEY` via `ctx.extension(PHYSICAL_TABLE_KEY)`. Look at `batch_key_from_ctx`:
```rust
use common_query::prelude::GREPTIME_PHYSICAL_TABLE;
```
But `PHYSICAL_TABLE_KEY` might be a different constant. Check the existing usage — `batch_key_from_ctx` at line 141 already references it, so the import must exist. Verify.

**Step 5: Verify compilation**

Run: `cargo check -p servers 2>&1 | tail -30`
Expected: Compiles successfully

**Step 6: Commit**

```bash
git add src/servers/src/pending_rows_batcher.rs
git commit -m "feat: implement physical table batching in flush_batch"
```

---

## Task 4: Add unit tests

**Files:**
- Modify: `src/servers/src/pending_rows_batcher.rs` (test module at bottom)

**Step 1: Add test for modify_batch_sparse integration**

Test that `modify_batch_sparse` produces the expected schema when called from the batcher context:

```rust
#[test]
fn test_modify_batch_sparse_produces_physical_schema() {
    use metric_engine::batch_modifier::{TagColumnInfo, modify_batch_sparse};
    use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("greptime_timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("greptime_value", DataType::Float64, true),
        Field::new("host", DataType::Utf8, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1000i64])),
            Arc::new(Float64Array::from(vec![42.0])),
            Arc::new(StringArray::from(vec!["h1"])),
            Arc::new(StringArray::from(vec!["us-east"])),
        ],
    )
    .unwrap();

    let tag_columns = vec![
        TagColumnInfo { name: "host".to_string(), index: 2, column_id: 10 },
        TagColumnInfo { name: "region".to_string(), index: 3, column_id: 11 },
    ];
    let non_tag_indices = vec![0, 1];
    let table_id = 1025u32;

    let modified = modify_batch_sparse(batch, table_id, &tag_columns, &non_tag_indices).unwrap();
    assert_eq!(modified.num_columns(), 3);
    assert_eq!(modified.schema().field(0).name(), PRIMARY_KEY_COLUMN_NAME);
    assert_eq!(modified.schema().field(1).name(), "greptime_timestamp");
    assert_eq!(modified.schema().field(2).name(), "greptime_value");
    assert_eq!(modified.num_rows(), 1);
}
```

**Step 2: Add test for merging multiple logical tables**

```rust
#[test]
fn test_merge_multiple_logical_tables_into_physical_schema() {
    use metric_engine::batch_modifier::{TagColumnInfo, modify_batch_sparse};

    // Logical table A with tag "host"
    let schema_a = Arc::new(ArrowSchema::new(vec![
        Field::new("greptime_timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("greptime_value", DataType::Float64, true),
        Field::new("host", DataType::Utf8, true),
    ]));
    let batch_a = RecordBatch::try_new(
        schema_a,
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![1000i64])),
            Arc::new(Float64Array::from(vec![1.0])),
            Arc::new(StringArray::from(vec!["h1"])),
        ],
    )
    .unwrap();

    // Logical table B with tag "region"
    let schema_b = Arc::new(ArrowSchema::new(vec![
        Field::new("greptime_timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
        Field::new("greptime_value", DataType::Float64, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let batch_b = RecordBatch::try_new(
        schema_b,
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![2000i64])),
            Arc::new(Float64Array::from(vec![2.0])),
            Arc::new(StringArray::from(vec!["us-east"])),
        ],
    )
    .unwrap();

    let modified_a = modify_batch_sparse(
        batch_a, 100,
        &[TagColumnInfo { name: "host".to_string(), index: 2, column_id: 10 }],
        &[0, 1],
    )
    .unwrap();

    let modified_b = modify_batch_sparse(
        batch_b, 200,
        &[TagColumnInfo { name: "region".to_string(), index: 2, column_id: 11 }],
        &[0, 1],
    )
    .unwrap();

    // Both have the same 3-column schema: __primary_key, greptime_timestamp, greptime_value
    assert_eq!(modified_a.schema(), modified_b.schema());

    let merged = concat_batches(&modified_a.schema(), &[modified_a, modified_b]).unwrap();
    assert_eq!(merged.num_rows(), 2);
    assert_eq!(merged.num_columns(), 3);
}
```

**Step 3: Run tests**

Run: `cargo test -p servers test_modify_batch_sparse_produces_physical_schema test_merge_multiple_logical_tables -- --nocapture 2>&1 | tail -20`
Expected: PASS

**Step 4: Commit**

```bash
git add src/servers/src/pending_rows_batcher.rs
git commit -m "test: add unit tests for physical table batching"
```

---

## Task 5: Integration verification

**Files:** None (verification only)

**Step 1: Run the full servers test suite**

Run: `cargo test -p servers 2>&1 | tail -30`
Expected: All tests pass

**Step 2: Run the metric-engine test suite**

Run: `cargo test -p metric-engine 2>&1 | tail -30`
Expected: All tests pass (batch_modifier visibility change shouldn't break anything)

**Step 3: Run clippy**

Run: `cargo clippy -p servers -p metric-engine 2>&1 | tail -30`
Expected: No new warnings

**Step 4: Final commit (if any fixes needed)**

```bash
git add -A
git commit -m "fix: address clippy and test issues"
```

---

## Summary of Changes

| File | Change |
|------|--------|
| `src/metric-engine/src/batch_modifier.rs` | Make `TagColumnInfo`, `compute_tsid_array`, `modify_batch_sparse`, `build_tag_arrays` public |
| `src/metric-engine/src/lib.rs` | Make `batch_modifier` module public |
| `src/servers/Cargo.toml` | Add `metric-engine` dependency |
| `src/servers/src/pending_rows_batcher.rs` | Add `prom_store_with_metric_engine` to `FlushBatch`/`start_worker`/`drain_batch`; add `flush_batch_physical` function; branch in `flush_batch`; add tests |

## Risk Mitigation

- **Fallback**: The `else` branch preserves the existing per-logical-table path exactly
- **Compilation**: Each task has a `cargo check` step before committing
- **Testing**: Unit tests verify the transformation and merge steps independently
- **Rollback**: The `prom_store_with_metric_engine` flag provides a runtime toggle
