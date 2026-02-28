// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Integration tests for `ADMIN GC_TABLE` and `ADMIN GC_REGIONS` functions.

use std::time::Duration;

use client::OutputData;
use common_meta::key::table_repart::TableRepartValue;
use common_recordbatch::RecordBatches;
use common_telemetry::info;
use common_test_util::recordbatch::check_output_stream;
use datatypes::arrow::array::{Array, AsArray};
use datatypes::arrow::datatypes::UInt64Type;
use store_api::storage::RegionId;

use super::{distributed_with_gc, list_sst_files};
use crate::test_util::{StorageType, execute_sql, try_execute_sql};

#[tokio::test]
async fn test_admin_gc_table_different_store() {
    let e = dotenv::dotenv();
    common_telemetry::init_default_ut_logging();
    let store_type = StorageType::build_storage_types_based_on_env();
    info!("store type with .env path: {e:?}: {:?}", store_type);
    for store in store_type {
        info!("Running admin GC table test with storage type: {}", store);
        test_admin_gc_table(&store).await;
    }
}

/// Test `ADMIN GC_TABLE('table_name')` function
async fn test_admin_gc_table(store_type: &StorageType) {
    let (test_context, _guard) = distributed_with_gc(store_type).await;
    let instance = test_context.frontend();

    // Step 1: Create table with append_mode to easily generate multiple files
    let create_table_sql = r#"
        CREATE TABLE test_admin_gc_table (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            host STRING
        ) WITH (append_mode = 'true')
    "#;
    execute_sql(&instance, create_table_sql).await;

    // Step 2: Generate SST files by inserting data and flushing multiple times
    for i in 0..4 {
        let insert_sql = format!(
            r#"
            INSERT INTO test_admin_gc_table (ts, val, host) VALUES
            ('2023-01-0{} 10:00:00', {}, 'host{}'),
            ('2023-01-0{} 11:00:00', {}, 'host{}'),
            ('2023-01-0{} 12:00:00', {}, 'host{}')
            "#,
            i + 1,
            10.0 + i as f64,
            i,
            i + 1,
            20.0 + i as f64,
            i,
            i + 1,
            30.0 + i as f64,
            i
        );
        execute_sql(&instance, &insert_sql).await;

        // Flush the table to create SST files
        let flush_sql = "ADMIN FLUSH_TABLE('test_admin_gc_table')";
        execute_sql(&instance, flush_sql).await;
    }

    // List SST files before compaction (for verification)
    let sst_files_before_compaction = list_sst_files(&test_context).await;
    info!(
        "SST files before compaction: {:?}",
        sst_files_before_compaction
    );
    assert_eq!(sst_files_before_compaction.len(), 4); // 4 files from 4 flushes

    // Step 3: Trigger compaction to create garbage SST files
    let compact_sql = "ADMIN COMPACT_TABLE('test_admin_gc_table')";
    execute_sql(&instance, compact_sql).await;

    // Wait for compaction to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // List SST files after compaction (should have both old and new files)
    let sst_files_after_compaction = list_sst_files(&test_context).await;
    info!(
        "SST files after compaction: {:?}",
        sst_files_after_compaction
    );
    assert_eq!(sst_files_after_compaction.len(), 5); // 4 old + 1 new

    // Step 4: Use ADMIN GC_TABLE to clean up garbage files
    let gc_table_sql = "ADMIN GC_TABLE('test_admin_gc_table')";
    let gc_output = execute_sql(&instance, gc_table_sql).await;
    info!("GC_TABLE output: {:?}", gc_output);

    // Step 5: Verify GC results
    let sst_files_after_gc = list_sst_files(&test_context).await;
    info!("SST files after GC: {:?}", sst_files_after_gc);
    assert_eq!(sst_files_after_gc.len(), 1); // Only the compacted file should remain after gc

    // Verify that data is still accessible
    let count_sql = "SELECT COUNT(*) FROM test_admin_gc_table";
    let count_output = execute_sql(&instance, count_sql).await;
    let expected = r#"
+----------+
| count(*) |
+----------+
| 12       |
+----------+"#
        .trim();
    check_output_stream(count_output.data, expected).await;

    let select_sql = "SELECT * FROM test_admin_gc_table ORDER BY ts";
    let select_output = execute_sql(&instance, select_sql).await;
    let expected = r#"
+---------------------+------+-------+
| ts                  | val  | host  |
+---------------------+------+-------+
| 2023-01-01T10:00:00 | 10.0 | host0 |
| 2023-01-01T11:00:00 | 20.0 | host0 |
| 2023-01-01T12:00:00 | 30.0 | host0 |
| 2023-01-02T10:00:00 | 11.0 | host1 |
| 2023-01-02T11:00:00 | 21.0 | host1 |
| 2023-01-02T12:00:00 | 31.0 | host1 |
| 2023-01-03T10:00:00 | 12.0 | host2 |
| 2023-01-03T11:00:00 | 22.0 | host2 |
| 2023-01-03T12:00:00 | 32.0 | host2 |
| 2023-01-04T10:00:00 | 13.0 | host3 |
| 2023-01-04T11:00:00 | 23.0 | host3 |
| 2023-01-04T12:00:00 | 33.0 | host3 |
+---------------------+------+-------+"#
        .trim();
    check_output_stream(select_output.data, expected).await;

    info!("ADMIN GC_TABLE test completed successfully");
}

#[tokio::test]
async fn test_admin_gc_regions_different_store() {
    let _ = dotenv::dotenv();
    common_telemetry::init_default_ut_logging();
    let store_type = StorageType::build_storage_types_based_on_env();
    info!("store type: {:?}", store_type);
    for store in store_type {
        info!("Running admin GC regions test with storage type: {}", store);
        test_admin_gc_regions(&store).await;
    }
}

/// Test `ADMIN GC_REGIONS(region_id)` function
async fn test_admin_gc_regions(store_type: &StorageType) {
    let (test_context, _guard) = distributed_with_gc(store_type).await;
    let instance = test_context.frontend();

    // Step 1: Create table with append_mode to easily generate multiple files
    let create_table_sql = r#"
        CREATE TABLE test_admin_gc_regions (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            host STRING
        ) WITH (append_mode = 'true')
    "#;
    execute_sql(&instance, create_table_sql).await;

    // Step 2: Generate SST files by inserting data and flushing multiple times
    for i in 0..4 {
        let insert_sql = format!(
            r#"
            INSERT INTO test_admin_gc_regions (ts, val, host) VALUES
            ('2023-01-0{} 10:00:00', {}, 'host{}'),
            ('2023-01-0{} 11:00:00', {}, 'host{}'),
            ('2023-01-0{} 12:00:00', {}, 'host{}')
            "#,
            i + 1,
            10.0 + i as f64,
            i,
            i + 1,
            20.0 + i as f64,
            i,
            i + 1,
            30.0 + i as f64,
            i
        );
        execute_sql(&instance, &insert_sql).await;

        // Flush the table to create SST files
        let flush_sql = "ADMIN FLUSH_TABLE('test_admin_gc_regions')";
        execute_sql(&instance, flush_sql).await;
    }

    // List SST files before compaction (for verification)
    let sst_files_before_compaction = list_sst_files(&test_context).await;
    info!(
        "SST files before compaction: {:?}",
        sst_files_before_compaction
    );
    assert_eq!(sst_files_before_compaction.len(), 4); // 4 files from 4 flushes

    // Step 3: Trigger compaction to create garbage SST files
    let compact_sql = "ADMIN COMPACT_TABLE('test_admin_gc_regions')";
    execute_sql(&instance, compact_sql).await;

    // Wait for compaction to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // List SST files after compaction (should have both old and new files)
    let sst_files_after_compaction = list_sst_files(&test_context).await;
    info!(
        "SST files after compaction: {:?}",
        sst_files_after_compaction
    );
    assert_eq!(sst_files_after_compaction.len(), 5); // 4 old + 1 new

    // Step 4: Get region id from information_schema
    let region_id_sql = "SELECT greptime_partition_id FROM information_schema.partitions WHERE table_name = 'test_admin_gc_regions' ORDER BY greptime_partition_id LIMIT 1";
    let region_output = execute_sql(&instance, region_id_sql).await;
    info!("Region ID output: {:?}", region_output);
    let OutputData::Stream(region_stream) = region_output.data else {
        panic!("Expected stream output for region id query");
    };

    // Extract the region id from the result
    let region_id = {
        let batches = RecordBatches::try_collect(region_stream).await.unwrap();
        let batch = &batches.iter().next().unwrap();
        let column = batch.column(0);
        let array = column.as_primitive::<UInt64Type>();
        array.value(0)
    };
    info!("Extracted region_id: {}", region_id);

    // Step 5: Use ADMIN GC_REGIONS to clean up garbage files for the specific region
    let gc_regions_sql = format!("ADMIN GC_REGIONS({})", region_id);
    let gc_output = execute_sql(&instance, &gc_regions_sql).await;
    info!("GC_REGIONS output: {:?}", gc_output);

    // Step 6: Verify GC results
    let sst_files_after_gc = list_sst_files(&test_context).await;
    info!("SST files after GC: {:?}", sst_files_after_gc);
    assert_eq!(sst_files_after_gc.len(), 1); // Only the compacted file should remain after gc

    // Verify that data is still accessible
    let count_sql = "SELECT COUNT(*) FROM test_admin_gc_regions";
    let count_output = execute_sql(&instance, count_sql).await;
    let expected = r#"
+----------+
| count(*) |
+----------+
| 12       |
+----------+"#
        .trim();
    check_output_stream(count_output.data, expected).await;

    let select_sql = "SELECT * FROM test_admin_gc_regions ORDER BY ts";
    let select_output = execute_sql(&instance, select_sql).await;
    let expected = r#"
+---------------------+------+-------+
| ts                  | val  | host  |
+---------------------+------+-------+
| 2023-01-01T10:00:00 | 10.0 | host0 |
| 2023-01-01T11:00:00 | 20.0 | host0 |
| 2023-01-01T12:00:00 | 30.0 | host0 |
| 2023-01-02T10:00:00 | 11.0 | host1 |
| 2023-01-02T11:00:00 | 21.0 | host1 |
| 2023-01-02T12:00:00 | 31.0 | host1 |
| 2023-01-03T10:00:00 | 12.0 | host2 |
| 2023-01-03T11:00:00 | 22.0 | host2 |
| 2023-01-03T12:00:00 | 32.0 | host2 |
| 2023-01-04T10:00:00 | 13.0 | host3 |
| 2023-01-04T11:00:00 | 23.0 | host3 |
| 2023-01-04T12:00:00 | 33.0 | host3 |
+---------------------+------+-------+"#
        .trim();
    check_output_stream(select_output.data, expected).await;

    info!("ADMIN GC_REGIONS test completed successfully");
}

#[tokio::test]
async fn test_admin_gc_missing_cases_different_store() {
    let _ = dotenv::dotenv();
    common_telemetry::init_default_ut_logging();
    let store_type = StorageType::build_storage_types_based_on_env();
    info!("store type: {:?}", store_type);
    for store in store_type {
        info!(
            "Running admin GC missing cases test with storage type: {}",
            store
        );
        test_admin_gc_missing_cases(&store).await;
    }
}

async fn test_admin_gc_missing_cases(store_type: &StorageType) {
    let (test_context, _guard) = distributed_with_gc(store_type).await;
    let instance = test_context.frontend();
    let metasrv = test_context.metasrv();

    // Case 1: Invalid table name
    let invalid_table_sql = "ADMIN GC_TABLE('no_such_table')";
    let invalid_table_result = try_execute_sql(&instance, invalid_table_sql).await;
    assert!(
        invalid_table_result.is_err(),
        "Expected error for invalid table name"
    );

    // Case 2: Invalid region id (same table, non-existent region number)
    let invalid_region_table = "test_admin_gc_invalid_region";
    create_append_table(&instance, invalid_region_table, None).await;
    let table_id = get_table_id(&instance, invalid_region_table).await;
    let region_ids = fetch_region_ids(&instance, invalid_region_table).await;
    let base_region = RegionId::from_u64(*region_ids.first().expect("region id exists"));
    let invalid_region = RegionId::new(table_id, base_region.region_number().saturating_add(100));
    let invalid_region_sql = format!("ADMIN GC_REGIONS({})", invalid_region.as_u64());
    let sst_before_invalid_region = list_sst_files(&test_context).await;
    let invalid_region_result = try_execute_sql(&instance, &invalid_region_sql).await;
    match invalid_region_result {
        Ok(output) => {
            let processed_regions = extract_u64_output(output.data).await;
            assert_eq!(processed_regions, 0);
            let sst_after_invalid_region = list_sst_files(&test_context).await;
            assert_eq!(
                sst_before_invalid_region.len(),
                sst_after_invalid_region.len()
            );
        }
        Err(_) => {
            let sst_after_invalid_region = list_sst_files(&test_context).await;
            assert_eq!(
                sst_before_invalid_region.len(),
                sst_after_invalid_region.len()
            );
        }
    }

    // Case 3: No garbage to collect (idempotent)
    let no_garbage_table = "test_admin_gc_no_garbage";
    create_append_table(&instance, no_garbage_table, None).await;
    insert_and_flush(&instance, no_garbage_table, 1).await;
    let sst_before_gc = list_sst_files(&test_context).await;
    let no_garbage_gc_sql = format!("ADMIN GC_TABLE('{no_garbage_table}')");
    let no_garbage_output = execute_sql(&instance, &no_garbage_gc_sql).await;
    let processed_regions = extract_u64_output(no_garbage_output.data).await;
    assert_eq!(processed_regions, 1);
    let sst_after_gc = list_sst_files(&test_context).await;
    assert_eq!(sst_before_gc.len(), sst_after_gc.len());

    // Case 4: Multi-region table, multi-region GC
    let multi_region_table = "test_admin_gc_multi_region";
    let partition_clause =
        Some("PARTITION ON COLUMNS (host) (host < 'm', host >= 'm')".to_string());
    create_append_table(&instance, multi_region_table, partition_clause.as_deref()).await;
    insert_and_flush(&instance, multi_region_table, 4).await;
    let sst_before_compaction = list_sst_files(&test_context).await;
    let compact_sql = format!("ADMIN COMPACT_TABLE('{multi_region_table}')");
    execute_sql(&instance, &compact_sql).await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let sst_after_compaction = list_sst_files(&test_context).await;
    assert!(sst_after_compaction.len() >= sst_before_compaction.len());
    let region_ids = fetch_region_ids(&instance, multi_region_table).await;
    let gc_regions_sql = format!(
        "ADMIN GC_REGIONS({})",
        region_ids
            .iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );
    let gc_regions_output = execute_sql(&instance, &gc_regions_sql).await;
    let processed_regions = extract_u64_output(gc_regions_output.data).await;
    assert_eq!(processed_regions as usize, region_ids.len());
    let sst_after_gc = list_sst_files(&test_context).await;
    assert!(sst_after_gc.len() < sst_after_compaction.len());

    // Case 5: Manual GC on dropped region
    let dropped_region_table = "test_admin_gc_dropped_region";
    create_append_table(&instance, dropped_region_table, partition_clause.as_deref()).await;
    let dropped_table_id = get_table_id(&instance, dropped_region_table).await;
    let (_routes, regions) =
        super::get_table_route(metasrv.table_metadata_manager(), dropped_table_id).await;
    let base_region = *regions.first().expect("table has at least one region");
    let dropped_region = RegionId::new(
        dropped_table_id,
        base_region.region_number().saturating_add(100),
    );
    let dst_region = RegionId::new(
        dropped_table_id,
        base_region.region_number().saturating_add(200),
    );
    let repart_mgr = metasrv.table_metadata_manager().table_repart_manager();
    let current = repart_mgr
        .get_with_raw_bytes(dropped_table_id)
        .await
        .unwrap();
    let mut repart_value = TableRepartValue::new();
    repart_value.update_mappings(dropped_region, &[dst_region]);
    repart_mgr
        .upsert_value(dropped_table_id, current, &repart_value)
        .await
        .unwrap();
    let dropped_gc_sql = format!("ADMIN GC_REGIONS({})", dropped_region.as_u64());
    let dropped_gc_output = execute_sql(&instance, &dropped_gc_sql).await;
    let processed_regions = extract_u64_output(dropped_gc_output.data).await;
    assert_eq!(processed_regions, 1);

    // Case 6: Cooldown bypass for manual GC (should still process regions on immediate re-run)
    let cooldown_region = *region_ids.first().expect("region id exists");
    let cooldown_gc_sql = format!("ADMIN GC_REGIONS({cooldown_region})");
    execute_sql(&instance, &cooldown_gc_sql).await;
    let cooldown_gc_output = execute_sql(&instance, &cooldown_gc_sql).await;
    let processed_regions = extract_u64_output(cooldown_gc_output.data).await;
    assert_eq!(processed_regions, 1);
}

async fn create_append_table(
    instance: &std::sync::Arc<frontend::instance::Instance>,
    table_name: &str,
    partition_clause: Option<&str>,
) {
    let partition_clause = partition_clause.unwrap_or("");
    let create_table_sql = format!(
        "\
        CREATE TABLE {table_name} (\
            ts TIMESTAMP TIME INDEX,\
            val DOUBLE,\
            host STRING\
        ) {partition_clause} WITH (append_mode = 'true')\
        "
    );
    execute_sql(instance, &create_table_sql).await;
}

async fn insert_and_flush(
    instance: &std::sync::Arc<frontend::instance::Instance>,
    table_name: &str,
    rounds: usize,
) {
    for i in 0..rounds {
        let insert_sql = format!(
            "\
            INSERT INTO {table_name} (ts, val, host) VALUES\
            ('2023-01-0{} 10:00:00', {}, 'host{}'),\
            ('2023-01-0{} 11:00:00', {}, 'host{}'),\
            ('2023-01-0{} 12:00:00', {}, 'host{}')\
            ",
            i + 1,
            10.0 + i as f64,
            i,
            i + 1,
            20.0 + i as f64,
            i,
            i + 1,
            30.0 + i as f64,
            i
        );
        execute_sql(instance, &insert_sql).await;
        let flush_sql = format!("ADMIN FLUSH_TABLE('{table_name}')");
        execute_sql(instance, &flush_sql).await;
    }
}

async fn fetch_region_ids(
    instance: &std::sync::Arc<frontend::instance::Instance>,
    table_name: &str,
) -> Vec<u64> {
    let region_id_sql = format!(
        "SELECT greptime_partition_id FROM information_schema.partitions WHERE table_name = '{}' ORDER BY greptime_partition_id",
        table_name
    );
    let region_output = execute_sql(instance, &region_id_sql).await;
    let OutputData::Stream(region_stream) = region_output.data else {
        panic!("Expected stream output for region id query");
    };
    let batches = RecordBatches::try_collect(region_stream).await.unwrap();
    let mut region_ids = Vec::new();
    for batch in batches.iter() {
        let column = batch.column(0);
        let array = column.as_primitive::<UInt64Type>();
        for idx in 0..array.len() {
            if array.is_valid(idx) {
                region_ids.push(array.value(idx));
            }
        }
    }
    region_ids
}

async fn get_table_id(
    instance: &std::sync::Arc<frontend::instance::Instance>,
    table_name: &str,
) -> table::metadata::TableId {
    let table = instance
        .catalog_manager()
        .table("greptime", "public", table_name, None)
        .await
        .unwrap()
        .unwrap();
    table.table_info().table_id()
}

async fn extract_u64_output(output: OutputData) -> u64 {
    let recordbatches = match output {
        OutputData::Stream(stream) => RecordBatches::try_collect(stream).await.unwrap(),
        OutputData::RecordBatches(recordbatches) => recordbatches,
        _ => panic!("Unexpected output type"),
    };
    let batch = recordbatches.iter().next().expect("non-empty recordbatch");
    let column = batch.column(0);
    let array = column.as_primitive::<UInt64Type>();
    array.value(0)
}
