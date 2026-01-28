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
use common_recordbatch::RecordBatches;
use common_telemetry::info;
use common_test_util::recordbatch::check_output_stream;
use datatypes::arrow::array::AsArray;
use datatypes::arrow::datatypes::UInt64Type;

use super::{distributed_with_gc, list_sst_files};
use crate::test_util::{StorageType, execute_sql};

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
