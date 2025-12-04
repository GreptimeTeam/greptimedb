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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_procedure::ProcedureWithId;
use common_telemetry::info;
use common_test_util::recordbatch::check_output_stream;
use futures::future::join_all;
use meta_srv::gc::{BatchGcProcedure, GcSchedulerOptions};
use mito2::gc::GcConfig;
use session::context::QueryContext;
use tokio::time::sleep;

use crate::cluster::GreptimeDbClusterBuilder;
use crate::test_util::{StorageType, TempDirGuard, TestGuard};
use crate::tests::gc::delay_layer::{DelayLayer, create_test_object_store_manager_with_delays};
use crate::tests::gc::{get_table_route, list_sst_files};
use crate::tests::test_util::{
    MockInstanceBuilder, TestContext, execute_sql, try_execute_sql_with, wait_procedure,
};

/// Test scenario: Manifest Update During Listing
///
/// This test simulates a race condition where GC listing takes too long,
/// and manifest gets updated during the listing operation.
///
/// Test steps:
/// 1. Create a cluster with delayed object store (slow listing)
/// 2. Create table and generate initial SST files
/// 3. Start GC operation (will be slow due to injected delays)
/// 4. While GC is listing files, perform manifest-updating operations:
///    - Insert new data and flush
///    - Trigger compaction  
///    - Drop tables/regions
/// 5. Verify GC handles manifest changes correctly
/// 6. Ensure no files are incorrectly deleted
pub async fn test_manifest_update_during_listing(store_type: &StorageType) {
    let test_name = format!("gc_race_test_{}", uuid::Uuid::new_v4());

    // Create cluster with delayed object store to simulate slow listing
    let (test_context, _guard) = create_delayed_cluster(store_type, &test_name).await;
    let instance = test_context.frontend();
    let metasrv = test_context.metasrv();

    // Step 1: Create table with append mode to easily generate multiple files
    let create_table_sql = r#"
        CREATE TABLE test_race_table (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            host STRING
        ) WITH (append_mode = 'true')
    "#;
    execute_sql(&instance, create_table_sql).await;

    // Step 2: Generate initial SST files
    for i in 0..3 {
        let insert_sql = format!(
            r#"
            INSERT INTO test_race_table (ts, val, host) VALUES
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

        // Flush to create SST files
        let flush_sql = "ADMIN FLUSH_TABLE('test_race_table')";
        execute_sql(&instance, flush_sql).await;
    }

    // Get table information
    let table = instance
        .catalog_manager()
        .table("greptime", "public", "test_race_table", None)
        .await
        .unwrap()
        .unwrap();
    let table_id = table.table_info().table_id();

    // Get initial SST file count
    let initial_sst_files = list_sst_files(&test_context).await;
    info!("Initial SST files: {}", initial_sst_files.len());
    assert_eq!(initial_sst_files.len(), 3); // 3 files from 3 flushes

    // Get table route information for GC procedure
    let (region_routes, regions) =
        get_table_route(metasrv.table_metadata_manager(), table_id).await;

    // Step 3: Start GC operation in background (will be slow due to delays)
    info!("Starting GC operation with delayed listing...");

    // Clone necessary data for the async task
    let mailbox = metasrv.mailbox().clone();
    let grpc_addr = metasrv.options().grpc.server_addr.clone();
    let regions_clone = regions.clone();
    let region_routes_clone = region_routes.clone();
    let procedure_manager = metasrv.procedure_manager().clone();

    let gc_handle = tokio::spawn(async move {
        let procedure = BatchGcProcedure::new(
            mailbox,
            grpc_addr,
            regions_clone,
            false, // full_file_listing
            region_routes_clone,
            HashMap::new(),          // related_regions
            Duration::from_secs(30), // timeout
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;

        let _watcher = procedure_manager.submit(procedure_with_id).await.unwrap();

        // Wait for the procedure to complete
        wait_procedure(&procedure_manager, procedure_id).await;

        info!("GC operation completed");
    });

    // Step 4: Wait a bit for GC to start listing (due to delays, it should still be listing)
    info!("Waiting for GC to start listing operation...");
    sleep(Duration::from_secs(2)).await;

    // Step 5: Perform manifest-updating operations while GC is listing
    info!("Performing manifest-updating operations while GC is listing...");

    // Operation 1: Insert new data and flush (creates new SST files)
    let insert_sql = r#"
        INSERT INTO test_race_table (ts, val, host) VALUES
        ('2023-01-05 10:00:00', 50.0, 'host4'),
        ('2023-01-05 11:00:00', 60.0, 'host4'),
        ('2023-01-05 12:00:00', 70.0, 'host4')
    "#;
    execute_sql(&instance, insert_sql).await;

    let flush_sql = "ADMIN FLUSH_TABLE('test_race_table')";
    execute_sql(&instance, flush_sql).await;
    info!("Inserted new data and flushed");

    // Operation 2: Trigger compaction (modifies manifest)
    let compact_sql = "ADMIN COMPACT_TABLE('test_race_table')";
    execute_sql(&instance, compact_sql).await;
    info!("Triggered compaction");

    // Wait for compaction to complete
    sleep(Duration::from_secs(2)).await;

    // Operation 3: Create another table and drop it (major manifest change)
    let create_table2_sql = r#"
        CREATE TABLE test_race_table2 (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            host STRING
        ) WITH (append_mode = 'true')
    "#;
    execute_sql(&instance, create_table2_sql).await;

    let insert_sql2 = r#"
        INSERT INTO test_race_table2 (ts, val, host) VALUES
        ('2023-01-06 10:00:00', 80.0, 'host5'),
        ('2023-01-06 11:00:00', 90.0, 'host5')
    "#;
    execute_sql(&instance, insert_sql2).await;

    let flush_sql2 = "ADMIN FLUSH_TABLE('test_race_table2')";
    execute_sql(&instance, flush_sql2).await;

    // Drop the second table
    let drop_table_sql = "DROP TABLE test_race_table2";
    execute_sql(&instance, drop_table_sql).await;
    info!("Created and dropped second table");

    // Step 6: Wait for GC to complete
    info!("Waiting for GC operation to complete...");
    gc_handle.await.unwrap();

    // Step 7: Verify results
    info!("Verifying GC results after manifest changes...");

    // Get final SST file count
    let final_sst_files = list_sst_files(&test_context).await;
    info!("Final SST files: {}", final_sst_files.len());

    // Verify data integrity for the remaining table
    let count_sql = "SELECT COUNT(*) FROM test_race_table";
    let count_output = execute_sql(&instance, count_sql).await;
    let expected = r#"
+----------+
| count(*) |
+----------+
| 12       |
+----------+"#
        .trim();
    check_output_stream(count_output.data, expected).await;

    // Verify that the dropped table's data is not accessible
    let select_dropped_sql = "SELECT COUNT(*) FROM test_race_table2";
    let select_dropped_result =
        try_execute_sql_with(&instance, select_dropped_sql, QueryContext::arc()).await;
    assert!(
        select_dropped_result.is_err(),
        "Expected error querying dropped table: {:?}",
        select_dropped_result
    );

    // The query should fail since the table was dropped
    // This is a basic check - in a real scenario we'd expect an error
    info!("Dropped table query result processed");

    // Additional verification: ensure no data loss occurred
    let select_sql = "SELECT * FROM test_race_table ORDER BY ts";
    let select_output = execute_sql(&instance, select_sql).await;
    let expected_data = r#"
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
| 2023-01-05T10:00:00 | 50.0 | host4 |
| 2023-01-05T11:00:00 | 60.0 | host4 |
| 2023-01-05T12:00:00 | 70.0 | host4 |
+---------------------+------+-------+"#
        .trim();
    check_output_stream(select_output.data, expected_data).await;

    info!("Manifest update during listing test completed successfully");
}

/// Helper function to create a cluster with delayed object store for testing race conditions
async fn create_delayed_cluster(
    store_type: &StorageType,
    test_name: &str,
) -> (TestContext, TestGuard) {
    // Create object store manager with delays
    let (delayed_store_manager, temp_dir_guard) = create_test_object_store_manager_with_delays(
        store_type,
        test_name,
        Duration::from_secs(5),     // list_delay
        Duration::from_millis(100), // delete_delay
        Duration::from_millis(500), // list_per_file_delay
    )
    .await
    .unwrap();

    // Build cluster with delayed store
    let builder = GreptimeDbClusterBuilder::new(test_name)
        .await
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
            ..Default::default()
        })
        .with_datanode_gc_config(GcConfig {
            enable: true,
            lingering_time: Some(Duration::ZERO), // Speed up test
            ..Default::default()
        })
        .with_object_store_manager(delayed_store_manager);

    (
        TestContext::new(MockInstanceBuilder::Distributed(builder)).await,
        temp_dir_guard,
    )
}

/// Test runner for manifest update during listing race condition test
#[tokio::test]
async fn test_manifest_update_during_listing_different_store() {
    let _ = dotenv::dotenv();
    common_telemetry::init_default_ut_logging();
    let store_type = StorageType::build_storage_types_based_on_env();
    info!(
        "Testing manifest update during listing with storage types: {:?}",
        store_type
    );

    for store in store_type {
        if store == StorageType::File {
            continue; // Skip filesystem storage for this race condition test
        }
        info!(
            "Running manifest update during listing test with storage type: {}",
            store
        );
        test_manifest_update_during_listing(&store).await;
    }
}
