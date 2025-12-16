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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use common_procedure::ProcedureWithId;
use common_telemetry::info;
use common_test_util::recordbatch::check_output_stream;
use itertools::Itertools as _;
use meta_srv::gc::{BatchGcProcedure, GcSchedulerOptions, Region2Peers};
use mito2::gc::GcConfig;
use mito2::sst::file::FileHandle;
use store_api::storage::RegionId;
use tokio::time::sleep;

use crate::cluster::GreptimeDbClusterBuilder;
use crate::test_util::{StorageType, TestGuard};
use crate::tests::gc::delay_layer::create_test_object_store_manager_with_delays;
use crate::tests::gc::{
    get_table_route, list_sst_files_from_manifest, list_sst_files_from_storage, sst_equal_check,
};
use crate::tests::test_util::{MockInstanceBuilder, TestContext, execute_sql, wait_procedure};

/// Get file handles for all SST files for all files in the given regions
async fn get_file_handle_for_regions(
    test_context: &TestContext,
    regions: &[RegionId],
) -> Vec<FileHandle> {
    let mut all_file_handles = vec![];
    let mut all_found_regions = HashSet::new();
    for datanode in test_context.datanodes().values() {
        let mito_engine = datanode.region_server().mito_engine().unwrap();
        for region_id in regions {
            let Some(region) = mito_engine.find_region(*region_id) else {
                continue;
            };
            all_found_regions.insert(*region_id);
            let manifest = region.manifest().await;
            let files = manifest.files.clone();
            let file_purger = region.file_purger();
            let file_handles = files
                .values()
                .map(|v| FileHandle::new(v.clone(), file_purger.clone()))
                .collect_vec();
            info!(
                "Region {:?} has {} file handles",
                region_id,
                file_handles.len()
            );
            all_file_handles.extend(file_handles);
        }
    }

    assert_eq!(
        all_found_regions.len(),
        regions.len(),
        "Some regions not found in datanodes: expected regions {:?}, found regions {:?}",
        regions,
        all_found_regions
    );

    all_file_handles
}

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
    for i in 0..4 {
        let insert_sql = format!(
            r#"
            INSERT INTO test_race_table (ts, val, host) VALUES
            ('2023-01-0{} 10:00:00', {}, 'host{}'),
            ('2023-01-0{} 10:00:01', {}, 'host{}'),
            ('2023-01-0{} 10:00:02', {}, 'host{}')
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
    let initial_sst_files = list_sst_files_from_manifest(&test_context).await;
    info!("Initial SST files: {}", initial_sst_files.len());

    // Get table route information for GC procedure
    let (region_routes, regions) =
        get_table_route(metasrv.table_metadata_manager(), table_id).await;

    // Step 3: Start GC operation in background (will be slow due to delays)
    info!("Starting GC operation with delayed listing...");

    let gc_handle = trigger_gc(
        metasrv,
        regions.clone(),
        region_routes.clone(),
        Duration::from_secs(30), // timeout
    );

    // Step 4: Wait a bit for GC to start listing (due to delays, it should still be listing)
    info!("Waiting for GC to start listing operation...");
    sleep(Duration::from_secs(2)).await;

    // Step 5: Perform manifest-updating operations while GC is listing
    // Trigger compaction (modifies manifest)
    let compact_sql = "ADMIN COMPACT_TABLE('test_race_table')";
    execute_sql(&instance, compact_sql).await;
    info!("Triggered compaction");

    // Wait for compaction to complete
    sleep(Duration::from_secs(2)).await;

    // Step 6: Wait for GC to complete
    info!("Waiting for GC operation to complete...");
    gc_handle.await.unwrap();

    // Step 7: Verify results
    info!("Verifying GC results after manifest changes...");

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

    // Additional verification: ensure no data loss occurred
    let select_sql = "SELECT * FROM test_race_table ORDER BY ts";
    let select_output = execute_sql(&instance, select_sql).await;
    let expected_data = r#"
+---------------------+------+-------+
| ts                  | val  | host  |
+---------------------+------+-------+
| 2023-01-01T10:00:00 | 10.0 | host0 |
| 2023-01-01T10:00:01 | 20.0 | host0 |
| 2023-01-01T10:00:02 | 30.0 | host0 |
| 2023-01-02T10:00:00 | 11.0 | host1 |
| 2023-01-02T10:00:01 | 21.0 | host1 |
| 2023-01-02T10:00:02 | 31.0 | host1 |
| 2023-01-03T10:00:00 | 12.0 | host2 |
| 2023-01-03T10:00:01 | 22.0 | host2 |
| 2023-01-03T10:00:02 | 32.0 | host2 |
| 2023-01-04T10:00:00 | 13.0 | host3 |
| 2023-01-04T10:00:01 | 23.0 | host3 |
| 2023-01-04T10:00:02 | 33.0 | host3 |
+---------------------+------+-------+"#
        .trim();
    check_output_stream(select_output.data, expected_data).await;

    info!("Manifest update during listing test completed successfully");

    let gc_handle = trigger_gc(
        metasrv,
        regions.clone(),
        region_routes.clone(),
        Duration::from_secs(30), // timeout
    );
    gc_handle.await.unwrap();
    // Perform SST equality check after final GC
    sst_equal_check(&test_context).await;
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

/// Trigger GC operation and return a handle to monitor its execution
///
/// This function creates a GC procedure and submits it for execution, returning
/// a JoinHandle that can be used to wait for GC completion.
///
/// # Arguments
/// * `metasrv` - The metasrv instance to use for GC operation
/// * `regions` - The regions to perform GC on
/// * `region_routes` - The region routes for GC operation
/// * `timeout` - Timeout duration for the GC procedure
///
/// # Returns
/// A JoinHandle that can be awaited for GC completion
fn trigger_gc(
    metasrv: &Arc<meta_srv::metasrv::Metasrv>,
    regions: Vec<RegionId>,
    region_routes: Region2Peers,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    // Clone necessary data for the async GC task
    let mailbox = metasrv.mailbox().clone();
    let grpc_addr = metasrv.options().grpc.server_addr.clone();
    let regions_clone = regions.clone();
    let region_routes_clone = region_routes.clone();
    let procedure_manager = metasrv.procedure_manager().clone();

    tokio::spawn(async move {
        let procedure = BatchGcProcedure::new(
            mailbox,
            grpc_addr,
            regions_clone,
            true, // full_file_listing in object store
            region_routes_clone,
            HashMap::new(), // related_regions
            timeout,
        );

        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;

        let _watcher = procedure_manager.submit(procedure_with_id).await.unwrap();

        // Wait for the procedure to complete
        wait_procedure(&procedure_manager, procedure_id).await;

        info!("GC operation completed");
    })
}

/// Test scenario: GC Execution During Query
///
/// This test simulates a race condition where GC runs while file handles are being held,
/// ensuring that files referenced by active file handles are not deleted prematurely.
///
/// Test steps:
/// 1. Create a cluster with GC enabled
/// 2. Create table and generate initial SST files with historical data
/// 3. Acquire file handles for all SST files (simulating a query holding references)
/// 4. Insert new data and trigger flush & compact to create garbage SST files
/// 5. Trigger GC while file handles are still being held
/// 6. Verify that files referenced by handles are not deleted during GC execution
/// 7. Release file handles (simulating query completion)
/// 8. Verify data integrity and that GC eventually cleaned up unreferenced files
/// 9. Trigger final GC to clean up all unreferenced files and verify cleanup
///
/// Note: This test uses file handles rather than an actual long-running query to simulate
/// the scenario where files are referenced during GC execution.
pub async fn test_gc_execution_during_query(store_type: &StorageType) {
    let test_name = format!("gc_query_race_test_{}", uuid::Uuid::new_v4());

    // Create cluster with GC enabled using regular store configuration
    let (store_config, _temp_dir_guard) = crate::test_util::get_test_store_config(store_type);

    let builder = GreptimeDbClusterBuilder::new(&test_name)
        .await
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
            ..Default::default()
        })
        .with_datanode_gc_config(GcConfig {
            enable: true,
            lingering_time: Some(Duration::from_secs(1)), // Short lingering time for test
            ..Default::default()
        })
        .with_store_config(store_config);

    let test_context = TestContext::new(MockInstanceBuilder::Distributed(builder)).await;
    let instance = test_context.frontend();
    let metasrv = test_context.metasrv();

    // Step 1: Create table with append mode to easily generate multiple files
    let create_table_sql = r#"
        CREATE TABLE test_query_race_table (
            ts TIMESTAMP TIME INDEX,
            val DOUBLE,
            host STRING
        )
    "#;
    execute_sql(&instance, create_table_sql).await;

    // Step 2: Generate initial SST files with historical data
    for i in 0..3 {
        let insert_sql = format!(
            r#"
            INSERT INTO test_query_race_table (ts, val, host) VALUES
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
        let flush_sql = "ADMIN FLUSH_TABLE('test_query_race_table')";
        execute_sql(&instance, flush_sql).await;
    }

    // Sleep for one second to make sure table is created before proceeding
    sleep(Duration::from_secs(1)).await;

    // Get table information
    let table = instance
        .catalog_manager()
        .table("greptime", "public", "test_query_race_table", None)
        .await
        .unwrap()
        .unwrap();
    let table_id = table.table_info().table_id();

    // Get initial SST file count
    let initial_sst_files = list_sst_files_from_storage(&test_context).await;
    info!("Initial SST files: {:?}", initial_sst_files);

    // Get table route information for GC procedure
    let (region_routes, regions) =
        get_table_route(metasrv.table_metadata_manager(), table_id).await;

    // Step 3: Simulate long-running query by getting file handles
    let all_file_handles = get_file_handle_for_regions(&test_context, &regions).await;

    // Extract file paths from the handles for verification later
    let referenced_file_paths: HashSet<_> = all_file_handles
        .iter()
        .map(|handle| handle.file_id())
        .collect();
    info!("Files referenced by handles: {:?}", referenced_file_paths);

    // Step 4: Trigger compaction to create garbage SST files & Insert new data and trigger flush & compact while query is running
    let compact_sql = "ADMIN COMPACT_TABLE('test_query_race_table')";
    execute_sql(&instance, compact_sql).await;

    info!("Inserting new data and triggering flush & compact while query is running...");

    // Insert new data
    let insert_sql = r#"
        INSERT INTO test_query_race_table (ts, val, host) VALUES
        ('2023-01-04 10:00:00', 50.0, 'host4'),
        ('2023-01-04 11:00:00', 60.0, 'host4'),
        ('2023-01-04 12:00:00', 70.0, 'host4')
    "#;
    execute_sql(&instance, insert_sql).await;

    // Flush to create new SST files
    let flush_sql = "ADMIN FLUSH_TABLE('test_query_race_table')";
    execute_sql(&instance, flush_sql).await;
    info!("New data inserted and flushed");

    let sst_files_before_compaction = list_sst_files_from_storage(&test_context).await;
    info!(
        "SST files before compaction: {:?}",
        sst_files_before_compaction
    );

    // Trigger compaction to remove some files that the query might be referencing
    let compact_sql = "ADMIN COMPACT_TABLE('test_query_race_table')";
    execute_sql(&instance, compact_sql).await;
    info!("Compaction triggered");

    // Wait for compaction to complete
    sleep(Duration::from_secs(2)).await;

    // Get SST file count after compaction (should have both old and new files)
    let sst_files_after_compaction = list_sst_files_from_storage(&test_context).await;
    info!(
        "SST files after compaction: {:?}",
        sst_files_after_compaction
    );

    // Step 5: Trigger GC while query is still running
    info!("Triggering GC while query is still running...");

    let gc_handle = trigger_gc(
        metasrv,
        regions.clone(),
        region_routes.clone(),
        Duration::from_secs(30), // timeout
    );

    // Step 6: Wait for both query and GC to complete
    info!("Waiting for query and GC to complete...");

    // vital here first wait for GC to ensure it doesn't delete files needed by query
    // Wait for GC to complete
    gc_handle.await.unwrap();

    // Step 6: Verify that files referenced by handles were not deleted during GC
    info!("Verifying that referenced files were not deleted during GC...");
    let sst_files_after_gc = list_sst_files_from_storage(&test_context).await;

    // Extract file IDs from the storage file paths for comparison
    let storage_file_ids: HashSet<String> = sst_files_after_gc
        .iter()
        .filter_map(|path| {
            // Extract file ID from path like "/path/to/region_id/file_id.sst"
            path.split('/')
                .next_back()
                .and_then(|filename| filename.strip_suffix(".parquet"))
                .map(|file_id_str| file_id_str.to_string())
        })
        .collect();

    // Check that all referenced files are still present in storage
    let missing_files: Vec<String> = referenced_file_paths
        .iter()
        .map(|f| f.file_id().to_string())
        .filter(|file_id| !storage_file_ids.contains(file_id))
        .collect();

    assert!(
        missing_files.is_empty(),
        "GC deleted files that were still referenced: {:?}, storage files: {:?}",
        missing_files,
        storage_file_ids
    );
    info!("All referenced files are still present after GC");
    // Step 7: Simulate end of long-running query by dropping file handles
    drop(all_file_handles);

    // Step 8: Verify results
    info!("Verifying GC results after query completion...");

    // Get final SST file count
    let sst_manifest_files_after_gc = list_sst_files_from_manifest(&test_context).await;
    let sst_files_store_after_gc = list_sst_files_from_storage(&test_context).await;
    info!(
        "Final SST files: {}, final in manifest files: {}",
        sst_files_store_after_gc.len(),
        sst_manifest_files_after_gc.len()
    );

    // Verify data integrity - all data should still be accessible
    let count_sql = "SELECT COUNT(*) FROM test_query_race_table";
    let count_output = execute_sql(&instance, count_sql).await;
    let expected = r#"
+----------+
| count(*) |
+----------+
| 12       |
+----------+"#
        .trim();
    check_output_stream(count_output.data, expected).await;

    // Verify that GC eventually cleaned up files after query finished
    // The exact number depends on compaction results, but should be less than after compaction
    info!("GC execution during query test completed successfully");

    // Step 9: Trigger GC again after releasing file handles to clean up unreferenced files
    info!("Triggering final GC to clean up unreferenced files...");
    let final_gc_handle = trigger_gc(
        metasrv,
        regions,
        region_routes,
        Duration::from_secs(30), // timeout
    );

    // Wait for final GC to complete
    info!("Waiting for final GC to complete...");
    final_gc_handle.await.unwrap();

    // Perform SST equality check after final GC
    sst_equal_check(&test_context).await;

    // Get final file count after GC cleanup
    let final_cleanup_sst_files = list_sst_files_from_storage(&test_context).await;
    info!(
        "Final SST files after cleanup: {}",
        final_cleanup_sst_files.len()
    );

    // Verify that files have been cleaned up (should be fewer than before final GC)
    assert!(
        final_cleanup_sst_files.len() <= sst_files_store_after_gc.len(),
        "Final GC should not increase file count. Before: {}, After: {}",
        sst_files_store_after_gc.len(),
        final_cleanup_sst_files.len()
    );

    // Verify data integrity is still maintained after final cleanup
    let final_count_output = execute_sql(&instance, count_sql).await;
    check_output_stream(final_count_output.data, expected).await;

    info!("Final GC cleanup completed successfully");
}

/// Test runner for GC execution during query race condition test
#[tokio::test]
async fn test_gc_execution_during_query_different_store() {
    let _ = dotenv::dotenv();
    common_telemetry::init_default_ut_logging();
    let store_type = StorageType::build_storage_types_based_on_env();
    info!(
        "Testing GC execution during query with storage types: {:?}",
        store_type
    );

    for store in store_type {
        if store == StorageType::File {
            continue; // Skip filesystem storage for this race condition test
        }
        info!(
            "Running GC execution during query test with storage type: {}",
            store
        );
        test_gc_execution_during_query(&store).await;
    }
}
