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
use std::time::Duration;

use common_procedure::ProcedureWithId;
use common_telemetry::info;
use common_test_util::recordbatch::check_output_stream;
use meta_srv::gc::{BatchGcProcedure, GcSchedulerOptions};
use mito2::gc::GcConfig;

use crate::cluster::GreptimeDbClusterBuilder;
use crate::test_util::{StorageType, TempDirGuard, get_test_store_config};
use crate::tests::gc::{
    get_table_route, list_sst_files_from_manifest, list_sst_files_from_storage, sst_equal_check,
};
use crate::tests::test_util::{MockInstanceBuilder, TestContext, execute_sql, wait_procedure};

/// Helper function to create a distributed cluster with GC enabled
async fn distributed_with_gc(store_type: &StorageType) -> (TestContext, TempDirGuard) {
    common_telemetry::init_default_ut_logging();
    let test_name = uuid::Uuid::new_v4().to_string();
    let (store_config, guard) = get_test_store_config(store_type);

    let builder = GreptimeDbClusterBuilder::new(&test_name)
        .await
        .with_metasrv_gc_config(GcSchedulerOptions {
            enable: true,
            ..Default::default()
        })
        .with_datanode_gc_config(GcConfig {
            enable: true,
            // set lingering time to zero for test speedup
            lingering_time: Some(Duration::ZERO),
            ..Default::default()
        })
        .with_store_config(store_config);
    (
        TestContext::new(MockInstanceBuilder::Distributed(builder)).await,
        guard,
    )
}

/// Basic GC functionality test as described in the test plan:
/// - Create table with append mode
/// - Insert data and flush multiple times to create SST files
/// - Trigger compaction to create garbage files
/// - Run GC and verify only compacted files remain
/// - Verify data integrity after GC
pub async fn test_gc_basic(store_type: &StorageType) {
    let (test_context, _guard) = distributed_with_gc(store_type).await;
    let instance = test_context.frontend();
    let metasrv = test_context.metasrv();

    // Step 1: Create table with append_mode to easily generate multiple files
    let create_table_sql = r#"
        CREATE TABLE test_gc_table (
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
            INSERT INTO test_gc_table (ts, val, host) VALUES
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
        let flush_sql = "ADMIN FLUSH_TABLE('test_gc_table')";
        execute_sql(&instance, flush_sql).await;
    }

    // Step 3: Get table information
    let table = instance
        .catalog_manager()
        .table("greptime", "public", "test_gc_table", None)
        .await
        .unwrap()
        .unwrap();
    let table_id = table.table_info().table_id();

    // List SST files before compaction (for verification)
    let sst_files_before_compaction = list_sst_files_from_storage(&test_context).await;
    info!(
        "SST files before compaction: {:?}",
        sst_files_before_compaction
    );
    assert_eq!(sst_files_before_compaction.len(), 4); // 4 files from 4 flushes

    // Step 4: Trigger compaction to create garbage SST files
    let compact_sql = "ADMIN COMPACT_TABLE('test_gc_table')";
    execute_sql(&instance, compact_sql).await;

    // Wait for compaction to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // List SST files after compaction (should have both old and new files)
    let sst_files_after_compaction = list_sst_files_from_storage(&test_context).await;
    info!(
        "SST files after compaction: {:?}",
        sst_files_after_compaction
    );
    assert_eq!(sst_files_after_compaction.len(), 5); // 4 old + 1 new

    // Step 5: Get table route information for GC procedure
    let (region_routes, regions) =
        get_table_route(metasrv.table_metadata_manager(), table_id).await;

    // Step 6: Create and execute BatchGcProcedure
    let procedure = BatchGcProcedure::new(
        metasrv.mailbox().clone(),
        metasrv.options().grpc.server_addr.clone(),
        regions.clone(),
        false, // full_file_listing
        region_routes,
        HashMap::new(),          // related_regions (empty for this simple test)
        Duration::from_secs(10), // timeout
    );

    // Submit the procedure to the procedure manager
    let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
    let procedure_id = procedure_with_id.id;

    let _watcher = metasrv
        .procedure_manager()
        .submit(procedure_with_id)
        .await
        .unwrap();

    // Wait for the procedure to complete
    wait_procedure(metasrv.procedure_manager(), procedure_id).await;

    // Step 7: Verify GC results
    sst_equal_check(&test_context).await;
    let sst_files_after_gc = list_sst_files_from_storage(&test_context).await;
    info!("SST files after GC: {:?}", sst_files_after_gc);
    assert_eq!(sst_files_after_gc.len(), 1); // Only the compacted file should remain after gc

    let sst_files_in_manifest = list_sst_files_from_manifest(&test_context).await;
    info!(
        "SST files in manifest after GC: {:?}",
        sst_files_in_manifest
    );
    assert_eq!(sst_files_in_manifest.len(), 1); // Manifest should also reflect only the compacted file

    // Verify that data is still accessible
    let count_sql = "SELECT COUNT(*) FROM test_gc_table";
    let count_output = execute_sql(&instance, count_sql).await;
    let expected = r#"
+----------+
| count(*) |
+----------+
| 12       |
+----------+"#
        .trim();
    check_output_stream(count_output.data, expected).await;

    let select_sql = "SELECT * FROM test_gc_table ORDER BY ts";
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

    // TODO: Add more specific assertions once we have proper file system access
    // For now, the test passes if the procedure executes without errors
    info!("GC test completed successfully");
}

/// Test runner for basic GC functionality with different storage types
#[tokio::test]
async fn test_gc_basic_different_store() {
    let _ = dotenv::dotenv();
    common_telemetry::init_default_ut_logging();
    let store_type = StorageType::build_storage_types_based_on_env();
    info!("Testing GC with storage types: {:?}", store_type);
    for store in store_type {
        if store == StorageType::File {
            continue; // no point in test gc in fs storage
        }
        info!("Running GC test with storage type: {}", store);
        test_gc_basic(&store).await;
    }
}
