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

//! Index build tests for mito engine.
//!
use std::collections::HashMap;
use std::sync::Arc;

use api::v1::Rows;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    AlterKind, RegionAlterRequest, RegionBuildIndexRequest, RegionRequest, SetIndexOption,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::{IndexBuildMode, MitoConfig, Mode};
use crate::engine::MitoEngine;
use crate::engine::compaction_test::put_and_flush;
use crate::engine::listener::{GateIndexBuildListener, IndexBuildListener};
use crate::read::scan_region::Scanner;
use crate::sst::location;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, reopen_region, rows_schema,
};

fn async_build_mode_config(is_create_on_flush: bool) -> MitoConfig {
    let mut config = MitoConfig::default();
    config.index.build_mode = IndexBuildMode::Async;
    if !is_create_on_flush {
        config.inverted_index.create_on_flush = Mode::Disable;
        config.fulltext_index.create_on_flush = Mode::Disable;
        config.bloom_filter_index.create_on_flush = Mode::Disable;
    }
    config
}

/// Get the number of generated index files for existed sst files in the scanner.
async fn num_of_index_files(engine: &MitoEngine, scanner: &Scanner, region_id: RegionId) -> usize {
    let region = engine.get_region(region_id).unwrap();
    let access_layer = region.access_layer.clone();
    // When there is no file, return 0 directly.
    // Because we can't know region file ids here.
    if scanner.file_ids().is_empty() {
        return 0;
    }
    let mut index_files_count: usize = 0;
    for region_index_id in scanner.index_ids() {
        let index_path = location::index_file_path(
            access_layer.table_dir(),
            region_index_id,
            access_layer.path_type(),
        );
        if access_layer
            .object_store()
            .exists(&index_path)
            .await
            .unwrap()
        {
            index_files_count += 1;
        }
    }
    index_files_count
}

fn assert_listener_counts(
    listener: &IndexBuildListener,
    expected_begin_count: usize,
    expected_success_count: usize,
) {
    assert_eq!(listener.begin_count(), expected_begin_count);
    assert_eq!(listener.finish_count(), expected_success_count);
}

#[tokio::test]
async fn test_index_build_type_flush() {
    let mut env = TestEnv::with_prefix("test_index_build_type_flush_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(true),
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new().build_with_index();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 2),
    };
    put_rows(&engine, region_id, rows).await;

    // Before first flush is finished, index file and data file should not exist.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_memtables(), 1);
    assert_eq!(scanner.num_files(), 0);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    flush_region(&engine, region_id, None).await;

    // When first flush is just finished, index file should not exist.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_memtables(), 0);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(2, 4),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    // After 2 index build task are finished, 2 index files should exist.
    listener.wait_finish(2).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 2);
}

#[tokio::test]
async fn test_index_build_type_compact() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::with_prefix("test_index_build_type_compact_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(true),
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "4")
        .build_with_index();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 35..45).await;

    common_telemetry::info!("After flush 3 files");

    // all index build tasks begin means flush tasks are all finished.
    listener.wait_begin(3).await;
    // Before compaction is triggered, files should be 4.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 3);
    assert!(num_of_index_files(&engine, &scanner, region_id).await <= 3);

    common_telemetry::info!("Checked 3 files, start compact");

    put_and_flush(&engine, region_id, &column_schemas, 45..50).await;

    listener.wait_begin(5).await; // 4 flush + 1 compaction begin

    // Wait a while to make sure index build tasks are finished.
    listener.wait_stop(5).await; // 4 flush + 1 compaction = some abort + some finish

    common_telemetry::info!("All stopped");

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    // Index files should be built.
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}

#[tokio::test]
async fn test_index_build_type_schema_change() {
    let mut env = TestEnv::with_prefix("test_index_build_type_schema_change_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(true),
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    // Create a region without index.
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush and make sure there is no index file.
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    // Set Index and make sure index file is built without flush or compaction.
    let set_index_request = RegionAlterRequest {
        kind: AlterKind::SetIndexes {
            options: vec![SetIndexOption::Inverted {
                column_name: "tag_0".to_string(),
            }],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(set_index_request))
        .await
        .unwrap();
    listener.wait_finish(1).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}

/// Tests that a schema change (ALTER SetIndexes) triggers index rebuild
/// for all pre-existing SST files, not just one.  Covers the scenario
/// where multiple SSTs were flushed before the index was defined:
/// 1. Create region without index, flush 3 files.
/// 2. Reset scheduler state via reopen_region (flush-triggered no-index
///    builds pollute building_files).
/// 3. Verify 3 SST files and 0 index files.
/// 4. ALTER SetIndexes — triggers rebuild of all 3 inconsistent SSTs.
/// 5. Wait for 3 finishes, then verify 3 SST files + 3 index files.
#[tokio::test]
async fn test_index_build_type_schema_change_multiple_files() {
    let mut env = TestEnv::with_prefix("test_index_build_type_schema_change_multiple_files_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(true),
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    // Create a region without index.
    let request = CreateRequestBuilder::new().build();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush 3 SST files without any index defined.
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 30..40).await;

    // Reset scheduler state: flush-triggered no-index builds mark files
    // as building but produce no index output, polluting building_files.
    // Reopen ensures subsequent ALTER rebuilds can schedule builds cleanly.
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;

    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 3);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    // Set Index via ALTER — triggers schema-change rebuild of all 3 SSTs.
    let set_index_request = RegionAlterRequest {
        kind: AlterKind::SetIndexes {
            options: vec![SetIndexOption::Inverted {
                column_name: "tag_0".to_string(),
            }],
        },
    };
    engine
        .handle_request(region_id, RegionRequest::Alter(set_index_request))
        .await
        .unwrap();

    // Wait for all 3 schema-change rebuilds to finish.
    tokio::time::timeout(std::time::Duration::from_secs(5), listener.wait_finish(3))
        .await
        .unwrap();
    assert_eq!(listener.finish_count(), 3);

    // Verify all 3 SST files now have corresponding index files.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 3);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 3);
}

#[tokio::test]
async fn test_index_build_type_manual_basic() {
    let mut env = TestEnv::with_prefix("test_index_build_type_manual_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(false), // Disable index file creation on flush.
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    // Create a region with index.
    let request = CreateRequestBuilder::new().build_with_index();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush and make sure there is no index file (because create_on_flush is disabled).
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // Index build task is triggered on flush, but not finished.
    assert_listener_counts(&listener, 1, 0);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    // Trigger manual index build task and make sure index file is built without flush or compaction.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    listener.wait_finish(1).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_listener_counts(&listener, 2, 1);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);

    // Test idempotency: Second manual index build request on the same file.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // Should still be 2 begin and 1 finish - no new task should be created for already indexed file.
    assert_listener_counts(&listener, 2, 1);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);

    // Test idempotency again: Third manual index build request to further verify.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_listener_counts(&listener, 2, 1);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}

#[tokio::test]
async fn test_index_build_type_manual_consistency() {
    let mut env = TestEnv::with_prefix("test_index_build_type_manual_consistency_").await;
    let listener = Arc::new(IndexBuildListener::default());
    let engine = env
        .create_engine_with(
            async_build_mode_config(true),
            None,
            Some(listener.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    // Create a region with index.
    let create_request = CreateRequestBuilder::new().build_with_index();
    let table_dir = create_request.table_dir.clone();
    let column_schemas = rows_schema(&create_request);
    engine
        .handle_request(region_id, RegionRequest::Create(create_request.clone()))
        .await
        .unwrap();
    assert_listener_counts(&listener, 0, 0);

    // Flush and make sure index file exists.
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    listener.wait_finish(1).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_listener_counts(&listener, 1, 1);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);

    // Check index build task for consistent file will be skipped.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    // Reopen the region to ensure the task wasn't skipped due to insufficient time.
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // Because the file is consistent, no new index build task is triggered.
    assert_listener_counts(&listener, 1, 1);
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);

    let mut altered_metadata = create_request.column_metadatas.clone();
    // Set index for field_0.
    altered_metadata[1].column_schema.set_inverted_index(true);
    let sync_columns_request = RegionAlterRequest {
        kind: AlterKind::SyncColumns {
            column_metadatas: altered_metadata,
        },
    };
    // Use SyncColumns to avoid triggering SchemaChange index build.
    engine
        .handle_request(region_id, RegionRequest::Alter(sync_columns_request))
        .await
        .unwrap();
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    // SyncColumns won't trigger index build.
    assert_listener_counts(&listener, 1, 1);

    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    listener.wait_finish(2).await; // previous 1 + new 1
    // Because the file is inconsistent, new index build task is triggered.
    assert_listener_counts(&listener, 2, 2);
}

#[tokio::test]
async fn test_gate_index_build_listener_smoke() {
    use store_api::storage::{FileId, RegionId};

    use crate::engine::listener::{EventListener, GateIndexBuildListener};
    use crate::sst::file::RegionFileId;

    let gate = Arc::new(GateIndexBuildListener::default());

    // Initial counts are zero.
    assert_eq!(gate.begin_count(), 0);
    assert_eq!(gate.finish_count(), 0);
    assert_eq!(gate.abort_count(), 0);

    // Spawn a task that will block in on_index_build_begin.
    let gate_clone = gate.clone();
    let handle = tokio::spawn(async move {
        gate_clone
            .on_index_build_begin(RegionFileId::new(RegionId::new(1, 1), FileId::random()))
            .await;
    });

    // Wait for begin to arrive.
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_begin(1))
        .await
        .unwrap();
    assert_eq!(gate.begin_count(), 1);
    assert_eq!(gate.finish_count(), 0);
    assert_eq!(gate.abort_count(), 0);

    // Release the blocked begin.
    gate.release_begin();

    // The spawned task should now complete.
    tokio::time::timeout(std::time::Duration::from_secs(5), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn test_index_build_type_manual_duplicate_in_flight() {
    let mut env = TestEnv::with_prefix("test_index_build_type_manual_duplicate_in_flight_").await;
    let gate = Arc::new(GateIndexBuildListener::default());
    let engine = Arc::new(
        env.create_engine_with(
            async_build_mode_config(false), // Disable index file creation on flush.
            None,
            Some(gate.clone()),
            None,
        )
        .await,
    );

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    // Create a region with index metadata.
    let request = CreateRequestBuilder::new().build_with_index();
    let table_dir = request.table_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush: the flush-triggered index build begins but is blocked by the gate.
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;

    // Wait for the flush-triggered build to begin (blocked by gate).
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_begin(1))
        .await
        .unwrap();
    assert_eq!(gate.begin_count(), 1);
    assert_eq!(gate.finish_count(), 0);
    assert_eq!(gate.abort_count(), 0);

    // Release the gate so the flush-triggered build can complete.
    // With create_on_flush=false the build produces no index file (file_size=0),
    // so on_index_build_finish is NOT called.
    gate.release_begin();

    // Reset scheduler state via reopen_region. This ensures the flush-triggered
    // build's entry is removed from building_files and the scheduler is clean.
    reopen_region(&engine, region_id, table_dir.clone(), true, HashMap::new()).await;

    // Verify no index file exists after flush (create_on_flush=false).
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 0);

    // Spawn the first manual BuildIndex in background. It will schedule the task,
    // the file enters building_files, and the begin is blocked by the gate.
    // handle_request blocks until the background collector sends the response,
    // so we must spawn it in a separate task.
    let engine_clone = engine.clone();
    let first_handle = tokio::spawn(async move {
        let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
        engine_clone.handle_request(region_id, request).await
    });

    // Wait for the first manual build to begin (blocked by gate).
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_begin(2))
        .await
        .unwrap(); // begin 1 = flush, begin 2 = first manual
    assert_eq!(gate.begin_count(), 2);
    assert_eq!(gate.finish_count(), 0);
    assert_eq!(gate.abort_count(), 0);

    // Issue the second manual BuildIndex for the same region/file.
    // Since the file is already in building_files (from the first manual build),
    // schedule_build detects the duplicate and calls on_index_build_abort.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();

    // The second request should have been aborted as duplicate.
    assert_eq!(gate.abort_count(), 1, "duplicate request should be aborted");
    assert_eq!(gate.begin_count(), 2, "no new begin for duplicate");
    assert_eq!(gate.finish_count(), 0, "first build hasn't finished yet");

    // Release the gate to let the first manual build proceed.
    gate.release_begin();
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_finish(1))
        .await
        .unwrap(); // first manual build completes

    // Final counts: only one successful build, one aborted duplicate.
    assert_eq!(gate.begin_count(), 2); // flush + first manual
    assert_eq!(gate.finish_count(), 1); // first manual only
    assert_eq!(gate.abort_count(), 1); // second manual duplicate abort

    // Await the first build's handle_request to ensure it completed cleanly.
    tokio::time::timeout(std::time::Duration::from_secs(5), first_handle)
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    // Verify exactly one SST and one index file.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}

/// Tests the race between an in-flight index build and a compaction that
/// removes the source SST.  The test blocks all flush-triggered index builds
/// via [`GateIndexBuildListener`] so that at least one old SST build is
/// still at `on_index_build_begin` when compaction completes.  After the
/// gate is released, the stale builds find the SSTs gone and abort, while the
/// compaction-triggered build for the new SST succeeds.
///
/// Deterministic orchestration (no sleeps):
/// 1. Flush 3 files — gate blocks all 3 index builds.
/// 2. Flush the 4th file (TWCS trigger_file_num=4) — compaction starts.
/// 3. Wait for 5 begins (4 flush + 1 compaction) — at this point compaction
///    is finished and the old SSTs are removed from the version.
/// 4. Release all 5 blocked begins.
/// 5. Wait for all 5 to stop.
/// 6. Assert: at least one abort, final state = 1 SST + 1 index file.
#[tokio::test]
async fn test_index_build_type_compact_abort_race() {
    common_telemetry::init_default_ut_logging();

    // We must raise max_background_index_builds because the gate blocks all
    // flush-triggered builds at `on_index_build_begin`, causing them to be
    // in "building_files" indefinitely.  The default limit (cpu/8, often ~2-4)
    // would prevent the compaction-triggered build from being scheduled.
    // Setting a generous limit ensures all 5 builds can be scheduled.
    let mut config = async_build_mode_config(true);
    config.max_background_index_builds = 8;

    let mut env = TestEnv::with_prefix("test_index_build_type_compact_abort_race_").await;
    let gate = Arc::new(GateIndexBuildListener::default());
    let engine = env
        .create_engine_with(config, None, Some(gate.clone()), None)
        .await;

    let region_id = RegionId::new(1, 1);
    env.get_schema_metadata_manager()
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
            env.get_kv_backend(),
        )
        .await;

    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "4")
        .build_with_index();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush 3 files — all 3 index builds blocked at begin by the gate.
    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 35..45).await;

    common_telemetry::info!("After flush 3 files, waiting for begins");

    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_begin(3))
        .await
        .unwrap();
    assert_eq!(gate.begin_count(), 3);
    assert_eq!(gate.finish_count(), 0);
    assert_eq!(gate.abort_count(), 0);

    // Flush 4th file — triggers compaction on the TWCS picker.
    put_and_flush(&engine, region_id, &column_schemas, 45..50).await;

    common_telemetry::info!("After flush 4th file, waiting for compaction begin");

    // Wait for 5 begins: 4 flush-triggered + 1 compaction-triggered.
    // The 5th begin indicates compaction has finished and the compacted SST's
    // index build is now blocked at begin. All old SST files have been
    // removed from the version at this point.
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_begin(5))
        .await
        .unwrap();

    common_telemetry::info!("All 5 builds blocked, releasing gates");

    // Release all blocked begins — the old SST builds will see their SSTs
    // are gone and abort; the compaction SST build will succeed.
    for _ in 0..5 {
        gate.release_begin();
    }

    // Wait for all builds to complete (finish or abort).
    tokio::time::timeout(std::time::Duration::from_secs(5), gate.wait_stop(5))
        .await
        .unwrap();

    common_telemetry::info!("All builds stopped, checking results");

    // Verify the compaction race caused the old SST index builds to abort. In
    // this blocked-then-compact scenario, all 4 flush-triggered builds abort
    // (their SST files were removed by compaction) and only the compacted SST
    // build finishes.
    assert_eq!(gate.begin_count(), 5);
    assert_eq!(gate.finish_count(), 1);
    assert_eq!(gate.abort_count(), 4);

    // Final state: all files compacted into 1 SST with 1 index file.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 1);
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 1);
}
