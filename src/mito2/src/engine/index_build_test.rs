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
use std::time::Duration;

use api::v1::Rows;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{AlterKind, RegionAlterRequest, RegionBuildIndexRequest, RegionRequest, SetIndexOption};
use store_api::storage::{RegionId, ScanRequest};
use tokio::time::sleep;

use crate::config::{IndexBuildMode, MitoConfig, Mode};
use crate::engine::MitoEngine;
use crate::engine::compaction_test::{compact, put_and_flush};
use crate::engine::listener::IndexBuildListener;
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
    for region_file_id in scanner.file_ids() {
        let index_path = location::index_file_path(
            access_layer.table_dir(),
            region_file_id,
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
        .insert_option("compaction.twcs.trigger_file_num", "100") // Make sure compaction is not triggered by file num.
        .build_with_index();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put_and_flush(&engine, region_id, &column_schemas, 10..20).await;
    put_and_flush(&engine, region_id, &column_schemas, 20..30).await;
    put_and_flush(&engine, region_id, &column_schemas, 15..25).await;
    put_and_flush(&engine, region_id, &column_schemas, 40..50).await;

    // all index build tasks begin means flush tasks are all finished.
    listener.wait_begin(4).await;
    // Before compaction is triggered, files should be 4, and not all index files are built.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 4);
    assert!(num_of_index_files(&engine, &scanner, region_id).await < 4);

    // Note: Compaction have been implicitly triggered by the flush operations above.
    // This explicit compaction call serves to make the process deterministic for the test.
    compact(&engine, region_id).await;

    listener.wait_begin(5).await; // 4 flush + 1 compaction begin
    // Before compaction is triggered, files should be 2, and not all index files are built.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 2);
    // Compaction is an async task, so it may be finished at this moment.
    assert!(num_of_index_files(&engine, &scanner, region_id).await <= 2);

    // Wait a while to make sure index build tasks are finished.
    listener.wait_stop(5).await; // 4 flush + 1 compaction = some abort + some finish
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_eq!(scanner.num_files(), 2);
    // Index files should be built.
    assert_eq!(num_of_index_files(&engine, &scanner, region_id).await, 2);
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
    reopen_region(&engine, region_id, table_dir, true, HashMap::new()).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    // Index build task is triggered on flush, but not finished.
    assert_listener_counts(&listener, 1, 0);
    assert!(scanner.num_files() == 1);
    assert!(num_of_index_files(&engine, &scanner, region_id).await == 0);

    // Trigger manual index build task and make sure index file is built without flush or compaction.
    let request = RegionRequest::BuildIndex(RegionBuildIndexRequest {});
    engine.handle_request(region_id, request).await.unwrap();
    listener.wait_finish(1).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert_listener_counts(&listener, 2, 1);
    assert!(scanner.num_files() == 1);
    assert!(num_of_index_files(&engine, &scanner, region_id).await == 1);
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
    assert!(scanner.num_files() == 1);
    assert!(num_of_index_files(&engine, &scanner, region_id).await == 1);

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
    assert!(scanner.num_files() == 1);
    assert!(num_of_index_files(&engine, &scanner, region_id).await == 1);

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