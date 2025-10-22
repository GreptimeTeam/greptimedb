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
use store_api::region_request::{AlterKind, RegionAlterRequest, RegionRequest, SetIndexOption};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::{IndexBuildMode, MitoConfig, Mode};
use crate::engine::MitoEngine;
use crate::engine::compaction_test::{compact, put_and_flush};
use crate::engine::listener::IndexBuildListener;
use crate::read::scan_region::Scanner;
use crate::sst::location;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, flush_region, put_rows, reopen_region, rows_schema,
};

// wait listener receives enough success count.
async fn wait_finish(listener: &IndexBuildListener, times: usize) {
    listener.wait_finish(times).await;
}

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

#[allow(dead_code)]
fn assert_listener_counts(
    listener: &IndexBuildListener,
    expected_begin_count: usize,

    expected_success_count: usize,
) {
    assert_eq!(listener.begin_count(), expected_begin_count);
    assert_eq!(listener.success_count(), expected_success_count);
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
    assert!(scanner.num_memtables() == 1);
    assert!(scanner.num_files() == 0);
    assert!(num_of_index_files(&engine, &scanner, region_id).await == 0);

    flush_region(&engine, region_id, None).await;

    // When first flush is just finished, index file should not exist.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert!(scanner.num_memtables() == 0);
    assert!(scanner.num_files() == 1);
    assert!(num_of_index_files(&engine, &scanner, region_id).await == 0);

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(2, 4),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    // After 2 index build task are finished, 2 index files should exist.
    wait_finish(&listener, 2).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert!(num_of_index_files(&engine, &scanner, region_id).await == 2);
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

    // Before compaction is triggered, files should be 4, and not all index files are built.
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert!(scanner.num_files() == 4);
    assert!(num_of_index_files(&engine, &scanner, region_id).await < 4);

    // Note: Compaction have been implicitly triggered by the flush operations above.
    // This explicit compaction call serves to make the process deterministic for the test.
    compact(&engine, region_id).await;

    // Before compaction is triggered, files should be 2, and not all index files are built.
    listener.clear_success_count();
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert!(scanner.num_files() == 2);
    assert!(num_of_index_files(&engine, &scanner, region_id).await < 2);

    // Wait a while to make sure index build tasks are finished.
    wait_finish(&listener, 2).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert!(scanner.num_files() == 2);
    assert!(num_of_index_files(&engine, &scanner, region_id).await == 2);
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
    assert!(scanner.num_files() == 1);
    assert!(num_of_index_files(&engine, &scanner, region_id).await == 0);

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
    wait_finish(&listener, 1).await;
    let scanner = engine
        .scanner(region_id, ScanRequest::default())
        .await
        .unwrap();
    assert!(scanner.num_files() == 1);
    assert!(num_of_index_files(&engine, &scanner, region_id).await == 1);
}
