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

//! Flush tests for mito engine.

use std::sync::Arc;

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::engine::listener::{FlushListener, StallListener};
use crate::test_util::{
    build_rows, build_rows_for_key, flush_region, put_rows, reopen_region, rows_schema,
    CreateRequestBuilder, MockWriteBufferManager, TestEnv,
};

#[tokio::test]
async fn test_manual_flush() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_flush_engine() {
    let mut env = TestEnv::new();
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let listener = Arc::new(FlushListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig::default(),
            Some(write_buffer_manager.clone()),
            Some(listener.clone()),
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Prepares rows for flush.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    write_buffer_manager.set_should_flush(true);

    // Writes to the mutable memtable and triggers flush.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    // Wait until flush is finished.
    listener.wait().await;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).unwrap();
    assert_eq!(1, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| b     | 0.0     | 1970-01-01T00:00:00 |
| b     | 1.0     | 1970-01-01T00:00:01 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_write_stall() {
    let mut env = TestEnv::new();
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let listener = Arc::new(StallListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig::default(),
            Some(write_buffer_manager.clone()),
            Some(listener.clone()),
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Prepares rows for flush.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    // Stalls the engine.
    write_buffer_manager.set_should_stall(true);

    let engine_cloned = engine.clone();
    // Spawns a task to flush the engine on stall.
    tokio::spawn(async move {
        listener.wait().await;

        flush_region(&engine_cloned, region_id, None).await;
    });

    // Triggers write stall.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).unwrap();
    assert_eq!(1, scanner.num_memtables());
    assert_eq!(1, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| b     | 0.0     | 1970-01-01T00:00:00 |
| b     | 1.0     | 1970-01-01T00:00:01 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_flush_empty() {
    let mut env = TestEnv::new();
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let engine = env
        .create_engine_with(
            MitoConfig::default(),
            Some(write_buffer_manager.clone()),
            None,
        )
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    flush_region(&engine, region_id, None).await;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).unwrap();
    assert_eq!(0, scanner.num_memtables());
    assert_eq!(0, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
++
++";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_flush_reopen_region() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 3, 0),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;
    let check_region = || {
        let region = engine.get_region(region_id).unwrap();
        let version_data = region.version_control.current();
        assert_eq!(1, version_data.last_entry_id);
        assert_eq!(3, version_data.committed_sequence);
        assert_eq!(1, version_data.version.flushed_entry_id);
        assert_eq!(3, version_data.version.flushed_sequence);
    };
    check_region();

    reopen_region(&engine, region_id, region_dir, true).await;
    check_region();

    // Puts again.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 10),
    };
    put_rows(&engine, region_id, rows).await;
    let region = engine.get_region(region_id).unwrap();
    let version_data = region.version_control.current();
    assert_eq!(2, version_data.last_entry_id);
    assert_eq!(5, version_data.committed_sequence);
}
