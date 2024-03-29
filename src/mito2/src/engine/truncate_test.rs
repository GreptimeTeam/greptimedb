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

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use common_telemetry::{info, init_default_ut_logging};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    RegionFlushRequest, RegionOpenRequest, RegionRequest, RegionTruncateRequest,
};
use store_api::storage::RegionId;

use super::ScanRequest;
use crate::config::MitoConfig;
use crate::engine::listener::FlushTruncateListener;
use crate::test_util::{
    build_rows, put_rows, rows_schema, CreateRequestBuilder, MockWriteBufferManager, TestEnv,
};

#[tokio::test]
async fn test_engine_truncate_region_basic() {
    let mut env = TestEnv::with_prefix("truncate-basic");
    let engine = env.create_engine(MitoConfig::default()).await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
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

    // Truncate the region.
    engine
        .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
        .await
        .unwrap();

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "++\n++";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_engine_put_data_after_truncate() {
    let mut env = TestEnv::with_prefix("truncate-put");
    let engine = env.create_engine(MitoConfig::default()).await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
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

    // Truncate the region.
    engine
        .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
        .await
        .unwrap();

    // Put data to the region again.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(5, 8),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_engine_truncate_after_flush() {
    let mut env = TestEnv::with_prefix("truncate-flush");
    let engine = env.create_engine(MitoConfig::default()).await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    // Flush the region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request.clone()).unwrap();
    assert_eq!(1, scanner.num_files());

    // Truncate the region.
    engine
        .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(5, 8),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan the region.
    let scanner = engine.scanner(region_id, request).unwrap();
    assert_eq!(0, scanner.num_files());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_engine_truncate_reopen() {
    let mut env = TestEnv::with_prefix("truncate-reopen");
    let engine = env.create_engine(MitoConfig::default()).await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let region = engine.get_region(region_id).unwrap();
    let last_entry_id = region.version_control.current().last_entry_id;

    // Truncate the region
    engine
        .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
        .await
        .unwrap();

    // Reopen the region again.
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(last_entry_id, region.version().flushed_entry_id);

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "++\n++";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_engine_truncate_during_flush() {
    init_default_ut_logging();
    let mut env = TestEnv::with_prefix("truncate-during-flush");
    let write_buffer_manager = Arc::new(MockWriteBufferManager::default());
    let listener = Arc::new(FlushTruncateListener::default());
    let engine = env
        .create_engine_with(
            MitoConfig::default(),
            Some(write_buffer_manager),
            Some(listener.clone()),
        )
        .await;

    // Create the region.
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put data to the region.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let region = engine.get_region(region_id).unwrap();

    let version_data = region.version_control.current();
    let entry_id = version_data.last_entry_id;
    let sequence = version_data.committed_sequence;

    // Flush region.
    let engine_cloned = engine.clone();
    let flush_task = tokio::spawn(async move {
        info!("do flush task!!!!");
        engine_cloned
            .handle_request(
                region_id,
                RegionRequest::Flush(RegionFlushRequest {
                    row_group_size: None,
                }),
            )
            .await
    });

    // Wait truncate before flush memtable.
    listener.wait_truncate().await;

    // Truncate the region.
    engine
        .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
        .await
        .unwrap();

    // Notify region to continue flushing.
    listener.notify_flush();

    // Wait handle flushed finish.
    let _err = flush_task.await.unwrap().unwrap_err();

    // Check sequences and entry id.
    let version_data = region.version_control.current();
    let truncated_entry_id = version_data.version.truncated_entry_id;
    let truncated_sequence = version_data.version.flushed_sequence;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request.clone()).unwrap();
    assert_eq!(0, scanner.num_files());
    assert_eq!(Some(entry_id), truncated_entry_id);
    assert_eq!(sequence, truncated_sequence);

    // Reopen the engine.
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let current_version = region.version_control.current().version;
    assert_eq!(current_version.truncated_entry_id, Some(entry_id));
}
