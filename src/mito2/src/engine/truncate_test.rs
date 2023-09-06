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

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use common_telemetry::init_default_ut_logging;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    RegionCloseRequest, RegionFlushRequest, RegionOpenRequest, RegionRequest, RegionTruncateRequest,
};
use store_api::storage::RegionId;

use super::ScanRequest;
use crate::config::MitoConfig;
use crate::test_util::{build_rows, put_rows, rows_schema, CreateRequestBuilder, TestEnv};

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
    init_default_ut_logging();
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
        .handle_request(region_id, RegionRequest::Flush(RegionFlushRequest {}))
        .await
        .unwrap();

    let request = ScanRequest::default();
    let scanner = engine.scan(region_id, request.clone()).unwrap();
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
    let stream = engine
        .handle_query(region_id, request.clone())
        .await
        .unwrap();
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

    tokio::time::sleep(Duration::from_millis(100)).await;

    let scanner = engine.scan(region_id, request).unwrap();
    assert_eq!(0, scanner.num_files());
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

    // Flush the region.
    engine
        .handle_request(region_id, RegionRequest::Flush(RegionFlushRequest {}))
        .await
        .unwrap();

    // Truncate the region
    engine
        .handle_request(region_id, RegionRequest::Truncate(RegionTruncateRequest {}))
        .await
        .unwrap();

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Reopen the region again.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
            }),
        )
        .await
        .unwrap();

    // Scan the region.
    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "++\n++";
    assert_eq!(expected, batches.pretty_print().unwrap());
}
