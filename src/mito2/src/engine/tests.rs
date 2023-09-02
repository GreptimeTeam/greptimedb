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

//! Tests for mito engine.

use std::collections::HashMap;

use api::helper::ColumnDataTypeWrapper;
use api::v1::value::ValueData;
use api::v1::{ColumnSchema, Row, Rows, SemanticType};
use common_recordbatch::RecordBatches;
use store_api::metadata::ColumnMetadata;
use store_api::region_request::{
    RegionCreateRequest, RegionDeleteRequest, RegionFlushRequest, RegionOpenRequest,
    RegionPutRequest,
};
use store_api::storage::RegionId;

use super::*;
use crate::error::Error;
use crate::region::version::VersionControlData;
use crate::test_util::{CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_engine_new_stop() {
    let mut env = TestEnv::with_prefix("engine-stop");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Stop the engine to reject further requests.
    engine.stop().await.unwrap();
    assert!(!engine.is_region_exists(region_id));

    let request = CreateRequestBuilder::new().build();
    let err = engine
        .handle_request(RegionId::new(1, 2), RegionRequest::Create(request))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::WorkerStopped { .. }),
        "unexpected err: {err}"
    );
}

fn column_metadata_to_column_schema(metadata: &ColumnMetadata) -> api::v1::ColumnSchema {
    api::v1::ColumnSchema {
        column_name: metadata.column_schema.name.clone(),
        datatype: ColumnDataTypeWrapper::try_from(metadata.column_schema.data_type.clone())
            .unwrap()
            .datatype() as i32,
        semantic_type: metadata.semantic_type as i32,
    }
}

fn build_rows(start: usize, end: usize) -> Vec<Row> {
    (start..end)
        .map(|i| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(i.to_string())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value(i as f64)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TsMillisecondValue(i as i64 * 1000)),
                },
            ],
        })
        .collect()
}

fn rows_schema(request: &RegionCreateRequest) -> Vec<ColumnSchema> {
    request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>()
}

async fn put_rows(engine: &MitoEngine, region_id: RegionId, rows: Rows) {
    let num_rows = rows.rows.len();
    let output = engine
        .handle_request(region_id, RegionRequest::Put(RegionPutRequest { rows }))
        .await
        .unwrap();
    let Output::AffectedRows(rows_inserted) = output else {
        unreachable!()
    };
    assert_eq!(num_rows, rows_inserted);
}

#[tokio::test]
async fn test_write_to_region() {
    let mut env = TestEnv::with_prefix("write-to-region");
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
        rows: build_rows(0, 42),
    };
    put_rows(&engine, region_id, rows).await;
}

#[tokio::test]
async fn test_region_replay() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("region-replay");
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
        rows: build_rows(0, 20),
    };
    put_rows(&engine, region_id, rows).await;

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(20, 42),
    };
    put_rows(&engine, region_id, rows).await;

    engine.stop().await.unwrap();

    let engine = MitoEngine::new(
        MitoConfig::default(),
        env.get_logstore().unwrap(),
        env.get_object_store().unwrap(),
    );

    let open_region = engine
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
    let Output::AffectedRows(rows) = open_region else {
        unreachable!()
    };
    assert_eq!(0, rows);

    let request = ScanRequest::default();
    let scanner = engine.handle_query(region_id, request).unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(42, batches.iter().map(|b| b.num_rows()).sum::<usize>());

    let region = engine.get_region(region_id).unwrap();
    let VersionControlData {
        committed_sequence,
        last_entry_id,
        ..
    } = region.version_control.current();

    assert_eq!(42, committed_sequence);
    assert_eq!(2, last_entry_id);

    engine.stop().await.unwrap();
}

// TODO(yingwen): build_rows() only generate one point for each series. We need to add tests
// for series with multiple points and other cases.
#[tokio::test]
async fn test_write_query_region() {
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

    let request = ScanRequest::default();
    let scanner = engine.handle_query(region_id, request).unwrap();
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

/// Build rows to put for specific `key`.
fn build_rows_for_key(key: &str, start: usize, end: usize, value_start: usize) -> Vec<Row> {
    (start..end)
        .enumerate()
        .map(|(idx, ts)| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(key.to_string())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value((value_start + idx) as f64)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TsMillisecondValue(ts as i64 * 1000)),
                },
            ],
        })
        .collect()
}

/// Build rows to delete for specific `key`.
fn build_delete_rows_for_key(key: &str, start: usize, end: usize) -> Vec<Row> {
    (start..end)
        .map(|ts| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(key.to_string())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TsMillisecondValue(ts as i64 * 1000)),
                },
            ],
        })
        .collect()
}

fn delete_rows_schema(request: &RegionCreateRequest) -> Vec<ColumnSchema> {
    request
        .column_metadatas
        .iter()
        .filter(|col| col.semantic_type != SemanticType::Field)
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>()
}

async fn delete_rows(engine: &MitoEngine, region_id: RegionId, rows: Rows) {
    let num_rows = rows.rows.len();
    let output = engine
        .handle_request(
            region_id,
            RegionRequest::Delete(RegionDeleteRequest { rows }),
        )
        .await
        .unwrap();
    let Output::AffectedRows(rows_inserted) = output else {
        unreachable!()
    };
    assert_eq!(num_rows, rows_inserted);
}

#[tokio::test]
async fn test_put_delete() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    let delete_schema = delete_rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 3, 0),
    };
    put_rows(&engine, region_id, rows).await;
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 3, 0),
    };
    put_rows(&engine, region_id, rows).await;
    // Delete (a, 2)
    let rows = Rows {
        schema: delete_schema.clone(),
        rows: build_delete_rows_for_key("a", 2, 3),
    };
    delete_rows(&engine, region_id, rows).await;
    // Delete (b, 0), (b, 1)
    let rows = Rows {
        schema: delete_schema,
        rows: build_delete_rows_for_key("b", 0, 2),
    };
    delete_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let scanner = engine.handle_query(region_id, request).unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| b     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_put_overwrite() {
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
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 3, 0),
    };
    put_rows(&engine, region_id, rows).await;
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 3, 0),
    };
    put_rows(&engine, region_id, rows).await;
    // Put (a, 0) => 5.0, (a, 1) => 6.0
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 5),
    };
    put_rows(&engine, region_id, rows).await;
    // Put (b, 0) => 3.0
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 1, 3),
    };
    put_rows(&engine, region_id, rows).await;
    // Put (b, 2) => 4.0
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_for_key("b", 2, 3, 4),
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let scanner = engine.handle_query(region_id, request).unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 5.0     | 1970-01-01T00:00:00 |
| a     | 6.0     | 1970-01-01T00:00:01 |
| a     | 2.0     | 1970-01-01T00:00:02 |
| b     | 3.0     | 1970-01-01T00:00:00 |
| b     | 1.0     | 1970-01-01T00:00:01 |
| b     | 4.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_manual_flush() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = request
        .column_metadatas
        .iter()
        .map(column_metadata_to_column_schema)
        .collect::<Vec<_>>();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    let Output::AffectedRows(rows) = engine
        .handle_request(region_id, RegionRequest::Flush(RegionFlushRequest {}))
        .await
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(0, rows);

    let request = ScanRequest::default();
    let scanner = engine.handle_query(region_id, request).unwrap();
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
