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

//! Basic tests for mito engine.

use std::collections::HashMap;

use api::v1::value::ValueData;
use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use datatypes::prelude::ConcreteDataType;
use store_api::region_request::RegionOpenRequest;
use store_api::storage::RegionId;

use super::*;
use crate::region::version::VersionControlData;
use crate::test_util::{
    build_delete_rows_for_key, build_rows, build_rows_for_key, delete_rows, delete_rows_schema,
    put_rows, rows_schema, CreateRequestBuilder, TestEnv,
};

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
        matches!(err.status_code(), StatusCode::Internal),
        "unexpected err: {err}"
    );
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

    let engine = env.reopen_engine(engine, MitoConfig::default()).await;

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
    let stream = engine.handle_query(region_id, request).await.unwrap();
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
}

#[tokio::test]
async fn test_write_different_order() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().tag_num(2).field_num(2).build();

    // tag_0, tag_1, field_0, field_1, ts,
    let mut column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Swap position of columns.
    column_schemas.swap(0, 3);
    column_schemas.swap(2, 4);

    // Now the schema is field_1, tag_1, ts, tag_0, field_0
    let rows = (0..3)
        .map(|i| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::F64Value((i + 10) as f64)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(format!("b{i}"))),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(i as i64 * 1000)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(format!("a{i}"))),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value(i as f64)),
                },
            ],
        })
        .collect();
    let rows = Rows {
        schema: column_schemas,
        rows,
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+-------+---------+---------+---------------------+
| tag_0 | tag_1 | field_0 | field_1 | ts                  |
+-------+-------+---------+---------+---------------------+
| a0    | b0    | 0.0     | 10.0    | 1970-01-01T00:00:00 |
| a1    | b1    | 1.0     | 11.0    | 1970-01-01T00:00:01 |
| a2    | b2    | 2.0     | 12.0    | 1970-01-01T00:00:02 |
+-------+-------+---------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_write_different_order2() {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    // tag_0, tag_1, field_0, field_1, ts,
    let mut request = CreateRequestBuilder::new().tag_num(2).field_num(2).build();
    // Change the field type of field_1.
    request.column_metadatas[3].column_schema.data_type = ConcreteDataType::string_datatype();

    let mut column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Swap position of columns.
    column_schemas.swap(2, 3);

    // Now the schema is tag_0, tag_1, field_1, field_0, ts
    let rows = (0..3)
        .map(|i| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(format!("a{i}"))),
                },
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(format!("b{i}"))),
                },
                api::v1::Value {
                    value_data: Some(ValueData::StringValue((i + 10).to_string())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::F64Value(i as f64)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(i as i64 * 1000)),
                },
            ],
        })
        .collect();
    let rows = Rows {
        schema: column_schemas,
        rows,
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let stream = engine.handle_query(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+-------+---------+---------+---------------------+
| tag_0 | tag_1 | field_0 | field_1 | ts                  |
+-------+-------+---------+---------+---------------------+
| a0    | b0    | 0.0     | 10      | 1970-01-01T00:00:00 |
| a1    | b1    | 1.0     | 11      | 1970-01-01T00:00:01 |
| a2    | b2    | 2.0     | 12      | 1970-01-01T00:00:02 |
+-------+-------+---------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
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
    let stream = engine.handle_query(region_id, request).await.unwrap();
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
    let stream = engine.handle_query(region_id, request).await.unwrap();
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
