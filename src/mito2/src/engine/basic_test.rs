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
use std::sync::atomic::Ordering;

use api::v1::helper::row;
use api::v1::value::ValueData;
use api::v1::{Rows, SemanticType};
use common_base::readable_size::ReadableSize;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use common_wal::options::WAL_OPTIONS_KEY;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use futures::TryStreamExt;
use itertools::Itertools;
use rstest::rstest;
use rstest_reuse::{self, apply};
use store_api::metadata::ColumnMetadata;
use store_api::region_request::{
    PathType, RegionCreateRequest, RegionFlushRequest, RegionOpenRequest, RegionPutRequest,
};
use store_api::storage::RegionId;

use super::*;
use crate::region::version::VersionControlData;
use crate::test_util::{
    CreateRequestBuilder, LogStoreFactory, TestEnv, build_delete_rows_for_key, build_rows,
    build_rows_for_key, delete_rows, delete_rows_schema, flush_region, kafka_log_store_factory,
    multiple_log_store_factories, prepare_test_for_kafka_log_store, put_rows,
    raft_engine_log_store_factory, reopen_region, rows_schema,
};

#[tokio::test]
async fn test_engine_new_stop() {
    test_engine_new_stop_with_format(false).await;
    test_engine_new_stop_with_format(true).await;
}

async fn test_engine_new_stop_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("engine-stop").await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
    test_write_to_region_with_format(false).await;
    test_write_to_region_with_format(true).await;
}

async fn test_write_to_region_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("write-to-region").await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
    let region = engine.get_region(region_id).unwrap();
    assert!(region.written_bytes.load(Ordering::Relaxed) > 0);
}

#[apply(multiple_log_store_factories)]
async fn test_region_replay(factory: Option<LogStoreFactory>) {
    test_region_replay_with_format(factory.clone(), false).await;
    test_region_replay_with_format(factory, true).await;
}

async fn test_region_replay_with_format(factory: Option<LogStoreFactory>, flat_format: bool) {
    use common_wal::options::{KafkaWalOptions, WalOptions};

    common_telemetry::init_default_ut_logging();
    let Some(factory) = factory else {
        return;
    };
    let mut env = TestEnv::with_prefix("region-replay")
        .await
        .with_log_store_factory(factory.clone());
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let topic = prepare_test_for_kafka_log_store(&factory).await;
    let request = CreateRequestBuilder::new()
        .kafka_topic(topic.clone())
        .build();

    let table_dir = request.table_dir.clone();

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

    let engine = env
        .reopen_engine(
            engine,
            MitoConfig {
                default_experimental_flat_format: flat_format,
                ..Default::default()
            },
        )
        .await;

    let mut options = HashMap::new();
    if let Some(topic) = &topic {
        options.insert(
            WAL_OPTIONS_KEY.to_string(),
            serde_json::to_string(&WalOptions::Kafka(KafkaWalOptions {
                topic: topic.clone(),
            }))
            .unwrap(),
        );
    };

    let result = engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                table_dir,
                path_type: store_api::region_request::PathType::Bare,
                options,
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();
    assert_eq!(0, result.affected_rows);

    // The replay won't update the write bytes rate meter.
    let region = engine.get_region(region_id).unwrap();
    assert_eq!(region.written_bytes.load(Ordering::Relaxed), 0);

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
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
    test_write_query_region_with_format(false).await;
    test_write_query_region_with_format(true).await;
}

async fn test_write_query_region_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
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
async fn test_different_order() {
    test_different_order_with_format(false).await;
    test_different_order_with_format(true).await;
}

async fn test_different_order_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
        .map(|i| {
            row(vec![
                ValueData::F64Value((i + 10) as f64),
                ValueData::StringValue(format!("b{i}")),
                ValueData::TimestampMillisecondValue(i as i64 * 1000),
                ValueData::StringValue(format!("a{i}")),
                ValueData::F64Value(i as f64),
            ])
        })
        .collect();
    let rows = Rows {
        schema: column_schemas,
        rows,
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
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
async fn test_different_order_and_type() {
    test_different_order_and_type_with_format(false).await;
    test_different_order_and_type_with_format(true).await;
}

async fn test_different_order_and_type_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
        .map(|i| {
            row(vec![
                ValueData::StringValue(format!("a{i}")),
                ValueData::StringValue(format!("b{i}")),
                ValueData::StringValue((i + 10).to_string()),
                ValueData::F64Value(i as f64),
                ValueData::TimestampMillisecondValue(i as i64 * 1000),
            ])
        })
        .collect();
    let rows = Rows {
        schema: column_schemas,
        rows,
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
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
    test_put_delete_with_format(false).await;
    test_put_delete_with_format(true).await;
}

async fn test_put_delete_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
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
async fn test_delete_not_null_fields() {
    test_delete_not_null_fields_with_format(false).await;
    test_delete_not_null_fields_with_format(true).await;
}

async fn test_delete_not_null_fields_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().all_not_null(true).build();
    let table_dir = request.table_dir.clone();

    let column_schemas = rows_schema(&request);
    let delete_schema = delete_rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 4, 0),
    };
    put_rows(&engine, region_id, rows).await;
    // Delete (a, 2)
    let rows = Rows {
        schema: delete_schema.clone(),
        rows: build_delete_rows_for_key("a", 2, 3),
    };
    delete_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| a     | 3.0     | 1970-01-01T00:00:03 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Reopen and scan again.
    reopen_region(&engine, region_id, table_dir, false, HashMap::new()).await;
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_put_overwrite() {
    test_put_overwrite_with_format(false).await;
    test_put_overwrite_with_format(true).await;
}

async fn test_put_overwrite_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

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
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
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
async fn test_absent_and_invalid_columns() {
    test_absent_and_invalid_columns_with_format(false).await;
    test_absent_and_invalid_columns_with_format(true).await;
}

async fn test_absent_and_invalid_columns_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    // tag_0, field_0, field_1, ts,
    let request = CreateRequestBuilder::new().field_num(2).build();

    let mut column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Change the type of field_1 in input.
    column_schemas[2].datatype = api::v1::ColumnDataType::String as i32;
    // Input tag_0, field_1 (invalid type string), ts
    column_schemas.remove(1);
    let rows = (0..3)
        .map(|i| {
            row(vec![
                ValueData::StringValue(format!("a{i}")),
                ValueData::StringValue(i.to_string()),
                ValueData::TimestampMillisecondValue(i as i64 * 1000),
            ])
        })
        .collect();
    let rows = Rows {
        schema: column_schemas,
        rows,
    };
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest { rows, hint: None }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::InvalidArguments, err.status_code());
}

#[tokio::test]
async fn test_region_usage() {
    test_region_usage_with_format(false).await;
    test_region_usage_with_format(true).await;
}

async fn test_region_usage_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("region_usage").await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);
    let delete_schema = delete_rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    // region is empty now, check manifest size
    let region = engine.get_region(region_id).unwrap();
    let region_stat = region.region_statistic();
    assert!(region_stat.manifest_size > 0);

    // put some rows
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 10, 0),
    };

    put_rows(&engine, region_id, rows).await;

    let region_stat = region.region_statistic();
    assert!(region_stat.wal_size > 0);
    assert_eq!(region_stat.num_rows, 10);

    // delete some rows
    let rows = Rows {
        schema: delete_schema.clone(),
        rows: build_delete_rows_for_key("a", 0, 3),
    };
    delete_rows(&engine, region_id, rows).await;

    let region_stat = region.region_statistic();
    assert!(region_stat.wal_size > 0);
    assert_eq!(region_stat.num_rows, 13);

    // flush region
    flush_region(&engine, region_id, None).await;

    let region_stat = region.region_statistic();
    assert!(region_stat.sst_size > 0);
    assert_eq!(region_stat.num_rows, 10);

    // region total usage
    // Some memtables may share items.
    assert!(region_stat.estimated_disk_size() > 3000);
}

#[tokio::test]
async fn test_engine_with_write_cache() {
    test_engine_with_write_cache_with_format(false).await;
    test_engine_with_write_cache_with_format(true).await;
}

async fn test_engine_with_write_cache_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let path = env.data_home().to_str().unwrap().to_string();
    let mito_config = MitoConfig {
        default_experimental_flat_format: flat_format,
        ..Default::default()
    }
    .enable_write_cache(path, ReadableSize::mb(512), None);
    let engine = env.create_engine(mito_config).await;

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

    flush_region(&engine, region_id, None).await;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();

    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 0.0     | 1970-01-01T00:00:00 |
| a     | 1.0     | 1970-01-01T00:00:01 |
| a     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_cache_null_primary_key() {
    test_cache_null_primary_key_with_format(false).await;
    test_cache_null_primary_key_with_format(true).await;
}

async fn test_cache_null_primary_key_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            vector_cache_size: ReadableSize::mb(32),
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let column_metadatas = vec![
        ColumnMetadata {
            column_schema: ColumnSchema::new("tag_0", ConcreteDataType::string_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 1,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new("tag_1", ConcreteDataType::int64_datatype(), true),
            semantic_type: SemanticType::Tag,
            column_id: 2,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new("field_0", ConcreteDataType::float64_datatype(), true),
            semantic_type: SemanticType::Field,
            column_id: 3,
        },
        ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: 4,
        },
    ];
    let request = RegionCreateRequest {
        engine: MITO_ENGINE_NAME.to_string(),
        column_metadatas,
        primary_key: vec![1, 2],
        options: HashMap::new(),
        table_dir: "test".to_string(),
        path_type: PathType::Bare,
        partition_expr_json: Some("".to_string()),
    };

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: vec![
            api::v1::Row {
                values: vec![
                    api::v1::Value {
                        value_data: Some(ValueData::StringValue("1".to_string())),
                    },
                    api::v1::Value { value_data: None },
                    api::v1::Value {
                        value_data: Some(ValueData::F64Value(10.0)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(1000)),
                    },
                ],
            },
            api::v1::Row {
                values: vec![
                    api::v1::Value { value_data: None },
                    api::v1::Value {
                        value_data: Some(ValueData::I64Value(200)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::F64Value(20.0)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(2000)),
                    },
                ],
            },
        ],
    };
    put_rows(&engine, region_id, rows).await;

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+-------+---------+---------------------+
| tag_0 | tag_1 | field_0 | ts                  |
+-------+-------+---------+---------------------+
|       | 200   | 20.0    | 1970-01-01T00:00:02 |
| 1     |       | 10.0    | 1970-01-01T00:00:01 |
+-------+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_list_ssts() {
    test_list_ssts_with_format(false, r#"
ManifestSstEntry { table_dir: "test/", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test/11_0000000001/<file_id>.parquet", file_size: 2513, index_file_path: Some("test/11_0000000001/index/<file_id>.puffin"), index_file_size: Some(250), num_rows: 10, num_row_groups: 1, num_series: Some(1), min_ts: 0::Millisecond, max_ts: 9000::Millisecond, sequence: Some(10), origin_region_id: 47244640257(11, 1), node_id: None, visible: true }
ManifestSstEntry { table_dir: "test/", region_id: 47244640258(11, 2), table_id: 11, region_number: 2, region_group: 0, region_sequence: 2, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test/11_0000000002/<file_id>.parquet", file_size: 2513, index_file_path: Some("test/11_0000000002/index/<file_id>.puffin"), index_file_size: Some(250), num_rows: 10, num_row_groups: 1, num_series: Some(1), min_ts: 0::Millisecond, max_ts: 9000::Millisecond, sequence: Some(10), origin_region_id: 47244640258(11, 2), node_id: None, visible: true }
ManifestSstEntry { table_dir: "test/", region_id: 94489280554(22, 42), table_id: 22, region_number: 42, region_group: 0, region_sequence: 42, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test/22_0000000042/<file_id>.parquet", file_size: 2513, index_file_path: Some("test/22_0000000042/index/<file_id>.puffin"), index_file_size: Some(250), num_rows: 10, num_row_groups: 1, num_series: Some(1), min_ts: 0::Millisecond, max_ts: 9000::Millisecond, sequence: Some(10), origin_region_id: 94489280554(22, 42), node_id: None, visible: true }"# ,
r#"
StorageSstEntry { file_path: "test/11_0000000001/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test/11_0000000001/index/<file_id>.puffin", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test/11_0000000002/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test/11_0000000002/index/<file_id>.puffin", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test/22_0000000042/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test/22_0000000042/index/<file_id>.puffin", file_size: None, last_modified_ms: None, node_id: None }"#).await;
    test_list_ssts_with_format(true, r#"
ManifestSstEntry { table_dir: "test/", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test/11_0000000001/<file_id>.parquet", file_size: 2837, index_file_path: Some("test/11_0000000001/index/<file_id>.puffin"), index_file_size: Some(250), num_rows: 10, num_row_groups: 1, num_series: Some(1), min_ts: 0::Millisecond, max_ts: 9000::Millisecond, sequence: Some(10), origin_region_id: 47244640257(11, 1), node_id: None, visible: true }
ManifestSstEntry { table_dir: "test/", region_id: 47244640258(11, 2), table_id: 11, region_number: 2, region_group: 0, region_sequence: 2, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test/11_0000000002/<file_id>.parquet", file_size: 2837, index_file_path: Some("test/11_0000000002/index/<file_id>.puffin"), index_file_size: Some(250), num_rows: 10, num_row_groups: 1, num_series: Some(1), min_ts: 0::Millisecond, max_ts: 9000::Millisecond, sequence: Some(10), origin_region_id: 47244640258(11, 2), node_id: None, visible: true }
ManifestSstEntry { table_dir: "test/", region_id: 94489280554(22, 42), table_id: 22, region_number: 42, region_group: 0, region_sequence: 42, file_id: "<file_id>", index_version: 0, level: 0, file_path: "test/22_0000000042/<file_id>.parquet", file_size: 2837, index_file_path: Some("test/22_0000000042/index/<file_id>.puffin"), index_file_size: Some(250), num_rows: 10, num_row_groups: 1, num_series: Some(1), min_ts: 0::Millisecond, max_ts: 9000::Millisecond, sequence: Some(10), origin_region_id: 94489280554(22, 42), node_id: None, visible: true }"#, 
r#"
StorageSstEntry { file_path: "test/11_0000000001/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test/11_0000000001/index/<file_id>.puffin", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test/11_0000000002/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test/11_0000000002/index/<file_id>.puffin", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test/22_0000000042/<file_id>.parquet", file_size: None, last_modified_ms: None, node_id: None }
StorageSstEntry { file_path: "test/22_0000000042/index/<file_id>.puffin", file_size: None, last_modified_ms: None, node_id: None }"#).await;
}

async fn test_list_ssts_with_format(
    flat_format: bool,
    expected_manifest_ssts: &str,
    expected_storage_ssts: &str,
) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    // Create 3 regions and write some rows to each of them
    let region_ids = vec![
        RegionId::new(11, 1),
        RegionId::new(11, 2),
        RegionId::new(22, 42),
    ];

    for region_id in &region_ids {
        let mut request = CreateRequestBuilder::new().build();
        request.column_metadatas[0]
            .column_schema
            .set_inverted_index(true); // set inverted index for tag_0
        let column_schemas = rows_schema(&request);
        engine
            .handle_request(*region_id, RegionRequest::Create(request))
            .await
            .unwrap();

        // write some rows
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: build_rows_for_key("x", 0, 10, 0),
        };
        put_rows(&engine, *region_id, rows).await;
    }

    // flush to generate sst
    for region_id in &region_ids {
        engine
            .handle_request(
                *region_id,
                RegionRequest::Flush(RegionFlushRequest {
                    row_group_size: None,
                }),
            )
            .await
            .unwrap();
    }

    // list from manifest
    let debug_format = engine
        .all_ssts_from_manifest()
        .await
        .into_iter()
        .map(|mut e| {
            e.file_path = e.file_path.replace(&e.file_id, "<file_id>");
            e.index_file_path = e
                .index_file_path
                .map(|p| p.replace(&e.file_id, "<file_id>"));
            e.file_id = "<file_id>".to_string();
            e.index_version = 0;
            format!("\n{:?}", e)
        })
        .sorted()
        .collect::<Vec<_>>()
        .join("");
    assert_eq!(debug_format, expected_manifest_ssts, "{}", debug_format);

    // list from storage
    let storage_entries = engine
        .all_ssts_from_storage()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let debug_format = storage_entries
        .into_iter()
        .map(|mut e| {
            let i = e.file_path.rfind('/').unwrap();
            e.file_path.replace_range(i..(i + 37), "/<file_id>");
            format!("\n{:?}", e)
        })
        .sorted()
        .collect::<Vec<_>>()
        .join("");
    assert_eq!(debug_format, expected_storage_ssts, "{}", debug_format);
}

#[tokio::test]
async fn test_all_index_metas_list_all_types() {
    test_all_index_metas_list_all_types_with_format(false, r#"
PuffinIndexMetaEntry { table_dir: "test/", index_file_path: "test/11_0000000001/index/<file_id>.puffin", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_file_size: Some(6500), index_type: "bloom_filter", target_type: "column", target_key: "1", target_json: "{\"column\":1}", blob_size: 751, meta_json: Some("{\"bloom\":{\"bloom_filter_size\":640,\"row_count\":20,\"rows_per_segment\":2,\"segment_count\":10}}"), node_id: None }
PuffinIndexMetaEntry { table_dir: "test/", index_file_path: "test/11_0000000001/index/<file_id>.puffin", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_file_size: Some(6500), index_type: "fulltext_bloom", target_type: "column", target_key: "4", target_json: "{\"column\":4}", blob_size: 89, meta_json: Some("{\"bloom\":{\"bloom_filter_size\":64,\"row_count\":20,\"rows_per_segment\":4,\"segment_count\":5},\"fulltext\":{\"analyzer\":\"English\",\"case_sensitive\":false}}"), node_id: None }
PuffinIndexMetaEntry { table_dir: "test/", index_file_path: "test/11_0000000001/index/<file_id>.puffin", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_file_size: Some(6500), index_type: "fulltext_tantivy", target_type: "column", target_key: "5", target_json: "{\"column\":5}", blob_size: 1100, meta_json: Some("{\"fulltext\":{\"analyzer\":\"Chinese\",\"case_sensitive\":true}}"), node_id: None }
PuffinIndexMetaEntry { table_dir: "test/", index_file_path: "test/11_0000000001/index/<file_id>.puffin", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_file_size: Some(6500), index_type: "inverted", target_type: "column", target_key: "1", target_json: "{\"column\":1}", blob_size: 518, meta_json: Some("{\"inverted\":{\"base_offset\":0,\"bitmap_type\":\"Roaring\",\"fst_size\":150,\"inverted_index_size\":518,\"null_bitmap_size\":8,\"relative_fst_offset\":368,\"relative_null_bitmap_offset\":0,\"segment_row_count\":1024,\"total_row_count\":20}}"), node_id: None }
PuffinIndexMetaEntry { table_dir: "test/", index_file_path: "test/11_0000000001/index/<file_id>.puffin", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_file_size: Some(6500), index_type: "inverted", target_type: "column", target_key: "2", target_json: "{\"column\":2}", blob_size: 515, meta_json: Some("{\"inverted\":{\"base_offset\":0,\"bitmap_type\":\"Roaring\",\"fst_size\":147,\"inverted_index_size\":515,\"null_bitmap_size\":8,\"relative_fst_offset\":368,\"relative_null_bitmap_offset\":0,\"segment_row_count\":1024,\"total_row_count\":20}}"), node_id: None }"#).await;
    test_all_index_metas_list_all_types_with_format(true, r#"
PuffinIndexMetaEntry { table_dir: "test/", index_file_path: "test/11_0000000001/index/<file_id>.puffin", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_file_size: Some(6500), index_type: "bloom_filter", target_type: "column", target_key: "1", target_json: "{\"column\":1}", blob_size: 751, meta_json: Some("{\"bloom\":{\"bloom_filter_size\":640,\"row_count\":20,\"rows_per_segment\":2,\"segment_count\":10}}"), node_id: None }
PuffinIndexMetaEntry { table_dir: "test/", index_file_path: "test/11_0000000001/index/<file_id>.puffin", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_file_size: Some(6500), index_type: "fulltext_bloom", target_type: "column", target_key: "4", target_json: "{\"column\":4}", blob_size: 89, meta_json: Some("{\"bloom\":{\"bloom_filter_size\":64,\"row_count\":20,\"rows_per_segment\":4,\"segment_count\":5},\"fulltext\":{\"analyzer\":\"English\",\"case_sensitive\":false}}"), node_id: None }
PuffinIndexMetaEntry { table_dir: "test/", index_file_path: "test/11_0000000001/index/<file_id>.puffin", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_file_size: Some(6500), index_type: "fulltext_tantivy", target_type: "column", target_key: "5", target_json: "{\"column\":5}", blob_size: 1100, meta_json: Some("{\"fulltext\":{\"analyzer\":\"Chinese\",\"case_sensitive\":true}}"), node_id: None }
PuffinIndexMetaEntry { table_dir: "test/", index_file_path: "test/11_0000000001/index/<file_id>.puffin", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_file_size: Some(6500), index_type: "inverted", target_type: "column", target_key: "1", target_json: "{\"column\":1}", blob_size: 518, meta_json: Some("{\"inverted\":{\"base_offset\":0,\"bitmap_type\":\"Roaring\",\"fst_size\":150,\"inverted_index_size\":518,\"null_bitmap_size\":8,\"relative_fst_offset\":368,\"relative_null_bitmap_offset\":0,\"segment_row_count\":1024,\"total_row_count\":20}}"), node_id: None }
PuffinIndexMetaEntry { table_dir: "test/", index_file_path: "test/11_0000000001/index/<file_id>.puffin", region_id: 47244640257(11, 1), table_id: 11, region_number: 1, region_group: 0, region_sequence: 1, file_id: "<file_id>", index_file_size: Some(6500), index_type: "inverted", target_type: "column", target_key: "2", target_json: "{\"column\":2}", blob_size: 515, meta_json: Some("{\"inverted\":{\"base_offset\":0,\"bitmap_type\":\"Roaring\",\"fst_size\":147,\"inverted_index_size\":515,\"null_bitmap_size\":8,\"relative_fst_offset\":368,\"relative_null_bitmap_offset\":0,\"segment_row_count\":1024,\"total_row_count\":20}}"), node_id: None }"#).await;
}

async fn test_all_index_metas_list_all_types_with_format(flat_format: bool, expect_format: &str) {
    use datatypes::schema::{
        FulltextAnalyzer, FulltextBackend, FulltextOptions, SkippingIndexOptions, SkippingIndexType,
    };

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    // One region with both fulltext backends and inverted index enabled, plus bloom skipping index
    let region_id = RegionId::new(11, 1);

    let mut request = CreateRequestBuilder::new().tag_num(1).field_num(2).build();
    // bloom filter skipping index on field_1
    let skipping = SkippingIndexOptions::new_unchecked(2, 0.01, SkippingIndexType::BloomFilter);
    request.column_metadatas[1]
        .column_schema
        .set_skipping_options(&skipping)
        .unwrap();

    // inverted index on field_1
    request.column_metadatas[2]
        .column_schema
        .set_inverted_index(true);
    // inverted index on tag_0
    request.column_metadatas[1]
        .column_schema
        .set_inverted_index(true);

    request.column_metadatas.push(ColumnMetadata {
        column_schema: ColumnSchema::new(
            "field_2".to_string(),
            ConcreteDataType::string_datatype(),
            true,
        ),
        semantic_type: SemanticType::Field,
        column_id: 4,
    });
    // fulltext bloom on field_2
    let ft_bloom = FulltextOptions::new_unchecked(
        true,
        FulltextAnalyzer::English,
        false,
        FulltextBackend::Bloom,
        4,
        0.001,
    );
    request
        .column_metadatas
        .last_mut()
        .unwrap()
        .column_schema
        .set_fulltext_options(&ft_bloom)
        .unwrap();

    request.column_metadatas.push(ColumnMetadata {
        column_schema: ColumnSchema::new(
            "field_3".to_string(),
            ConcreteDataType::string_datatype(),
            true,
        ),
        semantic_type: SemanticType::Field,
        column_id: 5,
    });
    // fulltext tantivy on field_3
    let ft_tantivy = FulltextOptions::new_unchecked(
        true,
        FulltextAnalyzer::Chinese,
        true,
        FulltextBackend::Tantivy,
        2,
        0.01,
    );
    request
        .column_metadatas
        .last_mut()
        .unwrap()
        .column_schema
        .set_fulltext_options(&ft_tantivy)
        .unwrap();

    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    // write some rows (schema: tag_0, field_0, field_1, field_2, field_3, ts)
    let column_schemas = rows_schema(&request);
    let rows_vec: Vec<api::v1::Row> = (0..20)
        .map(|ts| api::v1::Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::StringValue("x".to_string())),
                },
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::F64Value(ts as f64)),
                },
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::F64Value((20 - ts) as f64)),
                },
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::TimestampMillisecondValue(
                        ts as i64 * 1000,
                    )),
                },
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::StringValue("y".to_string())),
                },
                api::v1::Value {
                    value_data: Some(api::v1::value::ValueData::StringValue("z".to_string())),
                },
            ],
        })
        .collect();
    let rows = api::v1::Rows {
        schema: column_schemas.clone(),
        rows: rows_vec,
    };
    put_rows(&engine, region_id, rows).await;

    // flush to generate sst and indexes
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    fn bucket_size(size: u64) -> u64 {
        if size < 512 { size } else { (size / 100) * 100 }
    }

    let mut metas = engine.all_index_metas().await;
    for entry in &mut metas {
        entry.index_file_path = entry.index_file_path.replace(&entry.file_id, "<file_id>");
        entry.file_id = "<file_id>".to_string();
        entry.index_file_size = entry.index_file_size.map(bucket_size);
        if entry.index_type == "fulltext_tantivy" {
            entry.blob_size = bucket_size(entry.blob_size);
        }
        if let Some(meta_json) = entry.meta_json.as_mut()
            && let Ok(mut value) = serde_json::from_str::<serde_json::Value>(meta_json)
        {
            if let Some(inverted) = value.get_mut("inverted").and_then(|v| v.as_object_mut()) {
                inverted.insert("base_offset".to_string(), serde_json::Value::from(0));
            }
            *meta_json = value.to_string();
        }
    }
    metas.sort_by(|a, b| {
        (a.index_type.as_str(), a.target_key.as_str())
            .cmp(&(b.index_type.as_str(), b.target_key.as_str()))
    });

    let debug_format = metas
        .iter()
        .map(|entry| format!("\n{:?}", entry))
        .collect::<String>();

    assert_eq!(expect_format, debug_format);
}
