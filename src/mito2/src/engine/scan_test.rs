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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use api::helper::encode_json_value;
use api::v1::helper::row;
use api::v1::value::ValueData;
use api::v1::{ArrowIpc, ColumnDataType, Rows, SemanticType, WriteHint};
use common_base::readable_size::ReadableSize;
use common_error::ext::{ErrorExt, WhateverResult};
use common_error::status_code::StatusCode;
use common_recordbatch::{DfRecordBatch, RecordBatches};
use common_test_util::flight::encode_to_flight_data;
use common_time::Timestamp;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use datatypes::arrow::array::{
    ArrayRef, AsArray, Float64Array, StringArray, TimestampMillisecondArray,
};
use datatypes::arrow::datatypes::{
    DataType, Field, Float64Type, Schema, TimeUnit, TimestampMillisecondType, UInt64Type,
};
use datatypes::json::value::JsonValue;
use datatypes::prelude::ConcreteDataType;
use datatypes::types::json_type::{JsonNativeType, JsonObjectType};
use futures::TryStreamExt;
use futures::future::try_join_all;
use serde_json::json;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metric_engine_consts::PRIMARY_KEY_ENCODING;
use store_api::region_engine::{PrepareRequest, RegionEngine, RegionScanner};
use store_api::region_request::{
    AlterKind, RegionAlterRequest, RegionBulkInsertsRequest, RegionCompactRequest,
    RegionPutRequest, RegionRequest, SetRegionOption,
};
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;
use store_api::storage::{
    FileId, RegionId, ScanRequest, TimeSeriesDistribution, TimeSeriesRowSelector,
};

use crate::config::MitoConfig;
use crate::error::Error;
use crate::manifest::action::RegionEdit;
use crate::read::read_columns::ReadColumns;
use crate::read::scan_region::Scanner;
use crate::sst::file::FileMeta;
use crate::test_util;
use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};
use crate::test_util::{CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_json_type_hint_pushdown_scanner_returns_batches() -> WhateverResult<()> {
    // Create a region with a JSON2 field whose physical Parquet representation is a nested struct.
    // The scan below will only ask for field_0.a.x.

    let request = CreateRequestBuilder::new()
        .field_datatype(ConcreteDataType::json2(JsonNativeType::Object(
            JsonObjectType::from([
                (
                    "a".to_string(),
                    JsonNativeType::Object(JsonObjectType::from([
                        ("x".to_string(), JsonNativeType::i64()),
                        ("y".to_string(), JsonNativeType::String),
                    ])),
                ),
                ("b".to_string(), JsonNativeType::String),
            ]),
        )))
        .build();
    let schema = test_util::rows_schema(&request);

    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1024, 0);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await?;

    // Write full JSON objects, then flush them so the scanner has an Parquet file where nested
    // projection can be pushed down.
    let rows = Rows {
        schema,
        rows: vec![
            row(vec![
                ValueData::StringValue("tag-1".to_string()),
                ValueData::JsonValue(encode_json_value(JsonValue::from(json!({
                    "a": { "x": 10, "y": "ignored-a" },
                    "b": "ignored-b"
                })))),
                ValueData::TimestampMillisecondValue(1000),
            ]),
            row(vec![
                ValueData::StringValue("tag-2".to_string()),
                ValueData::JsonValue(encode_json_value(JsonValue::from(json!({
                    "a": { "x": 20, "y": "ignored-c" },
                    "b": "ignored-d"
                })))),
                ValueData::TimestampMillisecondValue(2000),
            ]),
        ],
    };
    test_util::put_rows(&engine, region_id, rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    // Without a type hint, the scanner reads the whole JSON2 root column.
    let request = ScanRequest {
        projection: Some(vec![1, 0]),
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await?;
    let Scanner::Seq(seq_scan) = &scanner else {
        unreachable!();
    };
    assert_eq!(
        seq_scan.input().read_cols,
        ReadColumns::new([1, 0])
            .with_json_target_types(BTreeMap::from([(1, JsonNativeType::Variant)]))
    );

    let stream = scanner.scan().await?;
    let batches = RecordBatches::try_collect(stream).await?;
    let expected = r#"
+------------------------------------------------+-------+
| field_0                                        | tag_0 |
+------------------------------------------------+-------+
| {"a":{"x":10,"y":"ignored-a"},"b":"ignored-b"} | tag-1 |
| {"a":{"x":20,"y":"ignored-c"},"b":"ignored-d"} | tag-2 |
+------------------------------------------------+-------+
"#;
    assert_eq!(batches.pretty_print()?, expected.trim());

    // Simulate a query expression like json_get(field_0, 'a.x'): the logical projection still
    // returns the JSON2 root column, while json_type_hint tells scan input construction which
    // nested physical path is needed.
    let json_type_hint = HashMap::from([(
        "field_0".to_string(),
        JsonNativeType::Object(JsonObjectType::from([(
            "a".to_string(),
            JsonNativeType::Object(JsonObjectType::from([(
                "x".to_string(),
                JsonNativeType::i64(),
            )])),
        )])),
    )]);
    let request = ScanRequest {
        projection: Some(vec![1, 0]),
        json_type_hint: json_type_hint.clone(),
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await?;
    let Scanner::Seq(seq_scan) = &scanner else {
        unreachable!();
    };
    // Verify the scan input only asks storage for field_0.a.x instead of the
    // whole JSON2 struct. tag_0 is still read as a normal root column.
    assert_eq!(
        seq_scan.input().read_cols,
        ReadColumns::new([1, 0])
            .with_json_target_types(BTreeMap::from([(1, json_type_hint["field_0"].clone())]))
    );

    // The scanner should still return a valid RecordBatch in the requested logical projection.
    // Fields outside the pushed-down path are pruned from the returned JSON2 type and value.

    let stream = scanner.scan().await?;
    let batches = RecordBatches::try_collect(stream).await?;
    let expected = r#"
+--------------+-------+
| field_0      | tag_0 |
+--------------+-------+
| {a: {x: 10}} | tag-1 |
| {a: {x: 20}} | tag-2 |
+--------------+-------+
"#;
    assert_eq!(batches.pretty_print()?, expected.trim());

    // A type hint only narrows a projected JSON2 root; it must not add an unprojected root.
    let request = ScanRequest {
        projection: Some(vec![0]),
        json_type_hint,
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await?;
    let Scanner::Seq(seq_scan) = &scanner else {
        unreachable!();
    };
    assert_eq!(seq_scan.input().read_cols, ReadColumns::new([0]));

    let stream = scanner.scan().await?;
    let batches = RecordBatches::try_collect(stream).await?;
    let expected = r#"
+-------+
| tag_0 |
+-------+
| tag-1 |
| tag-2 |
+-------+
"#;
    assert_eq!(batches.pretty_print()?, expected.trim());

    // JSON type hints are derived from the whole query plan, so they may include
    // columns unrelated to this scan. Mito2 should ignore hints for unknown
    // columns (`other_json`) instead of failing.
    let request = ScanRequest {
        projection: Some(vec![0]),
        json_type_hint: HashMap::from([("other_json".to_string(), JsonNativeType::String)]),
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await?;
    let stream = scanner.scan().await?;
    let batches = RecordBatches::try_collect(stream).await?;
    assert_eq!(batches.pretty_print()?, expected.trim());
    Ok(())
}

#[tokio::test]
async fn test_incremental_query_stale_error() {
    let mut env = TestEnv::with_prefix("test_incremental_query_stale_error").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    let err = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(0),
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expect stale incremental error");

    let min_readable_seq = match &err {
        Error::IncrementalQueryStale {
            region_id: err_region_id,
            given_seq,
            min_readable_seq,
            ..
        } => {
            assert_eq!(*err_region_id, region_id);
            assert_eq!(*given_seq, 0);
            assert!(*min_readable_seq > 0);
            *min_readable_seq
        }
        _ => panic!("unexpected err: {err}"),
    };
    assert_eq!(StatusCode::RequestOutdated, err.status_code());
    let err_msg = err.to_string();
    assert!(err_msg.contains("STALE_CURSOR"));
    assert!(err_msg.contains(&region_id.to_string()));
    assert!(err_msg.contains("given_seq: 0"));
    assert!(err_msg.contains(&format!("min_readable_seq: {min_readable_seq}")));

    let incremental_rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(3, 5),
    };
    test_util::put_rows(&engine, region_id, incremental_rows).await;

    let scanner = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(min_readable_seq),
                skip_sst_files: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        batches.pretty_print().unwrap(),
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
+-------+---------+---------------------+"
    );
}

#[tokio::test]
async fn test_scan_with_min_sst_sequence() {
    test_scan_with_min_sst_sequence_with_format(false).await;
    test_scan_with_min_sst_sequence_with_format(true).await;
}

#[tokio::test]
async fn test_full_snapshot_upper_bound_returns_outdated_after_late_flush() {
    let mut env =
        TestEnv::with_prefix("test_full_snapshot_upper_bound_returns_outdated_after_late_flush")
            .await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let first_rows = Rows {
        schema: column_schemas.clone(),
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, first_rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    let snapshot_upper_bound = engine.get_committed_sequence(region_id).await.unwrap();

    let second_rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(3, 5),
    };
    test_util::put_rows(&engine, region_id, second_rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    let err = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_max_sequence: Some(snapshot_upper_bound),
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expect stale snapshot fence error");

    assert_eq!(StatusCode::RequestOutdated, err.status_code());
    let err_msg = err.to_string();
    assert!(err_msg.contains("STALE_SNAPSHOT_FENCE"));
    assert!(err_msg.contains(&region_id.to_string()));
    assert!(err_msg.contains(&format!("given_seq: {snapshot_upper_bound}")));
}

#[tokio::test]
async fn test_snapshot_bound_query_binds_memtable_upper_bound_at_scan_open() {
    let mut env =
        TestEnv::with_prefix("test_snapshot_bound_query_binds_memtable_upper_bound_at_scan_open")
            .await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let first_rows = Rows {
        schema: column_schemas.clone(),
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, first_rows).await;

    let expected_snapshot = engine.get_committed_sequence(region_id).await.unwrap();
    let scanner = engine
        .scanner(
            region_id,
            ScanRequest {
                snapshot_on_scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(scanner.snapshot_sequence(), Some(expected_snapshot));

    let second_rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(3, 5),
    };
    test_util::put_rows(&engine, region_id, second_rows).await;

    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        batches.pretty_print().unwrap(),
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+"
    );
}

#[tokio::test]
async fn test_snapshot_bound_query_keeps_open_snapshot_after_late_flush() {
    let mut env =
        TestEnv::with_prefix("test_snapshot_bound_query_keeps_open_snapshot_after_late_flush")
            .await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let first_rows = Rows {
        schema: column_schemas.clone(),
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, first_rows).await;

    let expected_snapshot = engine.get_committed_sequence(region_id).await.unwrap();
    let scanner = engine
        .scanner(
            region_id,
            ScanRequest {
                snapshot_on_scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(scanner.snapshot_sequence(), Some(expected_snapshot));

    let second_rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(3, 5),
    };
    test_util::put_rows(&engine, region_id, second_rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    assert_eq!(scanner.snapshot_sequence(), Some(expected_snapshot));
}

#[tokio::test]
async fn test_snapshot_bound_query_keeps_correct_result_after_late_flush() {
    let mut env =
        TestEnv::with_prefix("test_snapshot_bound_query_keeps_correct_result_after_late_flush")
            .await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let first_rows = Rows {
        schema: column_schemas.clone(),
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, first_rows).await;

    let expected_snapshot = engine.get_committed_sequence(region_id).await.unwrap();
    let scanner = engine
        .scanner(
            region_id,
            ScanRequest {
                snapshot_on_scan: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(scanner.snapshot_sequence(), Some(expected_snapshot));

    let second_rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(3, 5),
    };
    test_util::put_rows(&engine, region_id, second_rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    assert_eq!(scanner.snapshot_sequence(), Some(expected_snapshot));

    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        batches.pretty_print().unwrap(),
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+"
    );
}

async fn test_scan_with_min_sst_sequence_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("test_scan_with_min_sst_sequence").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let put_rows = async |start, end| {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(start, end),
        };
        test_util::put_rows(&engine, region_id, rows).await;
        test_util::flush_region(&engine, region_id, None).await;
    };
    // generates 3 SST files
    put_rows(0, 3).await;
    put_rows(3, 6).await;
    put_rows(6, 9).await;

    let scan_engine = async |file_min_sequence, expected_files, expected_data| {
        let request = ScanRequest {
            sst_min_sequence: file_min_sequence,
            ..Default::default()
        };
        let scanner = engine.scanner(region_id, request).await.unwrap();
        assert_eq!(scanner.num_files(), expected_files);

        let stream = scanner.scan().await.unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        assert_eq!(batches.pretty_print().unwrap(), expected_data);
    };

    // scans with no sst minimal sequence limit
    scan_engine(
        None,
        3,
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
| 8     | 8.0     | 1970-01-01T00:00:08 |
+-------+---------+---------------------+",
    )
    .await;

    // scans with sst minimal sequence > 2
    scan_engine(
        Some(2),
        3,
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
| 8     | 8.0     | 1970-01-01T00:00:08 |
+-------+---------+---------------------+",
    )
    .await;

    // scans with sst minimal sequence > 3
    scan_engine(
        Some(3),
        2,
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
| 8     | 8.0     | 1970-01-01T00:00:08 |
+-------+---------+---------------------+",
    )
    .await;

    // scans with sst minimal sequence > 7
    scan_engine(
        Some(7),
        1,
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
| 8     | 8.0     | 1970-01-01T00:00:08 |
+-------+---------+---------------------+",
    )
    .await;

    // scans with sst minimal sequence > 9 (no sst files will be selected to scan)
    scan_engine(
        Some(9),
        0,
        "\
++
++",
    )
    .await;
}

#[tokio::test]
async fn test_max_concurrent_scan_files() {
    test_max_concurrent_scan_files_with_format(false).await;
    test_max_concurrent_scan_files_with_format(true).await;
}

async fn test_max_concurrent_scan_files_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("test_max_concurrent_scan_files").await;
    let config = MitoConfig {
        default_flat_format: flat_format,
        max_concurrent_scan_files: 2,
        ..Default::default()
    };
    let engine = env.create_engine(config).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let put_and_flush = async |start, end| {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(start, end),
        };
        test_util::put_rows(&engine, region_id, rows).await;
        test_util::flush_region(&engine, region_id, None).await;
    };

    // Write overlapping files.
    put_and_flush(0, 4).await;
    put_and_flush(3, 7).await;
    put_and_flush(6, 9).await;

    let request = ScanRequest::default();
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let Scanner::Seq(scanner) = scanner else {
        panic!("Scanner should be seq scan");
    };
    let error = scanner.check_scan_limit().unwrap_err();
    assert_eq!(StatusCode::RateLimited, error.status_code());

    let request = ScanRequest {
        distribution: Some(TimeSeriesDistribution::PerSeries),
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let Scanner::Series(scanner) = scanner else {
        panic!("Scanner should be series scan");
    };
    let error = scanner.check_scan_limit().unwrap_err();
    assert_eq!(StatusCode::RateLimited, error.status_code());
}

#[tokio::test]
async fn test_series_scan() {
    test_series_scan_with_format(false).await;
    test_series_scan_with_format(true).await;
}

async fn test_series_scan_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("test_series_scan").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let put_flush_rows = async |start, end| {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(start, end),
        };
        test_util::put_rows(&engine, region_id, rows).await;
        test_util::flush_region(&engine, region_id, None).await;
    };
    // generates 3 SST files
    put_flush_rows(0, 3).await;
    put_flush_rows(2, 6).await;
    put_flush_rows(3600, 3603).await;
    // Put to memtable.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: test_util::build_rows(7200, 7203),
    };
    test_util::put_rows(&engine, region_id, rows).await;

    let request = ScanRequest {
        distribution: Some(TimeSeriesDistribution::PerSeries),
        preserve_pk_dictionary_encoding: true,
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let Scanner::Series(mut scanner) = scanner else {
        panic!("Scanner should be series scan");
    };
    assert_eq!("legacy", scanner.mode());
    // 3 partition ranges for 3 time window.
    assert_eq!(
        3,
        scanner.properties().partitions[0].len(),
        "unexpected ranges: {:?}",
        scanner.properties().partitions
    );
    let raw_ranges: Vec<_> = scanner
        .properties()
        .partitions
        .iter()
        .flatten()
        .cloned()
        .collect();
    let mut new_ranges = Vec::with_capacity(3);
    for range in raw_ranges {
        new_ranges.push(vec![range]);
    }
    scanner
        .prepare(PrepareRequest {
            ranges: Some(new_ranges),
            ..Default::default()
        })
        .unwrap();

    let actual_rows = collect_partition_rows_round_robin(&scanner, 3).await;

    let mut expected_rows = Vec::new();
    for value in [0_i64, 1, 2, 3, 4, 5, 3600, 3601, 3602, 7200, 7201, 7202] {
        expected_rows.push((value.to_string(), value as f64, value * 1000));
    }
    expected_rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));

    assert_eq!(expected_rows, actual_rows);
}

#[tokio::test]
async fn test_two_phase_series_scan() {
    let mut env = TestEnv::with_prefix("test_two_phase_series_scan").await;
    let engine = env
        .create_engine(MitoConfig {
            experimental_series_scan_v2: true,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let metadata = Arc::new(sst_region_metadata_with_encoding(
        PrimaryKeyEncoding::Sparse,
    ));
    let mut request = CreateRequestBuilder::new().build();
    request.column_metadatas = metadata.column_metadatas.clone();
    request.primary_key = metadata.primary_key.clone();
    request
        .options
        .insert(PRIMARY_KEY_ENCODING.to_string(), "sparse".to_string());
    request
        .options
        .insert("memtable.type".to_string(), "bulk".to_string());
    request
        .options
        .insert("sst_format".to_string(), "flat".to_string());
    let full_row_schema = test_util::rows_schema(&request);
    let mut encoded_primary_key_schema = full_row_schema[0].clone();
    encoded_primary_key_schema.column_name = PRIMARY_KEY_COLUMN_NAME.to_string();
    encoded_primary_key_schema.datatype = ColumnDataType::Binary.into();
    encoded_primary_key_schema.semantic_type = SemanticType::Tag.into();
    let row_schema = vec![
        encoded_primary_key_schema,
        full_row_schema[5].clone(),
        full_row_schema[4].clone(),
    ];

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = |values: &[(u32, u64, &str, &str, u64, i64)]| Rows {
        schema: row_schema.clone(),
        rows: values
            .iter()
            .map(|(table_id, tsid, tag_0, tag_1, field, ts)| {
                row(vec![
                    ValueData::BinaryValue(new_sparse_primary_key(
                        &[*tag_0, *tag_1],
                        &metadata,
                        *table_id,
                        *tsid,
                    )),
                    ValueData::TimestampMillisecondValue(*ts),
                    ValueData::U64Value(*field),
                ])
            })
            .collect(),
    };
    let put = |rows| {
        RegionRequest::Put(RegionPutRequest {
            rows,
            hint: Some(WriteHint {
                primary_key_encoding: api::v1::PrimaryKeyEncoding::Sparse.into(),
            }),
            partition_expr_version: None,
        })
    };

    engine
        .handle_request(
            region_id,
            put(rows(&[
                (10, 0, "a", "x", 10, 1000),
                (10, u64::MAX, "b", "y", 20, 1000),
                (20, 0, "c", "z", 30, 1000),
            ])),
        )
        .await
        .unwrap();
    test_util::flush_region(&engine, region_id, None).await;
    engine
        .handle_request(
            region_id,
            put(rows(&[
                // Replaces the flushed row for this series and timestamp.
                (10, 0, "a", "x", 11, 1000),
                (10, 0, "a", "x", 12, 2000),
                (10, u64::MAX, "b", "y", 21, 2000),
                (20, u64::MAX, "d", "w", 40, 1000),
            ])),
        )
        .await
        .unwrap();

    let scanner = engine
        .scanner(
            region_id,
            ScanRequest {
                // Internal metric identifiers must still be read for candidate discovery.
                projection: Some(vec![2, 4, 5]),
                distribution: Some(TimeSeriesDistribution::PerSeries),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let Scanner::Series(mut scanner) = scanner else {
        panic!("Scanner should be series scan");
    };
    assert_eq!("two_phase", scanner.mode());

    let ranges = scanner
        .properties()
        .partitions
        .iter()
        .flatten()
        .copied()
        .collect::<Vec<_>>();
    scanner
        .prepare(
            PrepareRequest::default()
                .with_ranges(vec![ranges, Vec::new(), Vec::new()])
                .with_target_partitions(3),
        )
        .unwrap();

    let metrics_set = ExecutionPlanMetricsSet::default();
    let context = store_api::region_engine::QueryScanContext::default();
    let partition_batches = try_join_all((0..3).map(|partition| {
        let stream = scanner
            .scan_partition(&context, &metrics_set, partition)
            .unwrap();
        async move { stream.try_collect::<Vec<_>>().await }
    }))
    .await
    .unwrap();

    let mut series_to_partition = BTreeMap::new();
    let mut actual_rows = Vec::new();
    for (partition, batches) in partition_batches.into_iter().enumerate() {
        for batch in batches {
            let tags = batch.column_by_name("tag_0").unwrap();
            let fields = batch
                .column_by_name("field_0")
                .unwrap()
                .as_primitive::<UInt64Type>();
            let timestamps = batch
                .column_by_name("ts")
                .unwrap()
                .as_primitive::<TimestampMillisecondType>();
            for row in 0..batch.num_rows() {
                let tag = datatypes::arrow_array::string_array_value_at_index(tags, row)
                    .unwrap()
                    .to_string();
                let previous = series_to_partition.insert(tag.clone(), partition);
                if let Some(previous) = previous {
                    assert_eq!(previous, partition, "series {tag} moved partitions");
                }
                actual_rows.push((tag, fields.value(row), timestamps.value(row)));
            }
        }
    }
    actual_rows.sort();
    assert_eq!(
        vec![
            ("a".to_string(), 11, 1000),
            ("a".to_string(), 12, 2000),
            ("b".to_string(), 20, 1000),
            ("b".to_string(), 21, 2000),
            ("c".to_string(), 30, 1000),
            ("d".to_string(), 40, 1000),
        ],
        actual_rows
    );
    assert_eq!(4, series_to_partition.len());
    assert_eq!(Some(&0), series_to_partition.get("a"));
    assert_eq!(Some(&0), series_to_partition.get("c"));
    assert_eq!(Some(&2), series_to_partition.get("b"));
    assert_eq!(Some(&2), series_to_partition.get("d"));
}

/// Scans all partitions in round-robin fashion and returns rows sorted by (tag, ts).
/// Also asserts that each series appears in only one partition.
async fn collect_partition_rows_round_robin(
    scanner: &dyn RegionScanner,
    num_partitions: usize,
) -> Vec<(String, f64, i64)> {
    let metrics_set = ExecutionPlanMetricsSet::default();

    let mut partition_batches = vec![vec![]; num_partitions];
    let mut streams: Vec<_> = (0..num_partitions)
        .map(|partition| {
            let stream = scanner
                .scan_partition(&Default::default(), &metrics_set, partition)
                .unwrap();
            Some(stream)
        })
        .collect();
    let mut num_done = 0;
    let mut schema = None;
    // Pull streams in round-robin fashion to get the consistent output from the sender.
    while num_done < num_partitions {
        if schema.is_none() {
            schema = Some(streams[0].as_ref().unwrap().schema().clone());
        }
        for i in 0..num_partitions {
            let Some(mut stream) = streams[i].take() else {
                continue;
            };
            let Some(rb) = stream.try_next().await.unwrap() else {
                num_done += 1;
                continue;
            };
            partition_batches[i].push(rb);
            streams[i] = Some(stream);
        }
    }

    let schema = schema.unwrap();
    collect_and_assert_partition_rows(schema, partition_batches)
}

/// Collects rows sorted by (tag, ts) from partition batches.
/// Also asserts that each series appears in only one partition.
fn collect_and_assert_partition_rows(
    schema: datatypes::schema::SchemaRef,
    partition_batches: Vec<Vec<common_recordbatch::RecordBatch>>,
) -> Vec<(String, f64, i64)> {
    let mut series_to_partition = BTreeMap::new();
    let mut actual_rows = Vec::new();

    for (partition, batches) in partition_batches.into_iter().enumerate() {
        let batches = RecordBatches::try_new(schema.clone(), batches).unwrap();
        let mut partition_series = Vec::new();

        for batch in batches.iter() {
            let tags = batch.column_by_name("tag_0").unwrap();
            let fields = batch
                .column_by_name("field_0")
                .unwrap()
                .as_primitive::<Float64Type>();
            let ts = batch
                .column_by_name("ts")
                .unwrap()
                .as_primitive::<TimestampMillisecondType>();

            for row in 0..batch.num_rows() {
                let tag = datatypes::arrow_array::string_array_value_at_index(tags, row)
                    .unwrap()
                    .to_string();
                let field = fields.value(row);
                let ts = ts.value(row);
                partition_series.push(tag.clone());
                actual_rows.push((tag, field, ts));
            }
        }

        partition_series.sort();
        partition_series.dedup();
        for tag in partition_series {
            let prev = series_to_partition.insert(tag.clone(), partition);
            assert_eq!(
                None, prev,
                "series {tag} appears in multiple partitions: {prev:?} and {partition}"
            );
        }
    }

    actual_rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));
    actual_rows
}

/// Tests series scan with multiple partition ranges (each with multiple overlapping sources)
/// and small semaphore permits (controlled by num_partitions).
#[tokio::test]
async fn test_series_scan_flat_small_permits() {
    let mut env = TestEnv::with_prefix("test_series_scan_small_permits").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: true,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.time_window", "1h")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Create overlapping SSTs in each time window so partition ranges have multiple sources.
    let put_flush_rows = async |start, end| {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(start, end),
        };
        test_util::put_rows(&engine, region_id, rows).await;
        test_util::flush_region(&engine, region_id, None).await;
    };
    // Window 0 (0s-999s): 3 overlapping SSTs
    put_flush_rows(0, 3).await;
    put_flush_rows(1, 5).await;
    put_flush_rows(3, 7).await;
    // Window 1 (3600s-4599s): 2 overlapping SSTs
    put_flush_rows(3600, 3603).await;
    put_flush_rows(3601, 3605).await;
    // Window 2 (7200s-8199s): 2 overlapping SSTs
    put_flush_rows(7200, 7203).await;
    put_flush_rows(7201, 7204).await;

    let mut expected_rows = Vec::new();
    for value in [
        0_i64, 1, 2, 3, 4, 5, 6, 3600, 3601, 3602, 3603, 3604, 7200, 7201, 7202, 7203,
    ] {
        expected_rows.push((value.to_string(), value as f64, value * 1000));
    }
    expected_rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.2.cmp(&b.2)));

    // Test with different semaphore sizes (num_partitions controls Semaphore::new(num_partitions)).
    for num_partitions in [1, 2] {
        let request = ScanRequest {
            distribution: Some(TimeSeriesDistribution::PerSeries),
            ..Default::default()
        };
        let scanner = engine.scanner(region_id, request).await.unwrap();
        let Scanner::Series(mut scanner) = scanner else {
            panic!("Scanner should be series scan");
        };

        // Collect all partition ranges and redistribute into `num_partitions` partitions.
        let raw_ranges: Vec<_> = scanner
            .properties()
            .partitions
            .iter()
            .flatten()
            .cloned()
            .collect();
        assert!(
            raw_ranges.len() >= 3,
            "expected at least 3 partition ranges, got {}",
            raw_ranges.len()
        );

        let mut new_ranges = vec![vec![]; num_partitions];
        for (i, range) in raw_ranges.into_iter().enumerate() {
            new_ranges[i % num_partitions].push(range);
        }
        scanner
            .prepare(PrepareRequest {
                ranges: Some(new_ranges),
                ..Default::default()
            })
            .unwrap();

        let actual_rows = collect_partition_rows_round_robin(&scanner, num_partitions).await;
        assert_eq!(
            expected_rows, actual_rows,
            "mismatch with num_partitions={num_partitions}"
        );
    }
}

/// Regression test: `ts = a OR ts = b` extracts to a `TimestampRange` that
/// `GenericRange::or` widens into `[min(a, b), max(a, b) + 1)`. Two such
/// predicates with different `a` values can both extract to ranges that cover
/// the same partition while selecting different (or no) rows. The previous
/// cover check would strip both predicates from the cache key, letting the
/// second scan return the first scan's cached row.
#[tokio::test]
async fn test_range_cache_separates_or_equality_time_filters() {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: true,
            // Explicitly enable the range result cache: the bug only reproduces
            // when the second scan can replay the first scan's cached batches.
            range_result_cache_size: ReadableSize::mb(64),
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Partition rows ts=5..10 (5000ms..9000ms), flushed to SST.
    test_util::put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(5, 10),
        },
    )
    .await;
    test_util::flush_region(&engine, region_id, None).await;

    let ts_lit = |ms: i64| lit(ScalarValue::TimestampMillisecond(Some(ms), None));
    let tag_filter = || col("tag_0").gt_eq(lit(ScalarValue::Utf8(Some("0".to_string()))));

    // First scan: (ts = 5000) OR (ts = 100000) -- extracts to `[5000, 100001)`,
    // which covers the partition `[5000, 9000]`. Selects ts=5.
    let stream = engine
        .scan_to_stream(
            region_id,
            ScanRequest {
                filters: vec![
                    tag_filter(),
                    col("ts").eq(ts_lit(5000)).or(col("ts").eq(ts_lit(100000))),
                ],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected_first = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";
    assert_eq!(expected_first, batches.pretty_print().unwrap());

    // Second scan: (ts = 3000) OR (ts = 100000) -- extracts to `[3000, 100001)`,
    // which also covers the partition. Selects nothing. With the buggy cover
    // check both scans built the same cache key (tag filter only), so this scan
    // would replay the first scan's cached row and incorrectly return ts=5.
    let stream = engine
        .scan_to_stream(
            region_id,
            ScanRequest {
                filters: vec![
                    tag_filter(),
                    col("ts").eq(ts_lit(3000)).or(col("ts").eq(ts_lit(100000))),
                ],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(
        0,
        row_count,
        "expected empty result, got: {}",
        batches.pretty_print().unwrap()
    );
}

#[tokio::test]
async fn test_exact_sequence_read_compacted_sst_with_preserve_row_sequence() {
    let mut env = TestEnv::new().await;
    let engine = env.create_engine(MitoConfig::default()).await;
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
        .insert_option("compaction.twcs.trigger_file_num", "2")
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush 3 SSTs so the twcs compaction (trigger file num 2) can merge them.
    for (start, end) in [(0, 3), (3, 6), (6, 9)] {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(start, end),
        };
        test_util::put_rows(&engine, region_id, rows).await;
        test_util::flush_region(&engine, region_id, None).await;
    }

    let output = engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(output.affected_rows, 0);

    // The compacted SST still preserves per-row sequences: the exact (2, 7] range
    // returns rows with sequence 3..=7 even though C and H are older than the
    // flushed frontier (which would normally fail the stale fences).
    let stream = engine
        .scan_to_stream(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(2),
                memtable_max_sequence: Some(7),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        batches.pretty_print().unwrap(),
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
+-------+---------+---------------------+"
    );
}

/// P0 regression: historical MemtableOnly reads must keep every fence even when
/// the region preserves per-row sequences. The exact capability is only granted
/// to the explicit `sequence_range` mode; a memtable-only scan whose checkpoint
/// is behind the flushed frontier must stay stale instead of silently returning
/// an incomplete delta.
#[tokio::test]
async fn test_exact_sequence_read_memtable_only_keeps_fences_with_preserve_option() {
    let mut env = TestEnv::with_prefix("test_exact_sequence_read_memtable_only_keeps_fences").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    let err = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(0),
                memtable_max_sequence: Some(3),
                skip_sst_files: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expected stale cursor error for memtable-only after flush");
    assert!(
        matches!(err, Error::IncrementalQueryStale { .. }),
        "unexpected err: {err}"
    );
}

/// SequenceRange mode on a region without preserve_row_sequence:
/// the exact capability is unavailable, both bounds are enforceable, so the
/// engine must return a structured unsupported error instead of approximating.
#[tokio::test]
async fn test_exact_sequence_range_unsupported_when_option_off() {
    let mut env = TestEnv::with_prefix("test_exact_sequence_range_unsupported_option_off").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    // Bounds (3, 4] are both >= flushed frontier (3), so neither fence fires;
    // the region still cannot serve exact row-level filtering -> unsupported.
    let err = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(3),
                memtable_max_sequence: Some(4),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expected sequence-range unsupported error");
    assert!(
        matches!(err, Error::SequenceRangeUnsupported { .. }),
        "unexpected err: {err}"
    );
    assert_eq!(err.status_code(), StatusCode::Unsupported);

    // A stale checkpoint still yields the structured stale error (rebind path).
    let err = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(0),
                memtable_max_sequence: Some(4),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expected stale cursor error");
    assert!(
        matches!(err, Error::IncrementalQueryStale { .. }),
        "unexpected err: {err}"
    );
}

/// SequenceRange mode when a legacy file without the preserved-sequence marker
/// is present: exact capability unavailable -> structured unsupported error.
#[tokio::test]
async fn test_exact_sequence_range_unsupported_when_legacy_file() {
    let mut env = TestEnv::with_prefix("test_exact_sequence_range_unsupported_legacy_file").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    // Inject a legacy file without the preserved-sequence marker. The region
    // edit handler fills its max sequence with `committed + 1` (4), which is
    // greater than C=3: the unmarked file may still contain in-range rows, so
    // the exact capability is unavailable and the scan must fail with the
    // structured unsupported error. (An unmarked file with an unknown max
    // sequence fails closed in `files_allow_exact_sequence_range`, covered by
    // the unit test.)
    let edit = RegionEdit {
        files_to_add: vec![FileMeta {
            region_id,
            file_id: FileId::random(),
            time_range: (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(1000),
            ),
            level: 0,
            file_size: 0,
            max_row_group_uncompressed_size: 0,
            available_indexes: Default::default(),
            indexes: vec![],
            index_file_size: 0,
            index_version: 0,
            num_rows: 0,
            num_row_groups: 0,
            sequence: None,
            partition_expr: None,
            num_series: 0,
            preserve_row_sequence: false,
            ..Default::default()
        }],
        files_to_remove: vec![],
        timestamp_ms: None,
        compaction_time_window: None,
        flushed_entry_id: None,
        flushed_sequence: None,
        committed_sequence: None,
    };
    engine.edit_region(region_id, edit).await.unwrap();

    let err = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(3),
                memtable_max_sequence: Some(4),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expected sequence-range unsupported error");
    assert!(
        matches!(err, Error::SequenceRangeUnsupported { .. }),
        "unexpected err: {err}"
    );
    assert_eq!(err.status_code(), StatusCode::Unsupported);
}

/// Exact sequence-range reads reject an SST from another region as a broken
/// sequence domain instead of falling back to an unsupported scan.
#[tokio::test]
async fn test_exact_sequence_range_rejects_foreign_region_file() {
    let mut env =
        TestEnv::with_prefix("test_exact_sequence_range_rejects_foreign_region_file").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    test_util::put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: test_util::build_rows(0, 3),
        },
    )
    .await;
    test_util::flush_region(&engine, region_id, None).await;

    engine
        .edit_region(
            region_id,
            RegionEdit {
                files_to_add: vec![FileMeta {
                    region_id: RegionId::new(1, 2),
                    file_id: FileId::random(),
                    time_range: (
                        Timestamp::new_millisecond(0),
                        Timestamp::new_millisecond(1000),
                    ),
                    level: 0,
                    file_size: 0,
                    max_row_group_uncompressed_size: 0,
                    available_indexes: Default::default(),
                    indexes: vec![],
                    index_file_size: 0,
                    index_version: 0,
                    num_rows: 0,
                    num_row_groups: 0,
                    sequence: None,
                    partition_expr: None,
                    num_series: 0,
                    preserve_row_sequence: true,
                    ..Default::default()
                }],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            },
        )
        .await
        .unwrap();

    let err = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(3),
                memtable_max_sequence: Some(4),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expected broken sequence domain error");
    assert!(matches!(err, Error::RegionSequenceDomainBroken { .. }));
    assert_eq!(err.status_code(), StatusCode::Internal);
}

/// Exact sequence-range reads only require preserved row sequences in the
/// SSTs selected by their time range. A legacy SST outside that range cannot
/// contain a row in `(C, H]` for this request.
#[tokio::test]
async fn test_exact_sequence_range_ignores_time_pruned_unmarked_sst() {
    let mut env =
        TestEnv::with_prefix("test_exact_sequence_range_ignores_time_pruned_unmarked_sst").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // The old, unmarked SST covers timestamps 0s..2s.
    test_util::put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(0, 3),
        },
    )
    .await;
    test_util::flush_region(&engine, region_id, None).await;

    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetRegionOptions {
                    options: vec![SetRegionOption::PreserveRowSequence(true)],
                },
            }),
        )
        .await
        .unwrap();

    // The selected SST covers timestamps 10s..12s and carries the marker.
    test_util::put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: test_util::build_rows(10, 13),
        },
    )
    .await;
    test_util::flush_region(&engine, region_id, None).await;

    let stream = engine
        .scan_to_stream(
            region_id,
            ScanRequest {
                filters: vec![
                    col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(10_000), None))),
                ],
                memtable_min_sequence: Some(3),
                memtable_max_sequence: Some(6),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .expect("time-pruned unmarked SST must not disable exact reads");
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        3,
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>()
    );
}

/// Regression for #8865: a legacy (unmarked) SST written before the preserve
/// option was enabled permanently disables exact scans. Its file sequence may
/// be synthesized by an edit path and is never trusted for disjoint skipping;
/// normal scans still return everything.
#[tokio::test]
async fn test_exact_sequence_after_enable_with_old_unmarked_sst() {
    let mut env =
        TestEnv::with_prefix("test_exact_sequence_after_enable_with_old_unmarked_sst").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Old data flushed while the option was off: unmarked SST with max seq 3.
    test_util::put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(0, 3),
        },
    )
    .await;
    test_util::flush_region(&engine, region_id, None).await;

    // Enable the preserve option; later flushes carry the marker.
    engine
        .handle_request(
            region_id,
            RegionRequest::Alter(RegionAlterRequest {
                kind: AlterKind::SetRegionOptions {
                    options: vec![SetRegionOption::PreserveRowSequence(true)],
                },
            }),
        )
        .await
        .unwrap();

    // New marked data: seq 4..9 in two flushed SSTs.
    for (start, end) in [(3, 6), (6, 9)] {
        test_util::put_rows(
            &engine,
            region_id,
            Rows {
                schema: column_schemas.clone(),
                rows: test_util::build_rows(start, end),
            },
        )
        .await;
        test_util::flush_region(&engine, region_id, None).await;
    }

    let mut markers = engine
        .get_region(region_id)
        .unwrap()
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .map(|file| file.meta_ref().preserve_row_sequence)
        .collect::<Vec<_>>();
    markers.sort_unstable();
    assert_eq!(vec![false, true, true], markers);

    // Exact scans fail closed even though C=9 is after the unmarked file's
    // apparent sequence (3).
    let err = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(9),
                memtable_max_sequence: Some(10),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expected sequence-range unsupported error");
    assert!(
        matches!(err, Error::SequenceRangeUnsupported { .. }),
        "unexpected err: {err}"
    );

    // Normal scans still return everything.
    let stream = engine
        .scan_to_stream(region_id, ScanRequest::default())
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(9, batches.iter().map(|b| b.num_rows()).sum::<usize>());
}

/// Exact sequence-range mode must read every time-matching SST, so the legacy
/// `sst_min_sequence` file-pruning hint is incompatible: it could silently skip
/// a preserved file that still contains rows inside `(min, max]`. The request
/// must be rejected with the structured unsupported error, not approximated.
#[tokio::test]
async fn test_exact_sequence_range_rejects_sst_min_sequence_hint() {
    let mut env =
        TestEnv::with_prefix("test_exact_sequence_range_rejects_sst_min_sequence_hint").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    // Non-trivial `sst_min_sequence` hint combined with an exact range: the
    // pruning hint could drop the preserved file (whose max sequence is 3)
    // for lower thresholds, losing in-range rows, so reject explicitly.
    let err = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(0),
                memtable_max_sequence: Some(1),
                sst_min_sequence: Some(2),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .err()
        .expect("expected sequence-range unsupported error");
    assert!(
        matches!(err, Error::SequenceRangeUnsupported { .. }),
        "unexpected err: {err}"
    );
    assert_eq!(err.status_code(), StatusCode::Unsupported);

    // Without the hint the exact scan must still read the SST (no data loss).
    let stream = engine
        .scan_to_stream(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(0),
                memtable_max_sequence: Some(1),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(1, row_count, "expected seq 1 only");
}

/// Exact `(0, 1]` scans must not let the row-group-level `LastRow` shortcut drop
/// in-range rows: a series with seq 1 at t1 and seq 2 at t2 must return seq 1
/// (t1) instead of being reduced to seq 2 (t2) and then filtered out. Non-exact
/// `LastRow` scans keep their existing behavior (only the last row per series).
#[tokio::test]
async fn test_exact_sequence_read_with_last_row_selector_keeps_in_range_rows() {
    let mut env = TestEnv::with_prefix("test_exact_sequence_read_with_last_row_selector").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Same series/tag: seq 1 at t1 (field 1.0), seq 2 at t2 (field 2.0).
    let rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows_for_key("series", 1, 3, 1),
    };
    test_util::put_rows(&engine, region_id, rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    let scan = async |request: ScanRequest| -> String {
        let stream = engine.scan_to_stream(region_id, request).await.unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        batches.pretty_print().unwrap()
    };

    // Exact (0, 1] with the LastRow selector: seq 1 (t1) must survive the
    // row-level sequence filter and be selected as the last row.
    assert_eq!(
        "\
+--------+---------+---------------------+
| tag_0  | field_0 | ts                  |
+--------+---------+---------------------+
| series | 1.0     | 1970-01-01T00:00:01 |
+--------+---------+---------------------+",
        scan(ScanRequest {
            memtable_min_sequence: Some(0),
            memtable_max_sequence: Some(1),
            exact_sequence_range: true,
            series_row_selector: Some(TimeSeriesRowSelector::LastRow),
            ..Default::default()
        })
        .await
    );

    // Non-exact LastRow scan (no regression): still returns only the last row
    // of the series, seq 2 (t2).
    assert_eq!(
        "\
+--------+---------+---------------------+
| tag_0  | field_0 | ts                  |
+--------+---------+---------------------+
| series | 2.0     | 1970-01-01T00:00:02 |
+--------+---------+---------------------+",
        scan(ScanRequest {
            series_row_selector: Some(TimeSeriesRowSelector::LastRow),
            ..Default::default()
        })
        .await
    );
}

/// Exact delta spanning both SSTs and the memtable: rows after C that are partly
/// flushed and partly still in the memtable must all be returned, with the H
/// watermark respected.
#[tokio::test]
async fn test_exact_sequence_read_partial_sst_partial_memtable() {
    let mut env =
        TestEnv::with_prefix("test_exact_sequence_read_partial_sst_partial_memtable").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // seq 1..3 flushed into an SST.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, rows).await;
    test_util::flush_region(&engine, region_id, None).await;

    // seq 4..6 stay in the memtable.
    let rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(3, 6),
    };
    test_util::put_rows(&engine, region_id, rows).await;

    let scan_exact = async |min: Option<u64>, max: Option<u64>| -> String {
        let stream = engine
            .scan_to_stream(
                region_id,
                ScanRequest {
                    memtable_min_sequence: min,
                    memtable_max_sequence: max,
                    exact_sequence_range: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        batches.pretty_print().unwrap()
    };

    // The merged output order across SST + memtable sources is not guaranteed,
    // and each source batch repeats the pretty-printed header row, so compare
    // only the data rows (excluding header/separator lines).
    let result = scan_exact(Some(2), Some(6)).await;
    let mut rows = result
        .lines()
        .filter(|l| l.starts_with("| ") && !l.starts_with("| tag_0 "))
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        vec![
            "| 2     | 2.0     | 1970-01-01T00:00:02 |",
            "| 3     | 3.0     | 1970-01-01T00:00:03 |",
            "| 4     | 4.0     | 1970-01-01T00:00:04 |",
            "| 5     | 5.0     | 1970-01-01T00:00:05 |",
        ],
        rows,
        "unexpected set for (2, 6]:\n{result}"
    );
}

/// Builds a bulk insert request with rows `[start, end)` in the flat schema.
fn build_bulk_insert_request(
    region_id: RegionId,
    start: usize,
    end: usize,
) -> RegionBulkInsertsRequest {
    let schema = Arc::new(Schema::new(vec![
        Field::new("tag_0", DataType::Utf8, true),
        Field::new("field_0", DataType::Float64, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let tag = Arc::new(StringArray::from_iter_values(
        (start..end).map(|value| value.to_string()),
    )) as ArrayRef;
    let field = Arc::new(Float64Array::from_iter_values(
        (start..end).map(|value| value as f64),
    )) as ArrayRef;
    let ts = Arc::new(TimestampMillisecondArray::from_iter_values(
        (start..end).map(|value| value as i64 * 1000),
    )) as ArrayRef;
    let payload = DfRecordBatch::try_new(schema, vec![tag, field, ts]).unwrap();
    let (schema, record_batch) = encode_to_flight_data(payload.clone());

    RegionBulkInsertsRequest {
        region_id,
        payload,
        raw_data: ArrowIpc {
            schema: schema.data_header,
            data_header: record_batch.data_header,
            payload: record_batch.data_body,
        },
        partition_expr_version: None,
        aligned_schema_version: None,
    }
}

/// Bulk parts fold per-part sequences (commit-unit granularity): an exact range
/// keeps or drops each whole part. The same set must hold before flush, after
/// flush, and after compaction.
#[tokio::test]
async fn test_exact_sequence_read_bulk_parts() {
    let mut env = TestEnv::with_prefix("test_exact_sequence_read_bulk_parts").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .insert_option("memtable.type", "bulk")
        .build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Two bulk parts: [0,3) and [3,6). Each part carries one folded sequence.
    engine
        .handle_request(
            region_id,
            RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 0, 3)),
        )
        .await
        .unwrap();
    engine
        .handle_request(
            region_id,
            RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 3, 6)),
        )
        .await
        .unwrap();

    let scan_exact = async |min: Option<u64>, max: Option<u64>| -> String {
        let stream = engine
            .scan_to_stream(
                region_id,
                ScanRequest {
                    memtable_min_sequence: min,
                    memtable_max_sequence: max,
                    exact_sequence_range: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        batches.pretty_print().unwrap()
    };

    let expected_all = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
| 5     | 5.0     | 1970-01-01T00:00:05 |
+-------+---------+---------------------+";

    // A wide range covering every folded part sequence returns all rows.
    assert_eq!(expected_all, scan_exact(Some(0), Some(100)).await);

    // Commit-unit granularity: a lower bound that excludes the first part's
    // folded sequence drops the whole first part, keeps the second.
    let scan = scan_exact(Some(2), Some(100)).await;
    assert!(
        !scan.contains("| 0 ") && !scan.contains("| 1 ") && !scan.contains("| 2 "),
        "first bulk part should be fully excluded, got:\n{scan}"
    );
    assert!(scan.contains("| 3 "), "second bulk part missing:\n{scan}");

    test_util::flush_region(&engine, region_id, None).await;
    assert_eq!(scan_exact(Some(2), Some(100)).await, scan);

    engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();
    assert_eq!(scan_exact(Some(2), Some(100)).await, scan);
}

/// Bulk commit publication ordering: the committed sequence must never cover a
/// bulk part before its rows are physically installed in the memtable. A scan
/// opening in the window between ordinary-memtable handling and bulk
/// installation binds H to the pre-bulk committed sequence, sees no bulk rows,
/// and a follow-up exact scan over the fresh range returns the bulk rows once.
#[tokio::test]
async fn test_bulk_write_sequence_not_committed_before_install() {
    let mut env =
        TestEnv::with_prefix("test_bulk_write_sequence_not_committed_before_install").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .insert_option("memtable.type", "bulk")
        .build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let committed_sequence = || {
        engine
            .find_region(region_id)
            .unwrap()
            .find_committed_sequence()
    };
    assert_eq!(0, committed_sequence());

    let mut barrier = crate::region_write_ctx::test_hooks::arm_bulk_install_barrier(
        region_id,
        engine
            .get_region(region_id)
            .unwrap()
            .version_control
            .clone(),
    );

    // Start a bulk write in the background; it pauses before installation.
    let engine_clone = engine.clone();
    let write_handle = tokio::spawn(async move {
        engine_clone
            .handle_request(
                region_id,
                RegionRequest::BulkInserts(build_bulk_insert_request(region_id, 0, 3)),
            )
            .await
            .unwrap();
    });

    // Wait until the write paused between memtable handling and bulk install.
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        barrier.wait_until_reached(),
    )
    .await
    .expect("bulk write never reached the install barrier");

    // Open the snapshot-bound scanner while the write is still paused at the
    // barrier: it must bind H to the pre-bulk committed sequence (0) because
    // the bulk part is not installed yet.
    let scanner = engine
        .scanner(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(0),
                snapshot_on_scan: true,
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        Some(0),
        scanner.snapshot_sequence(),
        "snapshot-bound scan must bind H to the pre-bulk committed sequence"
    );

    // The committed sequence must still be the pre-bulk value: publication
    // happens strictly after the bulk part is in the memtable.
    assert_eq!(
        0,
        committed_sequence(),
        "committed sequence leaked before the bulk part was installed"
    );

    // Release the barrier: the bulk part installs and the committed sequence
    // advances to cover it.
    barrier.release();
    write_handle.await.expect("bulk write should complete");
    assert_eq!(3, committed_sequence());

    // The scanner opened while paused stays bound at the pre-bulk H and so
    // sees no bulk rows.
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        0,
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        "bulk rows visible to a snapshot bound before installation:\n{}",
        batches.pretty_print().unwrap()
    );

    // The follow-up exact scan over (pre_H, post_H] returns the bulk rows once.
    let stream = engine
        .scan_to_stream(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(0),
                memtable_max_sequence: Some(3),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(
        3,
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        "bulk rows must be returned exactly once:\n{}",
        batches.pretty_print().unwrap()
    );
    let pretty = batches.pretty_print().unwrap();
    for tag in ["0", "1", "2"] {
        assert!(
            pretty.contains(&format!("| {tag} ")),
            "bulk row {tag} missing from (0, 3]:\n{pretty}"
        );
    }
}

/// Compaction must not launder a legacy (unmarked) input SST into a trusted
/// output: with the region option ON but an input lacking the preserved-sequence
/// marker, the rewritten output must stay unmarked and exact scans must keep
/// returning the structured unsupported error (never silent data).
#[tokio::test]
async fn test_compaction_output_not_laundered_from_legacy_input() {
    let mut env =
        TestEnv::with_prefix("test_compaction_output_not_laundered_from_legacy_input").await;
    // Suppress automatic compactions (flush- and edit-triggered) so the
    // explicit Compact below is the only compaction in flight (deterministic).
    let engine = env
        .create_engine(MitoConfig {
            min_compaction_interval: std::time::Duration::from_secs(60 * 60),
            schedule_compaction_after_edit: false,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "2")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Two real marked SSTs on disk so the twcs compaction rewrites them.
    for (start, end) in [(0, 3), (3, 6)] {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(start, end),
        };
        test_util::put_rows(&engine, region_id, rows).await;
        test_util::flush_region(&engine, region_id, None).await;
    }

    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let level0 = &version.ssts.levels()[0].files;
    assert_eq!(2, level0.len());
    let first_file = level0
        .values()
        .next()
        .expect("flushed SST")
        .meta_ref()
        .clone();
    assert!(first_file.preserve_row_sequence);

    // Seed the first physical file as a legacy (unmarked) file: replace the
    // version's FileMeta so the marker is false while the file stays on disk.
    let mut unmarked = first_file.clone();
    unmarked.preserve_row_sequence = false;
    engine
        .edit_region(
            region_id,
            RegionEdit {
                files_to_add: vec![unmarked],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            },
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let level0 = &version.ssts.levels()[0].files;
    assert_eq!(2, level0.len());
    assert!(
        level0
            .values()
            .any(|file| !file.meta_ref().preserve_row_sequence),
        "seeded file should be unmarked"
    );

    // Compact: the rewritten output must NOT be laundered back to marked.
    engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let outputs = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .collect::<Vec<_>>();
    assert_eq!(
        1,
        outputs.len(),
        "two inputs should rewrite into one output file"
    );
    assert!(
        !outputs[0].meta_ref().preserve_row_sequence,
        "legacy input must not be laundered into a marked output"
    );

    // The compacted output remains unmarked. Its sequence may have been
    // synthesized by the region-edit handler, so exact scans fail closed for
    // any lower bound.
    for (min, max) in [(7, 8), (6, 7)] {
        let err = engine
            .scanner(
                region_id,
                ScanRequest {
                    memtable_min_sequence: Some(min),
                    memtable_max_sequence: Some(max),
                    exact_sequence_range: true,
                    ..Default::default()
                },
            )
            .await
            .err()
            .expect("expected sequence-range unsupported error for unmarked file");
        assert!(
            matches!(err, Error::SequenceRangeUnsupported { .. }),
            "unexpected err: {err}"
        );
    }
}

/// Multi-input primary-key-format compaction: two preserved pk-format files
/// rewrite into one output that still carries the marker, and an exact scan
/// over the compacted output returns every row in range.
#[tokio::test]
async fn test_exact_sequence_read_pk_format_compaction_multiple_inputs() {
    let mut env =
        TestEnv::with_prefix("test_exact_sequence_read_pk_format_compaction_multiple_inputs").await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .insert_option("sst_format", "primary_key")
        .insert_option("compaction.type", "twcs")
        .insert_option("compaction.twcs.trigger_file_num", "2")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Two preserved pk-format input files: seq 1..3 and seq 4..6.
    for (start, end) in [(0, 3), (3, 6)] {
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: test_util::build_rows(start, end),
        };
        test_util::put_rows(&engine, region_id, rows).await;
        test_util::flush_region(&engine, region_id, None).await;
    }

    engine
        .handle_request(
            region_id,
            RegionRequest::Compact(RegionCompactRequest::default()),
        )
        .await
        .unwrap();

    // The rewritten output still carries the preserved-sequence marker.
    let region = engine.get_region(region_id).unwrap();
    let version = region.version();
    let outputs = version
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files.values())
        .collect::<Vec<_>>();
    assert_eq!(
        1,
        outputs.len(),
        "two pk inputs should rewrite into one output file"
    );
    assert!(outputs[0].meta_ref().preserve_row_sequence);

    // Exact scan over the compacted pk-format output returns all rows in range.
    let stream = engine
        .scan_to_stream(
            region_id,
            ScanRequest {
                memtable_min_sequence: Some(1),
                memtable_max_sequence: Some(5),
                exact_sequence_range: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let pretty = batches.pretty_print().unwrap();
    for tag in ["1", "2", "3", "4"] {
        assert!(
            pretty.contains(&format!("| {tag} ")),
            "row {tag} missing from compacted pk output:\n{pretty}"
        );
    }
    assert_eq!(
        4,
        batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        "exact range rows:\n{pretty}"
    );
    assert!(
        !pretty.contains("| 0 ") && !pretty.contains("| 5 "),
        "rows outside (1, 5] leaked:\n{pretty}"
    );
}

/// Exact sequence-range reads through the active production `PerSeries` series
/// scan path: row-level filtering must hold across the flushed SST and the
/// memtable regardless of the requested time-series distribution.
#[tokio::test]
async fn test_exact_sequence_read_series_scan_per_series() {
    let mut env = TestEnv::with_prefix("test_exact_sequence_read_series_scan_per_series").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: true,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // seq 1..3 flushed into an SST, seq 4..6 stay in the memtable.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: test_util::build_rows(0, 3),
    };
    test_util::put_rows(&engine, region_id, rows).await;
    test_util::flush_region(&engine, region_id, None).await;
    let rows = Rows {
        schema: column_schemas,
        rows: test_util::build_rows(3, 6),
    };
    test_util::put_rows(&engine, region_id, rows).await;

    let scan_exact_series = async |min: Option<u64>, max: Option<u64>| -> String {
        let stream = engine
            .scan_to_stream(
                region_id,
                ScanRequest {
                    memtable_min_sequence: min,
                    memtable_max_sequence: max,
                    exact_sequence_range: true,
                    distribution: Some(TimeSeriesDistribution::PerSeries),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        batches.pretty_print().unwrap()
    };

    let result = scan_exact_series(Some(2), Some(6)).await;
    let mut rows = result
        .lines()
        .filter(|l| l.starts_with("| ") && !l.starts_with("| tag_0 "))
        .collect::<Vec<_>>();
    rows.sort_unstable();
    assert_eq!(
        vec![
            "| 2     | 2.0     | 1970-01-01T00:00:02 |",
            "| 3     | 3.0     | 1970-01-01T00:00:03 |",
            "| 4     | 4.0     | 1970-01-01T00:00:04 |",
            "| 5     | 5.0     | 1970-01-01T00:00:05 |",
        ],
        rows,
        "unexpected set for (2, 6] on PerSeries path:\n{result}"
    );
}

/// Range-cache fingerprint: identical files and filters with different (C, H]
/// sequence ranges must never share a cache entry, otherwise the second scan
/// would replay the first scan's filtered rows.
#[tokio::test]
async fn test_range_cache_key_separates_sequence_ranges() {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: true,
            // Explicitly enable the range result cache: the sharing bug only
            // reproduces when the second scan can replay the first scan's
            // cached batches.
            range_result_cache_size: ReadableSize::mb(64),
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("append_mode", "true")
        .insert_option("preserve_row_sequence", "true")
        .build();
    let column_schemas = test_util::rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Single flushed flat SST with rows seq 1..6.
    test_util::put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas,
            rows: test_util::build_rows(0, 6),
        },
    )
    .await;
    test_util::flush_region(&engine, region_id, None).await;

    let tag_filter = || col("tag_0").gt_eq(lit(ScalarValue::Utf8(Some("0".to_string()))));
    let scan_exact = async |min: Option<u64>, max: Option<u64>| -> Vec<String> {
        let stream = engine
            .scan_to_stream(
                region_id,
                ScanRequest {
                    filters: vec![tag_filter()],
                    memtable_min_sequence: min,
                    memtable_max_sequence: max,
                    exact_sequence_range: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let batches = RecordBatches::try_collect(stream).await.unwrap();
        let mut rows = batches
            .pretty_print()
            .unwrap()
            .lines()
            .filter(|l| l.starts_with("| ") && !l.starts_with("| tag_0 "))
            .map(|l| l.to_string())
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    };

    // (1, 3] -> rows with sequences 2..3: tags "1" and "2".
    let first = scan_exact(Some(1), Some(3)).await;
    assert_eq!(
        vec![
            "| 1     | 1.0     | 1970-01-01T00:00:01 |",
            "| 2     | 2.0     | 1970-01-01T00:00:02 |",
        ],
        first
    );

    // (3, 4] -> row with sequence 4: tag "3" only. If the cache key ignored
    // the sequence range, this would replay the (1, 3] cached rows instead.
    let second = scan_exact(Some(3), Some(4)).await;
    assert_eq!(
        vec!["| 3     | 3.0     | 1970-01-01T00:00:03 |"],
        second,
        "different (C, H] shared a range-cache entry"
    );
}
