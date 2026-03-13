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

use api::v1::value::ValueData;
use api::v1::{Row, Rows};
use common_recordbatch::RecordBatches;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{CreateRequestBuilder, TestEnv, flush_region, put_rows, rows_schema};

/// Build rows for multiple tags and fields.
fn build_rows_multi_tags_fields(
    tags: &[&str],
    field_starts: &[usize],
    ts_range: (usize, usize),
) -> Vec<Row> {
    (ts_range.0..ts_range.1)
        .enumerate()
        .map(|(idx, ts)| {
            let mut values = Vec::with_capacity(tags.len() + field_starts.len() + 1);
            for tag in tags {
                values.push(api::v1::Value {
                    value_data: Some(ValueData::StringValue(tag.to_string())),
                });
            }
            for field_start in field_starts {
                values.push(api::v1::Value {
                    value_data: Some(ValueData::F64Value((field_start + idx) as f64)),
                });
            }
            values.push(api::v1::Value {
                value_data: Some(ValueData::TimestampMillisecondValue(ts as i64 * 1000)),
            });

            api::v1::Row { values }
        })
        .collect()
}

/// Build rows for fields only (no tags).
fn build_rows_fields_only(field_starts: &[usize], ts_range: (usize, usize)) -> Vec<Row> {
    (ts_range.0..ts_range.1)
        .enumerate()
        .map(|(idx, ts)| {
            let mut values = Vec::with_capacity(field_starts.len() + 1);
            for field_start in field_starts {
                values.push(api::v1::Value {
                    value_data: Some(ValueData::F64Value((field_start + idx) as f64)),
                });
            }
            values.push(api::v1::Value {
                value_data: Some(ValueData::TimestampMillisecondValue(ts as i64 * 1000)),
            });

            api::v1::Row { values }
        })
        .collect()
}

#[tokio::test]
async fn test_scan_projection() {
    test_scan_projection_with_format(false).await;
    test_scan_projection_with_format(true).await;
}

async fn test_scan_projection_with_format(flat_format: bool) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    // [tag_0, tag_1, field_0, field_1, ts]
    let request = CreateRequestBuilder::new().tag_num(2).field_num(2).build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_multi_tags_fields(&["a", "b"], &[0, 10], (0, 3)),
    };
    put_rows(&engine, region_id, rows).await;

    // Scans tag_1, field_1, ts
    let request = ScanRequest {
        projection: Some(vec![1, 3, 4]),
        filters: Vec::new(),
        ..Default::default()
    };
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_1 | field_1 | ts                  |
+-------+---------+---------------------+
| b     | 10.0    | 1970-01-01T00:00:00 |
| b     | 11.0    | 1970-01-01T00:00:01 |
| b     | 12.0    | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_scan_projection_without_primary_key() {
    common_telemetry::init_default_ut_logging();

    test_scan_projection_without_primary_key_with_format(false).await;
    test_scan_projection_without_primary_key_with_format(true).await;
}

async fn test_scan_projection_without_primary_key_with_format(flat_format: bool) {
    common_telemetry::info!(
        "Test projection without pk start, flat format: {}",
        flat_format
    );

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    // [field_0, field_1, field_2, ts] - no primary key
    let request = CreateRequestBuilder::new().tag_num(0).field_num(3).build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Insert first batch and flush to SST #1 (ts: 0, 1, 2)
    // field_starts: [0, 10, 20] → field_0: 0,1,2, field_1: 10,11,12, field_2: 20,21,22
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_fields_only(&[0, 10, 20], (0, 3)),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    // Insert second batch and flush to SST #2 (ts: 3, 4, 5)
    // field_starts: [3, 13, 23] → field_0: 3,4,5, field_1: 13,14,15, field_2: 23,24,25
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_fields_only(&[3, 13, 23], (3, 6)),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    // Insert third batch, keep in memtable (ts: 6, 7, 8)
    // field_starts: [6, 16, 26] → field_0: 6,7,8, field_1: 16,17,18, field_2: 26,27,28
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_fields_only(&[6, 16, 26], (6, 9)),
    };
    put_rows(&engine, region_id, rows).await;

    // Scan with projection on field_0 and field_1, filter ts >= 2s
    let request = ScanRequest {
        projection: Some(vec![0, 1]), // field_0 and field_1 (not ts)
        filters: vec![col("ts").gt_eq(lit(ScalarValue::TimestampMillisecond(Some(2000), None)))],
        ..Default::default()
    };
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    // Filter ts >= 2s returns 7 rows:
    // - ts=2 from SST#1: field_0=2, field_1=12
    // - ts=3,4,5 from SST#2: field_0=3,4,5, field_1=13,14,15
    // - ts=6,7,8 from memtable: field_0=6,7,8, field_1=16,17,18
    let expected = "\
+---------+---------+
| field_0 | field_1 |
+---------+---------+
| 2.0     | 12.0    |
| 3.0     | 13.0    |
| 4.0     | 14.0    |
| 5.0     | 15.0    |
| 6.0     | 16.0    |
| 7.0     | 17.0    |
| 8.0     | 18.0    |
+---------+---------+";
    assert_eq!(
        expected,
        sort_batches_and_print(&batches, &["field_0", "field_1"])
    );

    common_telemetry::info!(
        "Test projection without pk success, flat format: {}",
        flat_format
    );
}
