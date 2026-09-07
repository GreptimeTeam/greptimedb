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
use api::v1::{Rows, Value};
use common_base::readable_size::ReadableSize;
use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use common_recordbatch::RecordBatches;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use datatypes::arrow::array::{Float64Array, StringArray, TimestampMillisecondArray};
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest, TimeSeriesRowSelector};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::read::scan_region::Scanner;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_delete_rows_for_key, build_rows_for_key, delete_rows,
    flush_region, put_rows, rows_schema,
};

async fn test_last_row(append_mode: bool, flat_format: bool) {
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
    let mut request_builder =
        CreateRequestBuilder::new().insert_option("append_mode", &append_mode.to_string());
    if flat_format {
        request_builder = request_builder.insert_option("sst_format", "flat");
    }
    let request = request_builder.build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Flush 3 SSTs.
    // a, field 1, 2
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 1, 3, 1),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;
    // a, field 0, 1
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;
    // b, field 0, 1
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 2, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    // Memtable.
    // a, field 2, 3
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_for_key("a", 2, 4, 2),
    };
    put_rows(&engine, region_id, rows).await;

    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 3.0     | 1970-01-01T00:00:03 |
| b     | 1.0     | 1970-01-01T00:00:01 |
+-------+---------+---------------------+";
    // Scans in parallel.
    let scanner = engine
        .scanner(
            region_id,
            ScanRequest {
                series_row_selector: Some(TimeSeriesRowSelector::LastRow),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(3, scanner.num_files());
    assert_eq!(1, scanner.num_memtables());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));
}

async fn scan_last_row(
    engine: &MitoEngine,
    region_id: RegionId,
    filters: Vec<datafusion_expr::Expr>,
) -> String {
    let scanner = engine
        .scanner(
            region_id,
            ScanRequest {
                filters,
                series_row_selector: Some(TimeSeriesRowSelector::LastRow),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    sort_batches_and_print(&batches, &["tag_0", "ts"])
}

async fn new_flat_last_row_engine(
    selector_result_cache_size: ReadableSize,
) -> (TestEnv, MitoEngine, RegionId) {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            selector_result_cache_size,
            ..Default::default()
        })
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
        .insert_option("sst_format", "flat")
        .build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_for_key("a", 0, 11, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, Some(16)).await;

    (env, engine, region_id)
}

fn mixed_time_filters() -> Vec<datafusion_expr::Expr> {
    let ts_lit = |value| lit(ScalarValue::TimestampMillisecond(Some(value), None));
    vec![col("ts").gt_eq(ts_lit(0)), col("ts").not_eq(ts_lit(10_000))]
}

const LAST_ROW_AT_NINE: &str = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 9.0     | 1970-01-01T00:00:09 |
+-------+---------+---------------------+";

const LAST_ROW_AT_TEN: &str = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 10.0    | 1970-01-01T00:00:10 |
+-------+---------+---------------------+";

async fn new_merge_last_row_engine(
    flat_format: bool,
) -> (
    TestEnv,
    MitoEngine,
    RegionId,
    Vec<api::v1::ColumnSchema>,
    Vec<api::v1::ColumnSchema>,
) {
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
    let sst_format = if flat_format { "flat" } else { "primary_key" };
    let request = CreateRequestBuilder::new()
        .insert_option("sst_format", sst_format)
        .build();
    let schema = rows_schema(&request);
    let delete_schema = crate::test_util::delete_rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();
    (env, engine, region_id, schema, delete_schema)
}

fn value_row(key: &str, value: f64, timestamp: i64) -> api::v1::Row {
    api::v1::Row {
        values: vec![
            Value {
                value_data: Some(ValueData::StringValue(key.to_string())),
            },
            Value {
                value_data: Some(ValueData::F64Value(value)),
            },
            Value {
                value_data: Some(ValueData::TimestampMillisecondValue(timestamp)),
            },
        ],
    }
}

async fn last_row_scanner(engine: &MitoEngine, region_id: RegionId) -> Scanner {
    engine
        .scanner(
            region_id,
            ScanRequest {
                series_row_selector: Some(TimeSeriesRowSelector::LastRow),
                ..Default::default()
            },
        )
        .await
        .unwrap()
}

async fn scan_last_row_batches(scanner: &Scanner) -> RecordBatches {
    RecordBatches::try_collect(scanner.scan().await.unwrap())
        .await
        .unwrap()
}

fn last_row_values(batches: &RecordBatches) -> Vec<(String, f64, i64)> {
    let mut rows = Vec::new();
    for batch in batches.iter() {
        let batch = batch.df_record_batch();
        let tags = batch
            .column_by_name("tag_0")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let fields = batch
            .column_by_name("field_0")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let timestamps = batch
            .column_by_name("ts")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                tags.value(index).to_string(),
                fields.value(index),
                timestamps.value(index),
            ));
        }
    }
    rows.sort_by(|left, right| left.0.cmp(&right.0));
    rows
}

#[tokio::test]
async fn test_last_row_merge_deduplicates_same_timestamp_across_ssts() {
    for flat_format in [false, true] {
        let (_env, engine, region_id, schema, _delete_schema) =
            new_merge_last_row_engine(flat_format).await;

        put_rows(
            &engine,
            region_id,
            Rows {
                schema: schema.clone(),
                rows: vec![value_row("a", 1.0, 1000)],
            },
        )
        .await;
        flush_region(&engine, region_id, None).await;
        put_rows(
            &engine,
            region_id,
            Rows {
                schema,
                rows: vec![value_row("a", 2.0, 1000)],
            },
        )
        .await;
        flush_region(&engine, region_id, None).await;

        let scanner = last_row_scanner(&engine, region_id).await;
        assert_eq!(2, scanner.num_files());
        assert_eq!(0, scanner.num_memtables());
        assert_eq!(
            vec![("a".to_string(), 2.0, 1000)],
            last_row_values(&scan_last_row_batches(&scanner).await)
        );
    }
}

#[tokio::test]
async fn test_last_row_returns_stale_marker_and_preserves_ordinary_nan() {
    for flat_format in [false, true] {
        let (_env, engine, region_id, schema, _delete_schema) =
            new_merge_last_row_engine(flat_format).await;
        put_rows(
            &engine,
            region_id,
            Rows {
                schema: schema.clone(),
                rows: vec![value_row("a", 1.0, 1000), value_row("b", f64::NAN, 1000)],
            },
        )
        .await;
        flush_region(&engine, region_id, None).await;
        put_rows(
            &engine,
            region_id,
            Rows {
                schema,
                rows: vec![value_row(
                    "a",
                    f64::from_bits(PROMETHEUS_STALE_NAN_BITS),
                    1000,
                )],
            },
        )
        .await;
        flush_region(&engine, region_id, None).await;

        let scanner = last_row_scanner(&engine, region_id).await;
        assert_eq!(2, scanner.num_files());
        assert_eq!(0, scanner.num_memtables());
        let values = last_row_values(&scan_last_row_batches(&scanner).await);
        assert_eq!(2, values.len(), "unexpected LastRow values: {values:?}");
        assert_eq!("a", values[0].0);
        assert_eq!(PROMETHEUS_STALE_NAN_BITS, values[0].1.to_bits());
        assert_eq!(1000, values[0].2);
        assert_eq!("b", values[1].0);
        assert!(values[1].1.is_nan());
        assert_ne!(PROMETHEUS_STALE_NAN_BITS, values[1].1.to_bits());
    }
}

#[tokio::test]
async fn test_last_row_delete_wins_across_ssts() {
    for flat_format in [false, true] {
        let (_env, engine, region_id, schema, delete_schema) =
            new_merge_last_row_engine(flat_format).await;
        put_rows(
            &engine,
            region_id,
            Rows {
                schema,
                rows: vec![value_row("a", 1.0, 1000)],
            },
        )
        .await;
        flush_region(&engine, region_id, None).await;
        delete_rows(
            &engine,
            region_id,
            Rows {
                schema: delete_schema,
                rows: build_delete_rows_for_key("a", 1, 2),
            },
        )
        .await;
        flush_region(&engine, region_id, None).await;

        let scanner = last_row_scanner(&engine, region_id).await;
        assert_eq!(2, scanner.num_files());
        assert_eq!(0, scanner.num_memtables());
        assert!(last_row_values(&scan_last_row_batches(&scanner).await).is_empty());
    }
}

#[tokio::test]
async fn test_last_row_empty_region_returns_empty() {
    let (_env, engine, region_id, _schema, _delete_schema) = new_merge_last_row_engine(false).await;
    let scanner = last_row_scanner(&engine, region_id).await;
    assert!(last_row_values(&scan_last_row_batches(&scanner).await).is_empty());
}

#[tokio::test]
async fn test_last_row_append_mode_disabled() {
    test_last_row(false, false).await;
}

#[tokio::test]
async fn test_last_row_append_mode_enabled() {
    test_last_row(true, false).await;
}

#[tokio::test]
async fn test_last_row_flat_format_append_mode_disabled() {
    test_last_row(false, true).await;
}

#[tokio::test]
async fn test_last_row_flat_format_append_mode_enabled() {
    test_last_row(true, true).await;
}

#[tokio::test]
async fn test_last_row_flat_format_non_tag_filter_without_selector_cache() {
    let (_env, engine, region_id) = new_flat_last_row_engine(ReadableSize(0)).await;

    let filtered = scan_last_row(&engine, region_id, mixed_time_filters()).await;
    assert_eq!(LAST_ROW_AT_NINE, filtered);
}

#[tokio::test]
async fn test_last_row_flat_format_non_tag_filter_does_not_reuse_selector_cache() {
    let (_env, engine, region_id) = new_flat_last_row_engine(ReadableSize::mb(1)).await;

    let unfiltered = scan_last_row(&engine, region_id, vec![]).await;
    assert_eq!(LAST_ROW_AT_TEN, unfiltered);

    let filtered = scan_last_row(&engine, region_id, mixed_time_filters()).await;
    assert_eq!(LAST_ROW_AT_NINE, filtered);

    let unfiltered = scan_last_row(&engine, region_id, vec![]).await;
    assert_eq!(LAST_ROW_AT_TEN, unfiltered);
}

#[tokio::test]
async fn test_last_row_flat_format_prefilter_does_not_poison_selector_cache() {
    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            selector_result_cache_size: ReadableSize::mb(1),
            ..Default::default()
        })
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
        .insert_option("sst_format", "flat")
        .build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: [
            build_rows_for_key("a", 0, 3, 0),
            build_rows_for_key("b", 0, 3, 10),
        ]
        .concat(),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, Some(16)).await;

    let filtered = scan_last_row(&engine, region_id, vec![col("tag_0").eq(lit("a"))]).await;
    assert_eq!(
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+",
        filtered
    );

    let unfiltered = scan_last_row(&engine, region_id, vec![]).await;
    assert_eq!(
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 2.0     | 1970-01-01T00:00:02 |
| b     | 12.0    | 1970-01-01T00:00:02 |
+-------+---------+---------------------+",
        unfiltered
    );
}
