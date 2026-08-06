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

use api::v1::Rows;
use api::v1::helper::row;
use api::v1::value::ValueData;
use common_base::readable_size::ReadableSize;
use common_recordbatch::RecordBatches;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest, TimeSeriesRowSelector};

use crate::config::MitoConfig;
use crate::engine::MitoEngine;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_delete_rows_for_key, build_rows_for_key, delete_rows,
    delete_rows_schema, flush_region, put_rows, rows_schema,
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

/// Build rows with a NaN (stale marker) field for `key` in `[start, end)`.
fn build_stale_rows_for_key(key: &str, start: usize, end: usize) -> Vec<api::v1::Row> {
    (start..end)
        .map(|ts| {
            row(vec![
                ValueData::StringValue(key.to_string()),
                ValueData::F64Value(f64::NAN),
                ValueData::TimestampMillisecondValue(ts as i64 * 1000),
            ])
        })
        .collect()
}

/// Helper to create a timestamp millisecond literal.
fn ts_millis_lit(val: i64) -> datafusion_expr::Expr {
    lit(ScalarValue::TimestampMillisecond(Some(val), None))
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

#[tokio::test]
async fn test_last_row_same_timestamp_across_ssts_prefers_newer_sequence() {
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
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // SST 1: key a, ts 0..3 (values 0, 1, 2).
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 3, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;
    // SST 2 (newer sequence): key a, ts 1..3 (values 10, 11). The maximum
    // timestamp 2 exists in both SSTs, so the row with the higher sequence
    // must win after the last-row scan.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_for_key("a", 1, 3, 10),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    let result = scan_last_row(&engine, region_id, vec![]).await;
    assert_eq!(
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 11.0    | 1970-01-01T00:00:02 |
+-------+---------+---------------------+",
        result
    );
}

#[tokio::test]
async fn test_last_row_returns_stale_marker_row() {
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
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Normal values at ts 0..3.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 3, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;
    // Stale markers (NaN) overwrite ts 2 and add ts 3. The storage layer must
    // return the stale row as the last row; staleness is judged upstream by
    // InstantManipulate.
    let rows = Rows {
        schema: column_schemas,
        rows: build_stale_rows_for_key("a", 2, 4),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    let result = scan_last_row(&engine, region_id, vec![]).await;
    assert_eq!(
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | NaN     | 1970-01-01T00:00:03 |
+-------+---------+---------------------+",
        result
    );
}

#[tokio::test]
async fn test_last_row_after_delete_returns_nothing() {
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
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    let delete_schema = delete_rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Put rows at ts 0..3, then delete the whole series.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("a", 0, 3, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    let rows = Rows {
        schema: delete_schema,
        rows: build_delete_rows_for_key("a", 0, 3),
    };
    delete_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    let result = scan_last_row(&engine, region_id, vec![]).await;
    assert_eq!(
        "\
+-------+---------+----+
| tag_0 | field_0 | ts |
+-------+---------+----+
+-------+---------+----+",
        result
    );
}

#[tokio::test]
async fn test_last_row_respects_lookback_left_boundary() {
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
    let request = CreateRequestBuilder::new().build();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // key a, ts 0..5 (values 0..4).
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows_for_key("a", 0, 5, 0),
    };
    put_rows(&engine, region_id, rows).await;
    flush_region(&engine, region_id, None).await;

    // An instant evaluation at 4s with a 2s lookback reads [2s, 4s]; samples
    // before the left boundary must not match even though they are the last
    // rows of the series overall.
    let result = scan_last_row(
        &engine,
        region_id,
        vec![
            col("ts").gt_eq(ts_millis_lit(2000)),
            col("ts").lt_eq(ts_millis_lit(4000)),
        ],
    )
    .await;
    assert_eq!(
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| a     | 4.0     | 1970-01-01T00:00:04 |
+-------+---------+---------------------+",
        result
    );
}
