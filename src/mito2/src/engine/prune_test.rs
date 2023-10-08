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
use common_query::logical_plan::DfExpr;
use common_query::prelude::Expr;
use common_recordbatch::RecordBatches;
use datafusion_common::ScalarValue;
use datafusion_expr::lit;
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::test_util::{
    build_rows, flush_region, put_rows, rows_schema, CreateRequestBuilder, TestEnv,
};

async fn check_prune_row_groups(expr: DfExpr, expected: &str) {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();

    let column_schemas = rows_schema(&request);

    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 10),
        },
    )
    .await;
    flush_region(&engine, region_id, Some(5)).await;

    let stream = engine
        .handle_query(
            region_id,
            ScanRequest {
                filters: vec![Expr::from(expr)],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, batches.pretty_print().unwrap());
}

#[tokio::test]
async fn test_read_parquet_stats() {
    common_telemetry::init_default_ut_logging();

    check_prune_row_groups(
        datafusion_expr::col("ts").gt(lit(ScalarValue::TimestampMillisecond(Some(4000), None))),
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
| 8     | 8.0     | 1970-01-01T00:00:08 |
| 9     | 9.0     | 1970-01-01T00:00:09 |
+-------+---------+---------------------+",
    )
    .await;

    check_prune_row_groups(
        datafusion_expr::col("tag_0").gt(lit(ScalarValue::Utf8(Some("4".to_string())))),
        "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 5     | 5.0     | 1970-01-01T00:00:05 |
| 6     | 6.0     | 1970-01-01T00:00:06 |
| 7     | 7.0     | 1970-01-01T00:00:07 |
| 8     | 8.0     | 1970-01-01T00:00:08 |
| 9     | 9.0     | 1970-01-01T00:00:09 |
+-------+---------+---------------------+",
    )
    .await;
}
