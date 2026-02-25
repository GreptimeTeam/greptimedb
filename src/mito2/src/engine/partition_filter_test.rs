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

//! Tests for partition expression filtering in SST readers.

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use datatypes::value::Value;
use partition::expr::col;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{
    EnterStagingRequest, RegionFlushRequest, RegionRequest, StagingPartitionDirective,
};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

/// Helper to create a partition expression for testing.
/// Creates `col_name >= start AND col_name < end`.
fn range_expr_string(col_name: &str, start: f64, end: f64) -> String {
    col(col_name)
        .gt_eq(Value::Float64(start.into()))
        .and(col(col_name).lt(Value::Float64(end.into())))
        .as_json_str()
        .unwrap()
}

#[tokio::test]
async fn test_partition_filter_basic() {
    test_partition_filter_basic_with_format(false).await;
    test_partition_filter_basic_with_format(true).await;
}

/// Test that partition filter correctly filters rows from SST.
///
/// This test:
/// 1. Creates a region with partition expr `tag_0 >= "0"` (covers all data)
/// 2. Writes data (tag_0 = "0"..="9") and flushes - SST has old partition expr
/// 3. Enters staging mode with partition expr `tag_0 >= "5"` (narrower)
/// 4. Writes more data and flushes in staging mode
/// 5. Exits staging mode - region now has new partition expr
/// 6. Scans all data - old SST data not matching new expr should be filtered
async fn test_partition_filter_basic_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1024, 0);

    // Create region with initial partition expr: tag_0 >= "0" (covers all data)
    let initial_partition_expr = range_expr_string("field_0", 0., 99.);
    let request = CreateRequestBuilder::new()
        .partition_expr_json(Some(initial_partition_expr))
        .build();
    let column_schemas = rows_schema(&request);

    // Create region
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Write initial data (tag_0 = "0".."4") and flush to SST
    // This SST will have partition_expr = "tag_0 >= 0"
    let rows_data = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 5),
    };
    put_rows(&engine, region_id, rows_data).await;

    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    // Enter staging mode with narrower partition expr: tag_0 >= "5"
    // After staging, region will only accept data where tag_0 >= "5"
    let new_partition_expr = range_expr_string("field_0", 5., 99.);
    engine
        .handle_request(
            region_id,
            RegionRequest::EnterStaging(EnterStagingRequest {
                partition_directive: StagingPartitionDirective::UpdatePartitionExpr(
                    new_partition_expr.clone(),
                ),
            }),
        )
        .await
        .unwrap();

    // Write data in staging mode (tag_0 = "5".."11")
    let rows_data = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(5, 11),
    };
    put_rows(&engine, region_id, rows_data).await;

    // Flush in staging mode
    engine
        .handle_request(
            region_id,
            RegionRequest::Flush(RegionFlushRequest {
                row_group_size: None,
            }),
        )
        .await
        .unwrap();

    // Scan data in staging mode - should only see initial 5 rows (staging SST not visible)
    let request = ScanRequest {
        projection: Some(vec![1]),
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let total_rows: usize = batches.iter().map(|rb| rb.num_rows()).sum();
    assert_eq!(
        total_rows, 5,
        "Should see 5 rows before exiting staging mode"
    );

    // Exit staging mode
    use store_api::region_engine::SettableRegionRoleState;
    engine
        .set_region_role_state_gracefully(region_id, SettableRegionRoleState::Leader)
        .await
        .unwrap();

    // Scan after exiting staging - the old SST (tag_0 = "0".."4") should have
    // rows filtered by partition expr (tag_0 >= "5"), which means none of them pass.
    // But the staging SST (tag_0 = "5".."10") satisfies the partition expr.
    let request = ScanRequest {
        projection: Some(vec![1]),
        ..Default::default()
    };
    let scanner = engine.scanner(region_id, request).await.unwrap();
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let total_rows: usize = batches.iter().map(|rb| rb.num_rows()).sum();
    // After exit:
    // - Old SST (rows 0-4): partition expr is "tag_0 >= 5", so these are filtered out
    // - Staging SST (rows 5-10): These satisfy partition expr, so they're visible
    assert_eq!(
        total_rows, 6,
        "Should see 6 rows after exiting staging (rows 5-10 from staging SST), flat_format: {}",
        flat_format
    );
}
