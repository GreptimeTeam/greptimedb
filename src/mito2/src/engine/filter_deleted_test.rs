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
use common_recordbatch::RecordBatches;
use store_api::region_engine::RegionEngine;
use store_api::region_request::RegionRequest;
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{
    CreateRequestBuilder, TestEnv, build_rows, delete_rows, flush_region, put_rows, rows_schema,
};

#[tokio::test]
async fn test_scan_without_filtering_deleted() {
    test_scan_without_filtering_deleted_with_format(false).await;
    test_scan_without_filtering_deleted_with_format(true).await;
}

async fn test_scan_without_filtering_deleted_with_format(flat_format: bool) {
    common_telemetry::init_default_ut_logging();

    let mut env = TestEnv::new().await;
    let engine = env
        .create_engine(MitoConfig {
            default_experimental_flat_format: flat_format,
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
        .insert_option("compaction.type", "twcs")
        .build();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // put 1, 2, 3, 4 and flush
    put_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(1, 5),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;

    // delete 2, 3 and flush
    delete_rows(
        &engine,
        region_id,
        Rows {
            schema: column_schemas.clone(),
            rows: build_rows(2, 4),
        },
    )
    .await;
    flush_region(&engine, region_id, None).await;

    // scan
    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
+-------+---------+---------------------+";
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));

    // Tries to use seq scan to test it under append mode.
    let mut scan = engine
        .scan_region(region_id, ScanRequest::default())
        .unwrap();
    scan.set_filter_deleted(false);

    let seq_scan = scan.seq_scan().await.unwrap();

    let stream = seq_scan.build_stream().unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());
}
