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
use store_api::storage::{RegionId, ScanRequest, TimeSeriesRowSelector};

use crate::config::MitoConfig;
use crate::test_util::batch_util::sort_batches_and_print;
use crate::test_util::{
    build_rows_for_key, flush_region, put_rows, rows_schema, CreateRequestBuilder, TestEnv,
};

async fn test_last_row(append_mode: bool) {
    let mut env = TestEnv::new();
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
        .insert_option("append_mode", &append_mode.to_string())
        .build();
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
        .unwrap();
    assert_eq!(3, scanner.num_files());
    assert_eq!(1, scanner.num_memtables());
    let stream = scanner.scan().await.unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    assert_eq!(expected, sort_batches_and_print(&batches, &["tag_0", "ts"]));
}

#[tokio::test]
async fn test_last_row_append_mode_disabled() {
    test_last_row(false).await;
}

#[tokio::test]
async fn test_last_row_append_mode_enabled() {
    test_last_row(true).await;
}
