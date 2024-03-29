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

//! Tests for parallel scan.

use std::collections::HashMap;

use api::v1::Rows;
use common_recordbatch::RecordBatches;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionOpenRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::test_util::{
    build_delete_rows_for_key, build_rows_for_key, delete_rows, delete_rows_schema, flush_region,
    put_rows, rows_schema, CreateRequestBuilder, TestEnv,
};

async fn scan_in_parallel(
    env: &mut TestEnv,
    region_id: RegionId,
    region_dir: &str,
    parallelism: usize,
    channel_size: usize,
) {
    let engine = env
        .open_engine(MitoConfig {
            scan_parallelism: parallelism,
            parallel_scan_channel_size: channel_size,
            ..Default::default()
        })
        .await;

    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: region_dir.to_string(),
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

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
async fn test_parallel_scan() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

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
    // SST0
    flush_region(&engine, region_id, None).await;

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows_for_key("b", 0, 3, 0),
    };
    put_rows(&engine, region_id, rows).await;
    // SST1
    flush_region(&engine, region_id, None).await;

    // Delete (a, 2)
    let rows = Rows {
        schema: delete_schema.clone(),
        rows: build_delete_rows_for_key("a", 2, 3),
    };
    delete_rows(&engine, region_id, rows).await;
    // SST2
    flush_region(&engine, region_id, None).await;

    // Delete (b, 0), (b, 1)
    let rows = Rows {
        schema: delete_schema,
        rows: build_delete_rows_for_key("b", 0, 2),
    };
    delete_rows(&engine, region_id, rows).await;

    engine.stop().await.unwrap();

    scan_in_parallel(&mut env, region_id, &region_dir, 0, 1).await;

    scan_in_parallel(&mut env, region_id, &region_dir, 1, 1).await;

    scan_in_parallel(&mut env, region_id, &region_dir, 2, 1).await;

    scan_in_parallel(&mut env, region_id, &region_dir, 2, 8).await;

    scan_in_parallel(&mut env, region_id, &region_dir, 4, 8).await;

    scan_in_parallel(&mut env, region_id, &region_dir, 8, 2).await;
}
