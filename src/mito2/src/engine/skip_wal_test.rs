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
use common_wal::options::{WAL_OPTIONS_KEY, WalOptions};
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::test_util::{CreateRequestBuilder, TestEnv, build_rows, put_rows, rows_schema};

#[tokio::test]
async fn test_close_region_skip_wal_with_pending_data() {
    test_close_region_skip_wal(true).await;
}

#[tokio::test]
async fn test_close_region_skip_wal_without_pending_data() {
    test_close_region_skip_wal(false).await;
}

async fn test_close_region_skip_wal(insert: bool) {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix(&format!("close-skip-wal-{}", insert)).await;
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let mut request = CreateRequestBuilder::new().build();

    // Set skip_wal = true via WalOptions::Noop
    let wal_options = WalOptions::Noop;
    request.options.insert(
        WAL_OPTIONS_KEY.to_string(),
        serde_json::to_string(&wal_options).unwrap(),
    );

    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    if insert {
        let column_schemas = rows_schema(&request);
        let rows = Rows {
            schema: column_schemas.clone(),
            rows: build_rows(0, 3),
        };
        put_rows(&engine, region_id, rows).await;
    }

    // The region should have data in memtable.
    let region = engine.get_region(region_id).unwrap();
    if insert {
        assert!(!region.version().memtables.is_empty());
    } else {
        assert!(region.version().memtables.is_empty());
    }

    // Close the region. This should trigger a flush.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // After closing, we reopen it and check if data is persisted.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(store_api::region_request::RegionOpenRequest {
                engine: String::new(),
                table_dir: request.table_dir.clone(),
                path_type: store_api::region_request::PathType::Bare,
                options: request.options.clone(),
                skip_wal_replay: false,
                checkpoint: None,
            }),
        )
        .await
        .unwrap();
    let scan_request = ScanRequest::default();
    let stream = engine
        .scan_to_stream(region_id, scan_request)
        .await
        .unwrap();
    let batches = common_recordbatch::RecordBatches::try_collect(stream)
        .await
        .unwrap();
    // If flush was triggered, data should be there even though WAL was skipped.
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    if insert {
        assert_eq!(3, total_rows);
    } else {
        assert_eq!(0, total_rows);
    }
}
