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

use std::assert_matches::assert_matches;
use std::collections::HashMap;

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use store_api::region_engine::{RegionEngine, SetReadonlyResponse};
use store_api::region_request::{RegionCatchupRequest, RegionOpenRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};

use crate::config::MitoConfig;
use crate::error::{self, Error};
use crate::test_util::{
    build_rows, flush_region, put_rows, rows_schema, CreateRequestBuilder, TestEnv,
};
use crate::wal::EntryId;

fn get_last_entry_id(resp: SetReadonlyResponse) -> Option<EntryId> {
    if let SetReadonlyResponse::Success { last_entry_id } = resp {
        last_entry_id
    } else {
        unreachable!();
    }
}

#[tokio::test]
async fn test_catchup_with_last_entry_id() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("last_entry_id");
    let leader_engine = env.create_engine(MitoConfig::default()).await;
    let follower_engine = env.create_follower_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    leader_engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    // Ensures the mutable is empty.
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(region.version().memtables.mutable.is_empty());

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };

    put_rows(&leader_engine, region_id, rows).await;

    let resp = leader_engine
        .set_readonly_gracefully(region_id)
        .await
        .unwrap();

    let last_entry_id = get_last_entry_id(resp);
    assert!(last_entry_id.is_some());

    // Replays the memtable.
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: false,
                entry_id: last_entry_id,
            }),
        )
        .await;
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(!region.is_writable());
    assert!(resp.is_ok());

    // Scans
    let request = ScanRequest::default();
    let stream = follower_engine
        .handle_query(region_id, request)
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Replays the memtable again, should be ok.
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                entry_id: last_entry_id,
            }),
        )
        .await;
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(region.is_writable());
    assert!(resp.is_ok());
}

#[tokio::test]
async fn test_catchup_with_incorrect_last_entry_id() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::with_prefix("incorrect_last_entry_id");
    let leader_engine = env.create_engine(MitoConfig::default()).await;
    let follower_engine = env.create_follower_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    leader_engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    // Ensures the mutable is empty.
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(region.version().memtables.mutable.is_empty());

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };

    put_rows(&leader_engine, region_id, rows).await;

    let resp = leader_engine
        .set_readonly_gracefully(region_id)
        .await
        .unwrap();

    let last_entry_id = get_last_entry_id(resp);
    assert!(last_entry_id.is_some());

    let incorrect_last_entry_id = last_entry_id.map(|e| e + 1);

    // Replays the memtable.
    let err = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: false,
                entry_id: incorrect_last_entry_id,
            }),
        )
        .await
        .unwrap_err();
    let err = err.as_any().downcast_ref::<Error>().unwrap();

    assert_matches!(err, error::Error::UnexpectedReplay { .. });

    // It should ignore requests to writable regions.
    region.set_writable(true);
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: false,
                entry_id: incorrect_last_entry_id,
            }),
        )
        .await;
    assert!(resp.is_ok());
}

#[tokio::test]
async fn test_catchup_without_last_entry_id() {
    let mut env = TestEnv::with_prefix("without_last_entry_id");
    let leader_engine = env.create_engine(MitoConfig::default()).await;
    let follower_engine = env.create_follower_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    leader_engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(0, 3),
    };
    put_rows(&leader_engine, region_id, rows).await;

    // Replays the memtable.
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: false,
                entry_id: None,
            }),
        )
        .await;
    assert!(resp.is_ok());
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(!region.is_writable());

    let request = ScanRequest::default();
    let stream = follower_engine
        .handle_query(region_id, request)
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Replays the memtable again, should be ok.
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                entry_id: None,
            }),
        )
        .await;
    assert!(resp.is_ok());
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(region.is_writable());
}

#[tokio::test]
async fn test_catchup_with_manifest_update() {
    let mut env = TestEnv::with_prefix("without_manifest_update");
    let leader_engine = env.create_engine(MitoConfig::default()).await;
    let follower_engine = env.create_follower_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    leader_engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    follower_engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&leader_engine, region_id, rows).await;

    // Triggers to create a new manifest file.
    flush_region(&leader_engine, region_id, None).await;

    // Puts to WAL.
    let rows = Rows {
        schema: column_schemas,
        rows: build_rows(3, 5),
    };
    put_rows(&leader_engine, region_id, rows).await;

    // Triggers to create a new manifest file.
    flush_region(&leader_engine, region_id, None).await;

    let region = follower_engine.get_region(region_id).unwrap();
    // Ensures the mutable is empty.
    assert!(region.version().memtables.mutable.is_empty());

    let manifest = region.manifest_manager.manifest().await;
    assert_eq!(manifest.manifest_version, 0);

    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: false,
                entry_id: None,
            }),
        )
        .await;
    assert!(resp.is_ok());

    // The inner region was replaced. We must get it again.
    let region = follower_engine.get_region(region_id).unwrap();
    let manifest = region.manifest_manager.manifest().await;
    assert_eq!(manifest.manifest_version, 2);
    assert!(!region.is_writable());

    let request = ScanRequest::default();
    let stream = follower_engine
        .handle_query(region_id, request)
        .await
        .unwrap();
    let batches = RecordBatches::try_collect(stream).await.unwrap();
    let expected = "\
+-------+---------+---------------------+
| tag_0 | field_0 | ts                  |
+-------+---------+---------------------+
| 0     | 0.0     | 1970-01-01T00:00:00 |
| 1     | 1.0     | 1970-01-01T00:00:01 |
| 2     | 2.0     | 1970-01-01T00:00:02 |
| 3     | 3.0     | 1970-01-01T00:00:03 |
| 4     | 4.0     | 1970-01-01T00:00:04 |
+-------+---------+---------------------+";
    assert_eq!(expected, batches.pretty_print().unwrap());

    // Replays the memtable again, should be ok.
    let resp = follower_engine
        .handle_request(
            region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                entry_id: None,
            }),
        )
        .await;
    let region = follower_engine.get_region(region_id).unwrap();
    assert!(resp.is_ok());
    assert!(region.is_writable());
}

#[tokio::test]
async fn test_catchup_not_exist() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let non_exist_region_id = RegionId::new(1, 1);

    let err = engine
        .handle_request(
            non_exist_region_id,
            RegionRequest::Catchup(RegionCatchupRequest {
                set_writable: true,
                entry_id: None,
            }),
        )
        .await
        .unwrap_err();
    assert_matches!(err.status_code(), StatusCode::RegionNotFound);
}
