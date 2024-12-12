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

use std::collections::HashMap;
use std::time::Duration;

use api::v1::Rows;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_recordbatch::RecordBatches;
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    RegionCloseRequest, RegionOpenRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::oneshot;

use crate::compaction::compactor::{open_compaction_region, OpenCompactionRegionRequest};
use crate::config::MitoConfig;
use crate::error;
use crate::region::options::RegionOptions;
use crate::test_util::{
    build_rows, flush_region, put_rows, reopen_region, rows_schema, CreateRequestBuilder, TestEnv,
};

#[tokio::test]
async fn test_engine_open_empty() {
    let mut env = TestEnv::with_prefix("open-empty");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: "empty".to_string(),
                options: HashMap::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, err.status_code());
    let err = engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, err.status_code());
    let role = engine.role(region_id);
    assert_eq!(role, None);
}

#[tokio::test]
async fn test_engine_open_existing() {
    let mut env = TestEnv::with_prefix("open-exiting");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    engine
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
}

#[tokio::test]
async fn test_engine_reopen_region() {
    let mut env = TestEnv::with_prefix("reopen-region");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    reopen_region(&engine, region_id, region_dir, false, Default::default()).await;
    assert!(engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_engine_open_readonly() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    reopen_region(&engine, region_id, region_dir, false, Default::default()).await;

    // Region is readonly.
    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 2),
    };
    let err = engine
        .handle_request(
            region_id,
            RegionRequest::Put(RegionPutRequest { rows: rows.clone() }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotReady, err.status_code());

    assert_eq!(Some(RegionRole::Follower), engine.role(region_id));
    // Converts region to leader.
    engine
        .set_region_role(region_id, RegionRole::Leader)
        .unwrap();
    assert_eq!(Some(RegionRole::Leader), engine.role(region_id));

    put_rows(&engine, region_id, rows).await;
}

#[tokio::test]
async fn test_engine_region_open_with_options() {
    let mut env = TestEnv::new();
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Open the region again with options.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::from([("ttl".to_string(), "4d".to_string())]),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(
        region.version().options.ttl,
        Some(Duration::from_secs(3600 * 24 * 4).into())
    );
}

#[tokio::test]
async fn test_engine_region_open_with_custom_store() {
    let mut env = TestEnv::new();
    let engine = env
        .create_engine_with_multiple_object_stores(MitoConfig::default(), None, None, &["Gcs"])
        .await;
    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new()
        .insert_option("storage", "Gcs")
        .build();
    let region_dir = request.region_dir.clone();

    // Create a custom region.
    engine
        .handle_request(region_id, RegionRequest::Create(request.clone()))
        .await
        .unwrap();

    // Close the custom region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Open the custom region.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: HashMap::from([("storage".to_string(), "Gcs".to_string())]),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    // The region should not be opened with the default object store.
    let region = engine.get_region(region_id).unwrap();
    let object_store_manager = env.get_object_store_manager().unwrap();
    assert!(!object_store_manager
        .default_object_store()
        .is_exist(region.access_layer.region_dir())
        .await
        .unwrap());
    assert!(object_store_manager
        .find("Gcs")
        .unwrap()
        .is_exist(region.access_layer.region_dir())
        .await
        .unwrap());
}

#[tokio::test]
async fn test_open_region_skip_wal_replay() {
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
        )
        .await;

    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();

    let column_schemas = rows_schema(&request);
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(0, 3),
    };
    put_rows(&engine, region_id, rows).await;

    flush_region(&engine, region_id, None).await;

    let rows = Rows {
        schema: column_schemas.clone(),
        rows: build_rows(3, 5),
    };
    put_rows(&engine, region_id, rows).await;

    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    // Skip the WAL replay .
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: region_dir.to_string(),
                options: Default::default(),
                skip_wal_replay: true,
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
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

    // Replay the WAL.
    let engine = env.reopen_engine(engine, MitoConfig::default()).await;
    // Open the region again with options.
    engine
        .handle_request(
            region_id,
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir,
                options: Default::default(),
                skip_wal_replay: false,
            }),
        )
        .await
        .unwrap();

    let request = ScanRequest::default();
    let stream = engine.scan_to_stream(region_id, request).await.unwrap();
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
}

#[tokio::test]
async fn test_open_region_wait_for_opening_region_ok() {
    let mut env = TestEnv::with_prefix("wait-for-opening-region-ok");
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let worker = engine.inner.workers.worker(region_id);
    let (tx, rx) = oneshot::channel();
    let opening_regions = worker.opening_regions().clone();
    opening_regions.insert_sender(region_id, tx.into());
    assert!(engine.is_region_opening(region_id));

    let handle_open = tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    region_dir: "empty".to_string(),
                    options: HashMap::default(),
                    skip_wal_replay: false,
                }),
            )
            .await
    });

    // Wait for conditions
    while opening_regions.sender_len(region_id) != 2 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let senders = opening_regions.remove_sender(region_id);
    for sender in senders {
        sender.send(Ok(0));
    }

    assert_eq!(handle_open.await.unwrap().unwrap().affected_rows, 0);
    assert_eq!(rx.await.unwrap().unwrap(), 0);
}

#[tokio::test]
async fn test_open_region_wait_for_opening_region_err() {
    let mut env = TestEnv::with_prefix("wait-for-opening-region-err");
    let engine = env.create_engine(MitoConfig::default()).await;
    let region_id = RegionId::new(1, 1);
    let worker = engine.inner.workers.worker(region_id);
    let (tx, rx) = oneshot::channel();
    let opening_regions = worker.opening_regions().clone();
    opening_regions.insert_sender(region_id, tx.into());
    assert!(engine.is_region_opening(region_id));

    let handle_open = tokio::spawn(async move {
        engine
            .handle_request(
                region_id,
                RegionRequest::Open(RegionOpenRequest {
                    engine: String::new(),
                    region_dir: "empty".to_string(),
                    options: HashMap::default(),
                    skip_wal_replay: false,
                }),
            )
            .await
    });

    // Wait for conditions
    while opening_regions.sender_len(region_id) != 2 {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let senders = opening_regions.remove_sender(region_id);
    for sender in senders {
        sender.send(Err(error::RegionNotFoundSnafu { region_id }.build()));
    }

    assert_eq!(
        handle_open.await.unwrap().unwrap_err().status_code(),
        StatusCode::RegionNotFound
    );
    assert_eq!(
        rx.await.unwrap().unwrap_err().status_code(),
        StatusCode::RegionNotFound
    );
}

#[tokio::test]
async fn test_open_compaction_region() {
    let mut env = TestEnv::new();
    let mut mito_config = MitoConfig::default();
    mito_config
        .sanitize(&env.data_home().display().to_string())
        .unwrap();

    let engine = env.create_engine(mito_config.clone()).await;

    let region_id = RegionId::new(1, 1);
    let schema_metadata_manager = env.get_schema_metadata_manager();
    schema_metadata_manager
        .register_region_table_info(
            region_id.table_id(),
            "test_table",
            "test_catalog",
            "test_schema",
            None,
        )
        .await;
    let request = CreateRequestBuilder::new().build();
    let region_dir = request.region_dir.clone();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    let object_store_manager = env.get_object_store_manager().unwrap();

    let req = OpenCompactionRegionRequest {
        region_id,
        region_dir: region_dir.clone(),
        region_options: RegionOptions::default(),
    };

    let compaction_region = open_compaction_region(
        &req,
        &mito_config,
        object_store_manager.clone(),
        schema_metadata_manager,
    )
    .await
    .unwrap();

    assert_eq!(region_id, compaction_region.region_id);
}
