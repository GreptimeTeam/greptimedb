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
use store_api::region_engine::{RegionEngine, RegionRole};
use store_api::region_request::{
    RegionCloseRequest, RegionOpenRequest, RegionPutRequest, RegionRequest,
};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::test_util::{
    build_rows, put_rows, reopen_region, rows_schema, CreateRequestBuilder, TestEnv,
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
                wal_options: HashMap::default(),
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(StatusCode::RegionNotFound, err.status_code());
    let err = engine.set_writable(region_id, true).unwrap_err();
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
                wal_options: HashMap::default(),
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

    reopen_region(&engine, region_id, region_dir, false).await;
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

    reopen_region(&engine, region_id, region_dir, false).await;

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
    assert_eq!(StatusCode::RegionReadonly, err.status_code());

    assert_eq!(Some(RegionRole::Follower), engine.role(region_id));
    // Set writable and write.
    engine.set_writable(region_id, true).unwrap();
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
                wal_options: HashMap::default(),
            }),
        )
        .await
        .unwrap();

    let region = engine.get_region(region_id).unwrap();
    assert_eq!(
        Duration::from_secs(3600 * 24 * 4),
        region.version().options.ttl.unwrap()
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
                wal_options: HashMap::default(),
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

// TODO(niebayes): add tests for test_engine_open_with_wal_options.
