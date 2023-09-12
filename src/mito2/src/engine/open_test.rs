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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionOpenRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::test_util::{reopen_region, CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_engine_open_empty() {
    let mut env = TestEnv::with_prefix("open-empty");
    let engine = env.create_engine(MitoConfig::default()).await;

    let err = engine
        .handle_request(
            RegionId::new(1, 1),
            RegionRequest::Open(RegionOpenRequest {
                engine: String::new(),
                region_dir: "empty".to_string(),
                options: HashMap::default(),
            }),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err.status_code(), StatusCode::RegionNotFound),
        "unexpected err: {err}"
    );
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

    reopen_region(&engine, region_id, region_dir).await;
    assert!(engine.is_region_exists(region_id));
}
