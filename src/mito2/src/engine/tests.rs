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

//! Tests for mito engine.

use store_api::storage::RegionId;

use super::*;
use crate::error::Error;
use crate::request::RegionOptions;
use crate::test_util::{CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_engine_new_stop() {
    let env = TestEnv::with_prefix("engine-stop");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new(region_id).build();
    engine.create_region(request).await.unwrap();

    // Stop the engine to reject further requests.
    engine.stop().await.unwrap();
    assert!(!engine.is_region_exists(region_id));

    let request = CreateRequestBuilder::new(RegionId::new(1, 2)).build();
    let err = engine.create_region(request).await.unwrap_err();
    assert!(
        matches!(err, Error::WorkerStopped { .. }),
        "unexpected err: {err}"
    );
}

#[tokio::test]
async fn test_engine_create_new_region() {
    let env = TestEnv::with_prefix("new-region");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new(region_id).build();
    engine.create_region(request).await.unwrap();

    assert!(engine.is_region_exists(region_id));
}

#[tokio::test]
async fn test_engine_create_region_if_not_exists() {
    let env = TestEnv::with_prefix("create-not-exists");
    let engine = env.create_engine(MitoConfig::default()).await;

    let builder = CreateRequestBuilder::new(RegionId::new(1, 1)).create_if_not_exists(true);
    engine.create_region(builder.build()).await.unwrap();

    // Create the same region again.
    engine.create_region(builder.build()).await.unwrap();
}

#[tokio::test]
async fn test_engine_create_existing_region() {
    let env = TestEnv::with_prefix("create-existing");
    let engine = env.create_engine(MitoConfig::default()).await;

    let builder = CreateRequestBuilder::new(RegionId::new(1, 1));
    engine.create_region(builder.build()).await.unwrap();

    // Create the same region again.
    let err = engine.create_region(builder.build()).await.unwrap_err();
    assert!(
        matches!(err, Error::RegionExists { .. }),
        "unexpected err: {err}"
    );
}

#[tokio::test]
async fn test_engine_open_empty() {
    let env = TestEnv::with_prefix("open-empty");
    let engine = env.create_engine(MitoConfig::default()).await;

    let err = engine
        .open_region(OpenRequest {
            region_id: RegionId::new(1, 1),
            region_dir: "empty".to_string(),
            options: RegionOptions::default(),
        })
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::RegionNotFound { .. }),
        "unexpected err: {err}"
    );
}

#[tokio::test]
async fn test_engine_open_existing() {
    let env = TestEnv::with_prefix("open-exiting");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new(region_id).build();
    let region_dir = request.region_dir.clone();
    engine.create_region(request).await.unwrap();

    engine
        .open_region(OpenRequest {
            region_id,
            region_dir,
            options: RegionOptions::default(),
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_engine_close_region() {
    let env = TestEnv::with_prefix("close");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    // It's okay to close a region doesn't exist.
    engine
        .close_region(CloseRequest { region_id })
        .await
        .unwrap();

    let request = CreateRequestBuilder::new(region_id).build();
    engine.create_region(request).await.unwrap();

    engine
        .close_region(CloseRequest { region_id })
        .await
        .unwrap();
    assert!(!engine.is_region_exists(region_id));

    // It's okay to close this region again.
    engine
        .close_region(CloseRequest { region_id })
        .await
        .unwrap();
}

#[tokio::test]
async fn test_engine_reopen_region() {
    let env = TestEnv::with_prefix("reopen-region");
    let engine = env.create_engine(MitoConfig::default()).await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new(region_id).build();
    let region_dir = request.region_dir.clone();
    engine.create_region(request).await.unwrap();

    // Close the region.
    engine
        .close_region(CloseRequest { region_id })
        .await
        .unwrap();

    // Open the region again.
    engine
        .open_region(OpenRequest {
            region_id,
            region_dir,
            options: RegionOptions::default(),
        })
        .await
        .unwrap();
    assert!(engine.is_region_exists(region_id));
}
