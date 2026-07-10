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

use std::sync::Arc;
use std::sync::atomic::Ordering;

use common_base::Plugins;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{RegionCloseRequest, RegionRequest};
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::engine::flush_test::MockRegionHook;
use crate::engine::region_hook::RegionHookRef;
use crate::test_util::{CreateRequestBuilder, TestEnv};

#[tokio::test]
async fn test_engine_close_region() {
    test_engine_close_region_with_format(false).await;
    test_engine_close_region_with_format(true).await;
}

async fn test_engine_close_region_with_format(flat_format: bool) {
    let mut env = TestEnv::with_prefix("close").await;
    let engine = env
        .create_engine(MitoConfig {
            default_flat_format: flat_format,
            ..Default::default()
        })
        .await;

    let region_id = RegionId::new(1, 1);
    // It's okay to close a region doesn't exist.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Close the created region.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
    assert!(!engine.is_region_exists(region_id));

    // It's okay to close this region again.
    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();
}

#[tokio::test]
async fn test_region_hook_on_close() {
    common_telemetry::init_default_ut_logging();
    let mut env = TestEnv::new().await;

    let hook = Arc::new(MockRegionHook::new());
    let plugins = Plugins::new();
    plugins.insert(hook.clone() as RegionHookRef);

    let engine = env
        .create_engine_with_plugins(MitoConfig::default(), plugins)
        .await;

    let region_id = RegionId::new(1, 1);
    let request = CreateRequestBuilder::new().build();
    engine
        .handle_request(region_id, RegionRequest::Create(request))
        .await
        .unwrap();

    // Sanity: no lifecycle events before closing.
    assert_eq!(hook.closed_count.load(Ordering::Relaxed), 0);
    assert_eq!(hook.dropped_count.load(Ordering::Relaxed), 0);

    engine
        .handle_request(region_id, RegionRequest::Close(RegionCloseRequest {}))
        .await
        .unwrap();

    // Closing fires on_region_closed exactly once.
    assert_eq!(hook.closed_count.load(Ordering::Relaxed), 1);
    // Close must not be confused with drop.
    assert_eq!(hook.dropped_count.load(Ordering::Relaxed), 0);
    assert_eq!(hook.files_removed_count.load(Ordering::Relaxed), 0);
}
