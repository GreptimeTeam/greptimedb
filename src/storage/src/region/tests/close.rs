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

//! Region close tests.

use std::sync::Arc;

use common_test_util::temp_dir::create_temp_dir;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use store_api::storage::{
    AlterOperation, AlterRequest, CloseContext, Region, RegionMeta, WriteResponse,
};

use crate::config::EngineConfig;
use crate::engine;
use crate::error::Error;
use crate::flush::FlushStrategyRef;
use crate::region::tests::{self, FileTesterBase};
use crate::region::RegionImpl;
use crate::test_util::config_util;
use crate::test_util::flush_switch::{has_parquet_file, FlushSwitch};

const REGION_NAME: &str = "region-close-0";

/// Tester for region close
struct CloseTester {
    base: Option<FileTesterBase>,
}

/// Create a new region for close test
async fn create_region_for_close(
    store_dir: &str,
    flush_strategy: FlushStrategyRef,
) -> RegionImpl<RaftEngineLogStore> {
    let metadata = tests::new_metadata(REGION_NAME);

    let mut store_config =
        config_util::new_store_config(REGION_NAME, store_dir, EngineConfig::default()).await;
    store_config.flush_strategy = flush_strategy;

    RegionImpl::create(metadata, store_config).await.unwrap()
}

impl CloseTester {
    async fn new(store_dir: &str, flush_strategy: FlushStrategyRef) -> CloseTester {
        let region = create_region_for_close(store_dir, flush_strategy.clone()).await;

        CloseTester {
            base: Some(FileTesterBase::with_region(region)),
        }
    }

    #[inline]
    fn base(&self) -> &FileTesterBase {
        self.base.as_ref().unwrap()
    }

    async fn put(&self, data: &[(i64, Option<i64>)]) {
        let data = data
            .iter()
            .map(|(ts, v0)| (*ts, v0.map(|v| v.to_string())))
            .collect::<Vec<_>>();
        let _ = self.base().put(&data).await;
    }

    async fn try_put(&self, data: &[(i64, Option<i64>)]) -> Result<WriteResponse, Error> {
        let data = data
            .iter()
            .map(|(ts, v0)| (*ts, v0.map(|v| v.to_string())))
            .collect::<Vec<_>>();
        self.base().try_put(&data).await
    }

    async fn try_alter(&self, mut req: AlterRequest) -> Result<(), Error> {
        let version = self.version();
        req.version = version;

        self.base().region.alter(req).await
    }

    fn version(&self) -> u32 {
        let metadata = self.base().region.in_memory_metadata();
        metadata.version()
    }
}

#[tokio::test]
async fn test_close_basic() {
    common_telemetry::init_default_ut_logging();
    let dir = create_temp_dir("close-basic");
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = CloseTester::new(store_dir, flush_switch).await;

    tester
        .base()
        .region
        .close(&CloseContext::default())
        .await
        .unwrap();

    let data = [(1000, Some(100))];

    let closed_region_error = "Try to write the closed region".to_string();
    // Put one element should return ClosedRegion error
    assert_eq!(
        tester.try_put(&data).await.unwrap_err().to_string(),
        closed_region_error
    );

    // Alter table should return ClosedRegion error
    assert_eq!(
        tester
            .try_alter(AlterRequest {
                operation: AlterOperation::AddColumns {
                    columns: Vec::new(),
                },
                version: 0,
            })
            .await
            .unwrap_err()
            .to_string(),
        closed_region_error
    );
}

#[tokio::test]
async fn test_close_wait_flush_done() {
    common_telemetry::init_default_ut_logging();
    let dir = create_temp_dir("close-basic");
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = CloseTester::new(store_dir, flush_switch.clone()).await;

    let data = [(1000, Some(100))];

    // Now set should flush to true to trigger flush.
    flush_switch.set_should_flush(true);

    // Put one element so we have content to flush.
    tester.put(&data).await;

    let sst_dir = format!("{}/{}", store_dir, engine::region_sst_dir("", REGION_NAME));
    assert!(!has_parquet_file(&sst_dir));

    // Close should cancel the flush.
    tester
        .base()
        .region
        .close(&CloseContext::default())
        .await
        .unwrap();

    assert!(!has_parquet_file(&sst_dir));
}
