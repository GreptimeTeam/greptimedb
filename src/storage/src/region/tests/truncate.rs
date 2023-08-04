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

//! Region truncate tests.

use std::sync::Arc;

use common_test_util::temp_dir::create_temp_dir;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use store_api::manifest::{Manifest, MetaAction};
use store_api::storage::{FlushContext, OpenOptions, Region};

use crate::config::EngineConfig;
use crate::engine;
use crate::flush::FlushStrategyRef;
use crate::manifest::action::{RegionMetaAction, RegionMetaActionList, RegionTruncate};
use crate::region::tests::{self, FileTesterBase};
use crate::region::RegionImpl;
use crate::test_util::config_util;
use crate::test_util::flush_switch::{has_parquet_file, FlushSwitch};

const REGION_NAME: &str = "region-truncate-0";

/// Create a new region for truncate tests.
async fn create_region_for_truncate(
    store_dir: &str,
    flush_strategy: FlushStrategyRef,
) -> RegionImpl<RaftEngineLogStore> {
    let metadata = tests::new_metadata(REGION_NAME);

    let mut store_config =
        config_util::new_store_config(REGION_NAME, store_dir, EngineConfig::default()).await;
    store_config.flush_strategy = flush_strategy;

    RegionImpl::create(metadata, store_config).await.unwrap()
}

/// Tester for truncate tests.
struct TruncateTester {
    store_dir: String,
    base: Option<FileTesterBase>,
}

impl TruncateTester {
    async fn new(store_dir: &str, flush_strategy: FlushStrategyRef) -> TruncateTester {
        let region = create_region_for_truncate(store_dir, flush_strategy).await;
        TruncateTester {
            store_dir: store_dir.to_string(),
            base: Some(FileTesterBase::with_region(region)),
        }
    }

    #[inline]
    fn base(&self) -> &FileTesterBase {
        self.base.as_ref().unwrap()
    }

    async fn flush(&self) {
        let ctx = FlushContext::default();
        self.base().region.flush(&ctx).await.unwrap();
    }

    async fn truncate(&self) {
        self.base().region.truncate().await.unwrap();
    }

    async fn reopen(&mut self) {
        // Close the old region.
        if let Some(base) = self.base.as_ref() {
            base.close().await;
        }
        self.base = None;
        // Reopen the region.
        let store_config = config_util::new_store_config(
            REGION_NAME,
            &self.store_dir,
            EngineConfig {
                max_files_in_l0: usize::MAX,
                ..Default::default()
            },
        )
        .await;

        let opts = OpenOptions::default();
        let region = RegionImpl::open(REGION_NAME.to_string(), store_config, &opts)
            .await
            .unwrap()
            .unwrap();

        self.base = Some(FileTesterBase::with_region(region));
    }
}

#[tokio::test]
async fn test_truncate_basic() {
    let dir = create_temp_dir("truncate-basic");
    common_telemetry::init_default_ut_logging();
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = TruncateTester::new(store_dir, flush_switch.clone()).await;

    let data = [
        (1000, Some("1000".to_string())),
        (1001, Some("1001".to_string())),
        (1002, Some("1002".to_string())),
        (1003, Some("1003".to_string())),
    ];

    // Data in Memtable
    tester.base().put(&data).await;
    let res = tester.base().full_scan().await;
    assert_eq!(4, res.len());

    // Truncate region.
    tester.truncate().await;

    let res = tester.base().full_scan().await;
    assert_eq!(0, res.len());
}

#[tokio::test]
async fn test_put_data_after_truncate() {
    let dir = create_temp_dir("put_data_after_truncate");
    common_telemetry::init_default_ut_logging();
    let store_dir = dir.path().to_str().unwrap();

    let sst_dir = format!("{}/{}", store_dir, engine::region_sst_dir("", REGION_NAME));
    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = TruncateTester::new(store_dir, flush_switch.clone()).await;

    let data = [
        (1000, Some("1000".to_string())),
        (1001, Some("1001".to_string())),
        (1002, None),
        (1003, Some("1003".to_string())),
    ];

    tester.base().put(&data).await;

    // Manually trigger flush.
    tester.flush().await;
    assert!(has_parquet_file(&sst_dir));

    let data = [
        (1002, Some("1002".to_string())),
        (1004, Some("1004".to_string())),
        (1005, Some("1005".to_string())),
    ];
    tester.base().put(&data).await;

    // Truncate region.
    tester.truncate().await;
    let res = tester.base().full_scan().await;
    assert_eq!(0, res.len());

    let new_data = [
        (1010, Some("0".to_string())),
        (1011, Some("1".to_string())),
        (1012, Some("2".to_string())),
        (1013, Some("3".to_string())),
    ];
    tester.base().put(&new_data).await;

    let res = tester.base().full_scan().await;
    assert_eq!(new_data, res.as_slice());
}

#[tokio::test]
async fn test_truncate_reopen() {
    let dir = create_temp_dir("put_data_after_truncate");
    common_telemetry::init_default_ut_logging();
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let mut tester = TruncateTester::new(store_dir, flush_switch.clone()).await;

    let data = [
        (1000, Some("1000".to_string())),
        (1001, Some("1001".to_string())),
        (1002, None),
        (1003, Some("1003".to_string())),
    ];

    tester.base().put(&data).await;

    // Manually trigger flush.
    tester.flush().await;

    let data = [
        (1002, Some("1002".to_string())),
        (1004, Some("1004".to_string())),
        (1005, Some("1005".to_string())),
    ];
    tester.base().put(&data).await;

    let manifest = &tester.base().region.inner.manifest;
    let manifest_version = tester
        .base()
        .region
        .version_control()
        .current_manifest_version();

    let committed_sequence = tester.base().committed_sequence();
    let mut action_list =
        RegionMetaActionList::with_action(RegionMetaAction::Truncate(RegionTruncate {
            region_id: 0.into(),
            committed_sequence,
        }));

    // Persist the meta action.
    let prev_version = manifest_version;
    action_list.set_prev_version(prev_version);
    assert!(manifest.update(action_list).await.is_ok());

    // Reopen and put data.
    tester.reopen().await;
    let res = tester.base().full_scan().await;
    assert_eq!(0, res.len());

    let new_data = [
        (0, Some("0".to_string())),
        (1, Some("1".to_string())),
        (2, Some("2".to_string())),
        (3, Some("3".to_string())),
    ];

    tester.base().put(&new_data).await;
    let res = tester.base().full_scan().await;
    assert_eq!(new_data, res.as_slice());
}
