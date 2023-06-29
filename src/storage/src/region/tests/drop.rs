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

//! Region drop tests.

use std::path::Path;
use std::sync::Arc;

use common_telemetry::info;
use common_test_util::temp_dir::create_temp_dir;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use store_api::manifest::{Manifest, MetaAction};
use store_api::storage::{FlushContext, OpenOptions, Region};

use crate::config::EngineConfig;
use crate::engine;
use crate::flush::FlushStrategyRef;
use crate::manifest::action::{RegionMetaAction, RegionMetaActionList, RegionRemove};
use crate::region::tests::{self, FileTesterBase};
use crate::region::RegionImpl;
use crate::test_util::config_util;
use crate::test_util::flush_switch::{has_parquet_file, FlushSwitch};

const REGION_NAME: &str = "region-drop-0";

/// Create a new region for drop tests.
async fn create_region_for_drop(
    store_dir: &str,
    flush_strategy: FlushStrategyRef,
) -> RegionImpl<RaftEngineLogStore> {
    let metadata = tests::new_metadata(REGION_NAME);

    let mut store_config =
        config_util::new_store_config(REGION_NAME, store_dir, EngineConfig::default()).await;
    store_config.flush_strategy = flush_strategy;

    RegionImpl::create(metadata, store_config).await.unwrap()
}

/// Tester for drop tests.
struct DropTester {
    base: Option<FileTesterBase>,
}

impl DropTester {
    async fn new(store_dir: &str, flush_strategy: FlushStrategyRef) -> DropTester {
        let region = create_region_for_drop(store_dir, flush_strategy).await;
        DropTester {
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

    async fn flush(&self) {
        let ctx = FlushContext::default();
        self.base().region.flush(&ctx).await.unwrap();
    }

    async fn close(&mut self) {
        if let Some(base) = self.base.take() {
            base.close().await;
        }
    }
}

fn get_all_files(path: &str) -> Vec<String> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(path).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_file() {
            files.push(path.to_str().unwrap().to_string());
        } else if path.is_dir() {
            files.extend(get_all_files(path.to_str().unwrap()));
        }
    }
    files
}

#[tokio::test]
async fn test_drop_basic() {
    let dir = create_temp_dir("drop-basic");
    common_telemetry::init_default_ut_logging();
    let store_dir = dir.path().to_str().unwrap();

    let sst_dir = format!("{}/{}", store_dir, engine::region_sst_dir("", REGION_NAME));
    let manifest_dir = format!(
        "{}/{}",
        store_dir,
        engine::region_manifest_dir("", REGION_NAME)
    );
    let flush_switch = Arc::new(FlushSwitch::default());
    let mut tester = DropTester::new(store_dir, flush_switch.clone()).await;

    let data = [(1000, Some(100))];

    // Put one element so we have content to flush.
    tester.put(&data).await;

    // Manually trigger flush.
    tester.flush().await;

    assert!(has_parquet_file(&sst_dir));

    tester.base().checkpoint_manifest().await;
    let manifest_files = get_all_files(&manifest_dir);
    info!("manifest_files: {:?}", manifest_files);

    tester.base().region.drop_region().await.unwrap();
    tester.close().await;

    assert!(!Path::new(&manifest_dir).exists());
}

#[tokio::test]
async fn test_drop_reopen() {
    let dir = create_temp_dir("drop-basic");
    common_telemetry::init_default_ut_logging();
    let store_dir = dir.path().to_str().unwrap();

    let sst_dir = format!("{}/{}", store_dir, engine::region_sst_dir("", REGION_NAME));
    let manifest_dir = format!(
        "{}/{}",
        store_dir,
        engine::region_manifest_dir("", REGION_NAME)
    );
    let flush_switch = Arc::new(FlushSwitch::default());
    let mut tester = DropTester::new(store_dir, flush_switch.clone()).await;

    let data = [(1000, Some(100))];

    // Put one element so we have content to flush.
    tester.put(&data).await;
    // Manually trigger flush.
    tester.flush().await;

    assert!(has_parquet_file(&sst_dir));

    tester.base().checkpoint_manifest().await;
    let version_control = tester.base().region.version_control();

    let mut action_list =
        RegionMetaActionList::with_action(RegionMetaAction::Remove(RegionRemove {
            region_id: tester.base().region.id(),
        }));
    let prev_version = version_control.current_manifest_version();
    action_list.set_prev_version(prev_version);
    let manifest = &tester.base().region.inner.manifest;
    let _ = manifest.update(action_list).await.unwrap();
    tester.close().await;

    // Reopen the region.
    let store_config = config_util::new_store_config(
        REGION_NAME,
        store_dir,
        EngineConfig {
            max_files_in_l0: usize::MAX,
            ..Default::default()
        },
    )
    .await;

    let opts = OpenOptions::default();
    let region = RegionImpl::open(REGION_NAME.to_string(), store_config, &opts)
        .await
        .unwrap();
    assert!(region.is_none());
    assert!(!Path::new(&manifest_dir).exists());
}
