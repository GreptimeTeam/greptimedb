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

//! Region flush tests.

use std::sync::Arc;
use std::time::Duration;

use common_test_util::temp_dir::create_temp_dir;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use store_api::storage::{FlushContext, FlushReason, OpenOptions, Region, WriteResponse};

use crate::engine::{self, RegionMap};
use crate::flush::{FlushStrategyRef, FlushType};
use crate::region::tests::{self, FileTesterBase};
use crate::region::RegionImpl;
use crate::test_util::config_util;
use crate::test_util::flush_switch::{has_parquet_file, FlushSwitch};

const REGION_NAME: &str = "region-flush-0";

/// Create a new region for flush test
async fn create_region_for_flush(
    store_dir: &str,
    flush_strategy: FlushStrategyRef,
) -> (
    RegionImpl<RaftEngineLogStore>,
    Arc<RegionMap<RaftEngineLogStore>>,
) {
    let metadata = tests::new_metadata(REGION_NAME);

    let (mut store_config, regions) =
        config_util::new_store_config_and_region_map(REGION_NAME, store_dir, Some(usize::MAX))
            .await;
    store_config.flush_strategy = flush_strategy;

    (
        RegionImpl::create(metadata, store_config).await.unwrap(),
        regions,
    )
}

/// Tester for region flush.
struct FlushTester {
    base: Option<FileTesterBase>,
    store_dir: String,
    flush_strategy: FlushStrategyRef,
    regions: Arc<RegionMap<RaftEngineLogStore>>,
}

impl FlushTester {
    async fn new(store_dir: &str, flush_strategy: FlushStrategyRef) -> FlushTester {
        let (region, regions) = create_region_for_flush(store_dir, flush_strategy.clone()).await;

        FlushTester {
            base: Some(FileTesterBase::with_region(region)),
            store_dir: store_dir.to_string(),
            flush_strategy: flush_strategy.clone(),
            regions,
        }
    }

    async fn reopen(&mut self) {
        self.regions.clear();
        // Close the old region.
        if let Some(base) = self.base.as_ref() {
            base.close().await;
        }
        self.base = None;
        // Reopen the region.
        let mut store_config =
            config_util::new_store_config(REGION_NAME, &self.store_dir, Some(usize::MAX)).await;
        store_config.flush_strategy = self.flush_strategy.clone();
        let opts = OpenOptions::default();
        let region = RegionImpl::open(REGION_NAME.to_string(), store_config, &opts)
            .await
            .unwrap()
            .unwrap();
        self.base = Some(FileTesterBase::with_region(region));
    }

    #[inline]
    fn base(&self) -> &FileTesterBase {
        self.base.as_ref().unwrap()
    }

    async fn put(&self, data: &[(i64, Option<i64>)]) -> WriteResponse {
        let data = data
            .iter()
            .map(|(ts, v0)| (*ts, v0.map(|v| v.to_string())))
            .collect::<Vec<_>>();
        self.base().put(&data).await
    }

    async fn full_scan(&self) -> Vec<(i64, Option<String>)> {
        self.base().full_scan().await
    }

    async fn flush(&self, wait: Option<bool>) {
        let ctx = wait
            .map(|wait| FlushContext {
                wait,
                reason: FlushReason::Manually,
                ..Default::default()
            })
            .unwrap_or_default();
        self.base().region.flush(&ctx).await.unwrap();
    }
}

impl Drop for FlushTester {
    fn drop(&mut self) {
        self.regions.clear();
    }
}

#[tokio::test]
async fn test_flush_and_stall() {
    common_telemetry::init_default_ut_logging();

    let dir = create_temp_dir("flush-stall");
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = FlushTester::new(store_dir, flush_switch.clone()).await;

    let data = [(1000, Some(100))];
    // Put one element so we have content to flush.
    tester.put(&data).await;

    // Now set should flush to true to trigger flush.
    flush_switch.set_should_flush(true);
    // Put element to trigger flush.
    tester.put(&data).await;

    // Now put another data to trigger write stall and wait until last flush done to
    // ensure at least one parquet file is generated.
    tester.put(&data).await;

    // Check parquet files.
    let sst_dir = format!("{}/{}", store_dir, engine::region_sst_dir("", REGION_NAME));
    assert!(has_parquet_file(&sst_dir));
}

#[tokio::test]
async fn test_manual_flush() {
    common_telemetry::init_default_ut_logging();
    let dir = create_temp_dir("manual_flush");

    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = FlushTester::new(store_dir, flush_switch.clone()).await;

    let data = [(1000, Some(100))];
    // Put one element so we have content to flush.
    tester.put(&data).await;

    // No parquet file should be flushed.
    let sst_dir = format!("{}/{}", store_dir, engine::region_sst_dir("", REGION_NAME));
    assert!(!has_parquet_file(&sst_dir));

    tester.flush(None).await;

    assert!(has_parquet_file(&sst_dir));
}

#[tokio::test]
async fn test_flush_and_reopen() {
    common_telemetry::init_default_ut_logging();
    let dir = create_temp_dir("manual_flush");
    let store_dir = dir.path().to_str().unwrap();
    let flush_switch = Arc::new(FlushSwitch::default());
    let mut tester = FlushTester::new(store_dir, flush_switch.clone()).await;

    tester.put(&[(1000, Some(100))]).await;
    tester.flush(Some(true)).await;
    tester.reopen().await;
    let i = tester
        .base()
        .region
        .inner
        .shared
        .version_control
        .committed_sequence();

    // we wrote a request and flushed the region (involving writing a manifest), thus
    // committed_sequence should be 2.
    assert_eq!(2, i);
}

#[tokio::test]
async fn test_flush_empty() {
    let dir = create_temp_dir("flush-empty");
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = FlushTester::new(store_dir, flush_switch.clone()).await;

    // Flush empty table.
    tester.flush(None).await;
    let data = [(1000, Some(100))];
    // Put element to trigger flush.
    tester.put(&data).await;

    // Put again.
    let data = [(2000, Some(200))];
    tester.put(&data).await;

    // No parquet file should be flushed.
    let sst_dir = format!("{}/{}", store_dir, engine::region_sst_dir("", REGION_NAME));
    assert!(!has_parquet_file(&sst_dir));

    let expect = vec![(1000, Some(100.to_string())), (2000, Some(200.to_string()))];

    let output = tester.full_scan().await;
    assert_eq!(expect, output);
}

#[tokio::test]
async fn test_read_after_flush() {
    common_telemetry::init_default_ut_logging();

    let dir = create_temp_dir("read-flush");
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = FlushTester::new(store_dir, flush_switch.clone()).await;

    // Put elements so we have content to flush.
    tester.put(&[(1000, Some(100))]).await;
    tester.put(&[(2000, Some(200))]).await;

    // Flush.
    tester.flush(None).await;

    // Put element again.
    tester.put(&[(3000, Some(300))]).await;

    let expect = vec![
        (1000, Some(100.to_string())),
        (2000, Some(200.to_string())),
        (3000, Some(300.to_string())),
    ];

    let output = tester.full_scan().await;
    assert_eq!(expect, output);

    // Reopen
    let mut tester = tester;
    tester.reopen().await;

    // Scan after reopen.
    let output = tester.full_scan().await;
    assert_eq!(expect, output);
}

#[tokio::test]
async fn test_merge_read_after_flush() {
    let dir = create_temp_dir("merge-read-flush");
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = FlushTester::new(store_dir, flush_switch.clone()).await;

    // Put elements so we have content to flush (In SST1).
    tester.put(&[(3000, Some(300))]).await;
    tester.put(&[(2000, Some(200))]).await;

    // Flush content to SST1.
    tester.flush(None).await;

    // Put element (In SST2).
    tester.put(&[(2000, Some(201))]).await;

    // In SST2.
    tester.put(&[(2000, Some(202))]).await;
    tester.put(&[(1000, Some(100))]).await;

    // Trigger flush.
    tester.flush(None).await;

    // Overwrite row (In memtable).
    tester.put(&[(2000, Some(203))]).await;

    let expect = vec![
        (1000, Some(100.to_string())),
        (2000, Some(203.to_string())),
        (3000, Some(300.to_string())),
    ];

    let output = tester.full_scan().await;
    assert_eq!(expect, output);

    // Reopen
    let mut tester = tester;
    tester.reopen().await;

    // Scan after reopen.
    let output = tester.full_scan().await;
    assert_eq!(expect, output);
}

#[tokio::test]
async fn test_schedule_engine_flush() {
    common_telemetry::init_default_ut_logging();

    let dir = create_temp_dir("engine-flush");
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = FlushTester::new(store_dir, flush_switch.clone()).await;
    assert_eq!(0, tester.base().region.last_flush_millis());

    // Insert the region to the region map.
    tester.regions.get_or_occupy_slot(
        REGION_NAME,
        engine::RegionSlot::Ready(tester.base().region.clone()),
    );

    // Put elements so we have content to flush.
    tester.put(&[(1000, Some(100))]).await;
    tester.put(&[(2000, Some(200))]).await;

    flush_switch.set_flush_type(FlushType::Engine);

    // Put element and trigger an engine level flush.
    tester.put(&[(3000, Some(300))]).await;

    // Wait for flush.
    let mut count = 0;
    while tester.base().region.last_flush_millis() == 0 && count < 50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        count += 1;
    }

    // Check parquet files.
    let sst_dir = format!("{}/{}", store_dir, engine::region_sst_dir("", REGION_NAME));
    assert!(has_parquet_file(&sst_dir));
}
