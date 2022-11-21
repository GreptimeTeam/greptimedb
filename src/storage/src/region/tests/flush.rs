// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Region flush tests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use log_store::fs::log::LocalFileLogStore;
use store_api::storage::{OpenOptions, WriteResponse};
use tempdir::TempDir;

use crate::engine;
use crate::flush::{FlushStrategy, FlushStrategyRef};
use crate::region::tests::{self, FileTesterBase};
use crate::region::{RegionImpl, SharedDataRef};
use crate::test_util::config_util;

const REGION_NAME: &str = "region-flush-0";

/// Create a new region for flush test
async fn create_region_for_flush(
    store_dir: &str,
    enable_version_column: bool,
    flush_strategy: FlushStrategyRef,
) -> RegionImpl<LocalFileLogStore> {
    let metadata = tests::new_metadata(REGION_NAME, enable_version_column);

    let mut store_config = config_util::new_store_config(REGION_NAME, store_dir).await;
    store_config.flush_strategy = flush_strategy;

    RegionImpl::create(metadata, store_config).await.unwrap()
}

/// Tester for region flush.
struct FlushTester {
    base: Option<FileTesterBase>,
    store_dir: String,
    flush_strategy: FlushStrategyRef,
}

impl FlushTester {
    async fn new(store_dir: &str, flush_strategy: FlushStrategyRef) -> FlushTester {
        let region = create_region_for_flush(store_dir, false, flush_strategy.clone()).await;

        FlushTester {
            base: Some(FileTesterBase::with_region(region)),
            store_dir: store_dir.to_string(),
            flush_strategy: flush_strategy.clone(),
        }
    }

    async fn reopen(&mut self) {
        // Close the old region.
        self.base = None;
        // Reopen the region.
        let mut store_config = config_util::new_store_config(REGION_NAME, &self.store_dir).await;
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
        self.base().put(data).await
    }

    async fn full_scan(&self) -> Vec<(i64, Option<i64>)> {
        self.base().full_scan().await
    }

    async fn wait_flush_done(&self) {
        self.base().region.wait_flush_done().await.unwrap();
    }
}

#[derive(Debug, Default)]
struct FlushSwitch {
    should_flush: AtomicBool,
}

impl FlushSwitch {
    fn set_should_flush(&self, should_flush: bool) {
        self.should_flush.store(should_flush, Ordering::Relaxed);
    }
}

impl FlushStrategy for FlushSwitch {
    fn should_flush(
        &self,
        _shared: &SharedDataRef,
        _bytes_mutable: usize,
        _bytes_total: usize,
    ) -> bool {
        self.should_flush.load(Ordering::Relaxed)
    }
}

fn has_parquet_file(sst_dir: &str) -> bool {
    for entry in std::fs::read_dir(sst_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_dir() {
            assert_eq!("parquet", path.extension().unwrap());
            return true;
        }
    }

    false
}

#[tokio::test]
async fn test_flush_and_stall() {
    common_telemetry::init_default_ut_logging();

    let dir = TempDir::new("flush-stall").unwrap();
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
async fn test_flush_empty() {
    let dir = TempDir::new("flush-empty").unwrap();
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = FlushTester::new(store_dir, flush_switch.clone()).await;

    // Now set should flush to true to trigger flush.
    flush_switch.set_should_flush(true);
    let data = [(1000, Some(100))];
    // Put element to trigger flush.
    tester.put(&data).await;
    tester.wait_flush_done().await;

    // Disable flush.
    flush_switch.set_should_flush(false);
    // Put again.
    let data = [(2000, Some(200))];
    tester.put(&data).await;

    // No parquet file should be flushed.
    let sst_dir = format!("{}/{}", store_dir, engine::region_sst_dir("", REGION_NAME));
    assert!(!has_parquet_file(&sst_dir));

    let expect = vec![(1000, Some(100)), (2000, Some(200))];

    let output = tester.full_scan().await;
    assert_eq!(expect, output);
}

#[tokio::test]
async fn test_read_after_flush() {
    common_telemetry::init_default_ut_logging();

    let dir = TempDir::new("read-flush").unwrap();
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = FlushTester::new(store_dir, flush_switch.clone()).await;

    // Put elements so we have content to flush.
    tester.put(&[(1000, Some(100))]).await;
    tester.put(&[(2000, Some(200))]).await;

    // Now set should flush to true to trigger flush.
    flush_switch.set_should_flush(true);

    // Put element to trigger flush.
    tester.put(&[(3000, Some(300))]).await;
    tester.wait_flush_done().await;

    let expect = vec![(1000, Some(100)), (2000, Some(200)), (3000, Some(300))];

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
    let dir = TempDir::new("merge-read-flush").unwrap();
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    let tester = FlushTester::new(store_dir, flush_switch.clone()).await;

    // Put elements so we have content to flush (In SST1).
    tester.put(&[(3000, Some(300))]).await;
    tester.put(&[(2000, Some(200))]).await;

    // Now set should flush to true to trigger flush.
    flush_switch.set_should_flush(true);

    // Put element to trigger flush (In SST2).
    tester.put(&[(2000, Some(201))]).await;
    tester.wait_flush_done().await;

    // Disable flush.
    flush_switch.set_should_flush(false);
    // In SST2.
    tester.put(&[(2000, Some(202))]).await;
    tester.put(&[(1000, Some(100))]).await;

    // Enable flush.
    flush_switch.set_should_flush(true);
    // Trigger flush and overwrite row (In memtable).
    tester.put(&[(2000, Some(203))]).await;
    tester.wait_flush_done().await;

    let expect = vec![(1000, Some(100)), (2000, Some(203)), (3000, Some(300))];

    let output = tester.full_scan().await;
    assert_eq!(expect, output);

    // Reopen
    let mut tester = tester;
    tester.reopen().await;

    // Scan after reopen.
    let output = tester.full_scan().await;
    assert_eq!(expect, output);
}
