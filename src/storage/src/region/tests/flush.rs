//! Region flush tests.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use log_store::fs::log::LocalFileLogStore;
use store_api::storage::WriteResponse;
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
    base: FileTesterBase,
}

impl FlushTester {
    async fn new(store_dir: &str, flush_strategy: FlushStrategyRef) -> FlushTester {
        let region = create_region_for_flush(store_dir, false, flush_strategy).await;

        FlushTester {
            base: FileTesterBase::with_region(region),
        }
    }

    async fn put(&self, data: &[(i64, Option<i64>)]) -> WriteResponse {
        self.base.put(data).await
    }

    async fn full_scan(&self) -> Vec<(i64, Option<i64>)> {
        self.base.full_scan().await
    }

    async fn wait_flush_done(&self) {
        self.base.region.wait_flush_done().await.unwrap();
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

#[tokio::test]
async fn test_flush_and_stall() {
    common_telemetry::init_default_ut_logging();

    let dir = TempDir::new("flush-stall").unwrap();
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    // Always trigger flush before write.
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
    let mut has_parquet_file = false;
    for entry in std::fs::read_dir(sst_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if !path.is_dir() {
            assert_eq!("parquet", path.extension().unwrap());
            has_parquet_file = true;
        }
    }
    assert!(has_parquet_file);
}

#[tokio::test]
async fn test_read_after_flush() {
    common_telemetry::init_default_ut_logging();

    let dir = TempDir::new("read-flush").unwrap();
    let store_dir = dir.path().to_str().unwrap();

    let flush_switch = Arc::new(FlushSwitch::default());
    // Always trigger flush before write.
    let tester = FlushTester::new(store_dir, flush_switch.clone()).await;

    // Put elements so we have content to flush.
    tester.put(&[(1000, Some(100))]).await;
    tester.put(&[(2000, Some(200))]).await;

    // Now set should flush to true to trigger flush.
    flush_switch.set_should_flush(true);

    // Put element to trigger flush.
    tester.put(&[(3000, Some(300))]).await;
    tester.wait_flush_done().await;

    let expect = vec![(3000, Some(300)), (1000, Some(100)), (2000, Some(200))];

    let output = tester.full_scan().await;
    assert_eq!(expect, output);
}
