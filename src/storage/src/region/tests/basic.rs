//! Region read/write tests.

use log_store::fs::log::LocalFileLogStore;
use store_api::storage::{OpenOptions, SequenceNumber, WriteResponse};
use tempdir::TempDir;

use crate::error::Result;
use crate::region::tests::{self, FileTesterBase};
use crate::region::RegionImpl;
use crate::test_util::config_util;

const REGION_NAME: &str = "region-basic-0";

/// Create a new region for basic tests.
async fn create_region_for_basic(
    region_name: &str,
    store_dir: &str,
    enable_version_column: bool,
) -> RegionImpl<LocalFileLogStore> {
    let metadata = tests::new_metadata(region_name, enable_version_column);

    let store_config = config_util::new_store_config(region_name, store_dir).await;

    RegionImpl::create(metadata, store_config).await.unwrap()
}

/// Tester for basic tests.
struct Tester {
    region_name: String,
    store_dir: String,
    base: Option<FileTesterBase>,
}

impl Tester {
    async fn new(region_name: &str, store_dir: &str) -> Tester {
        let region = create_region_for_basic(region_name, store_dir, false).await;

        Tester {
            region_name: region_name.to_string(),
            store_dir: store_dir.to_string(),
            base: Some(FileTesterBase::with_region(region)),
        }
    }

    async fn empty(region_name: &str, store_dir: &str) -> Tester {
        Tester {
            region_name: region_name.to_string(),
            store_dir: store_dir.to_string(),
            base: None,
        }
    }

    async fn reopen(&mut self) {
        self.try_reopen().await.unwrap();
    }

    async fn try_reopen(&mut self) -> Result<bool> {
        // Close the old region.
        self.base = None;
        // Reopen the region.
        let store_config = config_util::new_store_config(&self.region_name, &self.store_dir).await;
        let opts = OpenOptions::default();
        let region = RegionImpl::open(self.region_name.clone(), store_config, &opts).await?;
        match region {
            None => Ok(false),
            Some(region) => {
                let base = FileTesterBase::with_region(region);
                self.base = Some(base);
                Ok(true)
            }
        }
    }

    #[inline]
    fn base(&self) -> &FileTesterBase {
        self.base.as_ref().unwrap()
    }

    #[inline]
    fn set_batch_size(&mut self, batch_size: usize) {
        self.base.as_mut().unwrap().read_ctx.batch_size = batch_size;
    }

    async fn put(&self, data: &[(i64, Option<i64>)]) -> WriteResponse {
        self.base().put(data).await
    }

    async fn full_scan(&self) -> Vec<(i64, Option<i64>)> {
        self.base().full_scan().await
    }

    fn committed_sequence(&self) -> SequenceNumber {
        self.base().committed_sequence()
    }
}

#[tokio::test]
async fn test_simple_put_scan() {
    let dir = TempDir::new("put-scan").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let tester = Tester::new(REGION_NAME, store_dir).await;

    let data = vec![
        (1000, Some(100)),
        (1001, Some(101)),
        (1002, None),
        (1003, Some(103)),
        (1004, Some(104)),
    ];

    tester.put(&data).await;

    let output = tester.full_scan().await;
    assert_eq!(data, output);
}

#[tokio::test]
async fn test_sequence_increase() {
    let dir = TempDir::new("sequence").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let tester = Tester::new(REGION_NAME, store_dir).await;

    let mut committed_sequence = tester.committed_sequence();
    for i in 0..100 {
        tester.put(&[(i, Some(1234))]).await;
        committed_sequence += 1;

        assert_eq!(committed_sequence, tester.committed_sequence());
    }
}

#[tokio::test]
async fn test_reopen() {
    common_telemetry::logging::init_default_ut_logging();

    let dir = TempDir::new("reopen").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let mut tester = Tester::new(REGION_NAME, store_dir).await;

    let mut all_data = Vec::new();
    // Reopen region multiple times.
    for i in 0..5 {
        let data = (i, Some(i));
        tester.put(&[data]).await;
        all_data.push(data);

        let output = tester.full_scan().await;
        assert_eq!(all_data, output);

        tester.reopen().await;

        // Scan after reopen.
        let output = tester.full_scan().await;
        assert_eq!(all_data, output);

        // Check committed sequence.
        assert_eq!(i + 1, tester.committed_sequence() as i64);
    }
}

#[tokio::test]
async fn test_open_empty() {
    let dir = TempDir::new("open-empty").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let mut tester = Tester::empty(REGION_NAME, store_dir).await;

    let ret = tester.try_reopen().await;
    assert!(!ret.unwrap());
}

#[tokio::test]
async fn test_scan_different_batch() {
    let dir = TempDir::new("different-batch").unwrap();
    let store_dir = dir.path().to_str().unwrap();
    let mut tester = Tester::new(REGION_NAME, store_dir).await;

    let data: Vec<_> = (0..=2000).map(|i| (i, Some(i))).collect();

    for chunk in data.chunks(100) {
        tester.put(&chunk).await;
    }

    let batch_sizes = [1, 2, 4, 16, 64, 128, 256, 512];
    for batch_size in batch_sizes {
        tester.set_batch_size(batch_size);

        let output = tester.full_scan().await;
        assert_eq!(data, output);
    }
}
