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

//! Region read/write tests.

use common_telemetry::info;
use common_test_util::temp_dir::create_temp_dir;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use store_api::storage::{OpenOptions, SequenceNumber, WriteResponse};

use crate::error::Result;
use crate::region::tests::{self, FileTesterBase};
use crate::region::RegionImpl;
use crate::test_util::config_util;

const REGION_NAME: &str = "region-basic-0";

/// Create a new region for basic tests.
async fn create_region_for_basic(
    region_name: &str,
    store_dir: &str,
) -> RegionImpl<RaftEngineLogStore> {
    let metadata = tests::new_metadata(region_name);
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
        let region = create_region_for_basic(region_name, store_dir).await;

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
        if let Some(base) = self.base.as_ref() {
            info!("Reopen tester base");
            base.close().await;
        }

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

    async fn delete(&self, keys: &[i64]) -> WriteResponse {
        self.base().delete(keys).await
    }
}

#[tokio::test]
async fn test_simple_put_scan() {
    let dir = create_temp_dir("put-scan");
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
    let dir = create_temp_dir("sequence");
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

    let dir = create_temp_dir("reopen");
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
    let dir = create_temp_dir("open-empty");
    let store_dir = dir.path().to_str().unwrap();
    let mut tester = Tester::empty(REGION_NAME, store_dir).await;

    let ret = tester.try_reopen().await;
    assert!(!ret.unwrap());
}

#[tokio::test]
async fn test_scan_different_batch() {
    let dir = create_temp_dir("different-batch");
    let store_dir = dir.path().to_str().unwrap();
    let mut tester = Tester::new(REGION_NAME, store_dir).await;

    let data: Vec<_> = (0..=2000).map(|i| (i, Some(i))).collect();

    for chunk in data.chunks(100) {
        tester.put(chunk).await;
    }

    let batch_sizes = [1, 2, 4, 16, 64, 128, 256, 512];
    for batch_size in batch_sizes {
        tester.set_batch_size(batch_size);

        let output = tester.full_scan().await;
        assert_eq!(data, output);
    }
}

#[tokio::test]
async fn test_put_delete_scan() {
    common_telemetry::init_default_ut_logging();
    let dir = create_temp_dir("put-delete-scan");
    let store_dir = dir.path().to_str().unwrap();
    let mut tester = Tester::new(REGION_NAME, store_dir).await;

    let data = vec![
        (1000, Some(100)),
        (1001, Some(101)),
        (1002, None),
        (1003, None),
        (1004, Some(104)),
    ];

    tester.put(&data).await;

    let keys = [1001, 1003];

    tester.delete(&keys).await;

    let output = tester.full_scan().await;
    let expect = vec![(1000, Some(100)), (1002, None), (1004, Some(104))];
    assert_eq!(expect, output);

    // Deletion is also persistent.
    tester.try_reopen().await.unwrap();
    let output = tester.full_scan().await;
    assert_eq!(expect, output);
}

#[tokio::test]
async fn test_put_delete_absent_key() {
    let dir = create_temp_dir("put-delete-scan");
    let store_dir = dir.path().to_str().unwrap();
    let mut tester = Tester::new(REGION_NAME, store_dir).await;

    let data = vec![
        (1000, Some(100)),
        (1001, Some(101)),
        (1002, None),
        (1003, None),
        (1004, Some(104)),
    ];

    tester.put(&data).await;

    // 999 and 1006 is absent.
    let keys = [999, 1002, 1004, 1006];

    tester.delete(&keys).await;

    let output = tester.full_scan().await;
    let expect = vec![(1000, Some(100)), (1001, Some(101)), (1003, None)];
    assert_eq!(expect, output);

    // Deletion is also persistent.
    tester.try_reopen().await.unwrap();
    let output = tester.full_scan().await;
    assert_eq!(expect, output);
}
