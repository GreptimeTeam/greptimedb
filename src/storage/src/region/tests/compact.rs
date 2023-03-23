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

//! Region compaction tests.

use std::sync::Arc;

use common_test_util::temp_dir::create_temp_dir;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use store_api::storage::{FlushContext, Region, WriteResponse};

use crate::compaction::{CompactionHandler, SimplePicker};
use crate::config::EngineConfig;
use crate::region::tests::{self, FileTesterBase};
use crate::region::{CompactContext, RegionImpl};
use crate::scheduler::{LocalScheduler, SchedulerConfig};
use crate::test_util::config_util;

const REGION_NAME: &str = "region-compact-0";

/// Create a new region for compaction test
async fn create_region_for_compaction(
    store_dir: &str,
    enable_version_column: bool,
    engine_config: EngineConfig,
) -> RegionImpl<RaftEngineLogStore> {
    let metadata = tests::new_metadata(REGION_NAME, enable_version_column);

    let mut store_config = config_util::new_store_config(REGION_NAME, store_dir).await;
    store_config.engine_config = Arc::new(engine_config);
    let picker = SimplePicker::default();
    let handler = CompactionHandler::new(picker);
    let config = SchedulerConfig::default();
    store_config.compaction_scheduler = Arc::new(LocalScheduler::new(config, handler));

    RegionImpl::create(metadata, store_config).await.unwrap()
}

/// Tester for region compaction.
struct CompactionTester {
    base: Option<FileTesterBase>,
}

impl CompactionTester {
    async fn new(store_dir: &str, engine_config: EngineConfig) -> CompactionTester {
        let region = create_region_for_compaction(store_dir, false, engine_config.clone()).await;

        CompactionTester {
            base: Some(FileTesterBase::with_region(region)),
        }
    }

    #[inline]
    fn base(&self) -> &FileTesterBase {
        self.base.as_ref().unwrap()
    }

    async fn put(&self, data: &[(i64, Option<i64>)]) -> WriteResponse {
        self.base().put(data).await
    }

    async fn flush(&self, wait: Option<bool>) {
        let ctx = wait.map(|wait| FlushContext { wait }).unwrap_or_default();
        self.base().region.flush(&ctx).await.unwrap();
    }

    async fn compact(&self) {
        // Trigger compaction and wait until it is done.
        self.base()
            .region
            .compact(CompactContext::default())
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn test_compact_during_read() {
    common_telemetry::init_default_ut_logging();

    let dir = create_temp_dir("compact_read");

    let store_dir = dir.path().to_str().unwrap();

    // Use a large max_files_in_l0 to avoid compaction automatically.
    let tester = CompactionTester::new(
        store_dir,
        EngineConfig {
            max_files_in_l0: 100,
            ..Default::default()
        },
    )
    .await;

    // Put elements so we have content to flush (In SST1).
    tester.put(&[(3000, Some(300))]).await;
    tester.put(&[(2000, Some(200))]).await;

    // Flush content to SST1.
    tester.flush(None).await;

    // Put element (In SST2).
    tester.put(&[(2000, Some(202))]).await;
    tester.put(&[(1000, Some(100))]).await;

    // Flush content to SST2.
    tester.flush(None).await;

    // Create a reader.
    let reader = tester.base().full_scan_reader().await;

    // Trigger compaction.
    tester.compact().await;

    // Read from the reader.
    let output = tester.base().collect_reader(reader).await;

    let expect = vec![(1000, Some(100)), (2000, Some(202)), (3000, Some(300))];
    assert_eq!(expect, output);
}
