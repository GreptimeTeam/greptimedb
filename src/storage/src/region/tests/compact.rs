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

use std::env;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common_telemetry::logging;
use common_test_util::temp_dir::create_temp_dir;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use object_store::services::{Fs, S3};
use object_store::ObjectStore;
use store_api::storage::{FlushContext, FlushReason, Region, WriteResponse};
use tokio::sync::Notify;

use crate::compaction::{CompactionHandler, SimplePicker};
use crate::config::EngineConfig;
use crate::error::Result;
use crate::file_purger::{FilePurgeHandler, FilePurgeRequest};
use crate::region::tests::{self, FileTesterBase};
use crate::region::{CompactContext, FlushStrategyRef, RegionImpl};
use crate::scheduler::rate_limit::BoxedRateLimitToken;
use crate::scheduler::{Handler, LocalScheduler, SchedulerConfig};
use crate::test_util::config_util;
use crate::test_util::flush_switch::FlushSwitch;

const REGION_NAME: &str = "region-compact-0";

fn new_object_store(store_dir: &str, s3_bucket: Option<String>) -> ObjectStore {
    if let Some(bucket) = s3_bucket {
        if !bucket.is_empty() {
            logging::info!("Use S3 object store");

            let root = uuid::Uuid::new_v4().to_string();

            let mut builder = S3::default();
            builder
                .root(&root)
                .access_key_id(&env::var("GT_S3_ACCESS_KEY_ID").unwrap())
                .secret_access_key(&env::var("GT_S3_ACCESS_KEY").unwrap())
                .region(&env::var("GT_S3_REGION").unwrap())
                .bucket(&bucket);

            return ObjectStore::new(builder).unwrap().finish();
        }
    }

    logging::info!("Use local fs object store");

    let mut builder = Fs::default();
    builder.root(store_dir);
    ObjectStore::new(builder).unwrap().finish()
}

/// Create a new region for compaction test
async fn create_region_for_compaction<
    H: Handler<Request = FilePurgeRequest> + Send + Sync + 'static,
>(
    store_dir: &str,
    engine_config: EngineConfig,
    purge_handler: H,
    flush_strategy: FlushStrategyRef,
    s3_bucket: Option<String>,
) -> (RegionImpl<RaftEngineLogStore>, ObjectStore) {
    let metadata = tests::new_metadata(REGION_NAME);

    let object_store = new_object_store(store_dir, s3_bucket);

    let (mut store_config, _) = config_util::new_store_config_with_object_store(
        REGION_NAME,
        store_dir,
        object_store.clone(),
        None,
    )
    .await;
    store_config.engine_config = Arc::new(engine_config);
    store_config.flush_strategy = flush_strategy;

    let picker = SimplePicker::default();
    let handler = CompactionHandler::new(picker);
    let config = SchedulerConfig::default();
    // Overwrite test compaction scheduler and file purger.
    store_config.compaction_scheduler = Arc::new(LocalScheduler::new(config, handler));
    store_config.file_purger = Arc::new(LocalScheduler::new(
        SchedulerConfig {
            max_inflight_tasks: store_config.engine_config.max_purge_tasks,
        },
        purge_handler,
    ));

    (
        RegionImpl::create(metadata, store_config).await.unwrap(),
        object_store,
    )
}

#[derive(Debug, Default, Clone)]
struct MockFilePurgeHandler {
    num_deleted: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Handler for MockFilePurgeHandler {
    type Request = FilePurgeRequest;

    async fn handle_request(
        &self,
        req: Self::Request,
        token: BoxedRateLimitToken,
        finish_notifier: Arc<Notify>,
    ) -> Result<()> {
        logging::info!(
            "Try to delete file: {:?}, num_deleted: {:?}",
            req.file_id,
            self.num_deleted
        );

        let handler = FilePurgeHandler;
        handler
            .handle_request(req, token, finish_notifier)
            .await
            .unwrap();

        self.num_deleted.fetch_add(1, Ordering::Relaxed);

        Ok(())
    }
}

impl MockFilePurgeHandler {
    fn num_deleted(&self) -> usize {
        self.num_deleted.load(Ordering::Relaxed)
    }
}

/// Tester for region compaction.
struct CompactionTester {
    base: Option<FileTesterBase>,
    purge_handler: MockFilePurgeHandler,
    object_store: ObjectStore,
}

impl CompactionTester {
    async fn new(
        store_dir: &str,
        engine_config: EngineConfig,
        flush_strategy: FlushStrategyRef,
        s3_bucket: Option<String>,
    ) -> CompactionTester {
        let purge_handler = MockFilePurgeHandler::default();
        let (region, object_store) = create_region_for_compaction(
            store_dir,
            engine_config.clone(),
            purge_handler.clone(),
            flush_strategy,
            s3_bucket,
        )
        .await;

        CompactionTester {
            base: Some(FileTesterBase::with_region(region)),
            purge_handler,
            object_store,
        }
    }

    #[inline]
    fn base(&self) -> &FileTesterBase {
        self.base.as_ref().unwrap()
    }

    #[inline]
    fn base_mut(&mut self) -> &mut FileTesterBase {
        self.base.as_mut().unwrap()
    }

    async fn put(&self, data: &[(i64, Option<i64>)]) -> WriteResponse {
        let data = data
            .iter()
            .map(|(ts, v0)| (*ts, v0.map(|v| v.to_string())))
            .collect::<Vec<_>>();
        self.base().put(&data).await
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

    async fn compact(&self) {
        // Trigger compaction and wait until it is done.
        self.base()
            .region
            .compact(CompactContext::default())
            .await
            .unwrap();
    }

    /// Close region and clean up files.
    async fn clean_up(mut self) {
        self.base = None;

        self.object_store.remove_all("/").await.unwrap();
    }
}

async fn compact_during_read(s3_bucket: Option<String>) {
    let dir = create_temp_dir("compact_read");
    let store_dir = dir.path().to_str().unwrap();

    // Use a large max_files_in_l0 to avoid compaction automatically.
    let mut tester = CompactionTester::new(
        store_dir,
        EngineConfig {
            max_files_in_l0: 100,
            ..Default::default()
        },
        // Disable auto-flush.
        Arc::new(FlushSwitch::default()),
        s3_bucket,
    )
    .await;

    let expect: Vec<_> = (0..200).map(|v| (v, Some(v))).collect();
    // Put elements so we have content to flush (In SST1).
    tester.put(&expect[0..100]).await;

    // Flush content to SST1.
    tester.flush(None).await;

    // Put element (In SST2).
    tester.put(&expect[100..200]).await;

    // Flush content to SST2.
    tester.flush(None).await;

    tester.base_mut().read_ctx.batch_size = 1;
    // Create a reader.
    let reader = tester.base().full_scan_reader().await;

    assert_eq!(0, tester.purge_handler.num_deleted());

    // Trigger compaction.
    tester.compact().await;

    // The files are still referenced.
    assert_eq!(0, tester.purge_handler.num_deleted());

    // Read from the reader.
    let output = tester.base().collect_reader(reader).await;

    assert_eq!(expect.len(), output.len());

    tester.clean_up().await;
}

#[tokio::test]
async fn test_compact_during_read_on_fs() {
    common_telemetry::init_default_ut_logging();

    compact_during_read(None).await;
}

#[tokio::test]
async fn test_compact_during_read_on_s3() {
    common_telemetry::init_default_ut_logging();

    if let Ok(bucket) = env::var("GT_S3_BUCKET") {
        if !bucket.is_empty() {
            compact_during_read(Some(bucket)).await;
        }
    }
}
