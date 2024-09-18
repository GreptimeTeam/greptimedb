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

//! Mito region engine.

#[cfg(test)]
mod alter_test;
#[cfg(test)]
mod append_mode_test;
#[cfg(test)]
mod basic_test;
#[cfg(test)]
mod batch_open_test;
#[cfg(test)]
mod catchup_test;
#[cfg(test)]
mod close_test;
#[cfg(test)]
mod compaction_test;
#[cfg(test)]
mod create_test;
#[cfg(test)]
mod drop_test;
#[cfg(test)]
mod edit_region_test;
#[cfg(test)]
mod filter_deleted_test;
#[cfg(test)]
mod flush_test;
#[cfg(any(test, feature = "test"))]
pub mod listener;
#[cfg(test)]
mod merge_mode_test;
#[cfg(test)]
mod open_test;
#[cfg(test)]
mod parallel_test;
#[cfg(test)]
mod projection_test;
#[cfg(test)]
mod prune_test;
#[cfg(test)]
mod row_selector_test;
#[cfg(test)]
mod set_readonly_test;
#[cfg(test)]
mod truncate_test;

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use api::region::RegionResponse;
use async_trait::async_trait;
use common_base::Plugins;
use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::tracing;
use common_wal::options::{WalOptions, WAL_OPTIONS_KEY};
use futures::future::{join_all, try_join_all};
use object_store::manager::ObjectStoreManagerRef;
use snafu::{ensure, OptionExt, ResultExt};
use store_api::logstore::provider::Provider;
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    BatchResponses, RegionEngine, RegionRole, RegionScannerRef, SetReadonlyResponse,
};
use store_api::region_request::{AffectedRows, RegionOpenRequest, RegionRequest};
use store_api::storage::{RegionId, ScanRequest};
use tokio::sync::{oneshot, Semaphore};

use crate::config::MitoConfig;
use crate::error::{
    InvalidRequestSnafu, JoinSnafu, RecvSnafu, RegionNotFoundSnafu, Result, SerdeJsonSnafu,
};
use crate::manifest::action::RegionEdit;
use crate::metrics::HANDLE_REQUEST_ELAPSED;
use crate::read::scan_region::{ScanParallism, ScanRegion, Scanner};
use crate::region::RegionUsage;
use crate::request::{RegionEditRequest, WorkerRequest};
use crate::wal::entry_distributor::{
    build_wal_entry_distributor_and_receivers, DEFAULT_ENTRY_RECEIVER_BUFFER_SIZE,
};
use crate::wal::raw_entry_reader::{LogStoreRawEntryReader, RawEntryReader};
use crate::worker::WorkerGroup;

pub const MITO_ENGINE_NAME: &str = "mito";

/// Region engine implementation for timeseries data.
#[derive(Clone)]
pub struct MitoEngine {
    inner: Arc<EngineInner>,
}

impl MitoEngine {
    /// Returns a new [MitoEngine] with specific `config`, `log_store` and `object_store`.
    pub async fn new<S: LogStore>(
        data_home: &str,
        mut config: MitoConfig,
        log_store: Arc<S>,
        object_store_manager: ObjectStoreManagerRef,
        plugins: Plugins,
    ) -> Result<MitoEngine> {
        config.sanitize(data_home)?;

        Ok(MitoEngine {
            inner: Arc::new(
                EngineInner::new(config, log_store, object_store_manager, plugins).await?,
            ),
        })
    }

    /// Returns true if the specific region exists.
    pub fn is_region_exists(&self, region_id: RegionId) -> bool {
        self.inner.workers.is_region_exists(region_id)
    }

    /// Returns true if the specific region exists.
    pub fn is_region_opening(&self, region_id: RegionId) -> bool {
        self.inner.workers.is_region_opening(region_id)
    }

    /// Returns the region disk/memory usage information.
    pub fn get_region_usage(&self, region_id: RegionId) -> Result<RegionUsage> {
        let region = self
            .inner
            .workers
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;

        Ok(region.region_usage())
    }

    /// Handle substrait query and return a stream of record batches
    ///
    /// Notice that the output stream's ordering is not guranateed. If order
    /// matter, please use [`scanner`] to build a [`Scanner`] to consume.
    #[tracing::instrument(skip_all)]
    pub async fn scan_to_stream(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<SendableRecordBatchStream, BoxedError> {
        self.scanner(region_id, request)
            .map_err(BoxedError::new)?
            .scan()
            .await
    }

    /// Returns a scanner to scan for `request`.
    fn scanner(&self, region_id: RegionId, request: ScanRequest) -> Result<Scanner> {
        self.scan_region(region_id, request)?.scanner()
    }

    /// Returns a region scanner to scan the region for `request`.
    async fn region_scanner(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef> {
        let scanner = self.scanner(region_id, request)?;
        scanner.region_scanner().await
    }

    /// Scans a region.
    fn scan_region(&self, region_id: RegionId, request: ScanRequest) -> Result<ScanRegion> {
        self.inner.handle_query(region_id, request)
    }

    /// Edit region's metadata by [RegionEdit] directly. Use with care.
    /// Now we only allow adding files to region (the [RegionEdit] struct can only contain a non-empty "files_to_add" field).
    /// Other region editing intention will result in an "invalid request" error.
    /// Also note that if a region is to be edited directly, we MUST not write data to it thereafter.
    pub async fn edit_region(&self, region_id: RegionId, edit: RegionEdit) -> Result<()> {
        let _timer = HANDLE_REQUEST_ELAPSED
            .with_label_values(&["edit_region"])
            .start_timer();

        ensure!(
            is_valid_region_edit(&edit),
            InvalidRequestSnafu {
                region_id,
                reason: "invalid region edit"
            }
        );

        let (tx, rx) = oneshot::channel();
        let request = WorkerRequest::EditRegion(RegionEditRequest {
            region_id,
            edit,
            tx,
        });
        self.inner
            .workers
            .submit_to_worker(region_id, request)
            .await?;
        rx.await.context(RecvSnafu)?
    }

    #[cfg(test)]
    pub(crate) fn get_region(&self, id: RegionId) -> Option<crate::region::MitoRegionRef> {
        self.inner.workers.get_region(id)
    }
}

/// Check whether the region edit is valid. Only adding files to region is considered valid now.
fn is_valid_region_edit(edit: &RegionEdit) -> bool {
    !edit.files_to_add.is_empty()
        && edit.files_to_remove.is_empty()
        && matches!(
            edit,
            RegionEdit {
                files_to_add: _,
                files_to_remove: _,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
            }
        )
}

/// Inner struct of [MitoEngine].
struct EngineInner {
    /// Region workers group.
    workers: WorkerGroup,
    /// Config of the engine.
    config: Arc<MitoConfig>,
    /// The Wal raw entry reader.
    wal_raw_entry_reader: Arc<dyn RawEntryReader>,
}

type TopicGroupedRegionOpenRequests = HashMap<String, Vec<(RegionId, RegionOpenRequest)>>;

/// Returns requests([TopicGroupedRegionOpenRequests]) grouped by topic and remaining requests.
fn prepare_batch_open_requests(
    requests: Vec<(RegionId, RegionOpenRequest)>,
) -> Result<(
    TopicGroupedRegionOpenRequests,
    Vec<(RegionId, RegionOpenRequest)>,
)> {
    let mut topic_to_regions: HashMap<String, Vec<(RegionId, RegionOpenRequest)>> = HashMap::new();
    let mut remaining_regions: Vec<(RegionId, RegionOpenRequest)> = Vec::new();
    for (region_id, request) in requests {
        let options = if let Some(options) = request.options.get(WAL_OPTIONS_KEY) {
            serde_json::from_str(options).context(SerdeJsonSnafu)?
        } else {
            WalOptions::RaftEngine
        };
        match options {
            WalOptions::Kafka(options) => {
                topic_to_regions
                    .entry(options.topic)
                    .or_default()
                    .push((region_id, request));
            }
            WalOptions::RaftEngine => {
                remaining_regions.push((region_id, request));
            }
        }
    }

    Ok((topic_to_regions, remaining_regions))
}

impl EngineInner {
    /// Returns a new [EngineInner] with specific `config`, `log_store` and `object_store`.
    async fn new<S: LogStore>(
        config: MitoConfig,
        log_store: Arc<S>,
        object_store_manager: ObjectStoreManagerRef,
        plugins: Plugins,
    ) -> Result<EngineInner> {
        let config = Arc::new(config);
        let wal_raw_entry_reader = Arc::new(LogStoreRawEntryReader::new(log_store.clone()));
        Ok(EngineInner {
            workers: WorkerGroup::start(config.clone(), log_store, object_store_manager, plugins)
                .await?,
            config,
            wal_raw_entry_reader,
        })
    }

    /// Stop the inner engine.
    async fn stop(&self) -> Result<()> {
        self.workers.stop().await
    }

    /// Get metadata of a region.
    ///
    /// Returns error if the region doesn't exist.
    fn get_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef> {
        // Reading a region doesn't need to go through the region worker thread.
        let region = self
            .workers
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        Ok(region.metadata())
    }

    async fn open_topic_regions(
        &self,
        topic: String,
        region_requests: Vec<(RegionId, RegionOpenRequest)>,
    ) -> Result<Vec<(RegionId, Result<AffectedRows>)>> {
        let region_ids = region_requests
            .iter()
            .map(|(region_id, _)| *region_id)
            .collect::<Vec<_>>();
        let provider = Provider::kafka_provider(topic);
        let (distributor, entry_receivers) = build_wal_entry_distributor_and_receivers(
            provider,
            self.wal_raw_entry_reader.clone(),
            &region_ids,
            DEFAULT_ENTRY_RECEIVER_BUFFER_SIZE,
        );

        let mut responses = Vec::with_capacity(region_requests.len());
        for ((region_id, request), entry_receiver) in
            region_requests.into_iter().zip(entry_receivers)
        {
            let (request, receiver) =
                WorkerRequest::new_open_region_request(region_id, request, Some(entry_receiver));
            self.workers.submit_to_worker(region_id, request).await?;
            responses.push(async move { receiver.await.context(RecvSnafu)? });
        }

        // Waits for entries distribution.
        let distribution =
            common_runtime::spawn_global(async move { distributor.distribute().await });
        // Waits for worker returns.
        let responses = join_all(responses).await;

        distribution.await.context(JoinSnafu)??;
        Ok(region_ids.into_iter().zip(responses).collect())
    }

    async fn handle_batch_open_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionOpenRequest)>,
    ) -> Result<Vec<(RegionId, Result<AffectedRows>)>> {
        let semaphore = Arc::new(Semaphore::new(parallelism));
        let (topic_to_region_requests, remaining_region_requests) =
            prepare_batch_open_requests(requests)?;
        let mut responses =
            Vec::with_capacity(topic_to_region_requests.len() + remaining_region_requests.len());

        if !topic_to_region_requests.is_empty() {
            let mut tasks = Vec::with_capacity(topic_to_region_requests.len());
            for (topic, region_requests) in topic_to_region_requests {
                let semaphore_moved = semaphore.clone();
                tasks.push(async move {
                    // Safety: semaphore must exist
                    let _permit = semaphore_moved.acquire().await.unwrap();
                    self.open_topic_regions(topic, region_requests).await
                })
            }
            let r = try_join_all(tasks).await?;
            responses.extend(r.into_iter().flatten());
        }

        if !remaining_region_requests.is_empty() {
            let mut tasks = Vec::with_capacity(remaining_region_requests.len());
            let mut region_ids = Vec::with_capacity(remaining_region_requests.len());
            for (region_id, request) in remaining_region_requests {
                let semaphore_moved = semaphore.clone();
                region_ids.push(region_id);
                tasks.push(async move {
                    // Safety: semaphore must exist
                    let _permit = semaphore_moved.acquire().await.unwrap();
                    let (request, receiver) =
                        WorkerRequest::new_open_region_request(region_id, request, None);

                    self.workers.submit_to_worker(region_id, request).await?;

                    receiver.await.context(RecvSnafu)?
                })
            }

            let results = join_all(tasks).await;
            responses.extend(region_ids.into_iter().zip(results));
        }

        Ok(responses)
    }

    /// Handles [RegionRequest] and return its executed result.
    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<AffectedRows> {
        let (request, receiver) = WorkerRequest::try_from_region_request(region_id, request)?;
        self.workers.submit_to_worker(region_id, request).await?;

        receiver.await.context(RecvSnafu)?
    }

    /// Handles the scan `request` and returns a [ScanRegion].
    fn handle_query(&self, region_id: RegionId, request: ScanRequest) -> Result<ScanRegion> {
        let query_start = Instant::now();
        // Reading a region doesn't need to go through the region worker thread.
        let region = self
            .workers
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        let version = region.version();
        // Get cache.
        let cache_manager = self.workers.cache_manager();
        let scan_parallelism = ScanParallism {
            parallelism: self.config.scan_parallelism,
            channel_size: self.config.parallel_scan_channel_size,
        };

        let scan_region = ScanRegion::new(
            version,
            region.access_layer.clone(),
            request,
            Some(cache_manager),
        )
        .with_parallelism(scan_parallelism)
        .with_ignore_inverted_index(self.config.inverted_index.apply_on_query.disabled())
        .with_ignore_fulltext_index(self.config.fulltext_index.apply_on_query.disabled())
        .with_start_time(query_start);

        Ok(scan_region)
    }

    /// Set writable mode for a region.
    fn set_writable(&self, region_id: RegionId, writable: bool) -> Result<()> {
        let region = self
            .workers
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;

        region.set_writable(writable);
        Ok(())
    }

    /// Sets read-only for a region and ensures no more writes in the region after it returns.
    async fn set_readonly_gracefully(&self, region_id: RegionId) -> Result<SetReadonlyResponse> {
        // Notes: It acquires the mutable ownership to ensure no other threads,
        // Therefore, we submit it to the worker.
        let (request, receiver) = WorkerRequest::new_set_readonly_gracefully(region_id);
        self.workers.submit_to_worker(region_id, request).await?;

        receiver.await.context(RecvSnafu)
    }

    fn role(&self, region_id: RegionId) -> Option<RegionRole> {
        self.workers.get_region(region_id).map(|region| {
            if region.is_readonly() {
                RegionRole::Follower
            } else {
                RegionRole::Leader
            }
        })
    }
}

#[async_trait]
impl RegionEngine for MitoEngine {
    fn name(&self) -> &str {
        MITO_ENGINE_NAME
    }

    #[tracing::instrument(skip_all)]
    async fn handle_batch_open_requests(
        &self,
        parallelism: usize,
        requests: Vec<(RegionId, RegionOpenRequest)>,
    ) -> Result<BatchResponses, BoxedError> {
        // TODO(weny): add metrics.
        self.inner
            .handle_batch_open_requests(parallelism, requests)
            .await
            .map(|responses| {
                responses
                    .into_iter()
                    .map(|(region_id, response)| {
                        (
                            region_id,
                            response.map(RegionResponse::new).map_err(BoxedError::new),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .map_err(BoxedError::new)
    }

    #[tracing::instrument(skip_all)]
    async fn handle_request(
        &self,
        region_id: RegionId,
        request: RegionRequest,
    ) -> Result<RegionResponse, BoxedError> {
        let _timer = HANDLE_REQUEST_ELAPSED
            .with_label_values(&[request.request_type()])
            .start_timer();

        self.inner
            .handle_request(region_id, request)
            .await
            .map(RegionResponse::new)
            .map_err(BoxedError::new)
    }

    #[tracing::instrument(skip_all)]
    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef, BoxedError> {
        self.region_scanner(region_id, request)
            .await
            .map_err(BoxedError::new)
    }

    /// Retrieve region's metadata.
    async fn get_metadata(
        &self,
        region_id: RegionId,
    ) -> std::result::Result<RegionMetadataRef, BoxedError> {
        self.inner.get_metadata(region_id).map_err(BoxedError::new)
    }

    /// Stop the engine.
    ///
    /// Stopping the engine doesn't stop the underlying log store as other components might
    /// still use it. (When no other components are referencing the log store, it will
    /// automatically shutdown.)
    async fn stop(&self) -> std::result::Result<(), BoxedError> {
        self.inner.stop().await.map_err(BoxedError::new)
    }

    fn region_disk_usage(&self, region_id: RegionId) -> Option<i64> {
        let size = self
            .get_region_usage(region_id)
            .map(|usage| usage.disk_usage())
            .ok()?;
        size.try_into().ok()
    }

    fn set_writable(&self, region_id: RegionId, writable: bool) -> Result<(), BoxedError> {
        self.inner
            .set_writable(region_id, writable)
            .map_err(BoxedError::new)
    }

    async fn set_readonly_gracefully(
        &self,
        region_id: RegionId,
    ) -> Result<SetReadonlyResponse, BoxedError> {
        let _timer = HANDLE_REQUEST_ELAPSED
            .with_label_values(&["set_readonly_gracefully"])
            .start_timer();

        self.inner
            .set_readonly_gracefully(region_id)
            .await
            .map_err(BoxedError::new)
    }

    fn role(&self, region_id: RegionId) -> Option<RegionRole> {
        self.inner.role(region_id)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// Tests methods.
#[cfg(any(test, feature = "test"))]
impl MitoEngine {
    /// Returns a new [MitoEngine] for tests.
    pub async fn new_for_test<S: LogStore>(
        data_home: &str,
        mut config: MitoConfig,
        log_store: Arc<S>,
        object_store_manager: ObjectStoreManagerRef,
        write_buffer_manager: Option<crate::flush::WriteBufferManagerRef>,
        listener: Option<crate::engine::listener::EventListenerRef>,
        time_provider: crate::time_provider::TimeProviderRef,
    ) -> Result<MitoEngine> {
        config.sanitize(data_home)?;

        let config = Arc::new(config);
        let wal_raw_entry_reader = Arc::new(LogStoreRawEntryReader::new(log_store.clone()));
        Ok(MitoEngine {
            inner: Arc::new(EngineInner {
                workers: WorkerGroup::start_for_test(
                    config.clone(),
                    log_store,
                    object_store_manager,
                    write_buffer_manager,
                    listener,
                    time_provider,
                )
                .await?,
                config,
                wal_raw_entry_reader,
            }),
        })
    }

    /// Returns the purge scheduler.
    pub fn purge_scheduler(&self) -> &crate::schedule::scheduler::SchedulerRef {
        self.inner.workers.purge_scheduler()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::sst::file::FileMeta;

    #[test]
    fn test_is_valid_region_edit() {
        // Valid: has only "files_to_add"
        let edit = RegionEdit {
            files_to_add: vec![FileMeta::default()],
            files_to_remove: vec![],
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
        };
        assert!(is_valid_region_edit(&edit));

        // Invalid: "files_to_add" is empty
        let edit = RegionEdit {
            files_to_add: vec![],
            files_to_remove: vec![],
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
        };
        assert!(!is_valid_region_edit(&edit));

        // Invalid: "files_to_remove" is not empty
        let edit = RegionEdit {
            files_to_add: vec![FileMeta::default()],
            files_to_remove: vec![FileMeta::default()],
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
        };
        assert!(!is_valid_region_edit(&edit));

        // Invalid: other fields are not all "None"s
        let edit = RegionEdit {
            files_to_add: vec![FileMeta::default()],
            files_to_remove: vec![],
            compaction_time_window: Some(Duration::from_secs(1)),
            flushed_entry_id: None,
            flushed_sequence: None,
        };
        assert!(!is_valid_region_edit(&edit));
        let edit = RegionEdit {
            files_to_add: vec![FileMeta::default()],
            files_to_remove: vec![],
            compaction_time_window: None,
            flushed_entry_id: Some(1),
            flushed_sequence: None,
        };
        assert!(!is_valid_region_edit(&edit));
        let edit = RegionEdit {
            files_to_add: vec![FileMeta::default()],
            files_to_remove: vec![],
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: Some(1),
        };
        assert!(!is_valid_region_edit(&edit));
    }
}
