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
mod scan_test;
#[cfg(test)]
mod set_role_state_test;
#[cfg(test)]
mod staging_test;
#[cfg(test)]
mod sync_test;
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
use common_meta::key::SchemaMetadataManagerRef;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{info, tracing};
use common_wal::options::{WAL_OPTIONS_KEY, WalOptions};
use futures::future::{join_all, try_join_all};
use futures::stream::{self, Stream, StreamExt};
use object_store::manager::ObjectStoreManagerRef;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::ManifestVersion;
use store_api::codec::PrimaryKeyEncoding;
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::metadata::{ColumnMetadata, RegionMetadataRef};
use store_api::metric_engine_consts::{
    MANIFEST_INFO_EXTENSION_KEY, TABLE_COLUMN_METADATA_EXTENSION_KEY,
};
use store_api::region_engine::{
    BatchResponses, RegionEngine, RegionManifestInfo, RegionRole, RegionScannerRef,
    RegionStatistic, SetRegionRoleStateResponse, SettableRegionRoleState, SyncManifestResponse,
};
use store_api::region_request::{AffectedRows, RegionOpenRequest, RegionRequest};
use store_api::sst_entry::{ManifestSstEntry, StorageSstEntry};
use store_api::storage::{RegionId, ScanRequest, SequenceNumber};
use tokio::sync::{Semaphore, oneshot};

use crate::cache::{CacheManagerRef, CacheStrategy};
use crate::config::MitoConfig;
use crate::error::{
    InvalidRequestSnafu, JoinSnafu, MitoManifestInfoSnafu, RecvSnafu, RegionNotFoundSnafu, Result,
    SerdeJsonSnafu, SerializeColumnMetadataSnafu,
};
#[cfg(feature = "enterprise")]
use crate::extension::BoxedExtensionRangeProviderFactory;
use crate::manifest::action::RegionEdit;
use crate::memtable::MemtableStats;
use crate::metrics::HANDLE_REQUEST_ELAPSED;
use crate::read::scan_region::{ScanRegion, Scanner};
use crate::read::stream::ScanBatchStream;
use crate::region::MitoRegionRef;
use crate::request::{RegionEditRequest, WorkerRequest};
use crate::sst::file::FileMeta;
use crate::sst::file_ref::FileReferenceManagerRef;
use crate::wal::entry_distributor::{
    DEFAULT_ENTRY_RECEIVER_BUFFER_SIZE, build_wal_entry_distributor_and_receivers,
};
use crate::wal::raw_entry_reader::{LogStoreRawEntryReader, RawEntryReader};
use crate::worker::WorkerGroup;

pub const MITO_ENGINE_NAME: &str = "mito";

pub struct MitoEngineBuilder<'a, S: LogStore> {
    data_home: &'a str,
    config: MitoConfig,
    log_store: Arc<S>,
    object_store_manager: ObjectStoreManagerRef,
    schema_metadata_manager: SchemaMetadataManagerRef,
    file_ref_manager: FileReferenceManagerRef,
    plugins: Plugins,
    #[cfg(feature = "enterprise")]
    extension_range_provider_factory: Option<BoxedExtensionRangeProviderFactory>,
}

impl<'a, S: LogStore> MitoEngineBuilder<'a, S> {
    pub fn new(
        data_home: &'a str,
        config: MitoConfig,
        log_store: Arc<S>,
        object_store_manager: ObjectStoreManagerRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
        file_ref_manager: FileReferenceManagerRef,
        plugins: Plugins,
    ) -> Self {
        Self {
            data_home,
            config,
            log_store,
            object_store_manager,
            schema_metadata_manager,
            file_ref_manager,
            plugins,
            #[cfg(feature = "enterprise")]
            extension_range_provider_factory: None,
        }
    }

    #[cfg(feature = "enterprise")]
    #[must_use]
    pub fn with_extension_range_provider_factory(
        self,
        extension_range_provider_factory: Option<BoxedExtensionRangeProviderFactory>,
    ) -> Self {
        Self {
            extension_range_provider_factory,
            ..self
        }
    }

    pub async fn try_build(mut self) -> Result<MitoEngine> {
        self.config.sanitize(self.data_home)?;

        let config = Arc::new(self.config);
        let workers = WorkerGroup::start(
            config.clone(),
            self.log_store.clone(),
            self.object_store_manager,
            self.schema_metadata_manager,
            self.file_ref_manager,
            self.plugins,
        )
        .await?;
        let wal_raw_entry_reader = Arc::new(LogStoreRawEntryReader::new(self.log_store));
        let inner = EngineInner {
            workers,
            config,
            wal_raw_entry_reader,
            #[cfg(feature = "enterprise")]
            extension_range_provider_factory: None,
        };

        #[cfg(feature = "enterprise")]
        let inner =
            inner.with_extension_range_provider_factory(self.extension_range_provider_factory);

        Ok(MitoEngine {
            inner: Arc::new(inner),
        })
    }
}

/// Region engine implementation for timeseries data.
#[derive(Clone)]
pub struct MitoEngine {
    inner: Arc<EngineInner>,
}

impl MitoEngine {
    /// Returns a new [MitoEngine] with specific `config`, `log_store` and `object_store`.
    pub async fn new<S: LogStore>(
        data_home: &str,
        config: MitoConfig,
        log_store: Arc<S>,
        object_store_manager: ObjectStoreManagerRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
        file_ref_manager: FileReferenceManagerRef,
        plugins: Plugins,
    ) -> Result<MitoEngine> {
        let builder = MitoEngineBuilder::new(
            data_home,
            config,
            log_store,
            object_store_manager,
            schema_metadata_manager,
            file_ref_manager,
            plugins,
        );
        builder.try_build().await
    }

    pub fn mito_config(&self) -> &MitoConfig {
        &self.inner.config
    }

    pub fn cache_manager(&self) -> CacheManagerRef {
        self.inner.workers.cache_manager()
    }

    pub fn file_ref_manager(&self) -> FileReferenceManagerRef {
        self.inner.workers.file_ref_manager()
    }

    /// Returns true if the specific region exists.
    pub fn is_region_exists(&self, region_id: RegionId) -> bool {
        self.inner.workers.is_region_exists(region_id)
    }

    /// Returns true if the specific region exists.
    pub fn is_region_opening(&self, region_id: RegionId) -> bool {
        self.inner.workers.is_region_opening(region_id)
    }

    /// Returns the region disk/memory statistic.
    pub fn get_region_statistic(&self, region_id: RegionId) -> Option<RegionStatistic> {
        self.find_region(region_id)
            .map(|region| region.region_statistic())
    }

    /// Returns primary key encoding of the region.
    pub fn get_primary_key_encoding(&self, region_id: RegionId) -> Option<PrimaryKeyEncoding> {
        self.find_region(region_id)
            .map(|r| r.primary_key_encoding())
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
            .await
            .map_err(BoxedError::new)?
            .scan()
            .await
    }

    /// Scan [`Batch`]es by [`ScanRequest`].
    pub async fn scan_batch(
        &self,
        region_id: RegionId,
        request: ScanRequest,
        filter_deleted: bool,
    ) -> Result<ScanBatchStream> {
        let mut scan_region = self.scan_region(region_id, request)?;
        scan_region.set_filter_deleted(filter_deleted);
        scan_region.scanner().await?.scan_batch()
    }

    /// Returns a scanner to scan for `request`.
    async fn scanner(&self, region_id: RegionId, request: ScanRequest) -> Result<Scanner> {
        self.scan_region(region_id, request)?.scanner().await
    }

    /// Scans a region.
    fn scan_region(&self, region_id: RegionId, request: ScanRequest) -> Result<ScanRegion> {
        self.inner.scan_region(region_id, request)
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
        self.find_region(id)
    }

    pub(crate) fn find_region(&self, region_id: RegionId) -> Option<MitoRegionRef> {
        self.inner.workers.get_region(region_id)
    }

    fn encode_manifest_info_to_extensions(
        region_id: &RegionId,
        manifest_info: RegionManifestInfo,
        extensions: &mut HashMap<String, Vec<u8>>,
    ) -> Result<()> {
        let region_manifest_info = vec![(*region_id, manifest_info)];

        extensions.insert(
            MANIFEST_INFO_EXTENSION_KEY.to_string(),
            RegionManifestInfo::encode_list(&region_manifest_info).context(SerdeJsonSnafu)?,
        );
        info!(
            "Added manifest info: {:?} to extensions, region_id: {:?}",
            region_manifest_info, region_id
        );
        Ok(())
    }

    fn encode_column_metadatas_to_extensions(
        region_id: &RegionId,
        column_metadatas: Vec<ColumnMetadata>,
        extensions: &mut HashMap<String, Vec<u8>>,
    ) -> Result<()> {
        extensions.insert(
            TABLE_COLUMN_METADATA_EXTENSION_KEY.to_string(),
            ColumnMetadata::encode_list(&column_metadatas).context(SerializeColumnMetadataSnafu)?,
        );
        info!(
            "Added column metadatas: {:?} to extensions, region_id: {:?}",
            column_metadatas, region_id
        );
        Ok(())
    }

    /// Find the current version's memtables and SSTs stats by region_id.
    /// The stats must be collected in one place one time to ensure data consistency.
    pub fn find_memtable_and_sst_stats(
        &self,
        region_id: RegionId,
    ) -> Result<(Vec<MemtableStats>, Vec<FileMeta>)> {
        let region = self
            .find_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;

        let version = region.version();
        let memtable_stats = version
            .memtables
            .list_memtables()
            .iter()
            .map(|x| x.stats())
            .collect::<Vec<_>>();

        let sst_stats = version
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files().map(|x| x.meta_ref()))
            .cloned()
            .collect::<Vec<_>>();
        Ok((memtable_stats, sst_stats))
    }

    /// Lists all SSTs from the manifest of all regions in the engine.
    pub fn all_ssts_from_manifest(&self) -> impl Iterator<Item = ManifestSstEntry> + use<'_> {
        self.inner
            .workers
            .all_regions()
            .flat_map(|region| region.manifest_sst_entries())
    }

    /// Lists all SSTs from the storage layer of all regions in the engine.
    pub fn all_ssts_from_storage(&self) -> impl Stream<Item = Result<StorageSstEntry>> {
        let regions = self.inner.workers.all_regions();

        let mut layers_distinct_table_dirs = HashMap::new();
        for region in regions {
            let table_dir = region.access_layer.table_dir();
            if !layers_distinct_table_dirs.contains_key(table_dir) {
                layers_distinct_table_dirs
                    .insert(table_dir.to_string(), region.access_layer.clone());
            }
        }

        stream::iter(layers_distinct_table_dirs)
            .map(|(_, access_layer)| access_layer.storage_sst_entries())
            .flatten()
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
                timestamp_ms: _,
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
    #[cfg(feature = "enterprise")]
    extension_range_provider_factory: Option<BoxedExtensionRangeProviderFactory>,
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
            WalOptions::RaftEngine | WalOptions::Noop => {
                remaining_regions.push((region_id, request));
            }
        }
    }

    Ok((topic_to_regions, remaining_regions))
}

impl EngineInner {
    #[cfg(feature = "enterprise")]
    #[must_use]
    fn with_extension_range_provider_factory(
        self,
        extension_range_provider_factory: Option<BoxedExtensionRangeProviderFactory>,
    ) -> Self {
        Self {
            extension_range_provider_factory,
            ..self
        }
    }

    /// Stop the inner engine.
    async fn stop(&self) -> Result<()> {
        self.workers.stop().await
    }

    fn find_region(&self, region_id: RegionId) -> Result<MitoRegionRef> {
        self.workers
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })
    }

    /// Get metadata of a region.
    ///
    /// Returns error if the region doesn't exist.
    fn get_metadata(&self, region_id: RegionId) -> Result<RegionMetadataRef> {
        // Reading a region doesn't need to go through the region worker thread.
        let region = self.find_region(region_id)?;
        Ok(region.metadata())
    }

    async fn open_topic_regions(
        &self,
        topic: String,
        region_requests: Vec<(RegionId, RegionOpenRequest)>,
    ) -> Result<Vec<(RegionId, Result<AffectedRows>)>> {
        let now = Instant::now();
        let region_ids = region_requests
            .iter()
            .map(|(region_id, _)| *region_id)
            .collect::<Vec<_>>();
        let provider = Provider::kafka_provider(topic);
        let (distributor, entry_receivers) = build_wal_entry_distributor_and_receivers(
            provider.clone(),
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

        let num_failure = responses.iter().filter(|r| r.is_err()).count();
        info!(
            "Opened {} regions for topic '{}', failures: {}, elapsed: {:?}",
            region_ids.len() - num_failure,
            // Safety: provider is kafka provider.
            provider.as_kafka_provider().unwrap(),
            num_failure,
            now.elapsed(),
        );
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
        let region_metadata = self.get_metadata(region_id).ok();
        let (request, receiver) =
            WorkerRequest::try_from_region_request(region_id, request, region_metadata)?;
        self.workers.submit_to_worker(region_id, request).await?;

        receiver.await.context(RecvSnafu)?
    }

    fn get_last_seq_num(&self, region_id: RegionId) -> Result<Option<SequenceNumber>> {
        // Reading a region doesn't need to go through the region worker thread.
        let region = self.find_region(region_id)?;
        Ok(Some(region.find_committed_sequence()))
    }

    /// Handles the scan `request` and returns a [ScanRegion].
    fn scan_region(&self, region_id: RegionId, request: ScanRequest) -> Result<ScanRegion> {
        let query_start = Instant::now();
        // Reading a region doesn't need to go through the region worker thread.
        let region = self.find_region(region_id)?;
        let version = region.version();
        // Get cache.
        let cache_manager = self.workers.cache_manager();

        let scan_region = ScanRegion::new(
            version,
            region.access_layer.clone(),
            request,
            CacheStrategy::EnableAll(cache_manager),
        )
        .with_parallel_scan_channel_size(self.config.parallel_scan_channel_size)
        .with_max_concurrent_scan_files(self.config.max_concurrent_scan_files)
        .with_ignore_inverted_index(self.config.inverted_index.apply_on_query.disabled())
        .with_ignore_fulltext_index(self.config.fulltext_index.apply_on_query.disabled())
        .with_ignore_bloom_filter(self.config.bloom_filter_index.apply_on_query.disabled())
        .with_start_time(query_start)
        // TODO(yingwen): Enable it after flat format is supported.
        .with_flat_format(false);

        #[cfg(feature = "enterprise")]
        let scan_region = self.maybe_fill_extension_range_provider(scan_region, region);

        Ok(scan_region)
    }

    #[cfg(feature = "enterprise")]
    fn maybe_fill_extension_range_provider(
        &self,
        mut scan_region: ScanRegion,
        region: MitoRegionRef,
    ) -> ScanRegion {
        if region.is_follower()
            && let Some(factory) = self.extension_range_provider_factory.as_ref()
        {
            scan_region
                .set_extension_range_provider(factory.create_extension_range_provider(region));
        }
        scan_region
    }

    /// Converts the [`RegionRole`].
    fn set_region_role(&self, region_id: RegionId, role: RegionRole) -> Result<()> {
        let region = self.find_region(region_id)?;
        region.set_role(role);
        Ok(())
    }

    /// Sets read-only for a region and ensures no more writes in the region after it returns.
    async fn set_region_role_state_gracefully(
        &self,
        region_id: RegionId,
        region_role_state: SettableRegionRoleState,
    ) -> Result<SetRegionRoleStateResponse> {
        // Notes: It acquires the mutable ownership to ensure no other threads,
        // Therefore, we submit it to the worker.
        let (request, receiver) =
            WorkerRequest::new_set_readonly_gracefully(region_id, region_role_state);
        self.workers.submit_to_worker(region_id, request).await?;

        receiver.await.context(RecvSnafu)
    }

    async fn sync_region(
        &self,
        region_id: RegionId,
        manifest_info: RegionManifestInfo,
    ) -> Result<(ManifestVersion, bool)> {
        ensure!(manifest_info.is_mito(), MitoManifestInfoSnafu);
        let manifest_version = manifest_info.data_manifest_version();
        let (request, receiver) =
            WorkerRequest::new_sync_region_request(region_id, manifest_version);
        self.workers.submit_to_worker(region_id, request).await?;

        receiver.await.context(RecvSnafu)?
    }

    fn role(&self, region_id: RegionId) -> Option<RegionRole> {
        self.workers.get_region(region_id).map(|region| {
            if region.is_follower() {
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

        let is_alter = matches!(request, RegionRequest::Alter(_));
        let is_create = matches!(request, RegionRequest::Create(_));
        let mut response = self
            .inner
            .handle_request(region_id, request)
            .await
            .map(RegionResponse::new)
            .map_err(BoxedError::new)?;

        if is_alter {
            self.handle_alter_response(region_id, &mut response)
                .map_err(BoxedError::new)?;
        } else if is_create {
            self.handle_create_response(region_id, &mut response)
                .map_err(BoxedError::new)?;
        }

        Ok(response)
    }

    #[tracing::instrument(skip_all)]
    async fn handle_query(
        &self,
        region_id: RegionId,
        request: ScanRequest,
    ) -> Result<RegionScannerRef, BoxedError> {
        self.scan_region(region_id, request)
            .map_err(BoxedError::new)?
            .region_scanner()
            .await
            .map_err(BoxedError::new)
    }

    async fn get_last_seq_num(
        &self,
        region_id: RegionId,
    ) -> Result<Option<SequenceNumber>, BoxedError> {
        self.inner
            .get_last_seq_num(region_id)
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

    fn region_statistic(&self, region_id: RegionId) -> Option<RegionStatistic> {
        self.get_region_statistic(region_id)
    }

    fn set_region_role(&self, region_id: RegionId, role: RegionRole) -> Result<(), BoxedError> {
        self.inner
            .set_region_role(region_id, role)
            .map_err(BoxedError::new)
    }

    async fn set_region_role_state_gracefully(
        &self,
        region_id: RegionId,
        region_role_state: SettableRegionRoleState,
    ) -> Result<SetRegionRoleStateResponse, BoxedError> {
        let _timer = HANDLE_REQUEST_ELAPSED
            .with_label_values(&["set_region_role_state_gracefully"])
            .start_timer();

        self.inner
            .set_region_role_state_gracefully(region_id, region_role_state)
            .await
            .map_err(BoxedError::new)
    }

    async fn sync_region(
        &self,
        region_id: RegionId,
        manifest_info: RegionManifestInfo,
    ) -> Result<SyncManifestResponse, BoxedError> {
        let (_, synced) = self
            .inner
            .sync_region(region_id, manifest_info)
            .await
            .map_err(BoxedError::new)?;

        Ok(SyncManifestResponse::Mito { synced })
    }

    fn role(&self, region_id: RegionId) -> Option<RegionRole> {
        self.inner.role(region_id)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl MitoEngine {
    fn handle_alter_response(
        &self,
        region_id: RegionId,
        response: &mut RegionResponse,
    ) -> Result<()> {
        if let Some(statistic) = self.region_statistic(region_id) {
            Self::encode_manifest_info_to_extensions(
                &region_id,
                statistic.manifest,
                &mut response.extensions,
            )?;
        }
        let column_metadatas = self
            .inner
            .find_region(region_id)
            .ok()
            .map(|r| r.metadata().column_metadatas.clone());
        if let Some(column_metadatas) = column_metadatas {
            Self::encode_column_metadatas_to_extensions(
                &region_id,
                column_metadatas,
                &mut response.extensions,
            )?;
        }
        Ok(())
    }

    fn handle_create_response(
        &self,
        region_id: RegionId,
        response: &mut RegionResponse,
    ) -> Result<()> {
        let column_metadatas = self
            .inner
            .find_region(region_id)
            .ok()
            .map(|r| r.metadata().column_metadatas.clone());
        if let Some(column_metadatas) = column_metadatas {
            Self::encode_column_metadatas_to_extensions(
                &region_id,
                column_metadatas,
                &mut response.extensions,
            )?;
        }
        Ok(())
    }
}

// Tests methods.
#[cfg(any(test, feature = "test"))]
#[allow(clippy::too_many_arguments)]
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
        schema_metadata_manager: SchemaMetadataManagerRef,
        file_ref_manager: FileReferenceManagerRef,
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
                    schema_metadata_manager,
                    file_ref_manager,
                    time_provider,
                )
                .await?,
                config,
                wal_raw_entry_reader,
                #[cfg(feature = "enterprise")]
                extension_range_provider_factory: None,
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
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
        };
        assert!(is_valid_region_edit(&edit));

        // Invalid: "files_to_add" is empty
        let edit = RegionEdit {
            files_to_add: vec![],
            files_to_remove: vec![],
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
        };
        assert!(!is_valid_region_edit(&edit));

        // Invalid: "files_to_remove" is not empty
        let edit = RegionEdit {
            files_to_add: vec![FileMeta::default()],
            files_to_remove: vec![FileMeta::default()],
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
        };
        assert!(!is_valid_region_edit(&edit));

        // Invalid: other fields are not all "None"s
        let edit = RegionEdit {
            files_to_add: vec![FileMeta::default()],
            files_to_remove: vec![],
            timestamp_ms: None,
            compaction_time_window: Some(Duration::from_secs(1)),
            flushed_entry_id: None,
            flushed_sequence: None,
        };
        assert!(!is_valid_region_edit(&edit));
        let edit = RegionEdit {
            files_to_add: vec![FileMeta::default()],
            files_to_remove: vec![],
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: Some(1),
            flushed_sequence: None,
        };
        assert!(!is_valid_region_edit(&edit));
        let edit = RegionEdit {
            files_to_add: vec![FileMeta::default()],
            files_to_remove: vec![],
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: Some(1),
        };
        assert!(!is_valid_region_edit(&edit));
    }
}
