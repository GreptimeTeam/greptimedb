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

//! Region opener.

use std::any::TypeId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use common_telemetry::{debug, error, info, warn};
use common_wal::options::WalOptions;
use futures::StreamExt;
use futures::future::BoxFuture;
use log_store::kafka::log_store::KafkaLogStore;
use log_store::noop::log_store::NoopLogStore;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use object_store::manager::ObjectStoreManagerRef;
use object_store::util::normalize_dir;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::logstore::LogStore;
use store_api::logstore::provider::Provider;
use store_api::metadata::{
    ColumnMetadata, RegionMetadata, RegionMetadataBuilder, RegionMetadataRef,
};
use store_api::region_engine::RegionRole;
use store_api::region_request::PathType;
use store_api::storage::{ColumnId, RegionId};

use crate::access_layer::AccessLayer;
use crate::cache::CacheManagerRef;
use crate::cache::file_cache::{FileCache, FileType, IndexKey};
use crate::config::MitoConfig;
use crate::error;
use crate::error::{
    EmptyRegionDirSnafu, InvalidMetadataSnafu, ObjectStoreNotFoundSnafu, RegionCorruptedSnafu,
    Result, StaleLogEntrySnafu,
};
use crate::manifest::action::RegionManifest;
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::memtable::MemtableBuilderProvider;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::time_partition::{TimePartitions, TimePartitionsRef};
use crate::metrics::{CACHE_FILL_DOWNLOADED_FILES, CACHE_FILL_PENDING_FILES};
use crate::region::options::RegionOptions;
use crate::region::version::{VersionBuilder, VersionControl, VersionControlRef};
use crate::region::{
    ManifestContext, ManifestStats, MitoRegion, MitoRegionRef, RegionLeaderState, RegionRoleState,
};
use crate::region_write_ctx::RegionWriteCtx;
use crate::request::OptionOutputTx;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::FormatType;
use crate::sst::file::RegionFileId;
use crate::sst::file_purger::{FilePurgerRef, create_file_purger};
use crate::sst::file_ref::FileReferenceManagerRef;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::sst::location::{self, region_dir_from_table_dir};
use crate::time_provider::TimeProviderRef;
use crate::wal::entry_reader::WalEntryReader;
use crate::wal::{EntryId, Wal};

/// A fetcher to retrieve partition expr for a region.
///
/// Compatibility: older regions didn't persist `partition_expr` in engine metadata,
/// while newer ones do. On open, we backfill it via this fetcher and persist it
/// to the manifest so future opens don't need refetching.
#[async_trait::async_trait]
pub trait PartitionExprFetcher {
    async fn fetch_expr(&self, region_id: RegionId) -> Option<String>;
}

pub type PartitionExprFetcherRef = Arc<dyn PartitionExprFetcher + Send + Sync>;

/// Builder to create a new [MitoRegion] or open an existing one.
pub(crate) struct RegionOpener {
    region_id: RegionId,
    metadata_builder: Option<RegionMetadataBuilder>,
    memtable_builder_provider: MemtableBuilderProvider,
    object_store_manager: ObjectStoreManagerRef,
    table_dir: String,
    path_type: PathType,
    purge_scheduler: SchedulerRef,
    options: Option<RegionOptions>,
    cache_manager: Option<CacheManagerRef>,
    skip_wal_replay: bool,
    puffin_manager_factory: PuffinManagerFactory,
    intermediate_manager: IntermediateManager,
    time_provider: TimeProviderRef,
    stats: ManifestStats,
    wal_entry_reader: Option<Box<dyn WalEntryReader>>,
    replay_checkpoint: Option<u64>,
    file_ref_manager: FileReferenceManagerRef,
    partition_expr_fetcher: PartitionExprFetcherRef,
}

impl RegionOpener {
    /// Returns a new opener.
    // TODO(LFC): Reduce the number of arguments.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        region_id: RegionId,
        table_dir: &str,
        path_type: PathType,
        memtable_builder_provider: MemtableBuilderProvider,
        object_store_manager: ObjectStoreManagerRef,
        purge_scheduler: SchedulerRef,
        puffin_manager_factory: PuffinManagerFactory,
        intermediate_manager: IntermediateManager,
        time_provider: TimeProviderRef,
        file_ref_manager: FileReferenceManagerRef,
        partition_expr_fetcher: PartitionExprFetcherRef,
    ) -> RegionOpener {
        RegionOpener {
            region_id,
            metadata_builder: None,
            memtable_builder_provider,
            object_store_manager,
            table_dir: normalize_dir(table_dir),
            path_type,
            purge_scheduler,
            options: None,
            cache_manager: None,
            skip_wal_replay: false,
            puffin_manager_factory,
            intermediate_manager,
            time_provider,
            stats: Default::default(),
            wal_entry_reader: None,
            replay_checkpoint: None,
            file_ref_manager,
            partition_expr_fetcher,
        }
    }

    /// Sets metadata builder of the region to create.
    pub(crate) fn metadata_builder(mut self, builder: RegionMetadataBuilder) -> Self {
        self.metadata_builder = Some(builder);
        self
    }

    /// Computes the region directory from table_dir and region_id.
    fn region_dir(&self) -> String {
        region_dir_from_table_dir(&self.table_dir, self.region_id, self.path_type)
    }

    /// Builds the region metadata.
    ///
    /// # Panics
    /// - Panics if `options` is not set.
    /// - Panics if `metadata_builder` is not set.
    fn build_metadata(&mut self) -> Result<RegionMetadata> {
        let options = self.options.as_ref().unwrap();
        let mut metadata_builder = self.metadata_builder.take().unwrap();
        metadata_builder.primary_key_encoding(options.primary_key_encoding());
        metadata_builder.build().context(InvalidMetadataSnafu)
    }

    /// Parses and sets options for the region.
    pub(crate) fn parse_options(self, options: HashMap<String, String>) -> Result<Self> {
        self.options(RegionOptions::try_from(&options)?)
    }

    /// Sets the replay checkpoint for the region.
    pub(crate) fn replay_checkpoint(mut self, replay_checkpoint: Option<u64>) -> Self {
        self.replay_checkpoint = replay_checkpoint;
        self
    }

    /// If a [WalEntryReader] is set, the [RegionOpener] will use [WalEntryReader] instead of
    /// constructing a new one from scratch.
    pub(crate) fn wal_entry_reader(
        mut self,
        wal_entry_reader: Option<Box<dyn WalEntryReader>>,
    ) -> Self {
        self.wal_entry_reader = wal_entry_reader;
        self
    }

    /// Sets options for the region.
    pub(crate) fn options(mut self, options: RegionOptions) -> Result<Self> {
        options.validate()?;
        self.options = Some(options);
        Ok(self)
    }

    /// Sets the cache manager for the region.
    pub(crate) fn cache(mut self, cache_manager: Option<CacheManagerRef>) -> Self {
        self.cache_manager = cache_manager;
        self
    }

    /// Sets the `skip_wal_replay`.
    pub(crate) fn skip_wal_replay(mut self, skip: bool) -> Self {
        self.skip_wal_replay = skip;
        self
    }

    /// Writes region manifest and creates a new region if it does not exist.
    /// Opens the region if it already exists.
    ///
    /// # Panics
    /// - Panics if `metadata_builder` is not set.
    /// - Panics if `options` is not set.
    pub(crate) async fn create_or_open<S: LogStore>(
        mut self,
        config: &MitoConfig,
        wal: &Wal<S>,
    ) -> Result<MitoRegionRef> {
        let region_id = self.region_id;
        let region_dir = self.region_dir();
        let metadata = self.build_metadata()?;
        // Tries to open the region.
        match self.maybe_open(config, wal).await {
            Ok(Some(region)) => {
                let recovered = region.metadata();
                // Checks the schema of the region.
                let expect = &metadata;
                check_recovered_region(
                    &recovered,
                    expect.region_id,
                    &expect.column_metadatas,
                    &expect.primary_key,
                )?;
                // To keep consistency with Create behavior, set the opened Region to RegionRole::Leader.
                region.set_role(RegionRole::Leader);

                return Ok(region);
            }
            Ok(None) => {
                debug!(
                    "No data under directory {}, region_id: {}",
                    region_dir, self.region_id
                );
            }
            Err(e) => {
                warn!(e;
                    "Failed to open region {} before creating it, region_dir: {}",
                    self.region_id, region_dir
                );
            }
        }
        // Safety: must be set before calling this method.
        let mut options = self.options.take().unwrap();
        let object_store = get_object_store(&options.storage, &self.object_store_manager)?;
        let provider = self.provider::<S>(&options.wal_options)?;
        let metadata = Arc::new(metadata);
        // Sets the sst_format based on options or flat_format flag
        let sst_format = if let Some(format) = options.sst_format {
            format
        } else if config.default_experimental_flat_format {
            options.sst_format = Some(FormatType::Flat);
            FormatType::Flat
        } else {
            // Default to PrimaryKeyParquet for newly created regions
            options.sst_format = Some(FormatType::PrimaryKey);
            FormatType::PrimaryKey
        };
        // Create a manifest manager for this region and writes regions to the manifest file.
        let mut region_manifest_options =
            RegionManifestOptions::new(config, &region_dir, &object_store);
        // Set manifest cache if available
        region_manifest_options.manifest_cache = self
            .cache_manager
            .as_ref()
            .and_then(|cm| cm.write_cache())
            .and_then(|wc| wc.manifest_cache());
        // For remote WAL, we need to set flushed_entry_id to current topic's latest entry id.
        let flushed_entry_id = provider.initial_flushed_entry_id::<S>(wal.store());
        let manifest_manager = RegionManifestManager::new(
            metadata.clone(),
            flushed_entry_id,
            region_manifest_options,
            sst_format,
            &self.stats,
        )
        .await?;

        let memtable_builder = self.memtable_builder_provider.builder_for_options(&options);
        let part_duration = options.compaction.time_window();
        // Initial memtable id is 0.
        let mutable = Arc::new(TimePartitions::new(
            metadata.clone(),
            memtable_builder.clone(),
            0,
            part_duration,
        ));

        debug!(
            "Create region {} with options: {:?}, default_flat_format: {}",
            region_id, options, config.default_experimental_flat_format
        );

        let version = VersionBuilder::new(metadata, mutable)
            .options(options)
            .build();
        let version_control = Arc::new(VersionControl::new(version));
        let access_layer = Arc::new(AccessLayer::new(
            self.table_dir.clone(),
            self.path_type,
            object_store,
            self.puffin_manager_factory,
            self.intermediate_manager,
        ));
        let now = self.time_provider.current_time_millis();

        Ok(Arc::new(MitoRegion {
            region_id,
            version_control,
            access_layer: access_layer.clone(),
            // Region is writable after it is created.
            manifest_ctx: Arc::new(ManifestContext::new(
                manifest_manager,
                RegionRoleState::Leader(RegionLeaderState::Writable),
            )),
            file_purger: create_file_purger(
                config.gc.enable,
                self.purge_scheduler,
                access_layer,
                self.cache_manager,
                self.file_ref_manager.clone(),
            ),
            provider,
            last_flush_millis: AtomicI64::new(now),
            last_compaction_millis: AtomicI64::new(now),
            time_provider: self.time_provider.clone(),
            topic_latest_entry_id: AtomicU64::new(0),
            written_bytes: Arc::new(AtomicU64::new(0)),
            stats: self.stats,
            staging_partition_expr: Mutex::new(None),
        }))
    }

    /// Opens an existing region in read only mode.
    ///
    /// Returns error if the region doesn't exist.
    pub(crate) async fn open<S: LogStore>(
        mut self,
        config: &MitoConfig,
        wal: &Wal<S>,
    ) -> Result<MitoRegionRef> {
        let region_id = self.region_id;
        let region_dir = self.region_dir();
        let region = self
            .maybe_open(config, wal)
            .await?
            .with_context(|| EmptyRegionDirSnafu {
                region_id,
                region_dir: &region_dir,
            })?;

        ensure!(
            region.region_id == self.region_id,
            RegionCorruptedSnafu {
                region_id: self.region_id,
                reason: format!(
                    "recovered region has different region id {}",
                    region.region_id
                ),
            }
        );

        Ok(region)
    }

    fn provider<S: LogStore>(&self, wal_options: &WalOptions) -> Result<Provider> {
        match wal_options {
            WalOptions::RaftEngine => {
                ensure!(
                    TypeId::of::<RaftEngineLogStore>() == TypeId::of::<S>()
                        || TypeId::of::<NoopLogStore>() == TypeId::of::<S>(),
                    error::IncompatibleWalProviderChangeSnafu {
                        global: "`kafka`",
                        region: "`raft_engine`",
                    }
                );
                Ok(Provider::raft_engine_provider(self.region_id.as_u64()))
            }
            WalOptions::Kafka(options) => {
                ensure!(
                    TypeId::of::<KafkaLogStore>() == TypeId::of::<S>()
                        || TypeId::of::<NoopLogStore>() == TypeId::of::<S>(),
                    error::IncompatibleWalProviderChangeSnafu {
                        global: "`raft_engine`",
                        region: "`kafka`",
                    }
                );
                Ok(Provider::kafka_provider(options.topic.clone()))
            }
            WalOptions::Noop => Ok(Provider::noop_provider()),
        }
    }

    /// Tries to open the region and returns `None` if the region directory is empty.
    async fn maybe_open<S: LogStore>(
        &mut self,
        config: &MitoConfig,
        wal: &Wal<S>,
    ) -> Result<Option<MitoRegionRef>> {
        let now = Instant::now();
        let mut region_options = self.options.as_ref().unwrap().clone();
        let object_storage = get_object_store(&region_options.storage, &self.object_store_manager)?;
        let mut region_manifest_options =
            RegionManifestOptions::new(config, &self.region_dir(), &object_storage);
        // Set manifest cache if available
        region_manifest_options.manifest_cache = self
            .cache_manager
            .as_ref()
            .and_then(|cm| cm.write_cache())
            .and_then(|wc| wc.manifest_cache());
        let Some(manifest_manager) =
            RegionManifestManager::open(region_manifest_options, &self.stats).await?
        else {
            return Ok(None);
        };

        // Backfill `partition_expr` if missing. Use the backfilled metadata in-memory during this open.
        let manifest = manifest_manager.manifest();
        let metadata = if manifest.metadata.partition_expr.is_none()
            && let Some(expr_json) = self.partition_expr_fetcher.fetch_expr(self.region_id).await
        {
            let metadata = manifest.metadata.as_ref().clone();
            let mut builder = RegionMetadataBuilder::from_existing(metadata);
            builder.partition_expr_json(Some(expr_json));
            Arc::new(builder.build().context(InvalidMetadataSnafu)?)
        } else {
            manifest.metadata.clone()
        };
        // Updates the region options with the manifest.
        sanitize_region_options(&manifest, &mut region_options);

        let region_id = self.region_id;
        let provider = self.provider::<S>(&region_options.wal_options)?;
        let wal_entry_reader = self
            .wal_entry_reader
            .take()
            .unwrap_or_else(|| wal.wal_entry_reader(&provider, region_id, None));
        let on_region_opened = wal.on_region_opened();
        let object_store = get_object_store(&region_options.storage, &self.object_store_manager)?;

        debug!(
            "Open region {} at {} with options: {:?}",
            region_id, self.table_dir, self.options
        );

        let access_layer = Arc::new(AccessLayer::new(
            self.table_dir.clone(),
            self.path_type,
            object_store,
            self.puffin_manager_factory.clone(),
            self.intermediate_manager.clone(),
        ));
        let file_purger = create_file_purger(
            config.gc.enable,
            self.purge_scheduler.clone(),
            access_layer.clone(),
            self.cache_manager.clone(),
            self.file_ref_manager.clone(),
        );
        // We should sanitize the region options before creating a new memtable.
        let memtable_builder = self
            .memtable_builder_provider
            .builder_for_options(&region_options);
        // Use compaction time window in the manifest if region doesn't provide
        // the time window option.
        let part_duration = region_options
            .compaction
            .time_window()
            .or(manifest.compaction_time_window);
        // Initial memtable id is 0.
        let mutable = Arc::new(TimePartitions::new(
            metadata.clone(),
            memtable_builder.clone(),
            0,
            part_duration,
        ));

        // Updates region options by manifest before creating version.
        let version_builder = version_builder_from_manifest(
            &manifest,
            metadata,
            file_purger.clone(),
            mutable,
            region_options,
        );
        let version = version_builder.build();
        let flushed_entry_id = version.flushed_entry_id;
        let version_control = Arc::new(VersionControl::new(version));

        let topic_latest_entry_id = if !self.skip_wal_replay {
            let replay_from_entry_id = self
                .replay_checkpoint
                .unwrap_or_default()
                .max(flushed_entry_id);
            info!(
                "Start replaying memtable at replay_from_entry_id: {} for region {}, manifest version: {}, flushed entry id: {}, elapsed: {:?}",
                replay_from_entry_id,
                region_id,
                manifest.manifest_version,
                flushed_entry_id,
                now.elapsed()
            );
            replay_memtable(
                &provider,
                wal_entry_reader,
                region_id,
                replay_from_entry_id,
                &version_control,
                config.allow_stale_entries,
                on_region_opened,
            )
            .await?;
            // For remote WAL, we need to set topic_latest_entry_id to current topic's latest entry id.
            // Only set after the WAL replay is completed.

            if provider.is_remote_wal() && version_control.current().version.memtables.is_empty() {
                wal.store().latest_entry_id(&provider).unwrap_or(0)
            } else {
                0
            }
        } else {
            info!(
                "Skip the WAL replay for region: {}, manifest version: {}, flushed_entry_id: {}, elapsed: {:?}",
                region_id,
                manifest.manifest_version,
                flushed_entry_id,
                now.elapsed()
            );

            0
        };

        if let Some(committed_in_manifest) = manifest.committed_sequence {
            let committed_after_replay = version_control.committed_sequence();
            if committed_in_manifest > committed_after_replay {
                info!(
                    "Overriding committed sequence, region: {}, flushed_sequence: {}, committed_sequence: {} -> {}",
                    self.region_id,
                    version_control.current().version.flushed_sequence,
                    version_control.committed_sequence(),
                    committed_in_manifest
                );
                version_control.set_committed_sequence(committed_in_manifest);
            }
        }

        let now = self.time_provider.current_time_millis();

        let region = MitoRegion {
            region_id: self.region_id,
            version_control: version_control.clone(),
            access_layer: access_layer.clone(),
            // Region is always opened in read only mode.
            manifest_ctx: Arc::new(ManifestContext::new(
                manifest_manager,
                RegionRoleState::Follower,
            )),
            file_purger,
            provider: provider.clone(),
            last_flush_millis: AtomicI64::new(now),
            last_compaction_millis: AtomicI64::new(now),
            time_provider: self.time_provider.clone(),
            topic_latest_entry_id: AtomicU64::new(topic_latest_entry_id),
            written_bytes: Arc::new(AtomicU64::new(0)),
            stats: self.stats.clone(),
            // TODO(weny): reload the staging partition expr from the manifest.
            staging_partition_expr: Mutex::new(None),
        };

        let region = Arc::new(region);

        maybe_load_cache(&region, config, &self.cache_manager);

        Ok(Some(region))
    }
}

/// Creates a version builder from a region manifest.
pub(crate) fn version_builder_from_manifest(
    manifest: &RegionManifest,
    metadata: RegionMetadataRef,
    file_purger: FilePurgerRef,
    mutable: TimePartitionsRef,
    region_options: RegionOptions,
) -> VersionBuilder {
    VersionBuilder::new(metadata, mutable)
        .add_files(file_purger, manifest.files.values().cloned())
        .flushed_entry_id(manifest.flushed_entry_id)
        .flushed_sequence(manifest.flushed_sequence)
        .truncated_entry_id(manifest.truncated_entry_id)
        .compaction_time_window(manifest.compaction_time_window)
        .options(region_options)
}

/// Updates region options by persistent options.
pub(crate) fn sanitize_region_options(manifest: &RegionManifest, options: &mut RegionOptions) {
    let option_format = options.sst_format.unwrap_or_default();
    if option_format != manifest.sst_format {
        common_telemetry::warn!(
            "Overriding SST format from {:?} to {:?} for region {}",
            option_format,
            manifest.sst_format,
            manifest.metadata.region_id,
        );
        options.sst_format = Some(manifest.sst_format);
    }
}

/// Returns an object store corresponding to `name`. If `name` is `None`, this method returns the default object store.
pub fn get_object_store(
    name: &Option<String>,
    object_store_manager: &ObjectStoreManagerRef,
) -> Result<object_store::ObjectStore> {
    if let Some(name) = name {
        Ok(object_store_manager
            .find(name)
            .with_context(|| ObjectStoreNotFoundSnafu {
                object_store: name.clone(),
            })?
            .clone())
    } else {
        Ok(object_store_manager.default_object_store().clone())
    }
}

/// A loader for loading metadata from a region dir.
#[derive(Debug, Clone)]
pub struct RegionMetadataLoader {
    config: Arc<MitoConfig>,
    object_store_manager: ObjectStoreManagerRef,
}

impl RegionMetadataLoader {
    /// Creates a new `RegionOpenerBuilder`.
    pub fn new(config: Arc<MitoConfig>, object_store_manager: ObjectStoreManagerRef) -> Self {
        Self {
            config,
            object_store_manager,
        }
    }

    /// Loads the metadata of the region from the region dir.
    pub async fn load(
        &self,
        region_dir: &str,
        region_options: &RegionOptions,
    ) -> Result<Option<RegionMetadataRef>> {
        let manifest = self
            .load_manifest(region_dir, &region_options.storage)
            .await?;
        Ok(manifest.map(|m| m.metadata.clone()))
    }

    /// Loads the manifest of the region from the region dir.
    pub async fn load_manifest(
        &self,
        region_dir: &str,
        storage: &Option<String>,
    ) -> Result<Option<Arc<RegionManifest>>> {
        let object_store = get_object_store(storage, &self.object_store_manager)?;
        let region_manifest_options =
            RegionManifestOptions::new(&self.config, region_dir, &object_store);
        let Some(manifest_manager) =
            RegionManifestManager::open(region_manifest_options, &Default::default()).await?
        else {
            return Ok(None);
        };

        let manifest = manifest_manager.manifest();
        Ok(Some(manifest))
    }
}

/// Checks whether the recovered region has the same schema as region to create.
pub(crate) fn check_recovered_region(
    recovered: &RegionMetadata,
    region_id: RegionId,
    column_metadatas: &[ColumnMetadata],
    primary_key: &[ColumnId],
) -> Result<()> {
    if recovered.region_id != region_id {
        error!(
            "Recovered region {}, expect region {}",
            recovered.region_id, region_id
        );
        return RegionCorruptedSnafu {
            region_id,
            reason: format!(
                "recovered metadata has different region id {}",
                recovered.region_id
            ),
        }
        .fail();
    }
    if recovered.column_metadatas != column_metadatas {
        error!(
            "Unexpected schema in recovered region {}, recovered: {:?}, expect: {:?}",
            recovered.region_id, recovered.column_metadatas, column_metadatas
        );

        return RegionCorruptedSnafu {
            region_id,
            reason: "recovered metadata has different schema",
        }
        .fail();
    }
    if recovered.primary_key != primary_key {
        error!(
            "Unexpected primary key in recovered region {}, recovered: {:?}, expect: {:?}",
            recovered.region_id, recovered.primary_key, primary_key
        );

        return RegionCorruptedSnafu {
            region_id,
            reason: "recovered metadata has different primary key",
        }
        .fail();
    }

    Ok(())
}

/// Replays the mutations from WAL and inserts mutations to memtable of given region.
pub(crate) async fn replay_memtable<F>(
    provider: &Provider,
    mut wal_entry_reader: Box<dyn WalEntryReader>,
    region_id: RegionId,
    flushed_entry_id: EntryId,
    version_control: &VersionControlRef,
    allow_stale_entries: bool,
    on_region_opened: F,
) -> Result<EntryId>
where
    F: FnOnce(RegionId, EntryId, &Provider) -> BoxFuture<Result<()>> + Send,
{
    let now = Instant::now();
    let mut rows_replayed = 0;
    // Last entry id should start from flushed entry id since there might be no
    // data in the WAL.
    let mut last_entry_id = flushed_entry_id;
    let replay_from_entry_id = flushed_entry_id + 1;

    let mut wal_stream = wal_entry_reader.read(provider, replay_from_entry_id)?;
    while let Some(res) = wal_stream.next().await {
        let (entry_id, entry) = res?;
        if entry_id <= flushed_entry_id {
            warn!(
                "Stale WAL entries read during replay, region id: {}, flushed entry id: {}, entry id read: {}",
                region_id, flushed_entry_id, entry_id
            );
            ensure!(
                allow_stale_entries,
                StaleLogEntrySnafu {
                    region_id,
                    flushed_entry_id,
                    unexpected_entry_id: entry_id,
                }
            );
        }
        last_entry_id = last_entry_id.max(entry_id);

        let mut region_write_ctx = RegionWriteCtx::new(
            region_id,
            version_control,
            provider.clone(),
            // For WAL replay, we don't need to track the write bytes rate.
            None,
        );
        for mutation in entry.mutations {
            rows_replayed += mutation
                .rows
                .as_ref()
                .map(|rows| rows.rows.len())
                .unwrap_or(0);
            region_write_ctx.push_mutation(
                mutation.op_type,
                mutation.rows,
                mutation.write_hint,
                OptionOutputTx::none(),
                // We should respect the sequence in WAL during replay.
                Some(mutation.sequence),
            );
        }

        for bulk_entry in entry.bulk_entries {
            let part = BulkPart::try_from(bulk_entry)?;
            rows_replayed += part.num_rows();
            // During replay, we should adopt the sequence from WAL.
            let bulk_sequence_from_wal = part.sequence;
            ensure!(
                region_write_ctx.push_bulk(
                    OptionOutputTx::none(),
                    part,
                    Some(bulk_sequence_from_wal)
                ),
                RegionCorruptedSnafu {
                    region_id,
                    reason: "unable to replay memtable with bulk entries",
                }
            );
        }

        // set next_entry_id and write to memtable.
        region_write_ctx.set_next_entry_id(last_entry_id + 1);
        region_write_ctx.write_memtable().await;
        region_write_ctx.write_bulk().await;
    }

    // TODO(weny): We need to update `flushed_entry_id` in the region manifest
    // to avoid reading potentially incomplete entries in the future.
    (on_region_opened)(region_id, flushed_entry_id, provider).await?;

    let series_count = version_control.current().series_count();
    info!(
        "Replay WAL for region: {}, provider: {:?}, rows recovered: {}, replay from entry id: {}, last entry id: {}, total timeseries replayed: {}, elapsed: {:?}",
        region_id,
        provider,
        rows_replayed,
        replay_from_entry_id,
        last_entry_id,
        series_count,
        now.elapsed()
    );
    Ok(last_entry_id)
}

/// A task to load and fill the region file cache.
pub(crate) struct RegionLoadCacheTask {
    region: MitoRegionRef,
}

impl RegionLoadCacheTask {
    pub(crate) fn new(region: MitoRegionRef) -> Self {
        Self { region }
    }

    /// Fills the file cache with index files from the region.
    pub(crate) async fn fill_cache(&self, file_cache: &FileCache) {
        let region_id = self.region.region_id;
        let table_dir = self.region.access_layer.table_dir();
        let path_type = self.region.access_layer.path_type();
        let object_store = self.region.access_layer.object_store();
        let version_control = &self.region.version_control;

        // Collects IndexKeys, file sizes, and max timestamps for files that need to be downloaded
        let mut files_to_download = Vec::new();
        let mut files_already_cached = 0;

        {
            let version = version_control.current().version;
            for level in version.ssts.levels() {
                for file_handle in level.files.values() {
                    let file_meta = file_handle.meta_ref();
                    if file_meta.exists_index() {
                        let puffin_key = IndexKey::new(
                            file_meta.region_id,
                            file_meta.file_id, // FIXME(discord9): confirm correctness
                            FileType::Puffin {
                                version: file_meta.index_version,
                            },
                        );

                        if !file_cache.contains_key(&puffin_key) {
                            files_to_download.push((
                                puffin_key,
                                file_meta.index_file_size,
                                file_meta.time_range.1, // max timestamp
                            ));
                        } else {
                            files_already_cached += 1;
                        }
                    }
                }
            }
            // Releases the Version after the scope to avoid holding the memtables and file handles
            // for a long time.
        }

        // Sorts files by max timestamp in descending order to loads latest files first
        files_to_download.sort_by(|a, b| b.2.cmp(&a.2));

        let total_files = files_to_download.len() as i64;

        info!(
            "Starting background index cache preload for region {}, total_files_to_download: {}, files_already_cached: {}",
            region_id, total_files, files_already_cached
        );

        CACHE_FILL_PENDING_FILES.add(total_files);

        let mut files_downloaded = 0;
        let mut files_skipped = 0;

        for (puffin_key, file_size, max_timestamp) in files_to_download {
            let current_size = file_cache.puffin_cache_size();
            let capacity = file_cache.puffin_cache_capacity();
            let region_state = self.region.state();
            if !can_load_cache(region_state) {
                info!(
                    "Stopping index cache by state: {:?}, region: {}, current_size: {}, capacity: {}",
                    region_state, region_id, current_size, capacity
                );
                break;
            }

            // Checks if adding this file would exceed capacity
            if current_size + file_size > capacity {
                info!(
                    "Stopping index cache preload due to capacity limit, region: {}, file_id: {}, current_size: {}, file_size: {}, capacity: {}, file_timestamp: {:?}",
                    region_id, puffin_key.file_id, current_size, file_size, capacity, max_timestamp
                );
                files_skipped = (total_files - files_downloaded) as usize;
                CACHE_FILL_PENDING_FILES.sub(total_files - files_downloaded);
                break;
            }

            let index_remote_path = location::index_file_path_legacy(
                table_dir,
                RegionFileId::new(puffin_key.region_id, puffin_key.file_id),
                // FIXME(discord9): confirm correctness, actually get index_version from somewhere
                path_type,
            );

            match file_cache
                .download(puffin_key, &index_remote_path, object_store, file_size)
                .await
            {
                Ok(_) => {
                    debug!(
                        "Downloaded index file to write cache, region: {}, file_id: {}",
                        region_id, puffin_key.file_id
                    );
                    files_downloaded += 1;
                    CACHE_FILL_DOWNLOADED_FILES.inc_by(1);
                    CACHE_FILL_PENDING_FILES.dec();
                }
                Err(e) => {
                    warn!(
                        e; "Failed to download index file to write cache, region: {}, file_id: {}",
                        region_id, puffin_key.file_id
                    );
                    CACHE_FILL_PENDING_FILES.dec();
                }
            }
        }

        info!(
            "Completed background cache fill task for region {}, total_files: {}, files_downloaded: {}, files_already_cached: {}, files_skipped: {}",
            region_id, total_files, files_downloaded, files_already_cached, files_skipped
        );
    }
}

/// Loads all index (Puffin) files from the version into the write cache.
fn maybe_load_cache(
    region: &MitoRegionRef,
    config: &MitoConfig,
    cache_manager: &Option<CacheManagerRef>,
) {
    let Some(cache_manager) = cache_manager else {
        return;
    };
    let Some(write_cache) = cache_manager.write_cache() else {
        return;
    };

    let preload_enabled = config.preload_index_cache;
    if !preload_enabled {
        return;
    }

    let task = RegionLoadCacheTask::new(region.clone());
    write_cache.load_region_cache(task);
}

fn can_load_cache(state: RegionRoleState) -> bool {
    match state {
        RegionRoleState::Leader(RegionLeaderState::Writable)
        | RegionRoleState::Leader(RegionLeaderState::Staging)
        | RegionRoleState::Leader(RegionLeaderState::Altering)
        | RegionRoleState::Leader(RegionLeaderState::EnteringStaging)
        | RegionRoleState::Leader(RegionLeaderState::Editing)
        | RegionRoleState::Follower => true,
        // The region will be closed soon if it is downgrading.
        RegionRoleState::Leader(RegionLeaderState::Downgrading)
        | RegionRoleState::Leader(RegionLeaderState::Dropping)
        | RegionRoleState::Leader(RegionLeaderState::Truncating) => false,
    }
}
