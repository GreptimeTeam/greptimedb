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
use std::sync::Arc;

use common_telemetry::{debug, error, info, warn};
use common_wal::options::WalOptions;
use futures::future::BoxFuture;
use futures::StreamExt;
use log_store::kafka::log_store::KafkaLogStore;
use log_store::raft_engine::log_store::RaftEngineLogStore;
use object_store::manager::ObjectStoreManagerRef;
use object_store::util::{join_dir, normalize_dir};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::logstore::provider::Provider;
use store_api::logstore::LogStore;
use store_api::metadata::{
    ColumnMetadata, RegionMetadata, RegionMetadataBuilder, RegionMetadataRef,
};
use store_api::region_engine::RegionRole;
use store_api::storage::{ColumnId, RegionId};

use crate::access_layer::AccessLayer;
use crate::cache::CacheManagerRef;
use crate::config::MitoConfig;
use crate::error;
use crate::error::{
    EmptyRegionDirSnafu, InvalidMetadataSnafu, ObjectStoreNotFoundSnafu, RegionCorruptedSnafu,
    Result, StaleLogEntrySnafu,
};
use crate::manifest::action::RegionManifest;
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::manifest::storage::manifest_compress_type;
use crate::memtable::time_partition::TimePartitions;
use crate::memtable::MemtableBuilderProvider;
use crate::region::options::RegionOptions;
use crate::region::version::{VersionBuilder, VersionControl, VersionControlRef};
use crate::region::{
    ManifestContext, ManifestStats, MitoRegion, RegionLeaderState, RegionRoleState,
};
use crate::region_write_ctx::RegionWriteCtx;
use crate::request::OptionOutputTx;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file_purger::LocalFilePurger;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::time_provider::TimeProviderRef;
use crate::wal::entry_reader::WalEntryReader;
use crate::wal::{EntryId, Wal};

/// Builder to create a new [MitoRegion] or open an existing one.
pub(crate) struct RegionOpener {
    region_id: RegionId,
    metadata_builder: Option<RegionMetadataBuilder>,
    memtable_builder_provider: MemtableBuilderProvider,
    object_store_manager: ObjectStoreManagerRef,
    region_dir: String,
    purge_scheduler: SchedulerRef,
    options: Option<RegionOptions>,
    cache_manager: Option<CacheManagerRef>,
    skip_wal_replay: bool,
    puffin_manager_factory: PuffinManagerFactory,
    intermediate_manager: IntermediateManager,
    time_provider: TimeProviderRef,
    stats: ManifestStats,
    wal_entry_reader: Option<Box<dyn WalEntryReader>>,
}

impl RegionOpener {
    /// Returns a new opener.
    // TODO(LFC): Reduce the number of arguments.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        region_id: RegionId,
        region_dir: &str,
        memtable_builder_provider: MemtableBuilderProvider,
        object_store_manager: ObjectStoreManagerRef,
        purge_scheduler: SchedulerRef,
        puffin_manager_factory: PuffinManagerFactory,
        intermediate_manager: IntermediateManager,
        time_provider: TimeProviderRef,
    ) -> RegionOpener {
        RegionOpener {
            region_id,
            metadata_builder: None,
            memtable_builder_provider,
            object_store_manager,
            region_dir: normalize_dir(region_dir),
            purge_scheduler,
            options: None,
            cache_manager: None,
            skip_wal_replay: false,
            puffin_manager_factory,
            intermediate_manager,
            time_provider,
            stats: Default::default(),
            wal_entry_reader: None,
        }
    }

    /// Sets metadata builder of the region to create.
    pub(crate) fn metadata_builder(mut self, builder: RegionMetadataBuilder) -> Self {
        self.metadata_builder = Some(builder);
        self
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
    ) -> Result<MitoRegion> {
        let region_id = self.region_id;
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
                // To keep consistence with Create behavior, set the opened Region to RegionRole::Leader.
                region.set_role(RegionRole::Leader);
                return Ok(region);
            }
            Ok(None) => {
                debug!(
                    "No data under directory {}, region_id: {}",
                    self.region_dir, self.region_id
                );
            }
            Err(e) => {
                warn!(e;
                    "Failed to open region {} before creating it, region_dir: {}",
                    self.region_id, self.region_dir
                );
            }
        }
        // Safety: must be set before calling this method.
        let options = self.options.take().unwrap();
        let object_store = get_object_store(&options.storage, &self.object_store_manager)?;
        let provider = self.provider::<S>(&options.wal_options)?;
        let metadata = Arc::new(metadata);
        // Create a manifest manager for this region and writes regions to the manifest file.
        let region_manifest_options = Self::manifest_options(
            config,
            &options,
            &self.region_dir,
            &self.object_store_manager,
        )?;
        let manifest_manager = RegionManifestManager::new(
            metadata.clone(),
            region_manifest_options,
            self.stats.total_manifest_size.clone(),
            self.stats.manifest_version.clone(),
        )
        .await?;

        let memtable_builder = self.memtable_builder_provider.builder_for_options(
            options.memtable.as_ref(),
            options.need_dedup(),
            options.merge_mode(),
        );
        let part_duration = options.compaction.time_window();
        // Initial memtable id is 0.
        let mutable = Arc::new(TimePartitions::new(
            metadata.clone(),
            memtable_builder.clone(),
            0,
            part_duration,
        ));

        debug!("Create region {} with options: {:?}", region_id, options);

        let version = VersionBuilder::new(metadata, mutable)
            .options(options)
            .build();
        let version_control = Arc::new(VersionControl::new(version));
        let access_layer = Arc::new(AccessLayer::new(
            self.region_dir,
            object_store,
            self.puffin_manager_factory,
            self.intermediate_manager,
        ));
        let now = self.time_provider.current_time_millis();

        Ok(MitoRegion {
            region_id,
            version_control,
            access_layer: access_layer.clone(),
            // Region is writable after it is created.
            manifest_ctx: Arc::new(ManifestContext::new(
                manifest_manager,
                RegionRoleState::Leader(RegionLeaderState::Writable),
            )),
            file_purger: Arc::new(LocalFilePurger::new(
                self.purge_scheduler,
                access_layer,
                self.cache_manager,
            )),
            provider,
            last_flush_millis: AtomicI64::new(now),
            last_compaction_millis: AtomicI64::new(now),
            time_provider: self.time_provider.clone(),
            topic_latest_entry_id: AtomicU64::new(0),
            memtable_builder,
            stats: self.stats,
        })
    }

    /// Opens an existing region in read only mode.
    ///
    /// Returns error if the region doesn't exist.
    pub(crate) async fn open<S: LogStore>(
        mut self,
        config: &MitoConfig,
        wal: &Wal<S>,
    ) -> Result<MitoRegion> {
        let region_id = self.region_id;
        let region = self
            .maybe_open(config, wal)
            .await?
            .context(EmptyRegionDirSnafu {
                region_id,
                region_dir: self.region_dir,
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
                    TypeId::of::<RaftEngineLogStore>() == TypeId::of::<S>(),
                    error::IncompatibleWalProviderChangeSnafu {
                        global: "`kafka`",
                        region: "`raft_engine`",
                    }
                );
                Ok(Provider::raft_engine_provider(self.region_id.as_u64()))
            }
            WalOptions::Kafka(options) => {
                ensure!(
                    TypeId::of::<KafkaLogStore>() == TypeId::of::<S>(),
                    error::IncompatibleWalProviderChangeSnafu {
                        global: "`raft_engine`",
                        region: "`kafka`",
                    }
                );
                Ok(Provider::kafka_provider(options.topic.to_string()))
            }
            WalOptions::Noop => Ok(Provider::noop_provider()),
        }
    }

    /// Tries to open the region and returns `None` if the region directory is empty.
    async fn maybe_open<S: LogStore>(
        &mut self,
        config: &MitoConfig,
        wal: &Wal<S>,
    ) -> Result<Option<MitoRegion>> {
        let region_options = self.options.as_ref().unwrap().clone();

        let region_manifest_options = Self::manifest_options(
            config,
            &region_options,
            &self.region_dir,
            &self.object_store_manager,
        )?;
        let Some(manifest_manager) = RegionManifestManager::open(
            region_manifest_options,
            self.stats.total_manifest_size.clone(),
            self.stats.manifest_version.clone(),
        )
        .await?
        else {
            return Ok(None);
        };

        let manifest = manifest_manager.manifest();
        let metadata = manifest.metadata.clone();

        let region_id = self.region_id;
        let provider = self.provider::<S>(&region_options.wal_options)?;
        let wal_entry_reader = self
            .wal_entry_reader
            .take()
            .unwrap_or_else(|| wal.wal_entry_reader(&provider, region_id, None));
        let on_region_opened = wal.on_region_opened();
        let object_store = get_object_store(&region_options.storage, &self.object_store_manager)?;

        debug!("Open region {} with options: {:?}", region_id, self.options);

        let access_layer = Arc::new(AccessLayer::new(
            self.region_dir.clone(),
            object_store,
            self.puffin_manager_factory.clone(),
            self.intermediate_manager.clone(),
        ));
        let file_purger = Arc::new(LocalFilePurger::new(
            self.purge_scheduler.clone(),
            access_layer.clone(),
            self.cache_manager.clone(),
        ));
        let memtable_builder = self.memtable_builder_provider.builder_for_options(
            region_options.memtable.as_ref(),
            region_options.need_dedup(),
            region_options.merge_mode(),
        );
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
        let version = VersionBuilder::new(metadata, mutable)
            .add_files(file_purger.clone(), manifest.files.values().cloned())
            .flushed_entry_id(manifest.flushed_entry_id)
            .flushed_sequence(manifest.flushed_sequence)
            .truncated_entry_id(manifest.truncated_entry_id)
            .compaction_time_window(manifest.compaction_time_window)
            .options(region_options)
            .build();
        let flushed_entry_id = version.flushed_entry_id;
        let version_control = Arc::new(VersionControl::new(version));
        if !self.skip_wal_replay {
            info!(
                "Start replaying memtable at flushed_entry_id + 1: {} for region {}, manifest version: {}",
                flushed_entry_id + 1,
                region_id,
                manifest.manifest_version
            );
            replay_memtable(
                &provider,
                wal_entry_reader,
                region_id,
                flushed_entry_id,
                &version_control,
                config.allow_stale_entries,
                on_region_opened,
            )
            .await?;
        } else {
            info!(
                "Skip the WAL replay for region: {}, manifest version: {}",
                region_id, manifest.manifest_version
            );
        }
        let now = self.time_provider.current_time_millis();

        let region = MitoRegion {
            region_id: self.region_id,
            version_control,
            access_layer,
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
            topic_latest_entry_id: AtomicU64::new(0),
            memtable_builder,
            stats: self.stats.clone(),
        };
        Ok(Some(region))
    }

    /// Returns a new manifest options.
    fn manifest_options(
        config: &MitoConfig,
        options: &RegionOptions,
        region_dir: &str,
        object_store_manager: &ObjectStoreManagerRef,
    ) -> Result<RegionManifestOptions> {
        let object_store = get_object_store(&options.storage, object_store_manager)?;
        Ok(RegionManifestOptions {
            manifest_dir: new_manifest_dir(region_dir),
            object_store,
            // We don't allow users to set the compression algorithm as we use it as a file suffix.
            // Currently, the manifest storage doesn't have good support for changing compression algorithms.
            compress_type: manifest_compress_type(config.compress_manifest),
            checkpoint_distance: config.manifest_checkpoint_distance,
        })
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
                object_store: name.to_string(),
            })?
            .clone())
    } else {
        Ok(object_store_manager.default_object_store().clone())
    }
}

/// A loader for loading metadata from a region dir.
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
        let manifest = self.load_manifest(region_dir, region_options).await?;
        Ok(manifest.map(|m| m.metadata.clone()))
    }

    /// Loads the manifest of the region from the region dir.
    pub async fn load_manifest(
        &self,
        region_dir: &str,
        region_options: &RegionOptions,
    ) -> Result<Option<Arc<RegionManifest>>> {
        let region_manifest_options = RegionOpener::manifest_options(
            &self.config,
            region_options,
            region_dir,
            &self.object_store_manager,
        )?;
        let Some(manifest_manager) = RegionManifestManager::open(
            region_manifest_options,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
        )
        .await?
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
    let mut rows_replayed = 0;
    // Last entry id should start from flushed entry id since there might be no
    // data in the WAL.
    let mut last_entry_id = flushed_entry_id;
    let replay_from_entry_id = flushed_entry_id + 1;

    let mut wal_stream = wal_entry_reader.read(provider, replay_from_entry_id)?;
    while let Some(res) = wal_stream.next().await {
        let (entry_id, entry) = res?;
        if entry_id <= flushed_entry_id {
            warn!("Stale WAL entries read during replay, region id: {}, flushed entry id: {}, entry id read: {}", region_id, flushed_entry_id, entry_id);
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

        let mut region_write_ctx =
            RegionWriteCtx::new(region_id, version_control, provider.clone());
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
            );
        }

        // set next_entry_id and write to memtable.
        region_write_ctx.set_next_entry_id(last_entry_id + 1);
        region_write_ctx.write_memtable().await;
    }

    // TODO(weny): We need to update `flushed_entry_id` in the region manifest
    // to avoid reading potentially incomplete entries in the future.
    (on_region_opened)(region_id, flushed_entry_id, provider).await?;

    info!(
        "Replay WAL for region: {}, rows recovered: {}, last entry id: {}",
        region_id, rows_replayed, last_entry_id
    );
    Ok(last_entry_id)
}

/// Returns the directory to the manifest files.
pub(crate) fn new_manifest_dir(region_dir: &str) -> String {
    join_dir(region_dir, "manifest")
}
