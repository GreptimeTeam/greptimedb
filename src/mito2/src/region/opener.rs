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

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64};
use std::sync::Arc;

use common_config::wal::WalOptions;
use common_telemetry::{debug, error, info, warn};
use common_time::util::current_time_millis;
use futures::StreamExt;
use object_store::manager::ObjectStoreManagerRef;
use object_store::util::{join_dir, normalize_dir};
use snafu::{ensure, OptionExt};
use store_api::logstore::LogStore;
use store_api::metadata::{ColumnMetadata, RegionMetadata};
use store_api::storage::{ColumnId, RegionId};

use crate::access_layer::AccessLayer;
use crate::cache::CacheManagerRef;
use crate::config::MitoConfig;
use crate::error::{EmptyRegionDirSnafu, ObjectStoreNotFoundSnafu, RegionCorruptedSnafu, Result};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::manifest::storage::manifest_compress_type;
use crate::memtable::MemtableBuilderRef;
use crate::region::options::RegionOptions;
use crate::region::version::{VersionBuilder, VersionControl, VersionControlRef};
use crate::region::MitoRegion;
use crate::region_write_ctx::RegionWriteCtx;
use crate::request::OptionOutputTx;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file_purger::LocalFilePurger;
use crate::wal::{EntryId, Wal};

/// Builder to create a new [MitoRegion] or open an existing one.
pub(crate) struct RegionOpener {
    region_id: RegionId,
    metadata: Option<RegionMetadata>,
    memtable_builder: MemtableBuilderRef,
    object_store_manager: ObjectStoreManagerRef,
    region_dir: String,
    scheduler: SchedulerRef,
    options: Option<RegionOptions>,
    cache_manager: Option<CacheManagerRef>,
    skip_wal_replay: bool,
}

impl RegionOpener {
    /// Returns a new opener.
    pub(crate) fn new(
        region_id: RegionId,
        region_dir: &str,
        memtable_builder: MemtableBuilderRef,
        object_store_manager: ObjectStoreManagerRef,
        scheduler: SchedulerRef,
    ) -> RegionOpener {
        RegionOpener {
            region_id,
            metadata: None,
            memtable_builder,
            object_store_manager,
            region_dir: normalize_dir(region_dir),
            scheduler,
            options: None,
            cache_manager: None,
            skip_wal_replay: false,
        }
    }

    /// Sets metadata of the region to create.
    pub(crate) fn metadata(mut self, metadata: RegionMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Parses and sets options for the region.
    pub(crate) fn parse_options(mut self, options: HashMap<String, String>) -> Result<Self> {
        self.options = Some(RegionOptions::try_from(&options)?);
        Ok(self)
    }

    /// Sets options for the region.
    pub(crate) fn options(mut self, options: RegionOptions) -> Self {
        self.options = Some(options);
        self
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
    /// - Panics if metadata is not set.
    /// - Panics if options is not set.
    pub(crate) async fn create_or_open<S: LogStore>(
        mut self,
        config: &MitoConfig,
        wal: &Wal<S>,
    ) -> Result<MitoRegion> {
        let region_id = self.region_id;

        // Tries to open the region.
        match self.maybe_open(config, wal).await {
            Ok(Some(region)) => {
                let recovered = region.metadata();
                // Checks the schema of the region.
                let expect = self.metadata.as_ref().unwrap();
                check_recovered_region(
                    &recovered,
                    expect.region_id,
                    &expect.column_metadatas,
                    &expect.primary_key,
                )?;
                // To keep consistence with Create behavior, set the opened Region writable.
                region.set_writable(true);
                return Ok(region);
            }
            Ok(None) => {
                debug!(
                    "No data under directory {}, region_id: {}",
                    self.region_dir, self.region_id
                );
            }
            Err(e) => {
                warn!(
                    "Failed to open region {} before creating it, region_dir: {}, err: {}",
                    self.region_id, self.region_dir, e
                );
            }
        }
        let options = self.options.take().unwrap();
        let wal_options = options.wal_options.clone();
        let object_store = self.object_store(&options.storage)?.clone();

        // Create a manifest manager for this region and writes regions to the manifest file.
        let region_manifest_options = self.manifest_options(config, &options)?;
        let metadata = Arc::new(self.metadata.unwrap());
        let manifest_manager =
            RegionManifestManager::new(metadata.clone(), region_manifest_options).await?;

        let mutable = self.memtable_builder.build(&metadata);

        let version = VersionBuilder::new(metadata, mutable)
            .options(options)
            .build();
        let version_control = Arc::new(VersionControl::new(version));
        let access_layer = Arc::new(AccessLayer::new(self.region_dir, object_store));

        Ok(MitoRegion {
            region_id,
            version_control,
            access_layer: access_layer.clone(),
            manifest_manager,
            file_purger: Arc::new(LocalFilePurger::new(
                self.scheduler,
                access_layer,
                self.cache_manager,
            )),
            wal_options,
            last_flush_millis: AtomicI64::new(current_time_millis()),
            // Region is writable after it is created.
            writable: AtomicBool::new(true),
        })
    }

    /// Opens an existing region in read only mode.
    ///
    /// Returns error if the region doesn't exist.
    pub(crate) async fn open<S: LogStore>(
        self,
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

    /// Tries to open the region and returns `None` if the region directory is empty.
    async fn maybe_open<S: LogStore>(
        &self,
        config: &MitoConfig,
        wal: &Wal<S>,
    ) -> Result<Option<MitoRegion>> {
        let region_options = self.options.as_ref().unwrap().clone();
        let wal_options = region_options.wal_options.clone();

        let region_manifest_options = self.manifest_options(config, &region_options)?;
        let Some(manifest_manager) = RegionManifestManager::open(region_manifest_options).await?
        else {
            return Ok(None);
        };

        let manifest = manifest_manager.manifest().await;
        let metadata = manifest.metadata.clone();

        let region_id = self.region_id;
        let object_store = self.object_store(&region_options.storage)?.clone();
        let access_layer = Arc::new(AccessLayer::new(self.region_dir.clone(), object_store));
        let file_purger = Arc::new(LocalFilePurger::new(
            self.scheduler.clone(),
            access_layer.clone(),
            self.cache_manager.clone(),
        ));
        let mutable = self.memtable_builder.build(&metadata);
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
                "Start replaying memtable at flushed_entry_id + 1 {} for region {}",
                flushed_entry_id + 1,
                region_id
            );
            replay_memtable(
                wal,
                &wal_options,
                region_id,
                flushed_entry_id,
                &version_control,
            )
            .await?;
        } else {
            info!("Skip the WAL replay for region: {}", region_id);
        }

        let region = MitoRegion {
            region_id: self.region_id,
            version_control,
            access_layer,
            manifest_manager,
            file_purger,
            wal_options,
            last_flush_millis: AtomicI64::new(current_time_millis()),
            // Region is always opened in read only mode.
            writable: AtomicBool::new(false),
        };
        Ok(Some(region))
    }

    /// Returns a new manifest options.
    fn manifest_options(
        &self,
        config: &MitoConfig,
        options: &RegionOptions,
    ) -> Result<RegionManifestOptions> {
        let object_store = self.object_store(&options.storage)?.clone();
        Ok(RegionManifestOptions {
            manifest_dir: new_manifest_dir(&self.region_dir),
            object_store,
            // We don't allow users to set the compression algorithm as we use it as a file suffix.
            // Currently, the manifest storage doesn't have good support for changing compression algorithms.
            compress_type: manifest_compress_type(config.compress_manifest),
            checkpoint_distance: config.manifest_checkpoint_distance,
        })
    }

    /// Returns an object store corresponding to `name`. If `name` is `None`, this method returns the default object store.
    fn object_store(&self, name: &Option<String>) -> Result<&object_store::ObjectStore> {
        if let Some(name) = name {
            Ok(self
                .object_store_manager
                .find(name)
                .context(ObjectStoreNotFoundSnafu {
                    object_store: name.to_string(),
                })?)
        } else {
            Ok(self.object_store_manager.default_object_store())
        }
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
pub(crate) async fn replay_memtable<S: LogStore>(
    wal: &Wal<S>,
    wal_options: &WalOptions,
    region_id: RegionId,
    flushed_entry_id: EntryId,
    version_control: &VersionControlRef,
) -> Result<EntryId> {
    let mut rows_replayed = 0;
    // Last entry id should start from flushed entry id since there might be no
    // data in the WAL.
    let mut last_entry_id = flushed_entry_id;
    let mut region_write_ctx = RegionWriteCtx::new(region_id, version_control, wal_options.clone());

    let replay_from_entry_id = flushed_entry_id + 1;
    let mut wal_stream = wal.scan(region_id, replay_from_entry_id, wal_options)?;
    while let Some(res) = wal_stream.next().await {
        let (entry_id, entry) = res?;
        debug_assert!(entry_id > flushed_entry_id);
        last_entry_id = last_entry_id.max(entry_id);
        for mutation in entry.mutations {
            rows_replayed += mutation
                .rows
                .as_ref()
                .map(|rows| rows.rows.len())
                .unwrap_or(0);
            region_write_ctx.push_mutation(mutation.op_type, mutation.rows, OptionOutputTx::none());
        }
    }

    // set next_entry_id and write to memtable.
    region_write_ctx.set_next_entry_id(last_entry_id + 1);
    region_write_ctx.write_memtable();

    info!(
        "Replay WAL for region: {}, rows recovered: {}, last entry id: {}",
        region_id, rows_replayed, last_entry_id
    );
    Ok(last_entry_id)
}

/// Returns the directory to the manifest files.
fn new_manifest_dir(region_dir: &str) -> String {
    join_dir(region_dir, "manifest")
}
