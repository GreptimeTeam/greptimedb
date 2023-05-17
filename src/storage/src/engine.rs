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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use common_telemetry::logging::{self, debug};
use object_store::{util, ObjectStore};
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::manifest::Manifest;
use store_api::storage::{
    CreateOptions, EngineContext, OpenOptions, Region, RegionDescriptor, StorageEngine,
};

use crate::compaction::CompactionSchedulerRef;
use crate::config::EngineConfig;
use crate::error::{self, Error, Result};
use crate::file_purger::{FilePurgeHandler, FilePurgerRef};
use crate::flush::{
    FlushScheduler, FlushSchedulerRef, FlushStrategyRef, PickerConfig, SizeBasedStrategy,
};
use crate::manifest::region::RegionManifest;
use crate::manifest::storage::manifest_compress_type;
use crate::memtable::{DefaultMemtableBuilder, MemtableBuilderRef};
use crate::metadata::RegionMetadata;
use crate::region::{RegionImpl, StoreConfig};
use crate::scheduler::{LocalScheduler, Scheduler, SchedulerConfig};
use crate::sst::FsAccessLayer;

/// [StorageEngine] implementation.
pub struct EngineImpl<S: LogStore> {
    inner: Arc<EngineInner<S>>,
}

impl<S: LogStore> Clone for EngineImpl<S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<S: LogStore> StorageEngine for EngineImpl<S> {
    type Error = Error;
    type Region = RegionImpl<S>;

    async fn open_region(
        &self,
        _ctx: &EngineContext,
        name: &str,
        opts: &OpenOptions,
    ) -> Result<Option<Self::Region>> {
        self.inner.open_region(name, opts).await
    }

    async fn close_region(&self, _ctx: &EngineContext, name: &str) -> Result<()> {
        self.inner.close_region(name).await
    }

    async fn create_region(
        &self,
        _ctx: &EngineContext,
        descriptor: RegionDescriptor,
        opts: &CreateOptions,
    ) -> Result<Self::Region> {
        self.inner.create_region(descriptor, opts).await
    }

    async fn drop_region(&self, _ctx: &EngineContext, _region: Self::Region) -> Result<()> {
        unimplemented!()
    }

    fn get_region(&self, _ctx: &EngineContext, name: &str) -> Result<Option<Self::Region>> {
        Ok(self.inner.get_region(name))
    }

    async fn close(&self, _ctx: &EngineContext) -> Result<()> {
        logging::info!("Stopping storage engine");

        self.inner.close().await?;

        logging::info!("Storage engine stopped");

        Ok(())
    }
}

impl<S: LogStore> EngineImpl<S> {
    pub fn new(
        config: EngineConfig,
        log_store: Arc<S>,
        object_store: ObjectStore,
        compaction_scheduler: CompactionSchedulerRef<S>,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(EngineInner::new(
                config,
                log_store,
                object_store,
                compaction_scheduler,
            )?),
        })
    }
}

/// Generate region sst path,
/// parent_dir is resolved in function `region_store_config` to ensure it's ended with '/'.
#[inline]
pub fn region_sst_dir(parent_dir: &str, region_name: &str) -> String {
    format!("{parent_dir}{region_name}/")
}

/// Generate region manifest path,
/// parent_dir is resolved in function `region_store_config` to ensure it's ended with '/'.
#[inline]
pub fn region_manifest_dir(parent_dir: &str, region_name: &str) -> String {
    format!("{parent_dir}{region_name}/manifest/")
}

/// A slot for region in the engine.
///
/// Also used as a placeholder in the region map when the region isn't ready, e.g. during
/// creating/opening.
#[derive(Debug)]
pub(crate) enum RegionSlot<S: LogStore> {
    /// The region is during creation.
    Creating,
    /// The region is during opening.
    Opening,
    /// The region is ready for access.
    Ready(RegionImpl<S>),
}

impl<S: LogStore> RegionSlot<S> {
    /// Try to get a ready region.
    fn try_get_ready_region(&self) -> Result<RegionImpl<S>> {
        if let RegionSlot::Ready(region) = self {
            Ok(region.clone())
        } else {
            error::InvalidRegionStateSnafu {
                state: self.state_name(),
            }
            .fail()
        }
    }

    /// Returns the ready region or `None`.
    fn get_ready_region(&self) -> Option<RegionImpl<S>> {
        if let RegionSlot::Ready(region) = self {
            Some(region.clone())
        } else {
            None
        }
    }

    fn state_name(&self) -> &'static str {
        match self {
            RegionSlot::Creating => "creating",
            RegionSlot::Opening => "opening",
            RegionSlot::Ready(_) => "ready",
        }
    }
}

impl<S: LogStore> Clone for RegionSlot<S> {
    // Manually implement Clone due to [rust#26925](https://github.com/rust-lang/rust/issues/26925).
    // Maybe we should require `LogStore` to be clonable to work around this.
    fn clone(&self) -> RegionSlot<S> {
        match self {
            RegionSlot::Creating => RegionSlot::Creating,
            RegionSlot::Opening => RegionSlot::Opening,
            RegionSlot::Ready(region) => RegionSlot::Ready(region.clone()),
        }
    }
}

/// Used to update slot or clean the slot on failure.
struct SlotGuard<'a, S: LogStore> {
    name: &'a str,
    regions: &'a RegionMap<S>,
    skip_clean: bool,
}

impl<'a, S: LogStore> SlotGuard<'a, S> {
    fn new(name: &'a str, regions: &'a RegionMap<S>) -> SlotGuard<'a, S> {
        SlotGuard {
            name,
            regions,
            skip_clean: false,
        }
    }

    /// Update the slot and skip cleaning on drop.
    fn update(&mut self, slot: RegionSlot<S>) {
        self.regions.update(self.name, slot);
        self.skip_clean = true;
    }
}

impl<'a, S: LogStore> Drop for SlotGuard<'a, S> {
    fn drop(&mut self) {
        if !self.skip_clean {
            self.regions.remove(self.name)
        }
    }
}

/// Region slot map.
pub struct RegionMap<S: LogStore>(RwLock<HashMap<String, RegionSlot<S>>>);

impl<S: LogStore> RegionMap<S> {
    /// Returns a new region map.
    pub fn new() -> RegionMap<S> {
        RegionMap(RwLock::new(HashMap::new()))
    }

    /// Returns the `Some(slot)` if there is existing slot with given `name`, or insert
    /// given `slot` and returns `None`.
    pub(crate) fn get_or_occupy_slot(
        &self,
        name: &str,
        slot: RegionSlot<S>,
    ) -> Option<RegionSlot<S>> {
        {
            // Try to get the region under read lock.
            let regions = self.0.read().unwrap();
            if let Some(slot) = regions.get(name) {
                return Some(slot.clone());
            }
        }

        // Get the region under write lock.
        let mut regions = self.0.write().unwrap();
        if let Some(slot) = regions.get(name) {
            return Some(slot.clone());
        }

        // No slot in map, we can insert the slot now.
        regions.insert(name.to_string(), slot);

        None
    }

    /// Gets the region by the specific name.
    fn get_region(&self, name: &str) -> Option<RegionImpl<S>> {
        let slot = self.0.read().unwrap().get(name).cloned()?;
        slot.get_ready_region()
    }

    /// Update the slot by name.
    fn update(&self, name: &str, slot: RegionSlot<S>) {
        let mut regions = self.0.write().unwrap();
        if let Some(old) = regions.get_mut(name) {
            *old = slot;
        }
    }

    /// Remove region by name.
    fn remove(&self, name: &str) {
        let mut regions = self.0.write().unwrap();
        regions.remove(name);
    }

    /// Collects regions.
    pub(crate) fn list_regions(&self) -> Vec<RegionImpl<S>> {
        let regions = self.0.read().unwrap();
        regions
            .values()
            .filter_map(|slot| slot.get_ready_region())
            .collect()
    }

    /// Clear the region map.
    fn clear(&self) {
        self.0.write().unwrap().clear();
    }
}

impl<S: LogStore> Default for RegionMap<S> {
    fn default() -> Self {
        Self::new()
    }
}

struct EngineInner<S: LogStore> {
    object_store: ObjectStore,
    log_store: Arc<S>,
    regions: Arc<RegionMap<S>>,
    memtable_builder: MemtableBuilderRef,
    flush_scheduler: FlushSchedulerRef<S>,
    flush_strategy: FlushStrategyRef,
    compaction_scheduler: CompactionSchedulerRef<S>,
    file_purger: FilePurgerRef,
    config: Arc<EngineConfig>,
}

impl<S: LogStore> EngineInner<S> {
    pub fn new(
        config: EngineConfig,
        log_store: Arc<S>,
        object_store: ObjectStore,
        compaction_scheduler: CompactionSchedulerRef<S>,
    ) -> Result<Self> {
        let regions = Arc::new(RegionMap::new());
        let flush_scheduler = Arc::new(FlushScheduler::new(
            SchedulerConfig {
                max_inflight_tasks: config.max_flush_tasks,
            },
            compaction_scheduler.clone(),
            regions.clone(),
            PickerConfig {
                schedule_interval: config.picker_schedule_interval,
                auto_flush_interval: config.auto_flush_interval,
            },
        )?);

        let file_purger = Arc::new(LocalScheduler::new(
            SchedulerConfig {
                max_inflight_tasks: config.max_purge_tasks,
            },
            FilePurgeHandler,
        ));
        Ok(Self {
            object_store,
            log_store,
            regions,
            memtable_builder: Arc::new(DefaultMemtableBuilder::default()),
            flush_scheduler,
            flush_strategy: Arc::new(SizeBasedStrategy::new(
                config.region_write_buffer_size.as_bytes() as usize,
            )),
            compaction_scheduler,
            file_purger,
            config: Arc::new(config),
        })
    }

    async fn close_region(&self, name: &str) -> Result<()> {
        if let Some(RegionSlot::Ready(region)) =
            self.regions.get_or_occupy_slot(name, RegionSlot::Opening)
        {
            region.close().await?;
        }

        self.regions.remove(name);

        Ok(())
    }

    async fn open_region(&self, name: &str, opts: &OpenOptions) -> Result<Option<RegionImpl<S>>> {
        // We can wait until the state of the slot has been changed to ready, but this will
        // make the code more complicate, so we just return the error here.
        if let Some(slot) = self.regions.get_or_occupy_slot(name, RegionSlot::Opening) {
            return slot.try_get_ready_region().map(Some);
        }

        let mut guard = SlotGuard::new(name, &self.regions);

        let store_config = self
            .region_store_config(
                &opts.parent_dir,
                opts.write_buffer_size,
                name,
                &self.config,
                opts.ttl,
                opts.compaction_time_window,
            )
            .await?;

        let region = match RegionImpl::open(name.to_string(), store_config, opts).await? {
            None => return Ok(None),
            Some(v) => v,
        };
        guard.update(RegionSlot::Ready(region.clone()));
        debug!(
            "Storage engine open region {}, id: {}",
            region.name(),
            region.id()
        );
        Ok(Some(region))
    }

    async fn create_region(
        &self,
        descriptor: RegionDescriptor,
        opts: &CreateOptions,
    ) -> Result<RegionImpl<S>> {
        if let Some(slot) = self
            .regions
            .get_or_occupy_slot(&descriptor.name, RegionSlot::Creating)
        {
            return slot.try_get_ready_region();
        }

        // Now the region in under `Creating` state.
        let region_name = descriptor.name.clone();
        let mut guard = SlotGuard::new(&region_name, &self.regions);

        let metadata: RegionMetadata =
            descriptor
                .try_into()
                .context(error::InvalidRegionDescSnafu {
                    region: &region_name,
                })?;
        let store_config = self
            .region_store_config(
                &opts.parent_dir,
                opts.write_buffer_size,
                &region_name,
                &self.config,
                opts.ttl,
                opts.compaction_time_window,
            )
            .await?;

        let region = RegionImpl::create(metadata, store_config).await?;

        guard.update(RegionSlot::Ready(region.clone()));

        debug!(
            "Storage engine create region {}, id: {}",
            region.name(),
            region.id()
        );

        Ok(region)
    }

    fn get_region(&self, name: &str) -> Option<RegionImpl<S>> {
        self.regions.get_region(name)
    }

    async fn region_store_config(
        &self,
        parent_dir: &str,
        write_buffer_size: Option<usize>,
        region_name: &str,
        config: &EngineConfig,
        ttl: Option<Duration>,
        compaction_time_window: Option<i64>,
    ) -> Result<StoreConfig<S>> {
        let parent_dir = util::normalize_dir(parent_dir);

        let sst_dir = &region_sst_dir(&parent_dir, region_name);
        let sst_layer = Arc::new(FsAccessLayer::new(sst_dir, self.object_store.clone()));
        let manifest_dir = region_manifest_dir(&parent_dir, region_name);
        let manifest = RegionManifest::with_checkpointer(
            &manifest_dir,
            self.object_store.clone(),
            manifest_compress_type(config.compress_manifest),
            config.manifest_checkpoint_margin,
            config.manifest_gc_duration,
        );
        manifest.start().await?;

        let flush_strategy = write_buffer_size
            .map(|size| Arc::new(SizeBasedStrategy::new(size)) as Arc<_>)
            .unwrap_or_else(|| self.flush_strategy.clone());

        Ok(StoreConfig {
            log_store: self.log_store.clone(),
            sst_layer,
            manifest,
            memtable_builder: self.memtable_builder.clone(),
            flush_scheduler: self.flush_scheduler.clone(),
            flush_strategy,
            compaction_scheduler: self.compaction_scheduler.clone(),
            engine_config: self.config.clone(),
            file_purger: self.file_purger.clone(),
            ttl,
            compaction_time_window,
        })
    }

    async fn close(&self) -> Result<()> {
        let regions = self.regions.list_regions();
        for region in regions {
            // Tolerate failure during closing regions.
            if let Err(e) = region.close().await {
                logging::error!(e; "Failed to close region {}", region.id());
            }
        }
        // Clear regions to release references to regions in the region map.
        self.regions.clear();

        self.compaction_scheduler.stop(true).await?;
        self.flush_scheduler.stop().await?;
        self.file_purger.stop(true).await
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use datatypes::type_id::LogicalTypeId;
    use log_store::test_util::log_store_util;
    use object_store::services::Fs;
    use store_api::storage::Region;

    use super::*;
    use crate::compaction::noop::NoopCompactionScheduler;
    use crate::test_util::descriptor_util::RegionDescBuilder;

    #[tokio::test]
    async fn test_create_new_region() {
        let log_file_dir = create_temp_dir("test_engine_wal");
        let log_file_dir_path = log_file_dir.path().to_str().unwrap();
        let log_store = log_store_util::create_tmp_local_file_log_store(log_file_dir_path).await;
        let dir = create_temp_dir("test_create_new_region");
        let store_dir = dir.path().to_string_lossy();

        let mut builder = Fs::default();
        builder.root(&store_dir);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let config = EngineConfig::default();

        let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());

        let engine = EngineImpl::new(
            config,
            Arc::new(log_store),
            object_store,
            compaction_scheduler,
        )
        .unwrap();

        let region_name = "region-0";
        let desc = RegionDescBuilder::new(region_name)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_field_column(("v1", LogicalTypeId::Float32, true))
            .build();
        let ctx = EngineContext::default();
        let region = engine
            .create_region(&ctx, desc, &CreateOptions::default())
            .await
            .unwrap();

        assert_eq!(region_name, region.name());

        let region2 = engine.get_region(&ctx, region_name).unwrap().unwrap();
        assert_eq!(region_name, region2.name());

        assert!(engine.get_region(&ctx, "no such region").unwrap().is_none());
    }
}
