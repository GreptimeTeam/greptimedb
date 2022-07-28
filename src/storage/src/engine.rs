use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use common_telemetry::logging::info;
use object_store::{backend::fs::Backend, util, ObjectStore};
use snafu::ResultExt;
use store_api::manifest::action::ProtocolAction;
use store_api::{
    logstore::LogStore,
    manifest::Manifest,
    storage::{EngineContext, RegionDescriptor, StorageEngine},
};

use crate::background::JobPoolImpl;
use crate::config::{EngineConfig, ObjectStoreConfig};
use crate::error::{self, Error, Result};
use crate::flush::{FlushSchedulerImpl, FlushSchedulerRef, FlushStrategyRef, SizeBasedStrategy};
use crate::manifest::action::*;
use crate::manifest::region::RegionManifest;
use crate::memtable::{DefaultMemtableBuilder, MemtableBuilderRef};
use crate::metadata::RegionMetadata;
use crate::region::{RegionImpl, StoreConfig};
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

    async fn open_region(&self, _ctx: &EngineContext, name: &str) -> Result<Self::Region> {
        self.inner.open_region(name).await
    }

    async fn close_region(&self, _ctx: &EngineContext, _region: Self::Region) -> Result<()> {
        unimplemented!()
    }

    async fn create_region(
        &self,
        _ctx: &EngineContext,
        descriptor: RegionDescriptor,
    ) -> Result<Self::Region> {
        self.inner.create_region(descriptor).await
    }

    async fn drop_region(&self, _ctx: &EngineContext, _region: Self::Region) -> Result<()> {
        unimplemented!()
    }

    fn get_region(&self, _ctx: &EngineContext, name: &str) -> Result<Option<Self::Region>> {
        Ok(self.inner.get_region(name))
    }
}

impl<S: LogStore> EngineImpl<S> {
    pub async fn new(config: EngineConfig, log_store: Arc<S>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(EngineInner::new(config, log_store).await?),
        })
    }
}

async fn new_object_store(store_config: &ObjectStoreConfig) -> Result<ObjectStore> {
    // TODO(dennis): supports other backend
    let store_dir = util::normalize_dir(match store_config {
        ObjectStoreConfig::File(file) => &file.store_dir,
    });

    let accessor = Backend::build()
        .root(&store_dir)
        .finish()
        .await
        .context(error::InitBackendSnafu { dir: &store_dir })?;

    Ok(ObjectStore::new(accessor))
}

#[inline]
pub fn region_sst_dir(region_name: &str) -> String {
    format!("{}/", region_name)
}

#[inline]
pub fn region_manifest_dir(region_name: &str) -> String {
    format!("{}/manifest/", region_name)
}

/// A slot for region in the engine.
///
/// Also used as a placeholder in the region map when the region isn't ready, e.g. during
/// creating/opening.
#[derive(Debug)]
enum RegionSlot<S: LogStore> {
    /// The region is during creation.
    Creating,
    /// The region is during opening.
    Opening,
    /// The region is ready for access.
    Ready(RegionImpl<S>),
    // TODO(yingwen): Closing state.
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
    regions: &'a RwLock<RegionMap<S>>,
    skip_clean: bool,
}

impl<'a, S: LogStore> SlotGuard<'a, S> {
    fn new(name: &'a str, regions: &'a RwLock<RegionMap<S>>) -> SlotGuard<'a, S> {
        SlotGuard {
            name,
            regions,
            skip_clean: false,
        }
    }

    /// Update the slot and skip cleaning on drop.
    fn update(&mut self, slot: RegionSlot<S>) {
        {
            let mut regions = self.regions.write().unwrap();
            if let Some(old) = regions.get_mut(self.name) {
                *old = slot;
            }
        }

        self.skip_clean = true;
    }
}

impl<'a, S: LogStore> Drop for SlotGuard<'a, S> {
    fn drop(&mut self) {
        if !self.skip_clean {
            let mut regions = self.regions.write().unwrap();
            regions.remove(self.name);
        }
    }
}

type RegionMap<S> = HashMap<String, RegionSlot<S>>;

struct EngineInner<S: LogStore> {
    object_store: ObjectStore,
    log_store: Arc<S>,
    regions: RwLock<RegionMap<S>>,
    memtable_builder: MemtableBuilderRef,
    flush_scheduler: FlushSchedulerRef,
    flush_strategy: FlushStrategyRef,
}

impl<S: LogStore> EngineInner<S> {
    pub async fn new(config: EngineConfig, log_store: Arc<S>) -> Result<Self> {
        let job_pool = Arc::new(JobPoolImpl {});
        let flush_scheduler = Arc::new(FlushSchedulerImpl::new(job_pool));
        let object_store = new_object_store(&config.store_config).await?;

        Ok(Self {
            object_store,
            log_store,
            regions: RwLock::new(Default::default()),
            memtable_builder: Arc::new(DefaultMemtableBuilder {}),
            flush_scheduler,
            flush_strategy: Arc::new(SizeBasedStrategy::default()),
        })
    }

    /// Returns the `Some(slot)` if there is existing slot with given `name`, or insert
    /// given `slot` and returns `None`.
    fn get_or_occupy_slot(&self, name: &str, slot: RegionSlot<S>) -> Option<RegionSlot<S>> {
        {
            // Try to get the region under read lock.
            let regions = self.regions.read().unwrap();
            if let Some(slot) = regions.get(name) {
                return Some(slot.clone());
            }
        }

        // Get the region under write lock.
        let mut regions = self.regions.write().unwrap();
        if let Some(slot) = regions.get(name) {
            return Some(slot.clone());
        }

        // No slot in map, we can insert the slot now.
        regions.insert(name.to_string(), slot);

        None
    }

    async fn open_region(&self, name: &str) -> Result<RegionImpl<S>> {
        // We can wait until the state of the slot has been changed to ready, but this will
        // make the code more complicate, so we just return the error here.
        if let Some(slot) = self.get_or_occupy_slot(name, RegionSlot::Opening) {
            return slot.try_get_ready_region();
        }

        let _guard = SlotGuard::new(name, &self.regions);

        unimplemented!()
    }

    async fn create_region(&self, descriptor: RegionDescriptor) -> Result<RegionImpl<S>> {
        if let Some(slot) = self.get_or_occupy_slot(&descriptor.name, RegionSlot::Creating) {
            return slot.try_get_ready_region();
        }

        // Now the region in under `Creating` state.
        let region_id = descriptor.id;
        let region_name = descriptor.name.clone();
        let mut guard = SlotGuard::new(&region_name, &self.regions);

        let metadata: RegionMetadata =
            descriptor
                .try_into()
                .context(error::InvalidRegionDescSnafu {
                    region: &region_name,
                })?;
        let sst_dir = &region_sst_dir(&region_name);
        let sst_layer = Arc::new(FsAccessLayer::new(sst_dir, self.object_store.clone()));
        let manifest_dir = region_manifest_dir(&region_name);
        let manifest = RegionManifest::new(region_id, &manifest_dir, self.object_store.clone());

        let store_config = StoreConfig {
            log_store: self.log_store.clone(),
            sst_layer,
            manifest: manifest.clone(),
            memtable_builder: self.memtable_builder.clone(),
            flush_scheduler: self.flush_scheduler.clone(),
            flush_strategy: self.flush_strategy.clone(),
        };

        let region = RegionImpl::new(
            region_id,
            region_name.clone(),
            metadata.clone(),
            store_config,
        );
        // Persist region metadata
        manifest
            .update(RegionMetaActionList::new(vec![
                RegionMetaAction::Protocol(ProtocolAction::new()),
                RegionMetaAction::Change(RegionChange {
                    metadata: Arc::new(metadata),
                }),
            ]))
            .await?;

        guard.update(RegionSlot::Ready(region.clone()));

        info!("Storage engine create region {:?}", &region);

        Ok(region)
    }

    fn get_region(&self, name: &str) -> Option<RegionImpl<S>> {
        let slot = self.regions.read().unwrap().get(name).cloned()?;
        slot.get_ready_region()
    }
}

#[cfg(test)]
mod tests {
    use datatypes::type_id::LogicalTypeId;
    use log_store::test_util::log_store_util;
    use store_api::storage::Region;
    use tempdir::TempDir;

    use super::*;
    use crate::test_util::descriptor_util::RegionDescBuilder;

    #[tokio::test]
    async fn test_create_new_region() {
        let (log_store, _tmp) =
            log_store_util::create_tmp_local_file_log_store("test_engine_wal").await;
        let dir = TempDir::new("test_create_new_region").unwrap();
        let store_dir = dir.path().to_string_lossy();
        let config = EngineConfig::with_store_dir(&store_dir);

        let engine = EngineImpl::new(config, Arc::new(log_store)).await.unwrap();

        let region_name = "region-0";
        let desc = RegionDescBuilder::new(region_name)
            .push_key_column(("k1", LogicalTypeId::Int32, false))
            .push_value_column(("v1", LogicalTypeId::Float32, true))
            .build();
        let ctx = EngineContext::default();
        let region = engine.create_region(&ctx, desc).await.unwrap();

        assert_eq!(region_name, region.name());

        let region2 = engine.get_region(&ctx, region_name).unwrap().unwrap();
        assert_eq!(region_name, region2.name());

        assert!(engine.get_region(&ctx, "no such region").unwrap().is_none());
    }
}
