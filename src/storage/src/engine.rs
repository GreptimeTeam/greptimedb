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

    async fn open_region(&self, _ctx: &EngineContext, _name: &str) -> Result<Self::Region> {
        unimplemented!()
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

/// Engine share data
/// TODO(dennis): merge to EngineInner?
#[derive(Clone, Debug)]
struct SharedData {
    pub _config: EngineConfig,
    pub object_store: ObjectStore,
}

impl SharedData {
    async fn new(config: EngineConfig) -> Result<Self> {
        // TODO(dennis): supports other backend
        let store_dir = util::normalize_dir(match &config.store_config {
            ObjectStoreConfig::File(file) => &file.store_dir,
        });

        let accessor = Backend::build()
            .root(&store_dir)
            .finish()
            .await
            .context(error::InitBackendSnafu { dir: &store_dir })?;

        let object_store = ObjectStore::new(accessor);

        Ok(Self {
            _config: config,
            object_store,
        })
    }
}

#[inline]
pub fn region_sst_dir(region_name: &str) -> String {
    format!("{}/", region_name)
}

#[inline]
pub fn region_manifest_dir(region_name: &str) -> String {
    format!("{}/manifest/", region_name)
}

type RegionMap<S> = HashMap<String, RegionImpl<S>>;

struct EngineInner<S: LogStore> {
    log_store: Arc<S>,
    regions: RwLock<RegionMap<S>>,
    shared: SharedData,
    memtable_builder: MemtableBuilderRef,
    flush_scheduler: FlushSchedulerRef,
    flush_strategy: FlushStrategyRef,
}

impl<S: LogStore> EngineInner<S> {
    pub async fn new(config: EngineConfig, log_store: Arc<S>) -> Result<Self> {
        let job_pool = Arc::new(JobPoolImpl {});
        let flush_scheduler = Arc::new(FlushSchedulerImpl::new(job_pool));

        Ok(Self {
            log_store,
            regions: RwLock::new(Default::default()),
            shared: SharedData::new(config).await?,
            memtable_builder: Arc::new(DefaultMemtableBuilder {}),
            flush_scheduler,
            flush_strategy: Arc::new(SizeBasedStrategy::default()),
        })
    }

    async fn create_region(&self, descriptor: RegionDescriptor) -> Result<RegionImpl<S>> {
        {
            let regions = self.regions.read().unwrap();
            if let Some(region) = regions.get(&descriptor.name) {
                return Ok(region.clone());
            }
        }

        let region_id = descriptor.id;
        let region_name = descriptor.name.clone();
        let metadata: RegionMetadata =
            descriptor
                .try_into()
                .context(error::InvalidRegionDescSnafu {
                    region: &region_name,
                })?;
        let sst_dir = &region_sst_dir(&region_name);
        let sst_layer = Arc::new(FsAccessLayer::new(
            sst_dir,
            self.shared.object_store.clone(),
        ));
        let manifest_dir = region_manifest_dir(&region_name);
        let manifest =
            RegionManifest::new(region_id, &manifest_dir, self.shared.object_store.clone());

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

        {
            let mut regions = self.regions.write().unwrap();
            if let Some(region) = regions.get(&region_name) {
                return Ok(region.clone());
            }

            regions.insert(region_name.clone(), region.clone());
        }

        info!("Storage engine create region {:?}", &region);

        Ok(region)
    }

    fn get_region(&self, name: &str) -> Option<RegionImpl<S>> {
        self.regions.read().unwrap().get(name).cloned()
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
