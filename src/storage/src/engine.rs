use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use common_telemetry::logging::info;
use object_store::{backend::fs::Backend, ObjectStore};
use snafu::ResultExt;
use store_api::{
    logstore::LogStore,
    manifest::Manifest,
    storage::{EngineContext, RegionDescriptor, StorageEngine},
};

use crate::config::{EngineConfig, ObjectStoreConfig};
use crate::error::{self, Error, Result};
use crate::manifest::region::RegionManifest;
use crate::region::RegionImpl;
use crate::sst::FsAccessLayer;
use crate::wal::Wal;

/// [StorageEngine] implementation.
pub struct EngineImpl<S> {
    inner: Arc<EngineInner<S>>,
}

impl<S> Clone for EngineImpl<S> {
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

impl<S> EngineImpl<S> {
    pub async fn new(config: EngineConfig, log_store: Arc<S>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(EngineInner::new(config, log_store).await?),
        })
    }
}

/// Engine share data
#[derive(Clone, Debug)]
struct SharedData {
    pub _config: EngineConfig,
    pub store_dir: String,
    pub object_store: ObjectStore,
}

impl SharedData {
    async fn new(config: EngineConfig) -> Result<Self> {
        // TODO(dennis): supports other backend
        let store_dir = match &config.store_config {
            ObjectStoreConfig::File(file) => file.store_dir.clone(),
        };

        let accessor = Backend::build()
            .root(&store_dir)
            .finish()
            .await
            .context(error::InitBackendSnafu { dir: &store_dir })?;

        let object_store = ObjectStore::new(accessor);

        Ok(Self {
            _config: config,
            store_dir,
            object_store,
        })
    }

    #[inline]
    fn region_sst_dir(&self, region_name: &str) -> String {
        format!("{}/{}/", self.store_dir, region_name)
    }

    #[inline]
    fn region_manifest_dir(&self, region_name: &str) -> String {
        format!("{}/{}/manifest/", self.store_dir, region_name)
    }
}

type RegionMap<S> = HashMap<String, RegionImpl<S>>;

struct EngineInner<S> {
    log_store: Arc<S>,
    regions: RwLock<RegionMap<S>>,
    shared: SharedData,
}

impl<S> EngineInner<S> {
    pub async fn new(config: EngineConfig, log_store: Arc<S>) -> Result<Self> {
        Ok(Self {
            log_store,
            regions: RwLock::new(Default::default()),
            shared: SharedData::new(config).await?,
        })
    }
}

impl<S: LogStore> EngineInner<S> {
    async fn create_region(&self, descriptor: RegionDescriptor) -> Result<RegionImpl<S>> {
        {
            let regions = self.regions.read().unwrap();
            if let Some(region) = regions.get(&descriptor.name) {
                return Ok(region.clone());
            }
        }

        let region_id = descriptor.id;
        let region_name = descriptor.name.clone();
        let metadata = descriptor
            .try_into()
            .context(error::InvalidRegionDescSnafu {
                region: &region_name,
            })?;
        let wal = Wal::new(region_name.clone(), self.log_store.clone());
        let sst_dir = &self.shared.region_sst_dir(&region_name);
        let sst_layer = Arc::new(FsAccessLayer::new(
            sst_dir,
            self.shared.object_store.clone(),
        ));
        let manifest_dir = self.shared.region_manifest_dir(&region_name);
        let manifest =
            RegionManifest::new(region_id, &manifest_dir, self.shared.object_store.clone());

        let region = RegionImpl::new(
            region_id,
            region_name.clone(),
            metadata,
            wal,
            sst_layer,
            manifest,
        );

        {
            let mut regions = self.regions.write().unwrap();
            if let Some(region) = regions.get(&region_name) {
                return Ok(region.clone());
            }

            regions.insert(region_name.clone(), region.clone());
        }
        // TODO(yingwen): Persist region metadata to log.

        // TODO(yingwen): Impl Debug format for region and print region info briefly in log.
        info!("Storage engine create region {}", region_name);

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
