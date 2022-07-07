use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use common_telemetry::logging::info;
use snafu::ResultExt;
use store_api::{
    logstore::LogStore,
    storage::{EngineContext, RegionDescriptor, StorageEngine},
};

use crate::error::{self, Error, Result};
use crate::region::RegionImpl;
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
    pub fn new(log_store: Arc<S>) -> Self {
        Self {
            inner: Arc::new(EngineInner::new(log_store)),
        }
    }
}

type RegionMap<S> = HashMap<String, RegionImpl<S>>;

#[derive(Default)]
struct EngineInner<S> {
    log_store: Arc<S>,
    regions: RwLock<RegionMap<S>>,
}

impl<S> EngineInner<S> {
    pub fn new(log_store: Arc<S>) -> Self {
        Self {
            log_store,
            regions: RwLock::new(Default::default()),
        }
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

        let region_name = descriptor.name.clone();
        let metadata = descriptor
            .try_into()
            .context(error::InvalidRegionDescSnafu {
                region: &region_name,
            })?;
        let wal = Wal::new(region_name.clone(), self.log_store.clone());
        let region = RegionImpl::new(region_name.clone(), metadata, wal);

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

    use super::*;
    use crate::test_util::descriptor_util::RegionDescBuilder;

    #[tokio::test]
    async fn test_create_new_region() {
        let (log_store, _tmp) =
            log_store_util::create_tmp_local_file_log_store("test_engine_wal").await;
        let engine = EngineImpl::new(Arc::new(log_store));

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
