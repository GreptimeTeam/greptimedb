use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use common_telemetry::logging::info;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::storage::{EngineContext, RegionDescriptor, StorageEngine};

use crate::error::{self, Error, Result};
use crate::region::RegionImpl;

/// [StorageEngine] implementation.
pub struct EngineImpl<L> {
    inner: Arc<EngineInner<L>>,
}

impl<L> Clone for EngineImpl<L> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

#[async_trait]
impl<L: LogStore> StorageEngine for EngineImpl<L> {
    type Error = Error;
    type Region = RegionImpl;

    async fn open_region(&self, _ctx: &EngineContext, _name: &str) -> Result<RegionImpl> {
        unimplemented!()
    }

    async fn close_region(&self, _ctx: &EngineContext, _region: RegionImpl) -> Result<()> {
        unimplemented!()
    }

    async fn create_region(
        &self,
        _ctx: &EngineContext,
        descriptor: RegionDescriptor,
    ) -> Result<RegionImpl> {
        self.inner.create_region(descriptor).await
    }

    async fn drop_region(&self, _ctx: &EngineContext, _region: RegionImpl) -> Result<()> {
        unimplemented!()
    }

    fn get_region(&self, _ctx: &EngineContext, name: &str) -> Result<Option<RegionImpl>> {
        Ok(self.inner.get_region(name))
    }
}

impl<L: LogStore> EngineImpl<L> {
    pub fn new(log_store: Arc<L>) -> Self {
        Self {
            inner: Arc::new(EngineInner::new(log_store)),
        }
    }
}

type RegionMap = HashMap<String, RegionImpl>;

#[derive(Default)]
struct EngineInner<L> {
    log_store: Arc<L>,
    regions: RwLock<RegionMap>,
}

impl<L> EngineInner<L> {
    pub fn new(log_store: Arc<L>) -> Self {
        Self {
            log_store,
            regions: RwLock::new(Default::default()),
        }
    }
}

impl<L: LogStore> EngineInner<L> {
    async fn create_region(&self, descriptor: RegionDescriptor) -> Result<RegionImpl> {
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
        let region = RegionImpl::new(region_name.clone(), metadata);

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

    fn get_region(&self, name: &str) -> Option<RegionImpl> {
        self.regions.read().unwrap().get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use datatypes::type_id::LogicalTypeId;
    use log_store::test_util as log_test_util;
    use store_api::storage::Region;

    use super::*;
    use crate::test_util::descriptor_util::RegionDescBuilder;

    #[tokio::test]
    async fn test_create_new_region() {
        let engine =
            EngineImpl::new(log_test_util::local_log_store_util::create_tmp_log_store().await);

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
