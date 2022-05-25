use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use snafu::ResultExt;
use store_api::storage::{EngineContext, RegionDescriptor, StorageEngine};

use crate::error::{self, Error, Result};
use crate::region::RegionImpl;

/// [StorageEngine] implementation.
#[derive(Clone)]
pub struct EngineImpl {
    inner: Arc<EngineInner>,
}

#[async_trait]
impl StorageEngine for EngineImpl {
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

type RegionMap = HashMap<String, RegionImpl>;

struct EngineInner {
    regions: RwLock<RegionMap>,
}

impl EngineInner {
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
        let region = RegionImpl::new(metadata);

        {
            let mut regions = self.regions.write().unwrap();
            if let Some(region) = regions.get(&region_name) {
                return Ok(region.clone());
            }

            regions.insert(region_name, region.clone());
        }

        Ok(region)
    }

    fn get_region(&self, name: &str) -> Option<RegionImpl> {
        let regions = self.regions.read().unwrap();
        regions.get(name).cloned()
    }
}
