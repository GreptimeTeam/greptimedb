use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use common_telemetry::logging::info;
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

impl EngineImpl {
    pub fn new() -> EngineImpl {
        EngineImpl {
            inner: Arc::new(EngineInner {
                regions: RwLock::new(HashMap::new()),
            }),
        }
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
        let regions = self.regions.read().unwrap();
        regions.get(name).cloned()
    }
}

#[cfg(test)]
mod tests {
    use datatypes::type_id::LogicalTypeId;
    use store_api::storage::Region;

    use super::*;
    use crate::test_util::descriptor_util::RegionDescBuilder;

    #[tokio::test]
    async fn test_create_new_region() {
        let engine = EngineImpl::new();

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
