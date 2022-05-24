use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use store_api::storage::{EngineContext, RegionDescriptor, StorageEngine};

use crate::error::{Error, Result};
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
        _descriptor: RegionDescriptor,
    ) -> Result<RegionImpl> {
        unimplemented!()
    }

    async fn drop_region(&self, _ctx: &EngineContext, _region: RegionImpl) -> Result<()> {
        unimplemented!()
    }

    fn get_region(&self, _ctx: &EngineContext, name: &str) -> Result<Option<RegionImpl>> {
        Ok(self.inner.regions.get(name).cloned())
    }
}

struct EngineInner {
    regions: HashMap<String, RegionImpl>,
}
