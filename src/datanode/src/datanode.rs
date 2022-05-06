use std::sync::Arc;

use query::catalog::memory;
use query::catalog::CatalogListRef;
use snafu::ResultExt;

use crate::error::{QuerySnafu, Result};
use crate::instance::{Instance, InstanceRef};
use crate::server::Services;

/// DataNode service.
pub struct DataNode {
    services: Services,
    _catalog_list: CatalogListRef,
    _instance: InstanceRef,
}

impl DataNode {
    pub fn new() -> Result<DataNode> {
        let catalog_list = memory::new_memory_catalog_list().context(QuerySnafu)?;
        let instance = Arc::new(Instance::new(catalog_list.clone()));

        Ok(Self {
            services: Services::new(instance.clone()),
            _catalog_list: catalog_list,
            _instance: instance,
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.services.start().await
    }
}
