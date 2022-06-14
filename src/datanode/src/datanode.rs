use std::sync::Arc;

use query::catalog::memory;
use query::catalog::CatalogListRef;
use snafu::ResultExt;

use crate::error::{NewCatalogSnafu, Result};
use crate::instance::{Instance, InstanceRef};
use crate::server::Services;

#[derive(Debug)]
pub struct DataNodeOptions {
    pub http_addr: String,
    pub rpc_addr: String,
}
/// DataNode service.
pub struct DataNode {
    services: Services,
    _catalog_list: CatalogListRef,
    _instance: InstanceRef,
}

impl DataNode {
    pub fn new(opts: &DataNodeOptions) -> Result<DataNode> {
        let catalog_list = memory::new_memory_catalog_list().context(NewCatalogSnafu)?;
        let instance = Arc::new(Instance::new(catalog_list.clone()));

        Ok(Self {
            services: Services::new(opts, instance.clone()),
            _catalog_list: catalog_list,
            _instance: instance,
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.services.start().await
    }
}
