use std::sync::Arc;

use catalog::CatalogListRef;
use snafu::ResultExt;

use crate::error::{NewCatalogSnafu, Result};
use crate::instance::{Instance, InstanceRef};
use crate::server::Services;

#[derive(Clone, Debug)]
pub struct DatanodeOptions {
    pub http_addr: String,
    pub rpc_addr: String,
    pub wal_dir: String,
}

impl Default for DatanodeOptions {
    fn default() -> Self {
        Self {
            http_addr: Default::default(),
            rpc_addr: Default::default(),
            wal_dir: "/tmp/wal".to_string(),
        }
    }
}

/// Datanode service.
pub struct Datanode {
    opts: DatanodeOptions,
    services: Services,
    _catalog_list: CatalogListRef,
    instance: InstanceRef,
}

impl Datanode {
    pub async fn new(opts: DatanodeOptions) -> Result<Datanode> {
        let catalog_list = catalog::memory::new_memory_catalog_list().context(NewCatalogSnafu)?;
        let instance = Arc::new(Instance::new(&opts, catalog_list.clone()).await?);

        Ok(Self {
            opts,
            services: Services::new(instance.clone()),
            _catalog_list: catalog_list,
            instance,
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.instance.start().await?;
        self.services.start(&self.opts).await
    }
}
