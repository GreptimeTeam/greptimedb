use std::sync::Arc;

use common_options::GreptimeOptions;
use query::catalog::memory;
use query::catalog::CatalogListRef;
use snafu::ResultExt;

use crate::error::{NewCatalogSnafu, Result};
use crate::instance::{Instance, InstanceRef};
use crate::server::Services;

/// Datanode service.
pub struct Datanode {
    opts: GreptimeOptions,
    services: Services,
    _catalog_list: CatalogListRef,
    instance: InstanceRef,
}

impl Datanode {
    pub async fn new(opts: GreptimeOptions) -> Result<Datanode> {
        let catalog_list = memory::new_memory_catalog_list().context(NewCatalogSnafu)?;
        let instance = Arc::new(Instance::new(&opts, catalog_list.clone()).await);

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
