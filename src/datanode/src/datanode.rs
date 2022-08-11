use std::sync::Arc;

use crate::error::Result;
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
    instance: InstanceRef,
}

impl Datanode {
    pub async fn new(opts: DatanodeOptions) -> Result<Datanode> {
        let instance = Arc::new(Instance::new(&opts).await?);

        Ok(Self {
            opts,
            services: Services::new(instance.clone()),
            instance,
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.instance.start().await?;
        self.services.start(&self.opts).await
    }
}
