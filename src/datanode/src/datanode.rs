use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::instance::{Instance, InstanceRef};
use crate::server::Services;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    File { data_dir: String },
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        ObjectStoreConfig::File {
            data_dir: "/tmp/greptimedb/data/".to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DatanodeOptions {
    pub http_addr: String,
    pub rpc_addr: String,
    pub mysql_addr: String,
    pub mysql_runtime_size: u32,
    pub wal_dir: String,
    pub storage: ObjectStoreConfig,
}

impl Default for DatanodeOptions {
    fn default() -> Self {
        Self {
            http_addr: "0.0.0.0:3000".to_string(),
            rpc_addr: "0.0.0.0:3001".to_string(),
            mysql_addr: "0.0.0.0:3306".to_string(),
            mysql_runtime_size: 2,
            wal_dir: "/tmp/greptimedb/wal".to_string(),
            storage: ObjectStoreConfig::default(),
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
        let services = Services::try_new(instance.clone(), &opts)?;
        Ok(Self {
            opts,
            services,
            instance,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        self.instance.start().await?;
        self.services.start(&self.opts).await
    }
}
