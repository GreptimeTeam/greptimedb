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
    pub node_id: u64,
    pub http_addr: String,
    pub rpc_addr: String,
    pub mysql_addr: String,
    pub mysql_runtime_size: u32,
    pub postgres_addr: String,
    pub postgres_runtime_size: u32,
    pub meta_client_opts: MetaClientOpts,
    pub wal_dir: String,
    pub storage: ObjectStoreConfig,
}

impl Default for DatanodeOptions {
    fn default() -> Self {
        Self {
            node_id: 0,
            http_addr: "0.0.0.0:3000".to_string(),
            rpc_addr: "0.0.0.0:3001".to_string(),
            mysql_addr: "0.0.0.0:3306".to_string(),
            mysql_runtime_size: 2,
            postgres_addr: "0.0.0.0:5432".to_string(),
            postgres_runtime_size: 2,
            meta_client_opts: MetaClientOpts::default(),
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

// Options for meta client in datanode instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetaClientOpts {
    pub metasrv_addr: String,
    pub timeout_millis: u64,
    pub connect_timeout_millis: u64,
    pub tcp_nodelay: bool,
}

impl Default for MetaClientOpts {
    fn default() -> Self {
        Self {
            metasrv_addr: "127.0.0.1:3002".to_string(),
            timeout_millis: 3_000u64,
            connect_timeout_millis: 5_000u64,
            tcp_nodelay: true,
        }
    }
}
