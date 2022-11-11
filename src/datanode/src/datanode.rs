use std::sync::Arc;

use common_telemetry::info;
use frontend::frontend::Mode;
use meta_client::MetaClientOpts;
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
    pub rpc_runtime_size: usize,
    pub mysql_addr: String,
    pub mysql_runtime_size: usize,
    pub postgres_addr: String,
    pub postgres_runtime_size: usize,
    pub meta_client_opts: MetaClientOpts,
    pub wal_dir: String,
    pub storage: ObjectStoreConfig,
    pub mode: Mode,
}

impl Default for DatanodeOptions {
    fn default() -> Self {
        Self {
            node_id: 0,
            http_addr: "0.0.0.0:3000".to_string(),
            rpc_addr: "0.0.0.0:3001".to_string(),
            rpc_runtime_size: 8,
            mysql_addr: "0.0.0.0:3306".to_string(),
            mysql_runtime_size: 2,
            postgres_addr: "0.0.0.0:5432".to_string(),
            postgres_runtime_size: 2,
            meta_client_opts: MetaClientOpts::default(),
            wal_dir: "/tmp/greptimedb/wal".to_string(),
            storage: ObjectStoreConfig::default(),
            mode: Mode::Standalone,
        }
    }
}

/// Datanode service.
pub struct Datanode {
    datanode_opts: DatanodeOptions,
    services: Services,
    instance: InstanceRef,
}

impl Datanode {
    pub async fn new(opts: DatanodeOptions) -> Result<Datanode> {
        let instance = Arc::new(Instance::new(&opts).await?);
        let services = Services::try_new(instance.clone(), &opts).await?;
        Ok(Self {
            datanode_opts: opts,
            services,
            instance,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting datanode instance...");
        self.instance.start().await?;
        self.services.start(&self.datanode_opts).await?;
        Ok(())
    }
}
