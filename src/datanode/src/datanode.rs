// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_telemetry::info;
use meta_client::MetaClientOpts;
use serde::{Deserialize, Serialize};
use servers::Mode;

use crate::error::Result;
use crate::instance::{Instance, InstanceRef};
use crate::server::Services;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ObjectStoreConfig {
    File(FileConfig),
    S3(S3Config),
    Oss(OssConfig),
}

#[derive(Debug, Clone, Serialize, Default, Deserialize)]
#[serde(default)]
pub struct FileConfig {
    pub data_dir: String,
}

#[derive(Debug, Clone, Serialize, Default, Deserialize)]
#[serde(default)]
pub struct S3Config {
    pub bucket: String,
    pub root: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

#[derive(Debug, Clone, Serialize, Default, Deserialize)]
#[serde(default)]
pub struct OssConfig {
    pub bucket: String,
    pub root: String,
    pub access_key_id: String,
    pub access_key_secret: String,
    pub endpoint: String,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        ObjectStoreConfig::File(FileConfig {
            data_dir: "/tmp/greptimedb/data/".to_string(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalConfig {
    // wal directory
    pub dir: String,
    // wal file size in bytes
    pub file_size: ReadableSize,
    // wal purge threshold in bytes
    pub purge_threshold: ReadableSize,
    // purge interval in seconds
    #[serde(with = "humantime_serde")]
    pub purge_interval: Duration,
    // read batch size
    pub read_batch_size: usize,
    // whether to sync log file after every write
    pub sync_write: bool,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: "/tmp/greptimedb/wal".to_string(),
            file_size: ReadableSize::gb(1),        // log file size 1G
            purge_threshold: ReadableSize::gb(50), // purge threshold 50G
            purge_interval: Duration::from_secs(600),
            read_batch_size: 128,
            sync_write: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct DatanodeOptions {
    pub node_id: Option<u64>,
    pub rpc_addr: String,
    pub rpc_hostname: Option<String>,
    pub rpc_runtime_size: usize,
    pub mysql_addr: String,
    pub mysql_runtime_size: usize,
    pub meta_client_opts: Option<MetaClientOpts>,
    pub wal: WalConfig,
    pub storage: ObjectStoreConfig,
    pub enable_memory_catalog: bool,
    pub mode: Mode,
}

impl Default for DatanodeOptions {
    fn default() -> Self {
        Self {
            node_id: None,
            rpc_addr: "127.0.0.1:3001".to_string(),
            rpc_hostname: None,
            rpc_runtime_size: 8,
            mysql_addr: "127.0.0.1:4406".to_string(),
            mysql_runtime_size: 2,
            meta_client_opts: None,
            wal: WalConfig::default(),
            storage: ObjectStoreConfig::default(),
            enable_memory_catalog: false,
            mode: Mode::Standalone,
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
        let services = Services::try_new(instance.clone(), &opts).await?;
        Ok(Self {
            opts,
            services,
            instance,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        info!("Starting datanode instance...");
        self.start_instance().await?;
        self.start_services().await
    }

    /// Start only the internal component of datanode.
    pub async fn start_instance(&mut self) -> Result<()> {
        self.instance.start().await
    }

    /// Start services of datanode. This method call will block until services are shutdown.
    pub async fn start_services(&mut self) -> Result<()> {
        self.services.start(&self.opts).await
    }

    pub fn get_instance(&self) -> InstanceRef {
        self.instance.clone()
    }
}
