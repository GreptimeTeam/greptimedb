// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    pub node_id: Option<u64>,
    pub rpc_addr: String,
    pub rpc_runtime_size: usize,
    pub mysql_addr: String,
    pub mysql_runtime_size: usize,
    pub meta_client_opts: Option<MetaClientOpts>,
    pub wal_dir: String,
    pub storage: ObjectStoreConfig,
    pub mode: Mode,
}

impl Default for DatanodeOptions {
    fn default() -> Self {
        Self {
            node_id: None,
            rpc_addr: "127.0.0.1:3001".to_string(),
            rpc_runtime_size: 8,
            mysql_addr: "127.0.0.1:4406".to_string(),
            mysql_runtime_size: 2,
            meta_client_opts: None,
            wal_dir: "/tmp/greptimedb/wal".to_string(),
            storage: ObjectStoreConfig::default(),
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
        self.instance.start().await?;
        self.services.start(&self.opts).await?;
        Ok(())
    }

    pub fn get_instance(&self) -> InstanceRef {
        self.instance.clone()
    }
}
