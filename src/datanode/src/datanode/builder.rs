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

use common_base::Plugins;
use meta_client::client::MetaClient;
use servers::Mode;
use snafu::OptionExt;

use crate::datanode::{Datanode, DatanodeOptions};
use crate::error::{MissingMetasrvOptsSnafu, MissingNodeIdSnafu, Result};
use crate::greptimedb_telemetry::get_greptimedb_telemetry_task;
use crate::heartbeat::{new_metasrv_client, HeartbeatTask};
use crate::server::Services;

pub struct DatanodeBuilder {
    opts: DatanodeOptions,
    plugins: Arc<Plugins>,
    meta_client: Option<MetaClient>,
}

impl DatanodeBuilder {
    pub fn new(opts: DatanodeOptions, plugins: Arc<Plugins>) -> Self {
        Self {
            opts,
            plugins,
            meta_client: None,
        }
    }

    pub fn with_meta_client(self, meta_client: MetaClient) -> Self {
        Self {
            meta_client: Some(meta_client),
            ..self
        }
    }

    pub async fn build(mut self) -> Result<Datanode> {
        let region_server = Datanode::new_region_server(&self.opts, self.plugins.clone()).await?;

        let mode = &self.opts.mode;

        let heartbeat_task = match mode {
            Mode::Distributed => {
                let meta_client = if let Some(meta_client) = self.meta_client.take() {
                    meta_client
                } else {
                    let node_id = self.opts.node_id.context(MissingNodeIdSnafu)?;

                    let meta_config = self
                        .opts
                        .meta_client_options
                        .as_ref()
                        .context(MissingMetasrvOptsSnafu)?;

                    new_metasrv_client(node_id, meta_config).await?
                };

                let heartbeat_task =
                    HeartbeatTask::try_new(&self.opts, region_server.clone(), meta_client).await?;
                Some(heartbeat_task)
            }
            Mode::Standalone => None,
        };

        let services = match mode {
            Mode::Distributed => Some(Services::try_new(region_server.clone(), &self.opts).await?),
            Mode::Standalone => None,
        };

        let greptimedb_telemetry_task = get_greptimedb_telemetry_task(
            Some(self.opts.storage.data_home.clone()),
            mode,
            self.opts.enable_telemetry,
        )
        .await;

        Ok(Datanode {
            opts: self.opts,
            services,
            heartbeat_task,
            region_server,
            greptimedb_telemetry_task,
            plugins: self.plugins.clone(),
        })
    }
}
