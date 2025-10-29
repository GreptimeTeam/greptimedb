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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Duration;

use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::node_manager::{DatanodeManager, DatanodeRef, FlownodeManager, FlownodeRef};
use common_meta::peer::Peer;
use moka::future::{Cache, CacheBuilder};

use crate::Client;
use crate::flow::FlowRequester;
use crate::region::RegionRequester;

pub struct NodeClients {
    channel_manager: ChannelManager,
    clients: Cache<Peer, Client>,
}

impl Default for NodeClients {
    fn default() -> Self {
        Self::new(ChannelConfig::new())
    }
}

impl Debug for NodeClients {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeClients")
            .field("channel_manager", &self.channel_manager)
            .finish()
    }
}

#[async_trait::async_trait]
impl DatanodeManager for NodeClients {
    async fn datanode(&self, datanode: &Peer) -> DatanodeRef {
        let client = self.get_client(datanode).await;

        let ChannelConfig {
            send_compression,
            accept_compression,
            ..
        } = self.channel_manager.config();
        Arc::new(RegionRequester::new(
            client,
            *send_compression,
            *accept_compression,
        ))
    }
}

#[async_trait::async_trait]
impl FlownodeManager for NodeClients {
    async fn flownode(&self, flownode: &Peer) -> FlownodeRef {
        let client = self.get_client(flownode).await;

        Arc::new(FlowRequester::new(client))
    }
}

impl NodeClients {
    pub fn new(config: ChannelConfig) -> Self {
        Self {
            channel_manager: ChannelManager::with_config(config, None),
            clients: CacheBuilder::new(1024)
                .time_to_live(Duration::from_secs(30 * 60))
                .time_to_idle(Duration::from_secs(5 * 60))
                .build(),
        }
    }

    pub async fn get_client(&self, datanode: &Peer) -> Client {
        self.clients
            .get_with_by_ref(datanode, async move {
                Client::with_manager_and_urls(
                    self.channel_manager.clone(),
                    vec![datanode.addr.clone()],
                )
            })
            .await
    }

    #[cfg(feature = "testing")]
    pub async fn insert_client(&self, datanode: Peer, client: Client) {
        self.clients.insert(datanode, client).await
    }
}
