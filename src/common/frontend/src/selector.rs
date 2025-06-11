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

use std::time::Duration;

use client::Client;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::cluster::{ClusterInfo, Role};
use greptime_proto::v1::frontend::{frontend_client, ListProcessRequest, ListProcessResponse};
use meta_client::MetaClientRef;
use snafu::ResultExt;

use crate::error::{GrpcSnafu, MetaSnafu, Result};

pub type FrontendClientPtr = Box<dyn FrontendClient>;

#[async_trait::async_trait]
pub trait FrontendClient {
    async fn list_process(&mut self, req: ListProcessRequest) -> Result<ListProcessResponse>;
}

#[async_trait::async_trait]
impl FrontendClient for frontend_client::FrontendClient<tonic::transport::channel::Channel> {
    async fn list_process(&mut self, req: ListProcessRequest) -> Result<ListProcessResponse> {
        let response: ListProcessResponse = frontend_client::FrontendClient::<
            tonic::transport::channel::Channel,
        >::list_process(&mut self, req)
        .await
        .map_err(client::error::Error::from)
        .context(GrpcSnafu)?
        .into_inner();
        Ok(response)
    }
}

#[async_trait::async_trait]
pub trait FrontendSelector {
    async fn select_all(&self) -> Result<Vec<FrontendClientPtr>>;
}

#[derive(Debug, Clone)]
pub struct MetaClientSelector {
    meta_client: MetaClientRef,
    channel_manager: ChannelManager,
}

#[async_trait::async_trait]
impl FrontendSelector for MetaClientSelector {
    async fn select_all(&self) -> Result<Vec<FrontendClientPtr>> {
        let nodes = self
            .meta_client
            .list_nodes(Some(Role::Frontend))
            .await
            .context(MetaSnafu)?;

        let res = nodes
            .into_iter()
            .map(|node| {
                Client::with_manager_and_urls(self.channel_manager.clone(), vec![node.peer.addr])
                    .frontend_client()
                    .map(|c| Box::new(c) as FrontendClientPtr)
            })
            .collect::<client::Result<Vec<_>>>()
            .context(GrpcSnafu)?;
        Ok(res)
    }
}

impl MetaClientSelector {
    pub fn new(meta_client: MetaClientRef) -> Self {
        let cfg = ChannelConfig::new()
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(30));
        let channel_manager = ChannelManager::with_config(cfg);
        Self {
            meta_client,
            channel_manager,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::vec;

    use meta_client::client::MetaClientBuilder;

    use super::*;

    #[tokio::test]
    async fn test_meta_client_selector() {
        let mut meta_client = MetaClientBuilder::frontend_default_options().build();
        meta_client
            .start(vec!["192.168.50.164:3002"])
            .await
            .unwrap();
        let selector = MetaClientSelector::new(Arc::new(meta_client));
        let clients = selector.select_all().await.unwrap();
        for mut client in clients {
            let resp = client.list_process(ListProcessRequest {}).await;
            println!("{:?}", resp);
        }
    }
}
