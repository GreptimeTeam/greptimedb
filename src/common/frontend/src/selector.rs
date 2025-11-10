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

use std::fmt::Debug;
use std::time::Duration;

use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::cluster::{ClusterInfo, NodeInfo, Role};
use greptime_proto::v1::frontend::{
    KillProcessRequest, KillProcessResponse, ListProcessRequest, ListProcessResponse,
    frontend_client,
};
use meta_client::MetaClientRef;
use snafu::ResultExt;
use tonic::Response;

use crate::error;
use crate::error::{MetaSnafu, Result};

pub type FrontendClientPtr = Box<dyn FrontendClient>;

#[async_trait::async_trait]
pub trait FrontendClient: Send + Debug {
    async fn list_process(&mut self, req: ListProcessRequest) -> Result<ListProcessResponse>;

    async fn kill_process(&mut self, req: KillProcessRequest) -> Result<KillProcessResponse>;
}

#[async_trait::async_trait]
impl FrontendClient for frontend_client::FrontendClient<tonic::transport::channel::Channel> {
    async fn list_process(&mut self, req: ListProcessRequest) -> Result<ListProcessResponse> {
        frontend_client::FrontendClient::<tonic::transport::channel::Channel>::list_process(
            self, req,
        )
        .await
        .context(error::InvokeFrontendSnafu)
        .map(Response::into_inner)
    }

    async fn kill_process(&mut self, req: KillProcessRequest) -> Result<KillProcessResponse> {
        frontend_client::FrontendClient::<tonic::transport::channel::Channel>::kill_process(
            self, req,
        )
        .await
        .context(error::InvokeFrontendSnafu)
        .map(Response::into_inner)
    }
}

#[async_trait::async_trait]
pub trait FrontendSelector {
    async fn select<F>(&self, predicate: F) -> Result<Vec<FrontendClientPtr>>
    where
        F: Fn(&NodeInfo) -> bool + Send;
}

#[derive(Debug, Clone)]
pub struct MetaClientSelector {
    meta_client: MetaClientRef,
    channel_manager: ChannelManager,
}

#[async_trait::async_trait]
impl FrontendSelector for MetaClientSelector {
    async fn select<F>(&self, predicate: F) -> Result<Vec<FrontendClientPtr>>
    where
        F: Fn(&NodeInfo) -> bool + Send,
    {
        let nodes = self
            .meta_client
            .list_nodes(Some(Role::Frontend))
            .await
            .map_err(Box::new)
            .context(MetaSnafu)?;

        nodes
            .into_iter()
            .filter(predicate)
            .map(|node| {
                let channel = self
                    .channel_manager
                    .get(node.peer.addr)
                    .context(error::CreateChannelSnafu)?;
                let client = frontend_client::FrontendClient::new(channel);
                Ok(Box::new(client) as FrontendClientPtr)
            })
            .collect::<Result<Vec<_>>>()
    }
}

impl MetaClientSelector {
    pub fn new(meta_client: MetaClientRef) -> Self {
        let cfg = ChannelConfig::new()
            .connect_timeout(Duration::from_secs(30))
            .timeout(Duration::from_secs(30));
        let channel_manager = ChannelManager::with_config(cfg, None);
        Self {
            meta_client,
            channel_manager,
        }
    }
}
