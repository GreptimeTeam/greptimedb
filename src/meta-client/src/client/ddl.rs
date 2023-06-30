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

use std::collections::HashSet;
use std::sync::Arc;

use api::v1::meta::ddl_task_client::DdlTaskClient;
use api::v1::meta::heartbeat_client::HeartbeatClient;
use api::v1::meta::{
    AskLeaderRequest, ErrorCode, RequestHeader, Role, SubmitDdlTaskRequest, SubmitDdlTaskResponse,
};
use common_grpc::channel_manager::ChannelManager;
use common_telemetry::debug;
use snafu::{ensure, OptionExt, ResultExt};
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::client::Id;
use crate::error;
use crate::error::Result;

#[derive(Clone, Debug)]
// TODO(weny): removes this in following PRs.
#[allow(unused)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

// TODO(weny): removes this in following PRs.
#[allow(dead_code)]
impl Client {
    pub fn new(id: Id, role: Role, channel_manager: ChannelManager) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            id,
            role,
            channel_manager,
            peers: vec![],
            leader: None,
        }));

        Self { inner }
    }

    pub async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let mut inner = self.inner.write().await;
        inner.start(urls).await
    }

    pub async fn is_started(&self) -> bool {
        let inner = self.inner.read().await;
        inner.is_started()
    }

    pub async fn submit_ddl_task(
        &self,
        req: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        let mut inner = self.inner.write().await;
        inner.submit_ddl_task(req).await
    }
}

#[derive(Debug)]
// TODO(weny): removes this in following PRs.
#[allow(unused)]
struct Inner {
    id: Id,
    role: Role,
    channel_manager: ChannelManager,
    peers: Vec<String>,
    leader: Option<String>,
}

impl Inner {
    async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        ensure!(
            !self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Router client already started",
            }
        );

        self.peers = urls
            .as_ref()
            .iter()
            .map(|url| url.as_ref().to_string())
            .collect::<HashSet<_>>()
            .drain()
            .collect::<Vec<_>>();

        Ok(())
    }

    // TODO(weny): considers refactoring `ask_leader` into a common client.
    async fn ask_leader(&mut self) -> Result<()> {
        ensure!(
            self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client not start"
            }
        );

        let header = RequestHeader::new(self.id, self.role);
        let mut leader = None;
        for addr in &self.peers {
            let req = AskLeaderRequest {
                header: Some(header.clone()),
            };
            let mut client = self.make_heartbeat_client(addr)?;
            match client.ask_leader(req).await {
                Ok(res) => {
                    if let Some(endpoint) = res.into_inner().leader {
                        leader = Some(endpoint.addr);
                        break;
                    }
                }
                Err(status) => {
                    debug!("Failed to ask leader from: {}, {}", addr, status);
                }
            }
        }
        self.leader = Some(leader.context(error::AskLeaderSnafu)?);
        Ok(())
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<DdlTaskClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(DdlTaskClient::new(channel))
    }

    fn make_heartbeat_client(&self, addr: impl AsRef<str>) -> Result<HeartbeatClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(HeartbeatClient::new(channel))
    }

    #[inline]
    fn is_started(&self) -> bool {
        !self.peers.is_empty()
    }

    pub async fn submit_ddl_task(
        &mut self,
        mut req: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        req.set_header(self.id, self.role);

        loop {
            if let Some(leader) = &self.leader {
                let mut client = self.make_client(leader)?;
                let res = client
                    .submit_ddl_task(req.clone())
                    .await
                    .context(error::TonicStatusSnafu)?;

                let res = res.into_inner();

                if let Some(header) = res.header.as_ref() {
                    if let Some(err) = header.error.as_ref() {
                        if err.code == ErrorCode::NotLeader as i32 {
                            self.leader = None;
                            continue;
                        }
                    }
                }

                return Ok(res);
            } else if let Err(err) = self.ask_leader().await {
                return Err(err);
            }
        }
    }
}
