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

use api::v1::meta::ddl_task_client::DdlTaskClient;
use api::v1::meta::{ErrorCode, Role, SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use common_grpc::channel_manager::ChannelManager;
use snafu::{ensure, ResultExt};
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::client::heartbeat::Inner as HeartbeatInner;
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
            channel_manager: channel_manager.clone(),
            heartbeat_inner: HeartbeatInner::new(id, role, channel_manager),
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
    heartbeat_inner: HeartbeatInner,
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

        self.heartbeat_inner.start(urls).await?;
        Ok(())
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<DdlTaskClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(DdlTaskClient::new(channel))
    }

    #[inline]
    fn is_started(&self) -> bool {
        self.heartbeat_inner.is_started()
    }

    pub async fn submit_ddl_task(
        &mut self,
        mut req: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        req.set_header(self.id, self.role);

        loop {
            if let Some(leader) = &self.heartbeat_inner.get_leader() {
                let mut client = self.make_client(leader)?;
                let res = client
                    .submit_ddl_task(req.clone())
                    .await
                    .context(error::TonicStatusSnafu)?;

                let res = res.into_inner();

                if let Some(header) = res.header.as_ref() {
                    if let Some(err) = header.error.as_ref() {
                        if err.code == ErrorCode::NotLeader as i32 {
                            self.heartbeat_inner.reset_leader();
                            continue;
                        }
                    }
                }

                return Ok(res);
            } else if let Err(err) = self.heartbeat_inner.ask_leader().await {
                return Err(err);
            }
        }
    }
}
