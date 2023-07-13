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

use crate::client::ask_leader::AskLeader;
use crate::client::Id;
use crate::error;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(id: Id, role: Role, channel_manager: ChannelManager) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            id,
            role,
            channel_manager,
            ask_leader: None,
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
        let inner = self.inner.read().await;
        inner.submit_ddl_task(req).await
    }
}

#[derive(Debug)]

struct Inner {
    id: Id,
    role: Role,
    channel_manager: ChannelManager,
    ask_leader: Option<AskLeader>,
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
                err_msg: "DDL client already started",
            }
        );

        let peers = urls
            .as_ref()
            .iter()
            .map(|url| url.as_ref().to_string())
            .collect::<Vec<_>>();
        self.ask_leader = Some(AskLeader::new(
            self.id,
            self.role,
            peers,
            self.channel_manager.clone(),
        ));

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
        self.ask_leader.is_some()
    }

    pub async fn submit_ddl_task(
        &self,
        mut req: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        ensure!(
            self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "DDL client not start"
            }
        );

        req.set_header(self.id, self.role);
        let ask_leader = self.ask_leader.as_ref().unwrap();
        loop {
            if let Some(leader) = &ask_leader.get_leader() {
                let mut client = self.make_client(leader)?;
                let res = client
                    .submit_ddl_task(req.clone())
                    .await
                    .context(error::TonicStatusSnafu)?;

                let res = res.into_inner();

                if let Some(header) = res.header.as_ref() {
                    if let Some(err) = header.error.as_ref() {
                        if err.code == ErrorCode::NotLeader as i32 {
                            let _ = ask_leader.ask_leader().await?;
                            continue;
                        }
                    }
                }

                return Ok(res);
            } else if let Err(err) = ask_leader.ask_leader().await {
                return Err(err);
            }
        }
    }
}
