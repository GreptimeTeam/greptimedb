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
use api::v1::meta::{ErrorCode, ResponseHeader, Role, SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use common_grpc::channel_manager::ChannelManager;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{info, warn};
use snafu::{ensure, ResultExt};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::{Code, Status};

use crate::client::ask_leader::AskLeader;
use crate::client::Id;
use crate::error;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(id: Id, role: Role, channel_manager: ChannelManager, max_retry: usize) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            id,
            role,
            channel_manager,
            ask_leader: None,
            max_retry,
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
    max_retry: usize,
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
            self.max_retry,
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

        req.set_header(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );
        let ask_leader = self.ask_leader.as_ref().unwrap();
        let mut times = 0;

        while times < self.max_retry {
            if let Some(leader) = &ask_leader.get_leader() {
                let mut client = self.make_client(leader)?;
                match client.submit_ddl_task(req.clone()).await {
                    Ok(res) => {
                        let res = res.into_inner();
                        if is_not_leader(&res.header) {
                            warn!("Failed to submitting ddl to {leader}, not a leader");
                            let leader = ask_leader.ask_leader().await?;
                            info!("DDL client updated to new leader addr: {leader}");
                            times += 1;
                            continue;
                        }
                        return Ok(res);
                    }
                    Err(status) => {
                        // The leader may be unreachable.
                        if is_unreachable(&status) {
                            warn!("Failed to submitting ddl to {leader}, source: {status}");
                            let leader = ask_leader.ask_leader().await?;
                            info!("DDL client updated to new leader addr: {leader}");
                            times += 1;
                            continue;
                        } else {
                            return Err(error::Error::from(status));
                        }
                    }
                }
            } else if let Err(err) = ask_leader.ask_leader().await {
                return Err(err);
            }
        }

        error::RetryTimesExceededSnafu {
            msg: "Failed to submit DDL task",
            times: self.max_retry,
        }
        .fail()
    }
}

fn is_unreachable(status: &Status) -> bool {
    status.code() == Code::Unavailable || status.code() == Code::DeadlineExceeded
}

fn is_not_leader(header: &Option<ResponseHeader>) -> bool {
    if let Some(header) = header {
        if let Some(err) = header.error.as_ref() {
            return err.code == ErrorCode::NotLeader as i32;
        }
    }

    false
}
