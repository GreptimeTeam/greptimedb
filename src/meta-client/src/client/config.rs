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

use api::v1::meta::config_client::ConfigClient;
use api::v1::meta::{PullConfigRequest, PullConfigResponse, RequestHeader, Role};
use common_grpc::channel_manager::ChannelManager;
use common_meta::util;
use common_telemetry::tracing_context::TracingContext;
use snafu::{OptionExt, ResultExt, ensure};
use tokio::sync::RwLock;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use crate::client::{Id, LeaderProviderRef};
use crate::error;
use crate::error::{InvalidResponseHeaderSnafu, Result};

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(id: Id, role: Role, channel_manager: ChannelManager) -> Self {
        let inner = Arc::new(RwLock::new(Inner::new(id, role, channel_manager)));
        Self { inner }
    }

    pub(crate) async fn start_with(&self, leader_provider: LeaderProviderRef) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.start_with(leader_provider)
    }

    pub async fn pull_config(&self) -> Result<PullConfigResponse> {
        let inner = self.inner.read().await;
        inner.ask_leader().await?;
        inner.pull_config().await
    }
}

#[derive(Debug)]
struct Inner {
    id: Id,
    role: Role,
    channel_manager: ChannelManager,
    leader_provider: Option<LeaderProviderRef>,
}

impl Inner {
    fn new(id: Id, role: Role, channel_manager: ChannelManager) -> Self {
        Self {
            id,
            role,
            channel_manager,
            leader_provider: None,
        }
    }

    fn start_with(&mut self, leader_provider: LeaderProviderRef) -> Result<()> {
        ensure!(
            !self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Config client already started"
            }
        );
        self.leader_provider = Some(leader_provider);
        Ok(())
    }

    async fn ask_leader(&self) -> Result<String> {
        let Some(leader_provider) = self.leader_provider.as_ref() else {
            return error::IllegalGrpcClientStateSnafu {
                err_msg: "not started",
            }
            .fail();
        };
        leader_provider.ask_leader().await
    }

    async fn pull_config(&self) -> Result<PullConfigResponse> {
        ensure!(
            self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Config client not start"
            }
        );

        let leader_addr = self
            .leader_provider
            .as_ref()
            .unwrap()
            .leader()
            .context(error::NoLeaderSnafu)?;
        let mut client = self.make_client(&leader_addr)?;

        let header = RequestHeader::new(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );
        let req = PullConfigRequest {
            header: Some(header),
        };

        let res = client
            .pull_config(req)
            .await
            .map_err(error::Error::from)?
            .into_inner();

        util::check_response_header(res.header.as_ref()).context(InvalidResponseHeaderSnafu)?;

        Ok(res)
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<ConfigClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(ConfigClient::new(channel)
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd))
    }

    #[inline]
    fn is_started(&self) -> bool {
        self.leader_provider.is_some()
    }
}
