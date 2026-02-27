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

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use api::v1::meta::heartbeat_client::HeartbeatClient;
use api::v1::meta::{
    HeartbeatRequest, HeartbeatResponse, PullConfigRequest, PullConfigResponse, RequestHeader, Role,
};
use common_grpc::channel_manager::ChannelManager;
use common_meta::distributed_time_constants::BASE_HEARTBEAT_INTERVAL;
use common_meta::util;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{info, warn};
use snafu::{OptionExt, ResultExt, ensure};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;
use tonic::codec::CompressionEncoding;
use tonic::transport::Channel;

use crate::client::{Id, LeaderProviderRef};
use crate::error;
use crate::error::{InvalidResponseHeaderSnafu, Result};

/// Heartbeat configuration received from Metasrv during handshake.
#[derive(Debug, Clone, Copy)]
pub struct HeartbeatConfig {
    pub interval: Duration,
    pub retry_interval: Duration,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            interval: BASE_HEARTBEAT_INTERVAL,
            retry_interval: BASE_HEARTBEAT_INTERVAL,
        }
    }
}

impl fmt::Display for HeartbeatConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "interval={:?}, retry={:?}",
            self.interval, self.retry_interval
        )
    }
}

impl HeartbeatConfig {
    /// Extract configuration from HeartbeatResponse.
    pub fn from_response(res: &HeartbeatResponse) -> Self {
        if let Some(cfg) = &res.heartbeat_config {
            // Metasrv provided complete configuration
            Self {
                interval: Duration::from_millis(cfg.heartbeat_interval_ms),
                retry_interval: Duration::from_millis(cfg.retry_interval_ms),
            }
        } else {
            let fallback = Self::default();
            warn!(
                "Metasrv didn't provide heartbeat_config, using default: {}",
                fallback
            );
            fallback
        }
    }
}

pub struct HeartbeatSender {
    id: Id,
    role: Role,
    sender: mpsc::Sender<HeartbeatRequest>,
}

impl HeartbeatSender {
    #[inline]
    fn new(id: Id, role: Role, sender: mpsc::Sender<HeartbeatRequest>) -> Self {
        Self { id, role, sender }
    }

    #[inline]
    pub fn id(&self) -> Id {
        self.id
    }

    #[inline]
    pub async fn send(&self, mut req: HeartbeatRequest) -> Result<()> {
        req.set_header(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );
        self.sender.send(req).await.map_err(|e| {
            error::SendHeartbeatSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })
    }
}

#[derive(Debug)]
pub struct HeartbeatStream {
    id: Id,
    stream: Streaming<HeartbeatResponse>,
}

impl HeartbeatStream {
    #[inline]
    fn new(id: Id, stream: Streaming<HeartbeatResponse>) -> Self {
        Self { id, stream }
    }

    #[inline]
    pub fn id(&self) -> Id {
        self.id
    }

    /// Fetch the next message from this stream.
    #[inline]
    pub async fn message(&mut self) -> Result<Option<HeartbeatResponse>> {
        let res = self.stream.message().await.map_err(error::Error::from);
        if let Ok(Some(heartbeat)) = &res {
            util::check_response_header(heartbeat.header.as_ref())
                .context(InvalidResponseHeaderSnafu)?;
        }
        res
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(id: Id, role: Role, channel_manager: ChannelManager) -> Self {
        let inner = Arc::new(RwLock::new(Inner::new(id, role, channel_manager)));
        Self { inner }
    }

    /// Start the client with a [LeaderProvider].
    pub(crate) async fn start_with(&self, leader_provider: LeaderProviderRef) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.start_with(leader_provider)
    }

    pub async fn ask_leader(&mut self) -> Result<String> {
        let inner = self.inner.read().await;
        inner.ask_leader().await
    }

    pub async fn heartbeat(
        &mut self,
    ) -> Result<(HeartbeatSender, HeartbeatStream, HeartbeatConfig)> {
        let inner = self.inner.read().await;
        inner.ask_leader().await?;
        inner.heartbeat().await
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
                err_msg: "Heartbeat client already started"
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

    async fn heartbeat(&self) -> Result<(HeartbeatSender, HeartbeatStream, HeartbeatConfig)> {
        ensure!(
            self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client not start"
            }
        );

        let leader_addr = self
            .leader_provider
            .as_ref()
            .unwrap()
            .leader()
            .context(error::NoLeaderSnafu)?;
        let mut leader = self.make_client(&leader_addr)?;

        let (sender, receiver) = mpsc::channel::<HeartbeatRequest>(128);

        let header = RequestHeader::new(
            self.id,
            self.role,
            TracingContext::from_current_span().to_w3c(),
        );
        let handshake = HeartbeatRequest {
            header: Some(header),
            ..Default::default()
        };
        sender.send(handshake).await.map_err(|e| {
            error::SendHeartbeatSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })?;
        let receiver = ReceiverStream::new(receiver);

        let mut stream = leader
            .heartbeat(receiver)
            .await
            .map_err(error::Error::from)?
            .into_inner();

        let res = stream
            .message()
            .await
            .map_err(error::Error::from)?
            .context(error::CreateHeartbeatStreamSnafu)?;

        // Extract heartbeat configuration from handshake response
        let config = HeartbeatConfig::from_response(&res);

        info!(
            "Handshake successful with Metasrv at {}, received config: {}",
            leader_addr, config
        );

        Ok((
            HeartbeatSender::new(self.id, self.role, sender),
            HeartbeatStream::new(self.id, stream),
            config,
        ))
    }

    /// Pull meta config(plugin options) from Metasrv.
    /// This is called during the frontend's startup, would stop the startup if failed.
    async fn pull_config(&self) -> Result<PullConfigResponse> {
        ensure!(
            self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client not start"
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

    fn make_client(&self, addr: impl AsRef<str>) -> Result<HeartbeatClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(HeartbeatClient::new(channel)
            .accept_compressed(CompressionEncoding::Zstd)
            .accept_compressed(CompressionEncoding::Gzip)
            .send_compressed(CompressionEncoding::Zstd))
    }

    #[inline]
    pub(crate) fn is_started(&self) -> bool {
        self.leader_provider.is_some()
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::client::AskLeader;

    #[tokio::test]
    async fn test_already_start() {
        let client = Client::new(0, Role::Datanode, ChannelManager::default());
        let leader_provider = Arc::new(AskLeader::new(
            0,
            Role::Datanode,
            vec!["127.0.0.1:1000".to_string(), "127.0.0.1:1001".to_string()],
            ChannelManager::default(),
            3,
        ));
        client.start_with(leader_provider.clone()).await.unwrap();
        let res = client.start_with(leader_provider).await;
        assert!(res.is_err());
        assert!(matches!(
            res.err(),
            Some(error::Error::IllegalGrpcClientState { .. })
        ));
    }

    #[tokio::test]
    async fn test_heartbeat_stream() {
        let (sender, mut receiver) = mpsc::channel::<HeartbeatRequest>(100);
        let sender = HeartbeatSender::new(8, Role::Datanode, sender);
        let _handle = tokio::spawn(async move {
            for _ in 0..10 {
                sender.send(HeartbeatRequest::default()).await.unwrap();
            }
        });
        while let Some(req) = receiver.recv().await {
            let header = req.header.unwrap();
            assert_eq!(8, header.member_id);
        }
    }
}
