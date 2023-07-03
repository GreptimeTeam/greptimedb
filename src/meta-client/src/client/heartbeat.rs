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

use api::v1::meta::heartbeat_client::HeartbeatClient;
use api::v1::meta::{AskLeaderRequest, HeartbeatRequest, HeartbeatResponse, RequestHeader, Role};
use common_grpc::channel_manager::ChannelManager;
use common_meta::rpc::util;
use common_telemetry::{debug, info};
use snafu::{ensure, OptionExt, ResultExt};
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Streaming;

use crate::client::Id;
use crate::error;
use crate::error::{InvalidResponseHeaderSnafu, Result};

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
        req.set_header(self.id, self.role);
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
        let res = self.stream.message().await.context(error::TonicStatusSnafu);
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
        let inner = Arc::new(RwLock::new(Inner {
            id,
            role,
            channel_manager,
            peers: HashSet::default(),
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

    pub async fn ask_leader(&mut self) -> Result<()> {
        let mut inner = self.inner.write().await;
        inner.ask_leader().await
    }

    pub async fn heartbeat(&mut self) -> Result<(HeartbeatSender, HeartbeatStream)> {
        let mut inner = self.inner.write().await;
        inner.ask_leader().await?;
        inner.heartbeat().await
    }

    pub async fn is_started(&self) -> bool {
        let inner = self.inner.read().await;
        inner.is_started()
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    id: Id,
    role: Role,
    channel_manager: ChannelManager,
    peers: HashSet<String>,
    leader: Option<String>,
}

impl Inner {
    pub(crate) fn new(id: Id, role: Role, channel_manager: ChannelManager) -> Self {
        Self {
            id,
            role,
            channel_manager,
            peers: HashSet::new(),
            leader: None,
        }
    }
    pub(crate) async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        ensure!(
            !self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client already started"
            }
        );

        self.peers = urls
            .as_ref()
            .iter()
            .map(|url| url.as_ref().to_string())
            .collect();

        Ok(())
    }

    pub(crate) fn get_leader(&self) -> Option<String> {
        self.leader.clone()
    }

    pub(crate) fn reset_leader(&mut self) {
        self.leader = None;
    }

    pub(crate) async fn ask_leader(&mut self) -> Result<()> {
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
            let mut client = self.make_client(addr)?;
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

    async fn heartbeat(&self) -> Result<(HeartbeatSender, HeartbeatStream)> {
        let leader = self.leader.as_ref().context(error::NoLeaderSnafu)?;
        let mut leader = self.make_client(leader)?;

        let (sender, receiver) = mpsc::channel::<HeartbeatRequest>(128);

        let header = RequestHeader::new(self.id, self.role);
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
            .context(error::TonicStatusSnafu)?
            .into_inner();

        let res = stream
            .message()
            .await
            .context(error::TonicStatusSnafu)?
            .context(error::CreateHeartbeatStreamSnafu)?;
        info!("Success to create heartbeat stream to server: {:#?}", res);

        Ok((
            HeartbeatSender::new(self.id, self.role, sender),
            HeartbeatStream::new(self.id, stream),
        ))
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<HeartbeatClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(HeartbeatClient::new(channel))
    }

    #[inline]
    pub(crate) fn is_started(&self) -> bool {
        !self.peers.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_start_client() {
        let mut client = Client::new((0, 0), Role::Datanode, ChannelManager::default());
        assert!(!client.is_started().await);
        client
            .start(&["127.0.0.1:1000", "127.0.0.1:1001"])
            .await
            .unwrap();
        assert!(client.is_started().await);
    }

    #[tokio::test]
    async fn test_already_start() {
        let mut client = Client::new((0, 0), Role::Datanode, ChannelManager::default());
        client
            .start(&["127.0.0.1:1000", "127.0.0.1:1001"])
            .await
            .unwrap();
        assert!(client.is_started().await);
        let res = client.start(&["127.0.0.1:1002"]).await;
        assert!(res.is_err());
        assert!(matches!(
            res.err(),
            Some(error::Error::IllegalGrpcClientState { .. })
        ));
    }

    #[tokio::test]
    async fn test_start_with_duplicate_peers() {
        let mut client = Client::new((0, 0), Role::Datanode, ChannelManager::default());
        client
            .start(&["127.0.0.1:1000", "127.0.0.1:1000", "127.0.0.1:1000"])
            .await
            .unwrap();
        assert_eq!(1, client.inner.write().await.peers.len());
    }

    #[tokio::test]
    async fn test_heartbeat_stream() {
        let (sender, mut receiver) = mpsc::channel::<HeartbeatRequest>(100);
        let sender = HeartbeatSender::new((8, 8), Role::Datanode, sender);
        let _handle = tokio::spawn(async move {
            for _ in 0..10 {
                sender.send(HeartbeatRequest::default()).await.unwrap();
            }
        });
        while let Some(req) = receiver.recv().await {
            let header = req.header.unwrap();
            assert_eq!(8, header.cluster_id);
            assert_eq!(8, header.member_id);
        }
    }
}
