use std::collections::HashSet;
use std::sync::Arc;

use api::v1::meta::heartbeat_client::HeartbeatClient;
use api::v1::meta::AskLeaderRequest;
use api::v1::meta::HeartbeatRequest;
use api::v1::meta::HeartbeatResponse;
use api::v1::meta::RequestHeader;
use common_grpc::channel_manager::ChannelManager;
use common_telemetry::debug;
use common_telemetry::info;
use snafu::ensure;
use snafu::OptionExt;
use snafu::ResultExt;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Streaming;

use super::Id;
use crate::error;
use crate::error::Result;

pub struct HeartbeatSender {
    id: Id,
    sender: mpsc::Sender<HeartbeatRequest>,
}

impl HeartbeatSender {
    #[inline]
    const fn new(id: Id, sender: mpsc::Sender<HeartbeatRequest>) -> Self {
        Self { id, sender }
    }

    #[inline]
    pub fn id(&self) -> Id {
        self.id
    }

    #[inline]
    pub async fn send(&self, mut req: HeartbeatRequest) -> Result<()> {
        req.header = RequestHeader::new(self.id);
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
    const fn new(id: Id, stream: Streaming<HeartbeatResponse>) -> Self {
        Self { id, stream }
    }

    #[inline]
    pub fn id(&self) -> Id {
        self.id
    }

    /// Fetch the next message from this stream.
    #[inline]
    pub async fn message(&mut self) -> Result<Option<HeartbeatResponse>> {
        self.stream.message().await.context(error::TonicStatusSnafu)
    }
}

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(id: Id, channel_manager: ChannelManager) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            id,
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
        let inner = self.inner.read().await;
        inner.heartbeat().await
    }

    pub async fn is_started(&self) -> bool {
        let inner = self.inner.read().await;
        inner.is_started()
    }
}

#[derive(Debug)]
struct Inner {
    id: Id,
    channel_manager: ChannelManager,
    peers: HashSet<String>,
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

    async fn ask_leader(&mut self) -> Result<()> {
        ensure!(
            self.is_started(),
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client not start"
            }
        );

        let header = RequestHeader::new(self.id);
        let mut leader = None;
        for addr in &self.peers {
            let req = AskLeaderRequest {
                header: header.clone(),
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
        let handshake = HeartbeatRequest {
            header: RequestHeader::new(self.id),
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
            HeartbeatSender::new(self.id, sender),
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
    fn is_started(&self) -> bool {
        !self.peers.is_empty()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[tokio::test]
    async fn test_start_client() {
        let mut client = Client::new((0, 0), ChannelManager::default());
        assert!(!client.is_started().await);
        client
            .start(&["127.0.0.1:1000", "127.0.0.1:1001"])
            .await
            .unwrap();
        assert!(client.is_started().await);
    }

    #[tokio::test]
    async fn test_already_start() {
        let mut client = Client::new((0, 0), ChannelManager::default());
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
        let mut client = Client::new((0, 0), ChannelManager::default());
        client
            .start(&["127.0.0.1:1000", "127.0.0.1:1000", "127.0.0.1:1000"])
            .await
            .unwrap();
        assert_eq!(1, client.inner.write().await.peers.len());
    }

    #[tokio::test]
    async fn test_heartbeat_stream() {
        let (sender, mut receiver) = mpsc::channel::<HeartbeatRequest>(100);
        let sender = HeartbeatSender::new((8, 8), sender);
        tokio::spawn(async move {
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
