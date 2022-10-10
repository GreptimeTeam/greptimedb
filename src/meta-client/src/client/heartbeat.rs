use std::collections::HashSet;
use std::sync::Arc;

use api::v1::meta::heartbeat_client::HeartbeatClient;
use api::v1::meta::AskLeaderRequest;
use common_grpc::channel_manager::ChannelManager;
use common_telemetry::debug;
use snafu::ResultExt;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::error;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(channel_manager: ChannelManager) -> Self {
        let inner = Inner {
            channel_manager,
            peers: HashSet::default(),
            leader: None,
        };

        Self {
            inner: Arc::new(RwLock::new(inner)),
        }
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

    // TODO(jiachun) send heartbeat
}

#[derive(Debug)]
struct Inner {
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
        if !self.peers.is_empty() {
            return error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client already started",
            }
            .fail();
        }

        for url in urls.as_ref() {
            self.peers.insert(url.as_ref().to_string());
        }

        Ok(())
    }

    async fn ask_leader(&mut self) -> Result<()> {
        if self.peers.is_empty() {
            return error::IllegalGrpcClientStateSnafu {
                err_msg: "Heartbeat client not start",
            }
            .fail();
        }

        let mut leader = None;
        for addr in &self.peers {
            let req = AskLeaderRequest::default();
            let mut client = self.make_client(addr)?;
            match client.ask_leader(req).await {
                Ok(res) => {
                    if let Some(endpoint) = res.into_inner().leader {
                        leader = Some(endpoint.addr);
                        break;
                    }
                }
                Err(status) => {
                    debug!("Fail to ask leader from: {}, {}", addr, status);
                }
            }
        }

        match leader {
            Some(leader) => {
                self.leader = Some(leader);
                Ok(())
            }
            None => error::AskLeaderSnafu {}.fail(),
        }
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<HeartbeatClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(HeartbeatClient::new(channel))
    }
}
