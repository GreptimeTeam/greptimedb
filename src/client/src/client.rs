use std::collections::HashSet;
use std::sync::Arc;

use api::v1::{greptime_client::GreptimeClient, *};
use common_grpc::channel_manager::ChannelManager;
use parking_lot::RwLock;
use rand::Rng;
use snafu::{OptionExt, ResultExt};
use tonic::transport::Channel;

use crate::error;
use crate::Result;

#[derive(Clone, Debug, Default)]
pub struct Client {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    channel_manager: ChannelManager,
    peers: Arc<RwLock<Option<Vec<String>>>>,
}

impl Inner {
    fn with_manager(channel_manager: ChannelManager) -> Self {
        Self {
            channel_manager,
            peers: Default::default(),
        }
    }

    fn set_peers(&self, peers: Vec<String>) {
        let mut guard = self.peers.write();
        if guard.is_none() {
            *guard = Some(peers);
        }
    }

    fn random_peer(&self) -> Option<String> {
        let guard = self.peers.read();
        let peers = match &*guard {
            Some(peers) => peers,
            None => return None,
        };
        let len = peers.len();
        if len == 0 {
            return None;
        }
        let mut rng = rand::thread_rng();
        let i = rng.gen_range(0..len);
        peers.get(i).cloned()
    }
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            channel_manager: ChannelManager::new(),
            peers: Default::default(),
        }
    }
}

pub enum LB {
    Random,
    Specify(String),
}

impl Client {
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    pub fn with_manager(channel_manager: ChannelManager) -> Self {
        Self {
            inner: Arc::new(Inner::with_manager(channel_manager)),
        }
    }

    pub fn start<U, A>(&self, urls: A)
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let urls: Vec<String> = urls
            .as_ref()
            .iter()
            .map(|peer| peer.as_ref().to_string())
            .collect::<HashSet<_>>()
            .drain()
            .collect();

        self.inner.set_peers(urls);
    }

    pub async fn admin(
        &self,
        header: Option<RequestHeader>,
        req: AdminRequest,
        lb: LB,
    ) -> Result<AdminResponse> {
        let req = BatchRequest {
            admins: vec![req],
            header,
            ..Default::default()
        };

        let mut res = self.batch(req, lb).await?;
        res.admins.pop().context(error::MissingResultSnafu {
            name: "admins",
            expected: 1_usize,
            actual: 0_usize,
        })
    }

    pub async fn database(
        &self,
        header: Option<RequestHeader>,
        req: DatabaseRequest,
        lb: LB,
    ) -> Result<DatabaseResponse> {
        let req = BatchRequest {
            databases: vec![req],
            header,
            ..Default::default()
        };

        let mut res = self.batch(req, lb).await?;
        res.databases.pop().context(error::MissingResultSnafu {
            name: "database",
            expected: 1_usize,
            actual: 0_usize,
        })
    }

    async fn batch(&self, req: BatchRequest, lb: LB) -> Result<BatchResponse> {
        let mut client = match lb {
            LB::Random => self.random_client()?,
            LB::Specify(addr) => self.make_client(addr)?,
        };
        let result = client.batch(req).await.context(error::TonicStatusSnafu)?;
        Ok(result.into_inner())
    }

    fn random_client(&self) -> Result<GreptimeClient<Channel>> {
        let peer = self
            .inner
            .random_peer()
            .context(error::NotFoundClientSnafu)?;
        self.make_client(peer)
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<GreptimeClient<Channel>> {
        let channel = self
            .inner
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(GreptimeClient::new(channel))
    }

    pub fn add_channel(&self, addr: &str, channel: Channel) {
        self.inner.set_peers(vec![addr.to_string()]);
        self.inner.channel_manager.put_channel(addr, channel)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use api::v1::DatabaseRequest;

    use super::{Inner, LB};
    use crate::{error, Client};

    #[test]
    fn test_inner_random_peer() {
        let inner = Inner::default();
        let peer1 = "127.0.0.1:3001".to_string();
        let peer2 = "127.0.0.1:3002".to_string();
        inner.set_peers(vec![peer1.clone(), peer2.clone()]);

        let mut set = HashSet::with_capacity(2);
        set.insert(peer1);
        set.insert(peer2);

        for _ in 0..100 {
            let peer = inner.random_peer().unwrap();
            assert!(set.contains(&peer));
        }
    }

    #[test]
    fn test_start_with_duplicate_peers() {
        let client = Client::new();
        client.start(vec!["127.0.0.1:3001", "127.0.0.1:3001", "127.0.0.1:3001"]);
        let guard = client.inner.peers.read();
        let peers = guard.as_ref().unwrap();

        assert_eq!(&vec!["127.0.0.1:3001".to_string()], peers);
    }

    #[tokio::test]
    async fn test_create_unavailable() {
        let client = Client::new();
        client.start(&["unavailable_peer"]);

        let req = DatabaseRequest::default();
        let result = client.database(None, req, LB::Random).await;
        assert!(result.is_err());
        let err = result.err().unwrap();
        assert!(
            matches!(err, error::Error::TonicStatus { source, .. } if source.code() == tonic::Code::Unavailable)
        );
    }

    #[tokio::test]
    async fn test_random_client() {
        let client = Client::new();
        let result = client.random_client();
        assert!(result.is_err());

        client.start(vec!["127.0.0.1:3001"]);
        let result = client.random_client();
        assert!(result.is_ok());
    }
}
