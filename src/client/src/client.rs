use std::collections::HashSet;
use std::sync::Arc;

use api::v1::greptime_client::GreptimeClient;
use api::v1::*;
use common_grpc::channel_manager::ChannelManager;
use parking_lot::RwLock;
use snafu::OptionExt;
use snafu::ResultExt;
use tonic::transport::Channel;

use crate::error;
use crate::load_balance::LoadBalance;
use crate::load_balance::LB;
use crate::Result;

#[derive(Clone, Debug, Default)]
pub struct Client {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    channel_manager: ChannelManager,
    peers: Arc<RwLock<Vec<String>>>,
    load_balance: LB,
}

impl Inner {
    fn with_manager(channel_manager: ChannelManager) -> Self {
        Self {
            channel_manager,
            ..Default::default()
        }
    }

    fn set_peers(&self, peers: Vec<String>) {
        let mut guard = self.peers.write();
        if guard.is_empty() {
            *guard = peers;
        }
    }

    fn get_peer(&self) -> Option<String> {
        let guard = self.peers.read();
        self.load_balance.get_peer(&guard).cloned()
    }
}

impl Client {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_manager(channel_manager: ChannelManager) -> Self {
        let inner = Arc::new(Inner::with_manager(channel_manager));
        Self { inner }
    }

    pub fn with_urls<U, A>(urls: A) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        Self::with_manager_and_urls(ChannelManager::new(), urls)
    }

    pub fn with_manager_and_urls<U, A>(channel_manager: ChannelManager, urls: A) -> Self
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let inner = Inner::with_manager(channel_manager);
        let urls: Vec<String> = urls
            .as_ref()
            .iter()
            .map(|peer| peer.as_ref().to_string())
            .collect::<HashSet<_>>()
            .drain()
            .collect();
        inner.set_peers(urls);
        Self {
            inner: Arc::new(inner),
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

    pub async fn admin(&self, req: AdminRequest) -> Result<AdminResponse> {
        let req = BatchRequest {
            admins: vec![req],
            ..Default::default()
        };

        let mut res = self.batch(req).await?;
        res.admins.pop().context(error::MissingResultSnafu {
            name: "admins",
            expected: 1_usize,
            actual: 0_usize,
        })
    }

    pub async fn database(&self, req: DatabaseRequest) -> Result<DatabaseResponse> {
        let req = BatchRequest {
            databases: vec![req],
            ..Default::default()
        };

        let mut res = self.batch(req).await?;
        res.databases.pop().context(error::MissingResultSnafu {
            name: "database",
            expected: 1_usize,
            actual: 0_usize,
        })
    }

    pub async fn batch(&self, req: BatchRequest) -> Result<BatchResponse> {
        let peer = self
            .inner
            .get_peer()
            .context(error::NotFoundClientSnafu {})?;
        let mut client = self.make_client(peer)?;
        let result = client.batch(req).await.context(error::TonicStatusSnafu)?;
        Ok(result.into_inner())
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<GreptimeClient<Channel>> {
        let channel = self
            .inner
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;
        Ok(GreptimeClient::new(channel))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::Inner;
    use crate::load_balance::LB;

    fn mock_peers() -> Vec<String> {
        vec![
            "127.0.0.1:3001".to_string(),
            "127.0.0.1:3002".to_string(),
            "127.0.0.1:3003".to_string(),
        ]
    }

    #[test]
    fn test_inner() {
        let inner = Inner::default();

        assert!(matches!(
            inner.load_balance,
            LB::Random(crate::load_balance::Random)
        ));
        assert!(inner.get_peer().is_none());

        let peers = mock_peers();
        inner.set_peers(peers.clone());
        let all: HashSet<String> = peers.into_iter().collect();

        for _ in 0..20 {
            assert!(all.contains(&inner.get_peer().unwrap()));
        }
    }
}
