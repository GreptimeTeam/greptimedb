use std::collections::HashSet;
use std::sync::Arc;

use api::v1::meta::store_client::StoreClient;
use api::v1::meta::DeleteRangeRequest;
use api::v1::meta::DeleteRangeResponse;
use api::v1::meta::PutRequest;
use api::v1::meta::PutResponse;
use api::v1::meta::RangeRequest;
use api::v1::meta::RangeResponse;
use common_grpc::channel_manager::ChannelManager;
use snafu::ensure;
use snafu::OptionExt;
use snafu::ResultExt;
use tokio::sync::RwLock;
use tonic::transport::Channel;

use crate::client::load_balance as lb;
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
            peers: vec![],
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

    pub async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        let inner = self.inner.read().await;
        inner.range(req).await
    }

    pub async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let inner = self.inner.read().await;
        inner.put(req).await
    }

    pub async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let inner = self.inner.read().await;
        inner.delete_range(req).await
    }
}

#[derive(Debug)]
struct Inner {
    channel_manager: ChannelManager,
    peers: Vec<String>,
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
                err_msg: "Store client already started",
            }
        );

        self.peers = urls
            .as_ref()
            .iter()
            .map(|url| url.as_ref().to_string())
            .collect::<HashSet<_>>()
            .drain()
            .collect::<Vec<_>>();

        Ok(())
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        let mut client = self.random_client()?;

        let res = client.range(req).await.context(error::TonicStatusSnafu)?;

        Ok(res.into_inner())
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let mut client = self.random_client()?;

        let res = client.put(req).await.context(error::TonicStatusSnafu)?;

        Ok(res.into_inner())
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let mut client = self.random_client()?;

        let res = client
            .delete_range(req)
            .await
            .context(error::TonicStatusSnafu)?;

        Ok(res.into_inner())
    }

    fn random_client(&self) -> Result<StoreClient<Channel>> {
        let len = self.peers.len();
        let peer = lb::random_get(len, |i| Some(&self.peers[i])).context(
            error::IllegalGrpcClientStateSnafu {
                err_msg: "Empty peers, store client may not start yet",
            },
        )?;

        self.make_client(peer)
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<StoreClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(StoreClient::new(channel))
    }

    #[inline]
    fn is_started(&self) -> bool {
        !self.peers.is_empty()
    }
}
