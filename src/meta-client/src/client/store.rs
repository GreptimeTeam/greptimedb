use std::collections::HashSet;
use std::sync::Arc;

use api::v1::meta::request_header;
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

use super::Id;
use crate::client::load_balance as lb;
use crate::error;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<RwLock<Inner>>,
}

impl Client {
    pub fn new(id: Id, channel_manager: ChannelManager) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            id,
            channel_manager,
            peers: vec![],
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
    id: Id,
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

    async fn range(&self, mut req: RangeRequest) -> Result<RangeResponse> {
        let mut client = self.random_client()?;
        req.header = request_header(self.id);
        let res = client.range(req).await.context(error::TonicStatusSnafu)?;

        Ok(res.into_inner())
    }

    async fn put(&self, mut req: PutRequest) -> Result<PutResponse> {
        let mut client = self.random_client()?;
        req.header = request_header(self.id);
        let res = client.put(req).await.context(error::TonicStatusSnafu)?;

        Ok(res.into_inner())
    }

    async fn delete_range(&self, mut req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let mut client = self.random_client()?;
        req.header = request_header(self.id);
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
    async fn test_range_unavailable() {
        let mut client = Client::new((0, 0), ChannelManager::default());
        client.start(&["unknow_peer"]).await.unwrap();

        let req = RangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        };
        let res = client.range(req).await;

        assert!(res.is_err());

        let err = res.err().unwrap();
        assert!(
            matches!(err, error::Error::TonicStatus { source, .. } if source.code() == tonic::Code::Unavailable)
        );
    }

    #[tokio::test]
    async fn test_put_unavailable() {
        let mut client = Client::new((0, 0), ChannelManager::default());
        client.start(&["unavailable_peer"]).await.unwrap();

        let req = PutRequest {
            key: b"key1".to_vec(),
            value: b"value1".to_vec(),
            prev_kv: true,
            ..Default::default()
        };
        let res = client.put(req).await;

        assert!(res.is_err());

        let err = res.err().unwrap();
        assert!(
            matches!(err, error::Error::TonicStatus { source, .. } if source.code() == tonic::Code::Unavailable)
        );
    }

    #[tokio::test]
    async fn test_delete_range_unavailable() {
        let mut client = Client::new((0, 0), ChannelManager::default());
        client.start(&["unavailable_peer"]).await.unwrap();

        let req = DeleteRangeRequest {
            key: b"key1".to_vec(),
            ..Default::default()
        };
        let res = client.delete_range(req).await;

        assert!(res.is_err());

        let err = res.err().unwrap();
        assert!(
            matches!(err, error::Error::TonicStatus { source, .. } if source.code() == tonic::Code::Unavailable)
        );
    }
}
