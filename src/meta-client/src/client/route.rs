use std::sync::Arc;

use api::v1::meta::route_client::RouteClient;
use api::v1::meta::CreateRequest;
use api::v1::meta::CreateResponse;
use api::v1::meta::RouteRequest;
use api::v1::meta::RouteResponse;
use common_grpc::channel_manager::ChannelManager;
use rand::Rng;
use snafu::ResultExt;
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::error;
use crate::error::Result;

#[derive(Clone, Debug)]
pub struct Client {
    inner: Arc<Mutex<Inner>>,
}

impl Client {
    pub fn new(channel_manager: ChannelManager) -> Self {
        let inner = Inner {
            channel_manager,
            peers: vec![],
        };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]>,
    {
        let mut inner = self.inner.lock().await;
        inner.start(urls).await
    }

    pub async fn create(&self, req: CreateRequest) -> Result<CreateResponse> {
        let inner = self.inner.lock().await;
        inner.create(req).await
    }

    pub async fn route(&self, req: RouteRequest) -> Result<RouteResponse> {
        let inner = self.inner.lock().await;
        inner.route(req).await
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
        if !self.peers.is_empty() {
            return error::IllegalGrpcClientStateSnafu {
                err_msg: "Route client already started",
            }
            .fail();
        }

        for url in urls.as_ref() {
            self.peers.push(url.as_ref().to_string());
        }

        Ok(())
    }

    async fn route(&self, req: RouteRequest) -> Result<RouteResponse> {
        let mut client = self.make_client(self.random_peer())?;

        let res = client.route(req).await.context(error::TonicStatusSnafu)?;

        Ok(res.into_inner())
    }

    async fn create(&self, req: CreateRequest) -> Result<CreateResponse> {
        let mut client = self.make_client(self.random_peer())?;

        let res = client.create(req).await.context(error::TonicStatusSnafu)?;

        Ok(res.into_inner())
    }

    fn random_peer(&self) -> &str {
        let mut rng = rand::thread_rng();
        let i = rng.gen_range(0..self.peers.len());
        &self.peers[i]
    }

    fn make_client(&self, addr: impl AsRef<str>) -> Result<RouteClient<Channel>> {
        let channel = self
            .channel_manager
            .get(addr)
            .context(error::CreateChannelSnafu)?;

        Ok(RouteClient::new(channel))
    }
}
