mod heartbeat;
mod route;

use api::v1::meta::{CreateRequest, CreateResponse, RouteRequest, RouteResponse};
use common_grpc::channel_manager::ChannelManager;
use heartbeat::Client as HeartbeatClient;
use route::Client as RouteClient;

use crate::error::Result;

#[derive(Clone, Debug)]
pub struct MetaClient {
    heartbeat_client: HeartbeatClient,
    route_client: RouteClient,
}

impl MetaClient {
    pub fn new(channel_manager: ChannelManager) -> Self {
        let heartbeat_client = HeartbeatClient::new(channel_manager.clone());
        let route_client = RouteClient::new(channel_manager);
        Self {
            heartbeat_client,
            route_client,
        }
    }

    pub async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]> + Clone,
    {
        self.heartbeat_client.start(urls.clone()).await?;
        self.route_client.start(urls).await?;
        Ok(())
    }

    pub async fn create_route(&self, req: CreateRequest) -> Result<CreateResponse> {
        self.route_client.create(req).await
    }

    pub async fn route(&self, req: RouteRequest) -> Result<RouteResponse> {
        self.route_client.route(req).await
    }
}
