mod heartbeat;
mod load_balance;
mod router;
mod store;

use api::v1::meta::CreateRequest;
use api::v1::meta::CreateResponse;
use api::v1::meta::DeleteRangeRequest;
use api::v1::meta::DeleteRangeResponse;
use api::v1::meta::PutRequest;
use api::v1::meta::PutResponse;
use api::v1::meta::RangeRequest;
use api::v1::meta::RangeResponse;
use api::v1::meta::RouteRequest;
use api::v1::meta::RouteResponse;
use common_grpc::channel_manager::ChannelManager;
use heartbeat::Client as HeartbeatClient;
use router::Client as RouterClient;
use store::Client as StoreClient;

use crate::error::Result;

#[derive(Clone, Debug)]
pub struct MetaClient {
    heartbeat_client: HeartbeatClient,
    router_client: RouterClient,
    store_client: StoreClient,
}

impl MetaClient {
    pub fn new(channel_manager: ChannelManager) -> Self {
        let heartbeat_client = HeartbeatClient::new(channel_manager.clone());
        let router_client = RouterClient::new(channel_manager.clone());
        let store_client = StoreClient::new(channel_manager);
        Self {
            heartbeat_client,
            router_client,
            store_client,
        }
    }

    pub async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]> + Clone,
    {
        self.heartbeat_client.start(urls.clone()).await?;
        self.router_client.start(urls.clone()).await?;
        self.store_client.start(urls).await?;

        self.heartbeat_client.ask_leader().await?;

        Ok(())
    }

    pub async fn refresh_members(&mut self) {
        todo!()
    }

    pub async fn create_route(&self, req: CreateRequest) -> Result<CreateResponse> {
        self.router_client.create(req).await
    }

    /// Fetch routing information for tables. The smallest unit is the complete
    /// routing information(all regions) of a table.
    ///
    /// table_1
    ///    table_name
    ///    table_schema
    ///    regions
    ///      region_1
    ///        mutate_endpoint
    ///        select_endpoint_1, select_endpoint_2
    ///      region_2
    ///        mutate_endpoint
    ///        select_endpoint_1, select_endpoint_2, select_endpoint_3
    ///      region_xxx
    /// table_2
    ///    ...
    ///
    pub async fn route(&self, req: RouteRequest) -> Result<RouteResponse> {
        self.router_client.route(req).await
    }

    /// Range gets the keys in the range from the key-value store.
    pub async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        self.store_client.range(req).await
    }

    /// Put puts the given key into the key-value store.
    pub async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        self.store_client.put(req).await
    }

    /// DeleteRange deletes the given range from the key-value store.
    pub async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        self.store_client.delete_range(req).await
    }
}
