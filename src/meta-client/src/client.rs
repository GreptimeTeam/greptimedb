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
use common_grpc::channel_manager::Config;
use heartbeat::Client as HeartbeatClient;
use router::Client as RouterClient;
use snafu::OptionExt;
use store::Client as StoreClient;

use crate::error;
use crate::error::Result;

#[derive(Clone, Debug, Default)]
pub struct MetaClientBuilder {
    start_heartbeat_client: bool,
    start_router_client: bool,
    start_store_client: bool,
    channel_manager: Option<ChannelManager>,
}

impl MetaClientBuilder {
    pub fn new() -> Self {
        MetaClientBuilder::default()
    }

    pub fn start_heartbeat_client(self) -> Self {
        Self {
            start_heartbeat_client: true,
            ..self
        }
    }

    pub fn start_router_client(self) -> Self {
        Self {
            start_router_client: true,
            ..self
        }
    }

    pub fn start_store_client(self) -> Self {
        Self {
            start_store_client: true,
            ..self
        }
    }

    pub fn channel_manager(self, channel_manager: ChannelManager) -> Self {
        Self {
            channel_manager: Some(channel_manager),
            ..self
        }
    }

    pub fn build(self) -> MetaClient {
        let mut meta_client = if let Some(mgr) = self.channel_manager {
            MetaClient {
                channel_manager: mgr,
                ..Default::default()
            }
        } else {
            Default::default()
        };

        if self.start_heartbeat_client {
            meta_client.heartbeat_client =
                Some(HeartbeatClient::new(meta_client.channel_manager.clone()));
        }
        if self.start_router_client {
            meta_client.router_client =
                Some(RouterClient::new(meta_client.channel_manager.clone()));
        }
        if self.start_store_client {
            meta_client.store_client = Some(StoreClient::new(meta_client.channel_manager.clone()));
        }

        meta_client
    }
}

#[derive(Clone, Debug, Default)]
pub struct MetaClient {
    channel_manager: ChannelManager,
    heartbeat_client: Option<HeartbeatClient>,
    router_client: Option<RouterClient>,
    store_client: Option<StoreClient>,
}

impl MetaClient {
    pub async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]> + Clone,
    {
        if let Some(heartbeat_client) = &mut self.heartbeat_client {
            heartbeat_client.start(urls.clone()).await?;
        }
        if let Some(router_client) = &mut self.router_client {
            router_client.start(urls.clone()).await?;
        }
        if let Some(store_client) = &mut self.store_client {
            store_client.start(urls).await?;
        }

        Ok(())
    }

    pub async fn ask_leader(&self) -> Result<()> {
        self.heartbeat_client()
            .context(error::NotStartedSnafu {
                name: "heartbeat_client",
            })?
            .ask_leader()
            .await
    }

    pub async fn refresh_members(&mut self) {
        todo!()
    }

    pub async fn create_route(&self, req: CreateRequest) -> Result<CreateResponse> {
        self.router_client()
            .context(error::NotStartedSnafu {
                name: "route_client",
            })?
            .create(req)
            .await
    }

    /// Fetch routing information for tables. The smallest unit is the complete
    /// routing information(all regions) of a table.
    ///
    /// ```text
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
    /// ```
    ///
    pub async fn route(&self, req: RouteRequest) -> Result<RouteResponse> {
        self.router_client()
            .context(error::NotStartedSnafu {
                name: "route_client",
            })?
            .route(req)
            .await
    }

    /// Range gets the keys in the range from the key-value store.
    pub async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        self.store_client()
            .context(error::NotStartedSnafu {
                name: "store_client",
            })?
            .range(req)
            .await
    }

    /// Put puts the given key into the key-value store.
    pub async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        self.store_client()
            .context(error::NotStartedSnafu {
                name: "store_client",
            })?
            .put(req)
            .await
    }

    /// DeleteRange deletes the given range from the key-value store.
    pub async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        self.store_client()
            .context(error::NotStartedSnafu {
                name: "store_client",
            })?
            .delete_range(req)
            .await
    }

    #[inline]
    pub fn heartbeat_client(&self) -> Option<HeartbeatClient> {
        self.heartbeat_client.clone()
    }

    #[inline]
    pub fn router_client(&self) -> Option<RouterClient> {
        self.router_client.clone()
    }

    #[inline]
    pub fn store_client(&self) -> Option<StoreClient> {
        self.store_client.clone()
    }

    pub fn channel_manager_config(&self) -> Option<Config> {
        self.channel_manager.config()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_meta_client_builder() {
        let urls = &["127.0.0.1:3001", "127.0.0.1:3002"];

        let mut meta_client = MetaClientBuilder::new().start_heartbeat_client().build();
        assert!(meta_client.heartbeat_client().is_some());
        assert!(meta_client.router_client().is_none());
        assert!(meta_client.store_client().is_none());
        meta_client.start(urls).await.unwrap();
        assert!(meta_client.heartbeat_client().unwrap().is_started().await);

        let mut meta_client = MetaClientBuilder::new().start_router_client().build();
        assert!(meta_client.heartbeat_client().is_none());
        assert!(meta_client.router_client().is_some());
        assert!(meta_client.store_client().is_none());
        meta_client.start(urls).await.unwrap();
        assert!(meta_client.router_client().unwrap().is_started().await);

        let mut meta_client = MetaClientBuilder::new().start_store_client().build();
        assert!(meta_client.heartbeat_client().is_none());
        assert!(meta_client.router_client().is_none());
        assert!(meta_client.store_client().is_some());
        meta_client.start(urls).await.unwrap();
        assert!(meta_client.store_client().unwrap().is_started().await);

        let mut meta_client = MetaClientBuilder::new()
            .start_heartbeat_client()
            .start_router_client()
            .start_store_client()
            .build();
        assert!(meta_client.heartbeat_client().is_some());
        assert!(meta_client.router_client().is_some());
        assert!(meta_client.store_client().is_some());
        meta_client.start(urls).await.unwrap();
        assert!(meta_client.heartbeat_client().unwrap().is_started().await);
        assert!(meta_client.router_client().unwrap().is_started().await);
        assert!(meta_client.store_client().unwrap().is_started().await);
    }
}
