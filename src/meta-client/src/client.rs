mod heartbeat;
mod load_balance;
mod router;
mod store;

use common_grpc::channel_manager::ChannelConfig;
use common_grpc::channel_manager::ChannelManager;
use common_telemetry::info;
use heartbeat::Client as HeartbeatClient;
use router::Client as RouterClient;
use snafu::OptionExt;
use store::Client as StoreClient;

pub use self::heartbeat::HeartbeatSender;
pub use self::heartbeat::HeartbeatStream;
use crate::error;
use crate::error::Result;
use crate::rpc::BatchPutRequest;
use crate::rpc::BatchPutResponse;
use crate::rpc::CompareAndPutRequest;
use crate::rpc::CompareAndPutResponse;
use crate::rpc::CreateRequest;
use crate::rpc::DeleteRangeRequest;
use crate::rpc::DeleteRangeResponse;
use crate::rpc::PutRequest;
use crate::rpc::PutResponse;
use crate::rpc::RangeRequest;
use crate::rpc::RangeResponse;
use crate::rpc::RouteRequest;
use crate::rpc::RouteResponse;

pub type Id = (u64, u64);

#[derive(Clone, Debug, Default)]
pub struct MetaClientBuilder {
    id: Id,
    enable_heartbeat: bool,
    enable_router: bool,
    enable_store: bool,
    channel_manager: Option<ChannelManager>,
}

impl MetaClientBuilder {
    pub fn new(cluster_id: u64, member_id: u64) -> Self {
        Self {
            id: (cluster_id, member_id),
            ..Default::default()
        }
    }

    pub fn enable_heartbeat(self) -> Self {
        Self {
            enable_heartbeat: true,
            ..self
        }
    }

    pub fn enable_router(self) -> Self {
        Self {
            enable_router: true,
            ..self
        }
    }

    pub fn enable_store(self) -> Self {
        Self {
            enable_store: true,
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
        let mut client = if let Some(mgr) = self.channel_manager {
            MetaClient::with_channel_manager(self.id, mgr)
        } else {
            MetaClient::new(self.id)
        };

        if let (false, false, false) =
            (self.enable_heartbeat, self.enable_router, self.enable_store)
        {
            panic!("At least one client needs to be enabled.")
        }

        let mgr = client.channel_manager.clone();

        if self.enable_heartbeat {
            client.heartbeat = Some(HeartbeatClient::new(self.id, mgr.clone()));
        }
        if self.enable_router {
            client.router = Some(RouterClient::new(self.id, mgr.clone()));
        }
        if self.enable_store {
            client.store = Some(StoreClient::new(self.id, mgr));
        }

        client
    }
}

#[derive(Clone, Debug, Default)]
pub struct MetaClient {
    id: Id,
    channel_manager: ChannelManager,
    heartbeat: Option<HeartbeatClient>,
    router: Option<RouterClient>,
    store: Option<StoreClient>,
}

impl MetaClient {
    pub fn new(id: Id) -> Self {
        Self {
            id,
            ..Default::default()
        }
    }

    pub fn with_channel_manager(id: Id, channel_manager: ChannelManager) -> Self {
        Self {
            id,
            channel_manager,
            ..Default::default()
        }
    }

    pub async fn start<U, A>(&mut self, urls: A) -> Result<()>
    where
        U: AsRef<str>,
        A: AsRef<[U]> + Clone,
    {
        info!("MetaClient channel config: {:?}", self.channel_config());

        if let Some(client) = &mut self.heartbeat {
            client.start(urls.clone()).await?;
            info!("Heartbeat client started");
        }
        if let Some(client) = &mut self.router {
            client.start(urls.clone()).await?;
            info!("Router client started");
        }
        if let Some(client) = &mut self.store {
            client.start(urls).await?;
            info!("Store client started");
        }

        Ok(())
    }

    /// Ask the leader address of `metasrv`, and the heartbeat component
    /// needs to create a bidirectional streaming to the leader.
    pub async fn ask_leader(&self) -> Result<()> {
        self.heartbeat_client()?.ask_leader().await
    }

    /// Returns a heartbeat bidirectional streaming: (sender, recever), the
    /// other end is the leader of `metasrv`.
    ///
    /// The `datanode` needs to use the sender to continuously send heartbeat
    /// packets (some self-state data), and the receiver can receive a response
    /// from "metasrv" (which may contain some scheduling instructions).
    pub async fn heartbeat(&self) -> Result<(HeartbeatSender, HeartbeatStream)> {
        self.heartbeat_client()?.heartbeat().await
    }

    /// Provides routing information for distributed create table requests.
    ///
    /// When a distributed create table request is received, this method returns
    /// a list of `datanode` addresses that are generated based on the partition
    /// information contained in the request and using some intelligent policies,
    /// such as load-based.
    pub async fn create_route(&self, req: CreateRequest) -> Result<RouteResponse> {
        self.router_client()?.create(req.into()).await?.try_into()
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
    ///        leader_peer
    ///        follower_peer_1, follower_peer_2
    ///      region_2
    ///        leader_peer
    ///        follower_peer_1, follower_peer_2, follower_peer_3
    ///      region_xxx
    /// table_2
    ///    ...
    /// ```
    ///
    pub async fn route(&self, req: RouteRequest) -> Result<RouteResponse> {
        self.router_client()?.route(req.into()).await?.try_into()
    }

    /// Range gets the keys in the range from the key-value store.
    pub async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        self.store_client()?.range(req.into()).await?.try_into()
    }

    /// Put puts the given key into the key-value store.
    pub async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        self.store_client()?.put(req.into()).await?.try_into()
    }

    /// BatchPut atomically puts the given keys into the key-value store.
    pub async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        self.store_client()?.batch_put(req.into()).await?.try_into()
    }

    /// CompareAndPut atomically puts the value to the given updated
    /// value if the current value == the expected value.
    pub async fn compare_and_put(
        &self,
        req: CompareAndPutRequest,
    ) -> Result<CompareAndPutResponse> {
        self.store_client()?
            .compare_and_put(req.into())
            .await?
            .try_into()
    }

    /// DeleteRange deletes the given range from the key-value store.
    pub async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        self.store_client()?
            .delete_range(req.into())
            .await?
            .try_into()
    }

    #[inline]
    pub fn heartbeat_client(&self) -> Result<HeartbeatClient> {
        self.heartbeat.clone().context(error::NotStartedSnafu {
            name: "heartbeat_client",
        })
    }

    #[inline]
    pub fn router_client(&self) -> Result<RouterClient> {
        self.router.clone().context(error::NotStartedSnafu {
            name: "store_client",
        })
    }

    #[inline]
    pub fn store_client(&self) -> Result<StoreClient> {
        self.store.clone().context(error::NotStartedSnafu {
            name: "store_client",
        })
    }

    #[inline]
    pub fn channel_config(&self) -> &ChannelConfig {
        self.channel_manager.config()
    }

    #[inline]
    pub fn id(&self) -> Id {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::meta::HeartbeatRequest;
    use api::v1::meta::Peer;
    use meta_srv::metasrv::Context;
    use meta_srv::selector::Namespace;
    use meta_srv::selector::Selector;
    use meta_srv::Result as MetaResult;

    use super::*;
    use crate::mocks;
    use crate::rpc::Partition;
    use crate::rpc::TableName;

    #[tokio::test]
    async fn test_meta_client_builder() {
        let urls = &["127.0.0.1:3001", "127.0.0.1:3002"];

        let mut meta_client = MetaClientBuilder::new(0, 0).enable_heartbeat().build();
        assert!(meta_client.heartbeat_client().is_ok());
        assert!(meta_client.router_client().is_err());
        assert!(meta_client.store_client().is_err());
        meta_client.start(urls).await.unwrap();
        assert!(meta_client.heartbeat_client().unwrap().is_started().await);

        let mut meta_client = MetaClientBuilder::new(0, 0).enable_router().build();
        assert!(meta_client.heartbeat_client().is_err());
        assert!(meta_client.router_client().is_ok());
        assert!(meta_client.store_client().is_err());
        meta_client.start(urls).await.unwrap();
        assert!(meta_client.router_client().unwrap().is_started().await);

        let mut meta_client = MetaClientBuilder::new(0, 0).enable_store().build();
        assert!(meta_client.heartbeat_client().is_err());
        assert!(meta_client.router_client().is_err());
        assert!(meta_client.store_client().is_ok());
        meta_client.start(urls).await.unwrap();
        assert!(meta_client.store_client().unwrap().is_started().await);

        let mut meta_client = MetaClientBuilder::new(1, 2)
            .enable_heartbeat()
            .enable_router()
            .enable_store()
            .build();
        assert_eq!(1, meta_client.id().0);
        assert_eq!(2, meta_client.id().1);
        assert!(meta_client.heartbeat_client().is_ok());
        assert!(meta_client.router_client().is_ok());
        assert!(meta_client.store_client().is_ok());
        meta_client.start(urls).await.unwrap();
        assert!(meta_client.heartbeat_client().unwrap().is_started().await);
        assert!(meta_client.router_client().unwrap().is_started().await);
        assert!(meta_client.store_client().unwrap().is_started().await);
    }

    #[tokio::test]
    async fn test_not_start_heartbeat_client() {
        let urls = &["127.0.0.1:3001", "127.0.0.1:3002"];
        let mut meta_client = MetaClientBuilder::new(0, 0)
            .enable_router()
            .enable_store()
            .build();
        meta_client.start(urls).await.unwrap();
        let res = meta_client.ask_leader().await;
        assert!(matches!(res.err(), Some(error::Error::NotStarted { .. })));
    }

    #[tokio::test]
    async fn test_not_start_router_client() {
        let urls = &["127.0.0.1:3001", "127.0.0.1:3002"];
        let mut meta_client = MetaClientBuilder::new(0, 0)
            .enable_heartbeat()
            .enable_store()
            .build();
        meta_client.start(urls).await.unwrap();
        let req = CreateRequest::new(TableName::new("c", "s", "t"));
        let res = meta_client.create_route(req).await;
        assert!(matches!(res.err(), Some(error::Error::NotStarted { .. })));
    }

    #[tokio::test]
    async fn test_not_start_store_client() {
        let urls = &["127.0.0.1:3001", "127.0.0.1:3002"];
        let mut meta_client = MetaClientBuilder::new(0, 0)
            .enable_heartbeat()
            .enable_router()
            .build();

        meta_client.start(urls).await.unwrap();
        let res = meta_client.put(PutRequest::default()).await;
        assert!(matches!(res.err(), Some(error::Error::NotStarted { .. })));
    }

    #[should_panic]
    #[test]
    fn test_failed_when_start_nothing() {
        let _ = MetaClientBuilder::new(0, 0).build();
    }

    #[tokio::test]
    async fn test_ask_leader() {
        let client = mocks::mock_client_with_memstore().await;
        let res = client.ask_leader().await;
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let client = mocks::mock_client_with_memstore().await;
        let (sender, mut receiver) = client.heartbeat().await.unwrap();
        // send heartbeats
        tokio::spawn(async move {
            for _ in 0..5 {
                let req = HeartbeatRequest {
                    peer: Some(Peer {
                        id: 1,
                        addr: "meta_client_peer".to_string(),
                    }),
                    ..Default::default()
                };
                sender.send(req).await.unwrap();
            }
        });

        tokio::spawn(async move {
            while let Some(res) = receiver.message().await.unwrap() {
                assert_eq!(1000, res.header.unwrap().cluster_id);
            }
        });
    }

    struct MockSelector;

    #[async_trait::async_trait]
    impl Selector for MockSelector {
        type Context = Context;
        type Output = Vec<Peer>;

        async fn select(&self, _ns: Namespace, _ctx: &Self::Context) -> MetaResult<Self::Output> {
            Ok(vec![
                Peer {
                    id: 0,
                    addr: "peer0".to_string(),
                },
                Peer {
                    id: 1,
                    addr: "peer1".to_string(),
                },
                Peer {
                    id: 2,
                    addr: "peer2".to_string(),
                },
            ])
        }
    }

    #[tokio::test]
    async fn test_route() {
        let selector = Arc::new(MockSelector {});
        let client = mocks::mock_client_with_memorystore_and_selector(selector).await;

        let p1 = Partition {
            column_list: vec![b"col_1".to_vec(), b"col_2".to_vec()],
            value_list: vec![b"k1".to_vec(), b"k2".to_vec()],
        };
        let p2 = Partition {
            column_list: vec![b"col_1".to_vec(), b"col_2".to_vec()],
            value_list: vec![b"Max1".to_vec(), b"Max2".to_vec()],
        };
        let table_name = TableName::new("test_catalog", "test_schema", "test_table");
        let req = CreateRequest::new(table_name.clone())
            .add_partition(p1)
            .add_partition(p2);

        let res = client.create_route(req).await.unwrap();
        assert_eq!(1, res.table_routes.len());

        let req = RouteRequest::new().add_table_name(table_name);
        let res = client.route(req).await.unwrap();
        // empty table_routes since no TableGlobalValue is stored by datanode
        assert!(res.table_routes.is_empty());
    }

    async fn gen_data(client: &MetaClient) {
        for i in 0..10 {
            let req = PutRequest::new()
                .with_key(format!("{}-{}", "key", i).into_bytes())
                .with_value(format!("{}-{}", "value", i).into_bytes())
                .with_prev_kv();
            let res = client.put(req).await;
            assert!(res.is_ok());
        }
    }

    #[tokio::test]
    async fn test_range_get() {
        let client = mocks::mock_client_with_memstore().await;

        gen_data(&client).await;

        let req = RangeRequest::new().with_key(b"key-0".to_vec());
        let res = client.range(req).await;
        let mut kvs = res.unwrap().take_kvs();
        assert_eq!(1, kvs.len());
        let mut kv = kvs.pop().unwrap();
        assert_eq!(b"key-0".to_vec(), kv.take_key());
        assert_eq!(b"value-0".to_vec(), kv.take_value());
    }

    #[tokio::test]
    async fn test_range_get_prefix() {
        let client = mocks::mock_client_with_memstore().await;

        gen_data(&client).await;

        let req = RangeRequest::new().with_prefix(b"key-".to_vec());
        let res = client.range(req).await;
        let kvs = res.unwrap().take_kvs();
        assert_eq!(10, kvs.len());
        for (i, mut kv) in kvs.into_iter().enumerate() {
            assert_eq!(format!("{}-{}", "key", i).into_bytes(), kv.take_key());
            assert_eq!(format!("{}-{}", "value", i).into_bytes(), kv.take_value());
        }
    }

    #[tokio::test]
    async fn test_range() {
        let client = mocks::mock_client_with_memstore().await;

        gen_data(&client).await;

        let req = RangeRequest::new().with_range(b"key-5".to_vec(), b"key-8".to_vec());
        let res = client.range(req).await;
        let kvs = res.unwrap().take_kvs();
        assert_eq!(3, kvs.len());
        for (i, mut kv) in kvs.into_iter().enumerate() {
            assert_eq!(format!("{}-{}", "key", i + 5).into_bytes(), kv.take_key());
            assert_eq!(
                format!("{}-{}", "value", i + 5).into_bytes(),
                kv.take_value()
            );
        }
    }

    #[tokio::test]
    async fn test_range_keys_only() {
        let client = mocks::mock_client_with_memstore().await;

        gen_data(&client).await;

        let req = RangeRequest::new()
            .with_range(b"key-5".to_vec(), b"key-8".to_vec())
            .with_keys_only();
        let res = client.range(req).await;
        let kvs = res.unwrap().take_kvs();
        assert_eq!(3, kvs.len());
        for (i, mut kv) in kvs.into_iter().enumerate() {
            assert_eq!(format!("{}-{}", "key", i + 5).into_bytes(), kv.take_key());
            assert!(kv.take_value().is_empty());
        }
    }

    #[tokio::test]
    async fn test_put() {
        let client = mocks::mock_client_with_memstore().await;
        let req = PutRequest::new()
            .with_key(b"key".to_vec())
            .with_value(b"value".to_vec());
        let res = client.put(req).await;
        assert!(res.unwrap().take_prev_kv().is_none());
    }

    #[tokio::test]
    async fn test_put_with_prev_kv() {
        let client = mocks::mock_client_with_memstore().await;
        let req = PutRequest::new()
            .with_key(b"key".to_vec())
            .with_value(b"value".to_vec())
            .with_prev_kv();
        let res = client.put(req).await;
        assert!(res.unwrap().take_prev_kv().is_none());

        let req = PutRequest::new()
            .with_key(b"key".to_vec())
            .with_value(b"value1".to_vec())
            .with_prev_kv();
        let res = client.put(req).await;
        let mut kv = res.unwrap().take_prev_kv().unwrap();
        assert_eq!(b"key".to_vec(), kv.take_key());
        assert_eq!(b"value".to_vec(), kv.take_value());
    }

    #[tokio::test]
    async fn test_batch_put() {
        let client = mocks::mock_client_with_memstore().await;
        let req = BatchPutRequest::new()
            .add_kv(b"key".to_vec(), b"value".to_vec())
            .add_kv(b"key2".to_vec(), b"value2".to_vec());
        let res = client.batch_put(req).await;
        assert_eq!(0, res.unwrap().take_prev_kvs().len());

        let req = RangeRequest::new().with_range(b"key".to_vec(), b"key3".to_vec());
        let res = client.range(req).await;
        let kvs = res.unwrap().take_kvs();
        assert_eq!(2, kvs.len());
    }

    #[tokio::test]
    async fn test_batch_put_with_prev_kv() {
        let client = mocks::mock_client_with_memstore().await;
        let req = BatchPutRequest::new().add_kv(b"key".to_vec(), b"value".to_vec());
        let res = client.batch_put(req).await;
        assert_eq!(0, res.unwrap().take_prev_kvs().len());

        let req = BatchPutRequest::new()
            .add_kv(b"key".to_vec(), b"value-".to_vec())
            .add_kv(b"key2".to_vec(), b"value2-".to_vec())
            .with_prev_kv();
        let res = client.batch_put(req).await;
        let mut kvs = res.unwrap().take_prev_kvs();
        assert_eq!(1, kvs.len());
        let mut kv = kvs.pop().unwrap();
        assert_eq!(b"key".to_vec(), kv.take_key());
        assert_eq!(b"value".to_vec(), kv.take_value());
    }

    #[tokio::test]
    async fn test_compare_and_put() {
        let client = mocks::mock_client_with_memstore().await;
        let req = CompareAndPutRequest::new()
            .with_key(b"key".to_vec())
            .with_expect(b"expect".to_vec())
            .with_value(b"value".to_vec());
        let res = client.compare_and_put(req).await;
        assert!(!res.unwrap().is_success());

        // create if absent
        let req = CompareAndPutRequest::new()
            .with_key(b"key".to_vec())
            .with_value(b"value".to_vec());
        let res = client.compare_and_put(req).await;
        let mut res = res.unwrap();
        assert!(res.is_success());
        assert!(res.take_prev_kv().is_none());

        // compare and put fail
        let req = CompareAndPutRequest::new()
            .with_key(b"key".to_vec())
            .with_expect(b"not_eq".to_vec())
            .with_value(b"value2".to_vec());
        let res = client.compare_and_put(req).await;
        let mut res = res.unwrap();
        assert!(!res.is_success());
        assert_eq!(b"value".to_vec(), res.take_prev_kv().unwrap().take_value());

        // compare and put success
        let req = CompareAndPutRequest::new()
            .with_key(b"key".to_vec())
            .with_expect(b"value".to_vec())
            .with_value(b"value2".to_vec());
        let res = client.compare_and_put(req).await;
        let mut res = res.unwrap();
        assert!(res.is_success());
        assert_eq!(b"value".to_vec(), res.take_prev_kv().unwrap().take_value());
    }

    #[tokio::test]
    async fn test_delete_with_key() {
        let client = mocks::mock_client_with_memstore().await;

        gen_data(&client).await;

        let req = DeleteRangeRequest::new()
            .with_key(b"key-0".to_vec())
            .with_prev_kv();
        let res = client.delete_range(req).await;
        let mut res = res.unwrap();
        assert_eq!(1, res.deleted());
        let mut kvs = res.take_prev_kvs();
        assert_eq!(1, kvs.len());
        let mut kv = kvs.pop().unwrap();
        assert_eq!(b"value-0".to_vec(), kv.take_value());
    }

    #[tokio::test]
    async fn test_delete_with_prefix() {
        let client = mocks::mock_client_with_memstore().await;

        gen_data(&client).await;

        let req = DeleteRangeRequest::new()
            .with_prefix(b"key-".to_vec())
            .with_prev_kv();
        let res = client.delete_range(req).await;
        let mut res = res.unwrap();
        assert_eq!(10, res.deleted());
        let kvs = res.take_prev_kvs();
        assert_eq!(10, kvs.len());
        for (i, mut kv) in kvs.into_iter().enumerate() {
            assert_eq!(format!("{}-{}", "value", i).into_bytes(), kv.take_value());
        }
    }

    #[tokio::test]
    async fn test_delete_with_range() {
        let client = mocks::mock_client_with_memstore().await;

        gen_data(&client).await;

        let req = DeleteRangeRequest::new()
            .with_range(b"key-2".to_vec(), b"key-7".to_vec())
            .with_prev_kv();
        let res = client.delete_range(req).await;
        let mut res = res.unwrap();
        assert_eq!(5, res.deleted());
        let kvs = res.take_prev_kvs();
        assert_eq!(5, kvs.len());
        for (i, mut kv) in kvs.into_iter().enumerate() {
            assert_eq!(
                format!("{}-{}", "value", i + 2).into_bytes(),
                kv.take_value()
            );
        }
    }
}
