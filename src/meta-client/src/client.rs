// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod ask_leader;
mod heartbeat;
mod load_balance;
mod lock;
mod procedure;

mod cluster;
mod store;
mod util;

use api::v1::meta::Role;
use cluster::Client as ClusterClient;
use common_error::ext::BoxedError;
use common_grpc::channel_manager::{ChannelConfig, ChannelManager};
use common_meta::cluster::{
    ClusterInfo, MetasrvStatus, NodeInfo, NodeInfoKey, NodeStatus, Role as ClusterRole,
};
use common_meta::ddl::{ExecutorContext, ProcedureExecutor};
use common_meta::error::{self as meta_error, Result as MetaResult};
use common_meta::rpc::ddl::{SubmitDdlTaskRequest, SubmitDdlTaskResponse};
use common_meta::rpc::lock::{LockRequest, LockResponse, UnlockRequest};
use common_meta::rpc::procedure::{
    MigrateRegionRequest, MigrateRegionResponse, ProcedureStateResponse,
};
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use common_meta::ClusterId;
use common_telemetry::info;
use heartbeat::Client as HeartbeatClient;
use lock::Client as LockClient;
use procedure::Client as ProcedureClient;
use snafu::{OptionExt, ResultExt};
use store::Client as StoreClient;

pub use self::heartbeat::{HeartbeatSender, HeartbeatStream};
use crate::error::{
    ConvertMetaRequestSnafu, ConvertMetaResponseSnafu, Error, NotStartedSnafu, Result,
};

pub type Id = (u64, u64);

const DEFAULT_ASK_LEADER_MAX_RETRY: usize = 3;
const DEFAULT_SUBMIT_DDL_MAX_RETRY: usize = 3;
const DEFAULT_CLUSTER_CLIENT_MAX_RETRY: usize = 3;

#[derive(Clone, Debug, Default)]
pub struct MetaClientBuilder {
    id: Id,
    role: Role,
    enable_heartbeat: bool,
    enable_store: bool,
    enable_lock: bool,
    enable_procedure: bool,
    enable_access_cluster_info: bool,
    channel_manager: Option<ChannelManager>,
    ddl_channel_manager: Option<ChannelManager>,
    heartbeat_channel_manager: Option<ChannelManager>,
}

impl MetaClientBuilder {
    pub fn new(cluster_id: ClusterId, member_id: u64, role: Role) -> Self {
        Self {
            id: (cluster_id, member_id),
            role,
            ..Default::default()
        }
    }

    /// Returns the role of Frontend's default options.
    pub fn frontend_default_options(cluster_id: ClusterId) -> Self {
        // Frontend does not need a member id.
        Self::new(cluster_id, 0, Role::Frontend)
            .enable_store()
            .enable_heartbeat()
            .enable_procedure()
            .enable_access_cluster_info()
    }

    /// Returns the role of Datanode's default options.
    pub fn datanode_default_options(cluster_id: ClusterId, member_id: u64) -> Self {
        Self::new(cluster_id, member_id, Role::Datanode)
            .enable_store()
            .enable_heartbeat()
    }

    /// Returns the role of Flownode's default options.
    pub fn flownode_default_options(cluster_id: ClusterId, member_id: u64) -> Self {
        Self::new(cluster_id, member_id, Role::Flownode)
            .enable_store()
            .enable_heartbeat()
            .enable_procedure()
    }

    pub fn enable_heartbeat(self) -> Self {
        Self {
            enable_heartbeat: true,
            ..self
        }
    }

    pub fn enable_store(self) -> Self {
        Self {
            enable_store: true,
            ..self
        }
    }

    pub fn enable_lock(self) -> Self {
        Self {
            enable_lock: true,
            ..self
        }
    }

    pub fn enable_procedure(self) -> Self {
        Self {
            enable_procedure: true,
            ..self
        }
    }

    pub fn enable_access_cluster_info(self) -> Self {
        Self {
            enable_access_cluster_info: true,
            ..self
        }
    }

    pub fn channel_manager(self, channel_manager: ChannelManager) -> Self {
        Self {
            channel_manager: Some(channel_manager),
            ..self
        }
    }

    pub fn ddl_channel_manager(self, channel_manager: ChannelManager) -> Self {
        Self {
            ddl_channel_manager: Some(channel_manager),
            ..self
        }
    }

    pub fn heartbeat_channel_manager(self, channel_manager: ChannelManager) -> Self {
        Self {
            heartbeat_channel_manager: Some(channel_manager),
            ..self
        }
    }

    pub fn build(self) -> MetaClient {
        let mut client = if let Some(mgr) = self.channel_manager {
            MetaClient::with_channel_manager(self.id, mgr)
        } else {
            MetaClient::new(self.id)
        };

        let mgr = client.channel_manager.clone();

        if self.enable_heartbeat {
            let mgr = self.heartbeat_channel_manager.unwrap_or(mgr.clone());
            client.heartbeat = Some(HeartbeatClient::new(
                self.id,
                self.role,
                mgr,
                DEFAULT_ASK_LEADER_MAX_RETRY,
            ));
        }

        if self.enable_store {
            client.store = Some(StoreClient::new(self.id, self.role, mgr.clone()));
        }

        if self.enable_lock {
            client.lock = Some(LockClient::new(self.id, self.role, mgr.clone()));
        }

        if self.enable_procedure {
            let mgr = self.ddl_channel_manager.unwrap_or(mgr.clone());
            client.procedure = Some(ProcedureClient::new(
                self.id,
                self.role,
                mgr,
                DEFAULT_SUBMIT_DDL_MAX_RETRY,
            ));
        }

        if self.enable_access_cluster_info {
            client.cluster = Some(ClusterClient::new(
                self.id,
                self.role,
                mgr,
                DEFAULT_CLUSTER_CLIENT_MAX_RETRY,
            ))
        }

        client
    }
}

#[derive(Debug, Default)]
pub struct MetaClient {
    id: Id,
    channel_manager: ChannelManager,
    heartbeat: Option<HeartbeatClient>,
    store: Option<StoreClient>,
    lock: Option<LockClient>,
    procedure: Option<ProcedureClient>,
    cluster: Option<ClusterClient>,
}

#[async_trait::async_trait]
impl ProcedureExecutor for MetaClient {
    async fn submit_ddl_task(
        &self,
        _ctx: &ExecutorContext,
        request: SubmitDdlTaskRequest,
    ) -> MetaResult<SubmitDdlTaskResponse> {
        self.submit_ddl_task(request)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }

    async fn migrate_region(
        &self,
        _ctx: &ExecutorContext,
        request: MigrateRegionRequest,
    ) -> MetaResult<MigrateRegionResponse> {
        self.migrate_region(request)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }

    async fn query_procedure_state(
        &self,
        _ctx: &ExecutorContext,
        pid: &str,
    ) -> MetaResult<ProcedureStateResponse> {
        self.query_procedure_state(pid)
            .await
            .map_err(BoxedError::new)
            .context(meta_error::ExternalSnafu)
    }
}

#[async_trait::async_trait]
impl ClusterInfo for MetaClient {
    type Error = Error;

    async fn list_nodes(&self, role: Option<ClusterRole>) -> Result<Vec<NodeInfo>> {
        let cluster_client = self.cluster_client()?;

        let (get_metasrv_nodes, nodes_key_prefix) = match role {
            None => (
                true,
                Some(NodeInfoKey::key_prefix_with_cluster_id(self.id.0)),
            ),
            Some(ClusterRole::Metasrv) => (true, None),
            Some(role) => (
                false,
                Some(NodeInfoKey::key_prefix_with_role(self.id.0, role)),
            ),
        };

        let mut nodes = if get_metasrv_nodes {
            let last_activity_ts = -1; // Metasrv does not provide this information.

            let (leader, followers) = cluster_client.get_metasrv_peers().await?;
            followers
                .into_iter()
                .map(|node| NodeInfo {
                    peer: node.peer.map(|p| p.into()).unwrap_or_default(),
                    last_activity_ts,
                    status: NodeStatus::Metasrv(MetasrvStatus { is_leader: false }),
                    version: node.version,
                    git_commit: node.git_commit,
                    start_time_ms: node.start_time_ms,
                })
                .chain(leader.into_iter().map(|node| NodeInfo {
                    peer: node.peer.map(|p| p.into()).unwrap_or_default(),
                    last_activity_ts,
                    status: NodeStatus::Metasrv(MetasrvStatus { is_leader: true }),
                    version: node.version,
                    git_commit: node.git_commit,
                    start_time_ms: node.start_time_ms,
                }))
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };

        if let Some(prefix) = nodes_key_prefix {
            let req = RangeRequest::new().with_prefix(prefix);
            let res = cluster_client.range(req).await?;
            for kv in res.kvs {
                nodes.push(NodeInfo::try_from(kv.value).context(ConvertMetaResponseSnafu)?);
            }
        }

        Ok(nodes)
    }
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
        if let Some(client) = &mut self.store {
            client.start(urls.clone()).await?;
            info!("Store client started");
        }
        if let Some(client) = &mut self.lock {
            client.start(urls.clone()).await?;
            info!("Lock client started");
        }
        if let Some(client) = &mut self.procedure {
            client.start(urls.clone()).await?;
            info!("DDL client started");
        }
        if let Some(client) = &mut self.cluster {
            client.start(urls).await?;
            info!("Cluster client started");
        }

        Ok(())
    }

    /// Ask the leader address of `metasrv`, and the heartbeat component
    /// needs to create a bidirectional streaming to the leader.
    pub async fn ask_leader(&self) -> Result<String> {
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

    /// Range gets the keys in the range from the key-value store.
    pub async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        self.store_client()?
            .range(req.into())
            .await?
            .try_into()
            .context(ConvertMetaResponseSnafu)
    }

    /// Put puts the given key into the key-value store.
    pub async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        self.store_client()?
            .put(req.into())
            .await?
            .try_into()
            .context(ConvertMetaResponseSnafu)
    }

    /// BatchGet atomically get values by the given keys from the key-value store.
    pub async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        self.store_client()?
            .batch_get(req.into())
            .await?
            .try_into()
            .context(ConvertMetaResponseSnafu)
    }

    /// BatchPut atomically puts the given keys into the key-value store.
    pub async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        self.store_client()?
            .batch_put(req.into())
            .await?
            .try_into()
            .context(ConvertMetaResponseSnafu)
    }

    /// BatchDelete atomically deletes the given keys from the key-value store.
    pub async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        self.store_client()?
            .batch_delete(req.into())
            .await?
            .try_into()
            .context(ConvertMetaResponseSnafu)
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
            .context(ConvertMetaResponseSnafu)
    }

    /// DeleteRange deletes the given range from the key-value store.
    pub async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        self.store_client()?
            .delete_range(req.into())
            .await?
            .try_into()
            .context(ConvertMetaResponseSnafu)
    }

    pub async fn lock(&self, req: LockRequest) -> Result<LockResponse> {
        self.lock_client()?.lock(req.into()).await.map(Into::into)
    }

    pub async fn unlock(&self, req: UnlockRequest) -> Result<()> {
        let _ = self.lock_client()?.unlock(req.into()).await?;
        Ok(())
    }

    /// Query the procedure state by its id.
    pub async fn query_procedure_state(&self, pid: &str) -> Result<ProcedureStateResponse> {
        self.procedure_client()?.query_procedure_state(pid).await
    }

    /// Submit a region migration task.
    pub async fn migrate_region(
        &self,
        request: MigrateRegionRequest,
    ) -> Result<MigrateRegionResponse> {
        self.procedure_client()?
            .migrate_region(
                request.region_id,
                request.from_peer,
                request.to_peer,
                request.replay_timeout,
            )
            .await
    }

    /// Submit a DDL task
    pub async fn submit_ddl_task(
        &self,
        req: SubmitDdlTaskRequest,
    ) -> Result<SubmitDdlTaskResponse> {
        let res = self
            .procedure_client()?
            .submit_ddl_task(req.try_into().context(ConvertMetaRequestSnafu)?)
            .await?
            .try_into()
            .context(ConvertMetaResponseSnafu)?;

        Ok(res)
    }

    pub fn heartbeat_client(&self) -> Result<HeartbeatClient> {
        self.heartbeat.clone().context(NotStartedSnafu {
            name: "heartbeat_client",
        })
    }

    pub fn store_client(&self) -> Result<StoreClient> {
        self.store.clone().context(NotStartedSnafu {
            name: "store_client",
        })
    }

    pub fn lock_client(&self) -> Result<LockClient> {
        self.lock.clone().context(NotStartedSnafu {
            name: "lock_client",
        })
    }

    pub fn procedure_client(&self) -> Result<ProcedureClient> {
        self.procedure.clone().context(NotStartedSnafu {
            name: "procedure_client",
        })
    }

    pub fn cluster_client(&self) -> Result<ClusterClient> {
        self.cluster.clone().context(NotStartedSnafu {
            name: "cluster_client",
        })
    }

    pub fn channel_config(&self) -> &ChannelConfig {
        self.channel_manager.config()
    }

    pub fn id(&self) -> Id {
        self.id
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::{HeartbeatRequest, Peer};

    use super::*;
    use crate::{error, mocks};

    const TEST_KEY_PREFIX: &str = "__unit_test__meta__";

    struct TestClient {
        ns: String,
        client: MetaClient,
    }

    impl TestClient {
        async fn new(ns: impl Into<String>) -> Self {
            // can also test with etcd: mocks::mock_client_with_etcdstore("127.0.0.1:2379").await;
            let client = mocks::mock_client_with_memstore().await;
            Self {
                ns: ns.into(),
                client,
            }
        }

        fn key(&self, name: &str) -> Vec<u8> {
            format!("{}-{}-{}", TEST_KEY_PREFIX, self.ns, name).into_bytes()
        }

        async fn gen_data(&self) {
            for i in 0..10 {
                let req = PutRequest::new()
                    .with_key(self.key(&format!("key-{i}")))
                    .with_value(format!("{}-{}", "value", i).into_bytes())
                    .with_prev_kv();
                let res = self.client.put(req).await;
                let _ = res.unwrap();
            }
        }

        async fn clear_data(&self) {
            let req =
                DeleteRangeRequest::new().with_prefix(format!("{}-{}", TEST_KEY_PREFIX, self.ns));
            let res = self.client.delete_range(req).await;
            let _ = res.unwrap();
        }
    }

    async fn new_client(ns: impl Into<String>) -> TestClient {
        let client = TestClient::new(ns).await;
        client.clear_data().await;
        client
    }

    #[tokio::test]
    async fn test_meta_client_builder() {
        let urls = &["127.0.0.1:3001", "127.0.0.1:3002"];

        let mut meta_client = MetaClientBuilder::new(0, 0, Role::Datanode)
            .enable_heartbeat()
            .build();
        let _ = meta_client.heartbeat_client().unwrap();
        assert!(meta_client.store_client().is_err());
        meta_client.start(urls).await.unwrap();

        let mut meta_client = MetaClientBuilder::new(0, 0, Role::Datanode).build();
        assert!(meta_client.heartbeat_client().is_err());
        assert!(meta_client.store_client().is_err());
        meta_client.start(urls).await.unwrap();

        let mut meta_client = MetaClientBuilder::new(0, 0, Role::Datanode)
            .enable_store()
            .build();
        assert!(meta_client.heartbeat_client().is_err());
        let _ = meta_client.store_client().unwrap();
        meta_client.start(urls).await.unwrap();

        let mut meta_client = MetaClientBuilder::new(1, 2, Role::Datanode)
            .enable_heartbeat()
            .enable_store()
            .build();
        assert_eq!(1, meta_client.id().0);
        assert_eq!(2, meta_client.id().1);
        let _ = meta_client.heartbeat_client().unwrap();
        let _ = meta_client.store_client().unwrap();
        meta_client.start(urls).await.unwrap();
    }

    #[tokio::test]
    async fn test_not_start_heartbeat_client() {
        let urls = &["127.0.0.1:3001", "127.0.0.1:3002"];
        let mut meta_client = MetaClientBuilder::new(0, 0, Role::Datanode)
            .enable_store()
            .build();
        meta_client.start(urls).await.unwrap();
        let res = meta_client.ask_leader().await;
        assert!(matches!(res.err(), Some(error::Error::NotStarted { .. })));
    }

    #[tokio::test]
    async fn test_not_start_store_client() {
        let urls = &["127.0.0.1:3001", "127.0.0.1:3002"];
        let mut meta_client = MetaClientBuilder::new(0, 0, Role::Datanode)
            .enable_heartbeat()
            .build();

        meta_client.start(urls).await.unwrap();
        let res = meta_client.put(PutRequest::default()).await;
        assert!(matches!(res.err(), Some(error::Error::NotStarted { .. })));
    }

    #[tokio::test]
    async fn test_ask_leader() {
        let tc = new_client("test_ask_leader").await;
        tc.client.ask_leader().await.unwrap();
    }

    #[tokio::test]
    async fn test_heartbeat() {
        let tc = new_client("test_heartbeat").await;
        let (sender, mut receiver) = tc.client.heartbeat().await.unwrap();
        // send heartbeats
        let _handle = tokio::spawn(async move {
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

        let _handle = tokio::spawn(async move {
            while let Some(res) = receiver.message().await.unwrap() {
                assert_eq!(1000, res.header.unwrap().cluster_id);
            }
        });
    }

    #[tokio::test]
    async fn test_range_get() {
        let tc = new_client("test_range_get").await;
        tc.gen_data().await;

        let key = tc.key("key-0");
        let req = RangeRequest::new().with_key(key.as_slice());
        let res = tc.client.range(req).await;
        let mut kvs = res.unwrap().take_kvs();
        assert_eq!(1, kvs.len());
        let mut kv = kvs.pop().unwrap();
        assert_eq!(key, kv.take_key());
        assert_eq!(b"value-0".to_vec(), kv.take_value());
    }

    #[tokio::test]
    async fn test_range_get_prefix() {
        let tc = new_client("test_range_get_prefix").await;
        tc.gen_data().await;

        let req = RangeRequest::new().with_prefix(tc.key("key-"));
        let res = tc.client.range(req).await;
        let kvs = res.unwrap().take_kvs();
        assert_eq!(10, kvs.len());
        for (i, mut kv) in kvs.into_iter().enumerate() {
            assert_eq!(tc.key(&format!("key-{i}")), kv.take_key());
            assert_eq!(format!("{}-{}", "value", i).into_bytes(), kv.take_value());
        }
    }

    #[tokio::test]
    async fn test_range() {
        let tc = new_client("test_range").await;
        tc.gen_data().await;

        let req = RangeRequest::new().with_range(tc.key("key-5"), tc.key("key-8"));
        let res = tc.client.range(req).await;
        let kvs = res.unwrap().take_kvs();
        assert_eq!(3, kvs.len());
        for (i, mut kv) in kvs.into_iter().enumerate() {
            assert_eq!(tc.key(&format!("key-{}", i + 5)), kv.take_key());
            assert_eq!(
                format!("{}-{}", "value", i + 5).into_bytes(),
                kv.take_value()
            );
        }
    }

    #[tokio::test]
    async fn test_range_keys_only() {
        let tc = new_client("test_range_keys_only").await;
        tc.gen_data().await;

        let req = RangeRequest::new()
            .with_range(tc.key("key-5"), tc.key("key-8"))
            .with_keys_only();
        let res = tc.client.range(req).await;
        let kvs = res.unwrap().take_kvs();
        assert_eq!(3, kvs.len());
        for (i, mut kv) in kvs.into_iter().enumerate() {
            assert_eq!(tc.key(&format!("key-{}", i + 5)), kv.take_key());
            assert!(kv.take_value().is_empty());
        }
    }

    #[tokio::test]
    async fn test_put() {
        let tc = new_client("test_put").await;

        let req = PutRequest::new()
            .with_key(tc.key("key"))
            .with_value(b"value".to_vec());
        let res = tc.client.put(req).await;
        assert!(res.unwrap().prev_kv.is_none());
    }

    #[tokio::test]
    async fn test_put_with_prev_kv() {
        let tc = new_client("test_put_with_prev_kv").await;

        let key = tc.key("key");
        let req = PutRequest::new()
            .with_key(key.as_slice())
            .with_value(b"value".to_vec())
            .with_prev_kv();
        let res = tc.client.put(req).await;
        assert!(res.unwrap().prev_kv.is_none());

        let req = PutRequest::new()
            .with_key(key.as_slice())
            .with_value(b"value1".to_vec())
            .with_prev_kv();
        let res = tc.client.put(req).await;
        let mut kv = res.unwrap().prev_kv.unwrap();
        assert_eq!(key, kv.take_key());
        assert_eq!(b"value".to_vec(), kv.take_value());
    }

    #[tokio::test]
    async fn test_batch_put() {
        let tc = new_client("test_batch_put").await;

        let mut req = BatchPutRequest::new();
        for i in 0..275 {
            req = req.add_kv(
                tc.key(&format!("key-{}", i)),
                format!("value-{}", i).into_bytes(),
            );
        }

        let res = tc.client.batch_put(req).await;
        assert_eq!(0, res.unwrap().take_prev_kvs().len());

        let req = RangeRequest::new().with_prefix(tc.key("key-"));
        let res = tc.client.range(req).await;
        let kvs = res.unwrap().take_kvs();
        assert_eq!(275, kvs.len());
    }

    #[tokio::test]
    async fn test_batch_get() {
        let tc = new_client("test_batch_get").await;
        tc.gen_data().await;

        let mut req = BatchGetRequest::default();
        for i in 0..256 {
            req = req.add_key(tc.key(&format!("key-{}", i)));
        }
        let res = tc.client.batch_get(req).await.unwrap();
        assert_eq!(10, res.kvs.len());

        let req = BatchGetRequest::default()
            .add_key(tc.key("key-1"))
            .add_key(tc.key("key-999"));
        let res = tc.client.batch_get(req).await.unwrap();
        assert_eq!(1, res.kvs.len());
    }

    #[tokio::test]
    async fn test_batch_put_with_prev_kv() {
        let tc = new_client("test_batch_put_with_prev_kv").await;

        let key = tc.key("key");
        let key2 = tc.key("key2");
        let req = BatchPutRequest::new().add_kv(key.as_slice(), b"value".to_vec());
        let res = tc.client.batch_put(req).await;
        assert_eq!(0, res.unwrap().take_prev_kvs().len());

        let req = BatchPutRequest::new()
            .add_kv(key.as_slice(), b"value-".to_vec())
            .add_kv(key2.as_slice(), b"value2-".to_vec())
            .with_prev_kv();
        let res = tc.client.batch_put(req).await;
        let mut kvs = res.unwrap().take_prev_kvs();
        assert_eq!(1, kvs.len());
        let mut kv = kvs.pop().unwrap();
        assert_eq!(key, kv.take_key());
        assert_eq!(b"value".to_vec(), kv.take_value());
    }

    #[tokio::test]
    async fn test_compare_and_put() {
        let tc = new_client("test_compare_and_put").await;

        let key = tc.key("key");
        let req = CompareAndPutRequest::new()
            .with_key(key.as_slice())
            .with_expect(b"expect".to_vec())
            .with_value(b"value".to_vec());
        let res = tc.client.compare_and_put(req).await;
        assert!(!res.unwrap().is_success());

        // create if absent
        let req = CompareAndPutRequest::new()
            .with_key(key.as_slice())
            .with_value(b"value".to_vec());
        let res = tc.client.compare_and_put(req).await;
        let mut res = res.unwrap();
        assert!(res.is_success());
        assert!(res.take_prev_kv().is_none());

        // compare and put fail
        let req = CompareAndPutRequest::new()
            .with_key(key.as_slice())
            .with_expect(b"not_eq".to_vec())
            .with_value(b"value2".to_vec());
        let res = tc.client.compare_and_put(req).await;
        let mut res = res.unwrap();
        assert!(!res.is_success());
        assert_eq!(b"value".to_vec(), res.take_prev_kv().unwrap().take_value());

        // compare and put success
        let req = CompareAndPutRequest::new()
            .with_key(key.as_slice())
            .with_expect(b"value".to_vec())
            .with_value(b"value2".to_vec());
        let res = tc.client.compare_and_put(req).await;
        let mut res = res.unwrap();
        assert!(res.is_success());

        // If compare-and-put is success, previous value doesn't need to be returned.
        assert!(res.take_prev_kv().is_none());
    }

    #[tokio::test]
    async fn test_delete_with_key() {
        let tc = new_client("test_delete_with_key").await;
        tc.gen_data().await;

        let req = DeleteRangeRequest::new()
            .with_key(tc.key("key-0"))
            .with_prev_kv();
        let res = tc.client.delete_range(req).await;
        let mut res = res.unwrap();
        assert_eq!(1, res.deleted());
        let mut kvs = res.take_prev_kvs();
        assert_eq!(1, kvs.len());
        let mut kv = kvs.pop().unwrap();
        assert_eq!(b"value-0".to_vec(), kv.take_value());
    }

    #[tokio::test]
    async fn test_delete_with_prefix() {
        let tc = new_client("test_delete_with_prefix").await;
        tc.gen_data().await;

        let req = DeleteRangeRequest::new()
            .with_prefix(tc.key("key-"))
            .with_prev_kv();
        let res = tc.client.delete_range(req).await;
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
        let tc = new_client("test_delete_with_range").await;
        tc.gen_data().await;

        let req = DeleteRangeRequest::new()
            .with_range(tc.key("key-2"), tc.key("key-7"))
            .with_prev_kv();
        let res = tc.client.delete_range(req).await;
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
