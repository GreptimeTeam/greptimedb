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

use std::path::PathBuf;
use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::flow::{DirtyWindowRequests, FlowRequest, FlowResponse};
use api::v1::region::{InsertRequests, RegionRequest};
pub use common_base::AffectedRows;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use common_wal::config::kafka::MetasrvKafkaConfig;
use common_wal::config::kafka::common::{KafkaConnectionConfig, KafkaTopicConfig};

use crate::cache_invalidator::DummyCacheInvalidator;
use crate::ddl::flow_meta::FlowMetadataAllocator;
use crate::ddl::table_meta::TableMetadataAllocator;
use crate::ddl::{DdlContext, NoopRegionFailureDetectorControl};
use crate::error::Result;
use crate::flow_rpc::{FlowRpc, FlowRpcRef};
use crate::key::TableMetadataManager;
use crate::key::flow::FlowMetadataManager;
use crate::kv_backend::KvBackendRef;
use crate::kv_backend::memory::MemoryKvBackend;
use crate::peer::{Peer, PeerResolver};
use crate::region_keeper::MemoryRegionKeeper;
use crate::region_registry::LeaderRegionRegistry;
use crate::region_rpc::{RegionRpc, RegionRpcRef};
use crate::sequence::SequenceBuilder;
use crate::wal_provider::topic_pool::KafkaTopicPool;
use crate::wal_provider::{WalProvider, build_kafka_topic_creator};
use crate::{DatanodeId, FlownodeId};

#[async_trait::async_trait]
pub trait MockDatanodeHandler: Sync + Send + Clone {
    async fn handle(&self, peer: &Peer, request: RegionRequest) -> Result<RegionResponse>;

    async fn handle_query(
        &self,
        peer: &Peer,
        request: QueryRequest,
    ) -> Result<SendableRecordBatchStream>;
}

#[async_trait::async_trait]
pub trait MockFlownodeHandler: Sync + Send + Clone {
    async fn handle(&self, _peer: &Peer, _request: FlowRequest) -> Result<FlowResponse> {
        unimplemented!()
    }

    async fn handle_inserts(
        &self,
        _peer: &Peer,
        _requests: InsertRequests,
    ) -> Result<FlowResponse> {
        unimplemented!()
    }

    async fn handle_mark_window_dirty(
        &self,
        _peer: &Peer,
        _req: DirtyWindowRequests,
    ) -> Result<FlowResponse> {
        unimplemented!()
    }
}

/// A mock struct implements [`RegionRpc`] only for datanode-related methods.
#[derive(Clone)]
pub struct MockDatanodeRpc<T> {
    handler: T,
}

impl<T> MockDatanodeRpc<T> {
    pub fn new(handler: T) -> Self {
        Self { handler }
    }
}

/// A mock struct implements [`FlowRpc`] only for flownode-related methods.
#[derive(Clone)]
pub struct MockFlownodeRpc<T> {
    handler: T,
}

impl<T> MockFlownodeRpc<T> {
    pub fn new(handler: T) -> Self {
        Self { handler }
    }
}

/// Backward-compatible aliases for tests that still use the old names.
pub type MockDatanodeManager<T> = MockDatanodeRpc<T>;
pub type MockFlownodeManager<T> = MockFlownodeRpc<T>;

#[async_trait::async_trait]
impl<T: MockDatanodeHandler> RegionRpc for MockDatanodeRpc<T> {
    async fn handle_region(&self, peer: &Peer, request: RegionRequest) -> Result<RegionResponse> {
        self.handler.handle(peer, request).await
    }

    async fn handle_query(
        &self,
        peer: &Peer,
        request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        self.handler.handle_query(peer, request).await
    }
}

#[async_trait::async_trait]
impl<T: MockFlownodeHandler> FlowRpc for MockFlownodeRpc<T> {
    async fn handle_flow(&self, peer: &Peer, request: FlowRequest) -> Result<FlowResponse> {
        self.handler.handle(peer, request).await
    }

    async fn handle_flow_inserts(
        &self,
        peer: &Peer,
        request: InsertRequests,
    ) -> Result<FlowResponse> {
        self.handler.handle_inserts(peer, request).await
    }

    async fn handle_mark_window_dirty(
        &self,
        peer: &Peer,
        req: DirtyWindowRequests,
    ) -> Result<FlowResponse> {
        self.handler.handle_mark_window_dirty(peer, req).await
    }
}

#[derive(Debug, Default)]
struct UnimplementedFlowRpc;

#[async_trait::async_trait]
impl FlowRpc for UnimplementedFlowRpc {
    async fn handle_flow(&self, _peer: &Peer, _request: FlowRequest) -> Result<FlowResponse> {
        unimplemented!()
    }

    async fn handle_flow_inserts(
        &self,
        _peer: &Peer,
        _request: InsertRequests,
    ) -> Result<FlowResponse> {
        unimplemented!()
    }

    async fn handle_mark_window_dirty(
        &self,
        _peer: &Peer,
        _req: DirtyWindowRequests,
    ) -> Result<FlowResponse> {
        unimplemented!()
    }
}

#[derive(Debug, Default)]
struct UnimplementedRegionRpc;

#[async_trait::async_trait]
impl RegionRpc for UnimplementedRegionRpc {
    async fn handle_region(&self, _peer: &Peer, _request: RegionRequest) -> Result<RegionResponse> {
        unimplemented!()
    }

    async fn handle_query(
        &self,
        _peer: &Peer,
        _request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!()
    }
}

/// Returns a test purpose [DdlContext] with a [`RegionRpcRef`].
pub fn new_ddl_context(region_rpc: RegionRpcRef) -> DdlContext {
    let kv_backend = Arc::new(MemoryKvBackend::new());
    new_ddl_context_with_kv_backend(region_rpc, kv_backend)
}

/// Returns a test purpose [DdlContext] with a specified [KvBackendRef] and a [`RegionRpcRef`].
pub fn new_ddl_context_with_kv_backend(
    region_rpc: RegionRpcRef,
    kv_backend: KvBackendRef,
) -> DdlContext {
    let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
    let table_metadata_allocator = Arc::new(TableMetadataAllocator::new(
        Arc::new(
            SequenceBuilder::new("test", kv_backend.clone())
                .initial(1024)
                .build(),
        ),
        Arc::new(WalProvider::default()),
    ));
    let flow_metadata_manager = Arc::new(FlowMetadataManager::new(kv_backend.clone()));
    let flow_metadata_allocator =
        Arc::new(FlowMetadataAllocator::with_noop_peer_allocator(Arc::new(
            SequenceBuilder::new("flow-test", kv_backend)
                .initial(1024)
                .build(),
        )));
    DdlContext {
        region_rpc,
        flow_rpc: Arc::new(UnimplementedFlowRpc),
        cache_invalidator: Arc::new(DummyCacheInvalidator),
        memory_region_keeper: Arc::new(MemoryRegionKeeper::new()),
        leader_region_registry: Arc::new(LeaderRegionRegistry::default()),
        table_metadata_allocator,
        table_metadata_manager,
        flow_metadata_allocator,
        flow_metadata_manager,
        region_failure_detector_controller: Arc::new(NoopRegionFailureDetectorControl),
    }
}

/// Returns a test purpose [DdlContext] with a [`FlowRpcRef`].
pub fn new_ddl_context_with_flow(flow_rpc: FlowRpcRef) -> DdlContext {
    let kv_backend = Arc::new(MemoryKvBackend::new());
    new_ddl_context_with_flow_and_kv_backend(flow_rpc, kv_backend)
}

/// Returns a test purpose [DdlContext] with a specified [KvBackendRef] and a [`FlowRpcRef`].
pub fn new_ddl_context_with_flow_and_kv_backend(
    flow_rpc: FlowRpcRef,
    kv_backend: KvBackendRef,
) -> DdlContext {
    let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));
    let table_metadata_allocator = Arc::new(TableMetadataAllocator::new(
        Arc::new(
            SequenceBuilder::new("test", kv_backend.clone())
                .initial(1024)
                .build(),
        ),
        Arc::new(WalProvider::default()),
    ));
    let flow_metadata_manager = Arc::new(FlowMetadataManager::new(kv_backend.clone()));
    let flow_metadata_allocator =
        Arc::new(FlowMetadataAllocator::with_noop_peer_allocator(Arc::new(
            SequenceBuilder::new("flow-test", kv_backend)
                .initial(1024)
                .build(),
        )));

    DdlContext {
        region_rpc: Arc::new(UnimplementedRegionRpc),
        flow_rpc,
        cache_invalidator: Arc::new(DummyCacheInvalidator),
        memory_region_keeper: Arc::new(MemoryRegionKeeper::new()),
        leader_region_registry: Arc::new(LeaderRegionRegistry::default()),
        table_metadata_allocator,
        table_metadata_manager,
        flow_metadata_allocator,
        flow_metadata_manager,
        region_failure_detector_controller: Arc::new(NoopRegionFailureDetectorControl),
    }
}

pub struct NoopPeerResolver;

#[async_trait::async_trait]
impl PeerResolver for NoopPeerResolver {
    async fn datanode(&self, id: DatanodeId) -> Result<Option<Peer>> {
        Ok(Some(Peer::empty(id)))
    }

    async fn flownode(&self, id: FlownodeId) -> Result<Option<Peer>> {
        Ok(Some(Peer::empty(id)))
    }
}

/// Create a kafka topic pool for testing.
pub async fn test_kafka_topic_pool(
    broker_endpoints: Vec<String>,
    num_topics: usize,
    auto_create_topics: bool,
    topic_name_prefix: Option<&str>,
) -> KafkaTopicPool {
    let mut config = MetasrvKafkaConfig {
        connection: KafkaConnectionConfig {
            broker_endpoints,
            ..Default::default()
        },
        kafka_topic: KafkaTopicConfig {
            num_topics,

            ..Default::default()
        },
        auto_create_topics,
        ..Default::default()
    };
    if let Some(prefix) = topic_name_prefix {
        config.kafka_topic.topic_name_prefix = prefix.to_string();
    }
    let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
    let topic_creator = build_kafka_topic_creator(&config.connection, &config.kafka_topic)
        .await
        .unwrap();

    KafkaTopicPool::new(&config, kv_backend, topic_creator)
}

#[macro_export]
/// Skip the test if the environment variable `GT_POSTGRES_ENDPOINTS` is not set.
///
/// The format of the environment variable is:
/// ```text
/// GT_POSTGRES_ENDPOINTS=localhost:9092,localhost:9093
/// ```
macro_rules! maybe_skip_postgres_integration_test {
    () => {
        if std::env::var("GT_POSTGRES_ENDPOINTS").is_err() {
            common_telemetry::warn!("The endpoints is empty, skipping the test");
            return;
        }
    };
}

#[macro_export]
/// Skip the test if the environment variable `GT_MYSQL_ENDPOINTS` is not set.
///
/// The format of the environment variable is:
/// ```text
/// GT_MYSQL_ENDPOINTS=localhost:9092,localhost:9093
/// ```
macro_rules! maybe_skip_mysql_integration_test {
    () => {
        if std::env::var("GT_MYSQL_ENDPOINTS").is_err() {
            common_telemetry::warn!("The endpoints is empty, skipping the test");
            return;
        }
    };
}

#[macro_export]
/// Skip the test if the environment variable `GT_POSTGRES15_ENDPOINTS` is not set.
///
/// The format of the environment variable is:
/// ```text
/// GT_POSTGRES15_ENDPOINTS=postgres://user:password@127.0.0.1:5433/postgres
/// ```
macro_rules! maybe_skip_postgres15_integration_test {
    () => {
        if std::env::var("GT_POSTGRES15_ENDPOINTS").is_err() {
            common_telemetry::warn!("The PG15 endpoints is empty, skipping the test");
            return;
        }
    };
}

#[macro_export]
/// Skip the test if the environment variable `GT_ETCD_TLS_ENDPOINTS` is not set.
///
/// The format of the environment variable is:
/// ```text
/// GT_ETCD_TLS_ENDPOINTS=localhost:9092,localhost:9093
/// ```
macro_rules! maybe_skip_etcd_tls_integration_test {
    () => {
        if std::env::var("GT_ETCD_TLS_ENDPOINTS").is_err() {
            common_telemetry::warn!("The etcd with tls endpoints is empty, skipping the test");
            return;
        }
    };
}

/// Returns the directory of the etcd TLS certs.
pub fn etcd_certs_dir() -> PathBuf {
    let project_path = env!("CARGO_MANIFEST_DIR");
    let project_path = PathBuf::from(project_path);
    let base = project_path.ancestors().nth(3).unwrap();
    base.join("tests-integration")
        .join("fixtures")
        .join("etcd-tls-certs")
}

/// Returns the directory of the test certs.
pub fn test_certs_dir() -> PathBuf {
    let project_path = env!("CARGO_MANIFEST_DIR");
    let project_path = PathBuf::from(project_path);
    let base = project_path.ancestors().nth(3).unwrap();
    base.join("tests-integration")
        .join("fixtures")
        .join("certs")
}
