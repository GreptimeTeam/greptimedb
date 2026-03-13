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
use crate::key::TableMetadataManager;
use crate::key::flow::FlowMetadataManager;
use crate::kv_backend::KvBackendRef;
use crate::kv_backend::memory::MemoryKvBackend;
use crate::node_manager::{
    Datanode, DatanodeManager, DatanodeRef, Flownode, FlownodeManager, FlownodeRef, NodeManagerRef,
};
use crate::peer::{Peer, PeerResolver};
use crate::region_keeper::MemoryRegionKeeper;
use crate::region_registry::LeaderRegionRegistry;
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

/// A mock struct implements [NodeManager] only implement the `datanode` method.
#[derive(Clone)]
pub struct MockDatanodeManager<T> {
    handler: T,
}

impl<T> MockDatanodeManager<T> {
    pub fn new(handler: T) -> Self {
        Self { handler }
    }
}

/// A mock struct implements [NodeManager] only implement the `flownode` method.
#[derive(Clone)]
pub struct MockFlownodeManager<T> {
    handler: T,
}

impl<T> MockFlownodeManager<T> {
    pub fn new(handler: T) -> Self {
        Self { handler }
    }
}

/// A mock struct implements [Datanode].
#[derive(Clone)]
struct MockNode<T> {
    peer: Peer,
    handler: T,
}

#[async_trait::async_trait]
impl<T: MockDatanodeHandler> Datanode for MockNode<T> {
    async fn handle(&self, request: RegionRequest) -> Result<RegionResponse> {
        self.handler.handle(&self.peer, request).await
    }

    async fn handle_query(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        self.handler.handle_query(&self.peer, request).await
    }
}

#[async_trait::async_trait]
impl<T: MockDatanodeHandler + 'static> DatanodeManager for MockDatanodeManager<T> {
    async fn datanode(&self, peer: &Peer) -> DatanodeRef {
        Arc::new(MockNode {
            peer: peer.clone(),
            handler: self.handler.clone(),
        })
    }
}

#[async_trait::async_trait]
impl<T: 'static + Send + Sync> FlownodeManager for MockDatanodeManager<T> {
    async fn flownode(&self, _peer: &Peer) -> FlownodeRef {
        unimplemented!()
    }
}

#[async_trait::async_trait]
impl<T: MockFlownodeHandler> Flownode for MockNode<T> {
    async fn handle(&self, request: FlowRequest) -> Result<FlowResponse> {
        self.handler.handle(&self.peer, request).await
    }

    async fn handle_inserts(&self, requests: InsertRequests) -> Result<FlowResponse> {
        self.handler.handle_inserts(&self.peer, requests).await
    }

    async fn handle_mark_window_dirty(&self, req: DirtyWindowRequests) -> Result<FlowResponse> {
        self.handler.handle_mark_window_dirty(&self.peer, req).await
    }
}

#[async_trait::async_trait]
impl<T: MockFlownodeHandler + 'static> FlownodeManager for MockFlownodeManager<T> {
    async fn flownode(&self, peer: &Peer) -> FlownodeRef {
        Arc::new(MockNode {
            peer: peer.clone(),
            handler: self.handler.clone(),
        })
    }
}

#[async_trait::async_trait]
impl<T: 'static + Send + Sync> DatanodeManager for MockFlownodeManager<T> {
    async fn datanode(&self, _peer: &Peer) -> DatanodeRef {
        unimplemented!()
    }
}

/// Returns a test purpose [DdlContext].
pub fn new_ddl_context(node_manager: NodeManagerRef) -> DdlContext {
    let kv_backend = Arc::new(MemoryKvBackend::new());
    new_ddl_context_with_kv_backend(node_manager, kv_backend)
}

/// Returns a test purpose [DdlContext] with a specified [KvBackendRef].
pub fn new_ddl_context_with_kv_backend(
    node_manager: NodeManagerRef,
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
        node_manager,
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
