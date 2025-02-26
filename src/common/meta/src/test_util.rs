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

use std::sync::Arc;

use api::region::RegionResponse;
use api::v1::flow::{FlowRequest, FlowResponse};
use api::v1::region::{InsertRequests, RegionRequest};
pub use common_base::AffectedRows;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;

use crate::cache_invalidator::DummyCacheInvalidator;
use crate::ddl::flow_meta::FlowMetadataAllocator;
use crate::ddl::table_meta::TableMetadataAllocator;
use crate::ddl::{DdlContext, NoopRegionFailureDetectorControl};
use crate::error::Result;
use crate::key::flow::FlowMetadataManager;
use crate::key::TableMetadataManager;
use crate::kv_backend::memory::MemoryKvBackend;
use crate::kv_backend::KvBackendRef;
use crate::node_manager::{
    Datanode, DatanodeRef, Flownode, FlownodeRef, NodeManager, NodeManagerRef,
};
use crate::peer::{Peer, PeerLookupService};
use crate::region_keeper::MemoryRegionKeeper;
use crate::sequence::SequenceBuilder;
use crate::wal_options_allocator::WalOptionsAllocator;
use crate::{ClusterId, DatanodeId, FlownodeId};

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
impl<T: MockDatanodeHandler + 'static> NodeManager for MockDatanodeManager<T> {
    async fn datanode(&self, peer: &Peer) -> DatanodeRef {
        Arc::new(MockNode {
            peer: peer.clone(),
            handler: self.handler.clone(),
        })
    }

    async fn flownode(&self, _node: &Peer) -> FlownodeRef {
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
}

#[async_trait::async_trait]
impl<T: MockFlownodeHandler + 'static> NodeManager for MockFlownodeManager<T> {
    async fn datanode(&self, _peer: &Peer) -> DatanodeRef {
        unimplemented!()
    }

    async fn flownode(&self, peer: &Peer) -> FlownodeRef {
        Arc::new(MockNode {
            peer: peer.clone(),
            handler: self.handler.clone(),
        })
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
        Arc::new(WalOptionsAllocator::default()),
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
        table_metadata_allocator,
        table_metadata_manager,
        flow_metadata_allocator,
        flow_metadata_manager,
        region_failure_detector_controller: Arc::new(NoopRegionFailureDetectorControl),
    }
}

pub struct NoopPeerLookupService;

#[async_trait::async_trait]
impl PeerLookupService for NoopPeerLookupService {
    async fn datanode(&self, _cluster_id: ClusterId, id: DatanodeId) -> Result<Option<Peer>> {
        Ok(Some(Peer::empty(id)))
    }

    async fn flownode(&self, _cluster_id: ClusterId, id: FlownodeId) -> Result<Option<Peer>> {
        Ok(Some(Peer::empty(id)))
    }
}
