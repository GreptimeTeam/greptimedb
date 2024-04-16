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

use api::v1::region::{QueryRequest, RegionHandleResponse, RegionRequest};
pub use common_base::AffectedRows;
use common_recordbatch::SendableRecordBatchStream;

use crate::cache_invalidator::DummyCacheInvalidator;
use crate::datanode_manager::{Datanode, DatanodeManager, DatanodeManagerRef, DatanodeRef};
use crate::ddl::table_meta::TableMetadataAllocator;
use crate::ddl::DdlContext;
use crate::error::Result;
use crate::key::TableMetadataManager;
use crate::kv_backend::memory::MemoryKvBackend;
use crate::kv_backend::KvBackendRef;
use crate::peer::Peer;
use crate::region_keeper::MemoryRegionKeeper;
use crate::sequence::SequenceBuilder;
use crate::wal_options_allocator::WalOptionsAllocator;

#[async_trait::async_trait]
pub trait MockDatanodeHandler: Sync + Send + Clone {
    async fn handle(&self, peer: &Peer, request: RegionRequest) -> Result<RegionHandleResponse>;

    async fn handle_query(
        &self,
        peer: &Peer,
        request: QueryRequest,
    ) -> Result<SendableRecordBatchStream>;
}

/// A mock struct implements [DatanodeManager].
#[derive(Clone)]
pub struct MockDatanodeManager<T> {
    handler: T,
}

impl<T> MockDatanodeManager<T> {
    pub fn new(handler: T) -> Self {
        Self { handler }
    }
}

/// A mock struct implements [Datanode].
#[derive(Clone)]
struct MockDatanode<T> {
    peer: Peer,
    handler: T,
}

#[async_trait::async_trait]
impl<T: MockDatanodeHandler> Datanode for MockDatanode<T> {
    async fn handle(&self, request: RegionRequest) -> Result<RegionHandleResponse> {
        self.handler.handle(&self.peer, request).await
    }

    async fn handle_query(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        self.handler.handle_query(&self.peer, request).await
    }
}

#[async_trait::async_trait]
impl<T: MockDatanodeHandler + 'static> DatanodeManager for MockDatanodeManager<T> {
    async fn datanode(&self, peer: &Peer) -> DatanodeRef {
        Arc::new(MockDatanode {
            peer: peer.clone(),
            handler: self.handler.clone(),
        })
    }
}

/// Returns a test purpose [DdlContext].
pub fn new_ddl_context(datanode_manager: DatanodeManagerRef) -> DdlContext {
    let kv_backend = Arc::new(MemoryKvBackend::new());
    new_ddl_context_with_kv_backend(datanode_manager, kv_backend)
}

/// Returns a test purpose [DdlContext] with a specified [KvBackendRef].
pub fn new_ddl_context_with_kv_backend(
    datanode_manager: DatanodeManagerRef,
    kv_backend: KvBackendRef,
) -> DdlContext {
    let table_metadata_manager = Arc::new(TableMetadataManager::new(kv_backend.clone()));

    DdlContext {
        datanode_manager,
        cache_invalidator: Arc::new(DummyCacheInvalidator),
        memory_region_keeper: Arc::new(MemoryRegionKeeper::new()),
        table_metadata_allocator: Arc::new(TableMetadataAllocator::new(
            Arc::new(
                SequenceBuilder::new("test", kv_backend)
                    .initial(1024)
                    .build(),
            ),
            Arc::new(WalOptionsAllocator::default()),
        )),
        table_metadata_manager,
    }
}
