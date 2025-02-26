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

use tonic::async_trait;

use crate::error::Result;
use crate::key::FlowId;
use crate::peer::Peer;
use crate::sequence::SequenceRef;
use crate::ClusterId;

/// The reference of [FlowMetadataAllocator].
pub type FlowMetadataAllocatorRef = Arc<FlowMetadataAllocator>;

/// [FlowMetadataAllocator] provides the ability of:
/// - [FlowId] Allocation.
/// - [FlownodeId] Selection.
#[derive(Clone)]
pub struct FlowMetadataAllocator {
    flow_id_sequence: SequenceRef,
    partition_peer_allocator: PartitionPeerAllocatorRef,
}

impl FlowMetadataAllocator {
    /// Returns the [FlowMetadataAllocator] with [NoopPartitionPeerAllocator].
    pub fn with_noop_peer_allocator(flow_id_sequence: SequenceRef) -> Self {
        Self {
            flow_id_sequence,
            partition_peer_allocator: Arc::new(NoopPartitionPeerAllocator),
        }
    }

    pub fn with_peer_allocator(
        flow_id_sequence: SequenceRef,
        peer_allocator: Arc<dyn PartitionPeerAllocator>,
    ) -> Self {
        Self {
            flow_id_sequence,
            partition_peer_allocator: peer_allocator,
        }
    }

    /// Allocates a the [FlowId].
    pub(crate) async fn allocate_flow_id(&self) -> Result<FlowId> {
        let flow_id = self.flow_id_sequence.next().await? as FlowId;
        Ok(flow_id)
    }

    /// Allocates the [FlowId] and [Peer]s.
    pub async fn create(
        &self,
        cluster_id: ClusterId,
        partitions: usize,
    ) -> Result<(FlowId, Vec<Peer>)> {
        let flow_id = self.allocate_flow_id().await?;
        let peers = self
            .partition_peer_allocator
            .alloc(cluster_id, partitions)
            .await?;

        Ok((flow_id, peers))
    }
}

/// Allocates [Peer]s for partitions.
#[async_trait]
pub trait PartitionPeerAllocator: Send + Sync {
    /// Allocates [Peer] nodes for storing partitions.
    async fn alloc(&self, cluster_id: ClusterId, partitions: usize) -> Result<Vec<Peer>>;
}

/// [PartitionPeerAllocatorRef] allocates [Peer]s for partitions.
pub type PartitionPeerAllocatorRef = Arc<dyn PartitionPeerAllocator>;

struct NoopPartitionPeerAllocator;

#[async_trait]
impl PartitionPeerAllocator for NoopPartitionPeerAllocator {
    async fn alloc(&self, _cluster_id: ClusterId, partitions: usize) -> Result<Vec<Peer>> {
        Ok(vec![Peer::default(); partitions])
    }
}
