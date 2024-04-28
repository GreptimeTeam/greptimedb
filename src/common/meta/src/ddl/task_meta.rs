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
use crate::key::FlowTaskId;
use crate::peer::Peer;
use crate::sequence::SequenceRef;

/// The reference of [FlowTaskMetadataAllocator].
pub type FlowTaskMetadataAllocatorRef = Arc<FlowTaskMetadataAllocator>;

/// [FlowTaskMetadataAllocator] provides the ability of:
/// - [FlowTaskId] Allocation.
/// - [FlownodeId] Selection.
#[derive(Clone)]
pub struct FlowTaskMetadataAllocator {
    flow_task_id_sequence: SequenceRef,
    partition_peer_allocator: PartitionPeerAllocatorRef,
}

impl FlowTaskMetadataAllocator {
    /// Returns the [FlowTaskMetadataAllocator] with [NoopPartitionPeerAllocator].
    pub fn with_noop_peer_allocator(flow_task_id_sequence: SequenceRef) -> Self {
        Self {
            flow_task_id_sequence,
            partition_peer_allocator: Arc::new(NoopPartitionPeerAllocator),
        }
    }

    /// Allocates a the [FlowTaskId].
    pub(crate) async fn allocate_flow_task_id(&self) -> Result<FlowTaskId> {
        let flow_task_id = self.flow_task_id_sequence.next().await? as FlowTaskId;
        Ok(flow_task_id)
    }

    /// Allocates the [FlowTaskId] and [Peer]s.
    pub async fn create(&self, partitions: usize) -> Result<(FlowTaskId, Vec<Peer>)> {
        let flow_task_id = self.allocate_flow_task_id().await?;
        let peers = self.partition_peer_allocator.alloc(partitions).await?;

        Ok((flow_task_id, peers))
    }
}

/// Allocates [Peer]s for partitions.
#[async_trait]
pub trait PartitionPeerAllocator: Send + Sync {
    /// Allocates [Peer] nodes for storing partitions.
    async fn alloc(&self, partitions: usize) -> Result<Vec<Peer>>;
}

/// [PartitionPeerAllocatorRef] allocates [Peer]s for partitions.
pub type PartitionPeerAllocatorRef = Arc<dyn PartitionPeerAllocator>;

struct NoopPartitionPeerAllocator;

#[async_trait]
impl PartitionPeerAllocator for NoopPartitionPeerAllocator {
    async fn alloc(&self, partitions: usize) -> Result<Vec<Peer>> {
        Ok(vec![Peer::default(); partitions])
    }
}
