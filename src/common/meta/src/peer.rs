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

pub use api::v1::meta::Peer;
use api::v1::meta::heartbeat_request::NodeWorkloads;

use crate::error::Error;
use crate::{DatanodeId, FlownodeId};

/// [`PeerResolver`] provides methods to look up peer information by node ID.
///
/// This trait allows resolving both datanode and flownode peers, regardless of their current activity status.
/// Returned peers may be inactive (i.e., not currently alive in the cluster).
#[async_trait::async_trait]
pub trait PeerResolver: Send + Sync {
    /// Looks up a datanode peer by its ID.
    ///
    /// Returns `Some(Peer)` if the datanode exists, or `None` if not found.
    /// The returned peer may be inactive.
    async fn datanode(&self, id: DatanodeId) -> Result<Option<Peer>, Error>;

    /// Looks up a flownode peer by its ID.
    ///
    /// Returns `Some(Peer)` if the flownode exists, or `None` if not found.
    /// The returned peer may be inactive.
    async fn flownode(&self, id: FlownodeId) -> Result<Option<Peer>, Error>;
}

pub type PeerResolverRef = Arc<dyn PeerResolver>;

/// [`PeerDiscovery`] is a service for discovering active peers in the cluster.
#[async_trait::async_trait]
pub trait PeerDiscovery: Send + Sync {
    /// Returns all currently active frontend nodes.
    ///
    /// A frontend is considered active if it has reported a heartbeat within the most recent heartbeat interval,
    /// as determined by the in-memory backend.
    async fn active_frontends(&self) -> Result<Vec<Peer>, Error>;

    /// Returns all currently active datanodes, optionally filtered by a predicate on their workloads.
    ///
    /// A datanode is considered active if it has reported a heartbeat within the most recent heartbeat interval,
    /// as determined by the in-memory backend.
    /// The optional `filter` allows further selection based on the node's workloads.
    async fn active_datanodes(
        &self,
        filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
    ) -> Result<Vec<Peer>, Error>;

    /// Returns all currently active flownodes, optionally filtered by a predicate on their workloads.
    ///
    /// A flownode is considered active if it has reported a heartbeat within the most recent heartbeat interval,
    /// as determined by the in-memory backend.
    /// The optional `filter` allows further selection based on the node's workloads.
    async fn active_flownodes(
        &self,
        filter: Option<for<'a> fn(&'a NodeWorkloads) -> bool>,
    ) -> Result<Vec<Peer>, Error>;
}

pub type PeerDiscoveryRef = Arc<dyn PeerDiscovery>;

/// [`PeerAllocator`] allocates [`Peer`]s for creating region or flow.
#[async_trait::async_trait]
pub trait PeerAllocator: Send + Sync {
    async fn alloc(&self, num: usize) -> Result<Vec<Peer>, Error>;
}

pub type PeerAllocatorRef = Arc<dyn PeerAllocator>;

#[async_trait::async_trait]
impl<T: PeerAllocator + ?Sized> PeerAllocator for Arc<T> {
    async fn alloc(&self, num: usize) -> Result<Vec<Peer>, Error> {
        T::alloc(self, num).await
    }
}

pub struct NoopPeerAllocator;

#[async_trait::async_trait]
impl PeerAllocator for NoopPeerAllocator {
    async fn alloc(&self, num: usize) -> Result<Vec<Peer>, Error> {
        Ok(vec![Peer::default(); num])
    }
}
