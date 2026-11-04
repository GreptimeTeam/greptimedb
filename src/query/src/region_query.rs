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

use api::v1::region::{RemoteDynFilterUnregister, RemoteDynFilterUpdate};
use async_trait::async_trait;
use common_meta::node_manager::NodeManagerRef;
use common_meta::peer::Peer;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use partition::manager::PartitionRuleManagerRef;
use session::ReadPreference;
use tokio::sync::{Semaphore, SemaphorePermit};

use crate::error::Result;

/// Caps the region reads one query keeps in flight.
///
/// A region read is a `DoGet` whose body streams until that region is drained, so a query
/// reading several sources would otherwise run one body per source and partition at once.
/// The bound belongs to the query, not to one scan: its permit count is the query's target
/// partitions, so two scans of three regions no longer read four bodies at once.
pub(crate) struct RegionReadLimiter {
    permits: Semaphore,
}

impl RegionReadLimiter {
    pub(crate) fn new(permits: usize) -> Self {
        Self {
            permits: Semaphore::new(permits.max(1)),
        }
    }

    /// Waits for one region read to be allowed; the read stays counted until the permit drops.
    pub(crate) async fn acquire(&self) -> SemaphorePermit<'_> {
        self.permits
            .acquire()
            .await
            .expect("the region read limiter is never closed")
    }
}

/// The peer selected to serve a region query.
///
/// The target is frozen when the query is dispatched. Follow-up controls must
/// reuse it instead of resolving the region route again.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RegionQueryTarget(Peer);

impl RegionQueryTarget {
    pub fn new(peer: Peer) -> Self {
        Self(peer)
    }

    pub fn peer(&self) -> &Peer {
        &self.0
    }
}

/// A factory to create a [`RegionQueryHandler`].
pub trait RegionQueryHandlerFactory: Send + Sync {
    /// Build a [`RegionQueryHandler`] with the given partition manager and node manager.
    fn build(
        &self,
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
    ) -> RegionQueryHandlerRef;
}

pub type RegionQueryHandlerFactoryRef = Arc<dyn RegionQueryHandlerFactory>;

#[async_trait]
pub trait RegionQueryHandler: Send + Sync {
    async fn select_target(
        &self,
        read_preference: ReadPreference,
        region_id: store_api::storage::RegionId,
    ) -> Result<RegionQueryTarget>;

    async fn do_get(
        &self,
        target: &RegionQueryTarget,
        request: QueryRequest,
    ) -> Result<SendableRecordBatchStream>;

    async fn handle_remote_dyn_filter_update(
        &self,
        target: &RegionQueryTarget,
        query_id: String,
        update: RemoteDynFilterUpdate,
    ) -> Result<()>;

    async fn handle_remote_dyn_filter_unregister(
        &self,
        target: &RegionQueryTarget,
        query_id: String,
        unregister: RemoteDynFilterUnregister,
    ) -> Result<()>;
}

pub type RegionQueryHandlerRef = Arc<dyn RegionQueryHandler>;

#[cfg(test)]
pub(crate) mod tests {
    use async_trait::async_trait;
    use common_meta::peer::Peer;
    use session::ReadPreference;
    use store_api::storage::RegionId;

    use super::{RegionQueryHandler, RegionQueryTarget};

    pub(crate) struct PlanningOnlyRegionQueryHandler;

    #[async_trait]
    impl RegionQueryHandler for PlanningOnlyRegionQueryHandler {
        async fn select_target(
            &self,
            _read_preference: ReadPreference,
            _region_id: RegionId,
        ) -> crate::error::Result<RegionQueryTarget> {
            unimplemented!("planning test does not dispatch regions")
        }

        async fn do_get(
            &self,
            _target: &RegionQueryTarget,
            _request: common_query::request::QueryRequest,
        ) -> crate::error::Result<common_recordbatch::SendableRecordBatchStream> {
            unimplemented!("planning test does not dispatch regions")
        }

        async fn handle_remote_dyn_filter_update(
            &self,
            _target: &RegionQueryTarget,
            _query_id: String,
            _update: api::v1::region::RemoteDynFilterUpdate,
        ) -> crate::error::Result<()> {
            unimplemented!("planning test does not dispatch regions")
        }

        async fn handle_remote_dyn_filter_unregister(
            &self,
            _target: &RegionQueryTarget,
            _query_id: String,
            _unregister: api::v1::region::RemoteDynFilterUnregister,
        ) -> crate::error::Result<()> {
            unimplemented!("planning test does not dispatch regions")
        }
    }

    #[test]
    fn region_query_target_exposes_immutable_peer() {
        let peer = Peer {
            id: 42,
            addr: "127.0.0.1:3001".to_string(),
        };
        let target = RegionQueryTarget::new(peer.clone());

        assert_eq!(target.peer(), &peer);
    }
}
