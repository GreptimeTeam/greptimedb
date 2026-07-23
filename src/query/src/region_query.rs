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

use crate::error::Result;

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

/// The result of a region query together with its frozen target.
pub struct RoutedRegionQueryStream {
    pub stream: SendableRecordBatchStream,
    pub target: RegionQueryTarget,
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
    async fn do_get(
        &self,
        read_preference: ReadPreference,
        request: QueryRequest,
    ) -> Result<RoutedRegionQueryStream>;

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
mod tests {
    use common_meta::peer::Peer;

    use super::RegionQueryTarget;

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
