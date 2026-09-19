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

//! A [`RegionQueryHandler`] implementation for the datanode.
//!
//! A datanode executes the plans it receives through region queries. Such a plan may contain a
//! `MergeScan` node (see [`query::dist_plan::MergeScanLogicalPlan`]) that is planned into a
//! `MergeScanExec` by the dist planner of the datanode, which then has to query the regions of
//! that `MergeScan` from the datanodes that host them. This handler provides the datanode with the
//! client side of those region queries.

use std::sync::Arc;

use api::v1::region::{RemoteDynFilterUnregister, RemoteDynFilterUpdate};
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_meta::node_manager::NodeManagerRef;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use partition::manager::PartitionRuleManagerRef;
use query::error::{RegionQuerySnafu, Result as QueryResult, UnimplementedSnafu};
use query::region_query::{RegionQueryHandler, RegionQueryTarget};
use session::ReadPreference;
use snafu::ResultExt;
use store_api::storage::RegionId;

/// Serves the region queries issued by the datanode itself.
///
/// Known limitation: [`RegionQueryHandler::select_target`] resolves the leader of a region through
/// the table route cache, which uses `InitStrategy::VersionChecked`. The version counter of that
/// strategy belongs to the `CacheContainer` and not to a single table: any `TableId` invalidation
/// bumps the shared version, so a cold load of one table's route retries (and loads again) when an
/// *unrelated* table is invalidated concurrently. Clusters with frequent DDL or region movement can
/// therefore see amplified cold-load retries on the datanode; the tail latency of cold loads in
/// such clusters is worth verifying. The retries only add work, they keep the loaded route correct
/// (a stale route is never returned).
pub struct DatanodeRegionQueryHandler {
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
}

impl DatanodeRegionQueryHandler {
    pub fn arc(
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
    ) -> Arc<Self> {
        Arc::new(Self {
            partition_manager,
            node_manager,
        })
    }
}

#[async_trait]
impl RegionQueryHandler for DatanodeRegionQueryHandler {
    async fn select_target(
        &self,
        _read_preference: ReadPreference,
        region_id: RegionId,
    ) -> QueryResult<RegionQueryTarget> {
        // The leader of a region is the only peer that can serve a region query.
        //
        // This lookup goes through the version checked table route cache: see the known limitation
        // of `DatanodeRegionQueryHandler` about retries of unrelated invalidations.
        let peer = self
            .partition_manager
            .find_region_leader(region_id)
            .await
            .map_err(BoxedError::new)
            .context(RegionQuerySnafu)?;

        Ok(RegionQueryTarget::new(peer))
    }

    async fn do_get(
        &self,
        target: &RegionQueryTarget,
        mut request: QueryRequest,
    ) -> QueryResult<SendableRecordBatchStream> {
        // A datanode serves the region queries of the `MergeScan` nodes of the plan it received.
        // Such a region query is an execution stage of that plan, not an independent query, so it
        // must leave the concurrency permits to the query that dispatched the plan. The marker is
        // carried to the peer with the request: the client encodes it into the query context of the
        // header (`common_query::request::QUERY_INTERNAL_STAGE_EXTENSION_KEY`), where the peer reads
        // it back and admits the stage without a permit of its own (see
        // `RegionServer::handle_remote_read_inner`).
        request.internal = true;

        self.node_manager
            .datanode(target.peer())
            .await
            .handle_query(request)
            .await
            .map_err(BoxedError::new)
            .context(RegionQuerySnafu)
    }

    /// The remote dynamic filter of a nested merge scan is not supported yet (PoC): a datanode only
    /// dispatches region queries for the `MergeScan` nodes of the plans it receives, it doesn't
    /// produce remote dynamic filters of its own.
    async fn handle_remote_dyn_filter_update(
        &self,
        _target: &RegionQueryTarget,
        _query_id: String,
        _update: RemoteDynFilterUpdate,
    ) -> QueryResult<()> {
        UnimplementedSnafu {
            operation: "remote dynamic filter update from a datanode",
        }
        .fail()
    }

    /// See [`RegionQueryHandler::handle_remote_dyn_filter_update`].
    async fn handle_remote_dyn_filter_unregister(
        &self,
        _target: &RegionQueryTarget,
        _query_id: String,
        _unregister: RemoteDynFilterUnregister,
    ) -> QueryResult<()> {
        UnimplementedSnafu {
            operation: "remote dynamic filter unregister from a datanode",
        }
        .fail()
    }
}
