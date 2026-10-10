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

//! The datanode's [`RegionQueryHandler`] implementation for querying remote regions in a
//! `MergeScan` plan.

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

/// Serves region queries by resolving their leader and forwarding requests to its datanode.
pub(crate) struct DatanodeRegionQueryHandler {
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
        request: QueryRequest,
    ) -> QueryResult<SendableRecordBatchStream> {
        self.node_manager
            .datanode(target.peer())
            .await
            .handle_query(request)
            .await
            .map_err(BoxedError::new)
            .context(RegionQuerySnafu)
    }

    /// Datanodes forward received region queries and do not produce remote dynamic filters.
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
