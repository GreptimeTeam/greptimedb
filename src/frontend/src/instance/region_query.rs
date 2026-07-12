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
use client::region::{
    build_remote_dyn_filter_unregister_request, build_remote_dyn_filter_update_request,
};
use common_error::ext::BoxedError;
use common_meta::node_manager::NodeManagerRef;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use partition::manager::PartitionRuleManagerRef;
use query::error::{RegionQuerySnafu, Result as QueryResult};
use query::region_query::RegionQueryHandler;
use session::ReadPreference;
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::error::{FindRegionPeerSnafu, RequestQuerySnafu, Result};

pub(crate) struct FrontendRegionQueryHandler {
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
}

impl FrontendRegionQueryHandler {
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
impl RegionQueryHandler for FrontendRegionQueryHandler {
    async fn do_get(
        &self,
        read_preference: ReadPreference,
        request: QueryRequest,
    ) -> QueryResult<SendableRecordBatchStream> {
        self.do_get_inner(read_preference, request)
            .await
            .map_err(BoxedError::new)
            .context(RegionQuerySnafu)
    }

    async fn handle_remote_dyn_filter_update(
        &self,
        region_id: RegionId,
        query_id: String,
        update: RemoteDynFilterUpdate,
    ) -> QueryResult<()> {
        self.handle_remote_dyn_filter_update_inner(region_id, query_id, update)
            .await
            .map_err(BoxedError::new)
            .context(RegionQuerySnafu)
    }

    async fn handle_remote_dyn_filter_unregister(
        &self,
        region_id: RegionId,
        query_id: String,
        unregister: RemoteDynFilterUnregister,
    ) -> QueryResult<()> {
        self.handle_remote_dyn_filter_unregister_inner(region_id, query_id, unregister)
            .await
            .map_err(BoxedError::new)
            .context(RegionQuerySnafu)
    }
}

impl FrontendRegionQueryHandler {
    async fn do_get_inner(
        &self,
        read_preference: ReadPreference,
        request: QueryRequest,
    ) -> Result<SendableRecordBatchStream> {
        let region_id = request.region_id;

        let peer = &self
            .partition_manager
            .find_region_leader(region_id)
            .await
            .context(FindRegionPeerSnafu {
                region_id,
                read_preference,
            })?;

        let client = self.node_manager.datanode(peer).await;

        client
            .handle_query(request)
            .await
            .context(RequestQuerySnafu)
    }

    async fn handle_remote_dyn_filter_update_inner(
        &self,
        region_id: RegionId,
        query_id: String,
        update: RemoteDynFilterUpdate,
    ) -> Result<()> {
        let peer = &self
            .partition_manager
            .find_region_leader(region_id)
            .await
            .context(FindRegionPeerSnafu {
                region_id,
                read_preference: ReadPreference::Leader,
            })?;
        let client = self.node_manager.datanode(peer).await;
        client
            .handle(build_remote_dyn_filter_update_request(query_id, update))
            .await
            .context(RequestQuerySnafu)?;
        Ok(())
    }

    async fn handle_remote_dyn_filter_unregister_inner(
        &self,
        region_id: RegionId,
        query_id: String,
        unregister: RemoteDynFilterUnregister,
    ) -> Result<()> {
        let peer = &self
            .partition_manager
            .find_region_leader(region_id)
            .await
            .context(FindRegionPeerSnafu {
                region_id,
                read_preference: ReadPreference::Leader,
            })?;
        let client = self.node_manager.datanode(peer).await;
        client
            .handle(build_remote_dyn_filter_unregister_request(
                query_id, unregister,
            ))
            .await
            .context(RequestQuerySnafu)?;
        Ok(())
    }
}
