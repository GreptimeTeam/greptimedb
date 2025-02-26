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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_meta::node_manager::NodeManagerRef;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use partition::manager::PartitionRuleManagerRef;
use query::error::{RegionQuerySnafu, Result as QueryResult};
use query::region_query::RegionQueryHandler;
use snafu::ResultExt;

use crate::error::{FindTableRouteSnafu, RequestQuerySnafu, Result};

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
    async fn do_get(&self, request: QueryRequest) -> QueryResult<SendableRecordBatchStream> {
        self.do_get_inner(request)
            .await
            .map_err(BoxedError::new)
            .context(RegionQuerySnafu)
    }
}

impl FrontendRegionQueryHandler {
    async fn do_get_inner(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        let region_id = request.region_id;

        let peer = &self
            .partition_manager
            .find_region_leader(region_id)
            .await
            .context(FindTableRouteSnafu {
                table_id: region_id.table_id(),
            })?;

        let client = self.node_manager.datanode(peer).await;

        client
            .handle_query(request)
            .await
            .context(RequestQuerySnafu)
    }
}
