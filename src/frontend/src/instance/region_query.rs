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
use common_meta::region_rpc::RegionRpcRef;
use common_query::request::QueryRequest;
use common_recordbatch::SendableRecordBatchStream;
use partition::manager::PartitionRuleManagerRef;
use query::error::{RegionQuerySnafu, Result as QueryResult};
use query::region_query::RegionQueryHandler;
use session::ReadPreference;
use snafu::ResultExt;

use crate::error::{FindRegionPeerSnafu, RequestQuerySnafu, Result};

pub(crate) struct FrontendRegionQueryHandler {
    partition_manager: PartitionRuleManagerRef,
    region_rpc: RegionRpcRef,
}

impl FrontendRegionQueryHandler {
    pub fn arc(partition_manager: PartitionRuleManagerRef, region_rpc: RegionRpcRef) -> Arc<Self> {
        Arc::new(Self {
            partition_manager,
            region_rpc,
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

        self.region_rpc
            .handle_query(peer, request)
            .await
            .context(RequestQuerySnafu)
    }
}
