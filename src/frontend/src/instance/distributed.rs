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

use api::v1::region::QueryRequest;
use async_trait::async_trait;
use client::error::{HandleRequestSnafu, Result as ClientResult};
use client::region_handler::RegionRequestHandler;
use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::catalog::FrontendCatalogManager;
use crate::error::{FindDatanodeSnafu, FindTableRouteSnafu, RequestQuerySnafu, Result};

pub(crate) struct DistRegionRequestHandler {
    catalog_manager: Arc<FrontendCatalogManager>,
}

impl DistRegionRequestHandler {
    pub fn arc(catalog_manager: Arc<FrontendCatalogManager>) -> Arc<Self> {
        Arc::new(Self { catalog_manager })
    }
}

#[async_trait]
impl RegionRequestHandler for DistRegionRequestHandler {
    async fn do_get(&self, request: QueryRequest) -> ClientResult<SendableRecordBatchStream> {
        self.do_get_inner(request)
            .await
            .map_err(BoxedError::new)
            .context(HandleRequestSnafu)
    }
}

impl DistRegionRequestHandler {
    async fn do_get_inner(&self, request: QueryRequest) -> Result<SendableRecordBatchStream> {
        let region_id = RegionId::from_u64(request.region_id);

        let table_route = self
            .catalog_manager
            .partition_manager()
            .find_table_route(region_id.table_id())
            .await
            .context(FindTableRouteSnafu {
                table_id: region_id.table_id(),
            })?;
        let peer = table_route
            .find_region_leader(region_id.region_number())
            .context(FindDatanodeSnafu {
                region: region_id.region_number(),
            })?;

        let client = self.catalog_manager.datanode_manager().datanode(peer).await;

        client
            .handle_query(request)
            .await
            .context(RequestQuerySnafu)
    }
}
