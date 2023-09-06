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

pub mod deleter;
pub(crate) mod inserter;

use std::sync::Arc;

use api::v1::region::{region_request, QueryRequest};
use async_trait::async_trait;
use client::error::{HandleRequestSnafu, Result as ClientResult};
use client::region_handler::RegionRequestHandler;
use common_error::ext::BoxedError;
use common_meta::datanode_manager::AffectedRows;
use common_recordbatch::SendableRecordBatchStream;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use store_api::storage::RegionId;

use crate::catalog::FrontendCatalogManager;
use crate::error::{
    FindDatanodeSnafu, FindTableRouteSnafu, NotSupportedSnafu, RequestQuerySnafu, Result,
};
use crate::instance::distributed::deleter::DistDeleter;
use crate::instance::distributed::inserter::DistInserter;

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
    async fn handle(
        &self,
        request: region_request::Body,
        ctx: QueryContextRef,
    ) -> ClientResult<AffectedRows> {
        self.handle_inner(request, ctx)
            .await
            .map_err(BoxedError::new)
            .context(HandleRequestSnafu)
    }

    async fn do_get(&self, request: QueryRequest) -> ClientResult<SendableRecordBatchStream> {
        self.do_get_inner(request)
            .await
            .map_err(BoxedError::new)
            .context(HandleRequestSnafu)
    }
}

impl DistRegionRequestHandler {
    async fn handle_inner(
        &self,
        request: region_request::Body,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        match request {
            region_request::Body::Inserts(inserts) => {
                let inserter =
                    DistInserter::new(&self.catalog_manager).with_trace_id(ctx.trace_id());
                inserter.insert(inserts).await
            }
            region_request::Body::Deletes(deletes) => {
                let deleter = DistDeleter::new(&self.catalog_manager).with_trace_id(ctx.trace_id());
                deleter.delete(deletes).await
            }
            region_request::Body::Create(_) => NotSupportedSnafu {
                feat: "region create",
            }
            .fail(),
            region_request::Body::Drop(_) => NotSupportedSnafu {
                feat: "region drop",
            }
            .fail(),
            region_request::Body::Open(_) => NotSupportedSnafu {
                feat: "region open",
            }
            .fail(),
            region_request::Body::Close(_) => NotSupportedSnafu {
                feat: "region close",
            }
            .fail(),
            region_request::Body::Alter(_) => NotSupportedSnafu {
                feat: "region alter",
            }
            .fail(),
            region_request::Body::Flush(_) => NotSupportedSnafu {
                feat: "region flush",
            }
            .fail(),
            region_request::Body::Compact(_) => NotSupportedSnafu {
                feat: "region compact",
            }
            .fail(),
        }
    }

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
