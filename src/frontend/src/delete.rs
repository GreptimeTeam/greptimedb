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

use api::v1::region::region_request;
use api::v1::{DeleteRequests, RowDeleteRequests};
use catalog::CatalogManager;
use client::region::check_response_header;
use common_query::Output;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::error::{RequestDatanodeSnafu, Result};
use crate::instance::region_handler::RegionRequestHandler;
use crate::req_convert::delete::{ColumnToRow, RowToRegion};

pub(crate) struct Deleter<'a> {
    catalog_manager: &'a dyn CatalogManager,
    region_request_handler: &'a dyn RegionRequestHandler,
}

impl<'a> Deleter<'a> {
    pub fn new(
        catalog_manager: &'a dyn CatalogManager,
        region_request_handler: &'a dyn RegionRequestHandler,
    ) -> Self {
        Self {
            catalog_manager,
            region_request_handler,
        }
    }

    pub async fn handle_column_deletes(
        &self,
        requests: DeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let row_deletes = ColumnToRow::convert(requests)?;
        self.handle_row_deletes(row_deletes, ctx).await
    }

    pub async fn handle_row_deletes(
        &self,
        mut requests: RowDeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        // remove empty requests
        requests.deletes.retain(|req| {
            req.rows
                .as_ref()
                .map(|r| !r.rows.is_empty())
                .unwrap_or_default()
        });

        let region_request = RowToRegion::new(self.catalog_manager, &ctx)
            .convert(requests)
            .await?;
        let region_request = region_request::Body::Deletes(region_request);
        let response = self
            .region_request_handler
            .handle(region_request, ctx)
            .await?;
        check_response_header(response.header).context(RequestDatanodeSnafu)?;
        Ok(Output::AffectedRows(response.affected_rows as _))
    }
}
