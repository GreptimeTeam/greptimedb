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

use std::collections::HashSet;
use std::{iter, mem};

use api::v1::region::region_request;
use api::v1::{DeleteRequests, RowDeleteRequest, RowDeleteRequests};
use catalog::CatalogManager;
use client::region::check_response_header;
use client::region_handler::RegionRequestHandler;
use common_query::Output;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use table::TableRef;

use crate::error::{
    CatalogSnafu, InvalidDeleteRequestSnafu, MissingTimeIndexColumnSnafu, RequestDatanodeSnafu,
    Result, TableNotFoundSnafu,
};
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
        validate_row_count_match(&requests)?;

        let requests = self.trim_columns(requests, &ctx).await?;
        let region_request = RowToRegion::new(self.catalog_manager, &ctx)
            .convert(requests)
            .await?;
        let region_request = region_request::Body::Deletes(region_request);

        let response = self
            .region_request_handler
            .handle(region_request, ctx)
            .await
            .context(RequestDatanodeSnafu)?;
        check_response_header(response.header).context(RequestDatanodeSnafu)?;
        Ok(Output::AffectedRows(response.affected_rows as _))
    }
}

impl<'a> Deleter<'a> {
    async fn trim_columns(
        &self,
        mut requests: RowDeleteRequests,
        ctx: &QueryContextRef,
    ) -> Result<RowDeleteRequests> {
        for req in &mut requests.deletes {
            let table = self.get_table(req, ctx).await?;
            let key_column_names = self.key_column_names(&table)?;

            let rows = req.rows.as_mut().unwrap();
            let all_key = rows
                .schema
                .iter()
                .all(|column| key_column_names.contains(&column.column_name));
            if all_key {
                continue;
            }

            for row in &mut rows.rows {
                let source_values = mem::take(&mut row.values);
                row.values = rows
                    .schema
                    .iter()
                    .zip(source_values)
                    .filter_map(|(column, v)| {
                        key_column_names.contains(&column.column_name).then_some(v)
                    })
                    .collect();
            }
            rows.schema
                .retain(|column| key_column_names.contains(&column.column_name));
        }
        Ok(requests)
    }

    fn key_column_names(&self, table: &TableRef) -> Result<HashSet<String>> {
        let time_index = table
            .schema()
            .timestamp_column()
            .with_context(|| table::error::MissingTimeIndexColumnSnafu {
                table_name: table.table_info().name.clone(),
            })
            .context(MissingTimeIndexColumnSnafu)?
            .name
            .clone();

        let key_column_names = table
            .table_info()
            .meta
            .row_key_column_names()
            .cloned()
            .chain(iter::once(time_index))
            .collect();

        Ok(key_column_names)
    }

    async fn get_table(&self, req: &RowDeleteRequest, ctx: &QueryContextRef) -> Result<TableRef> {
        self.catalog_manager
            .table(ctx.current_catalog(), ctx.current_schema(), &req.table_name)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: req.table_name.clone(),
            })
    }
}

fn validate_row_count_match(requests: &RowDeleteRequests) -> Result<()> {
    for request in &requests.deletes {
        let rows = request.rows.as_ref().unwrap();
        let column_count = rows.schema.len();
        ensure!(
            rows.rows.iter().all(|r| r.values.len() == column_count),
            InvalidDeleteRequestSnafu {
                reason: "row count mismatch"
            }
        )
    }
    Ok(())
}
