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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::{iter, mem};

use api::v1::region::{DeleteRequests as RegionDeleteRequests, RegionRequestHeader};
use api::v1::{DeleteRequests, RowDeleteRequests};
use catalog::CatalogManagerRef;
use common_meta::node_manager::{AffectedRows, NodeManagerRef};
use common_meta::peer::Peer;
use common_query::Output;
use common_telemetry::tracing_context::TracingContext;
use futures_util::future;
use partition::manager::PartitionRuleManagerRef;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use table::requests::DeleteRequest as TableDeleteRequest;
use table::TableRef;

use crate::error::{
    CatalogSnafu, FindRegionLeaderSnafu, InvalidDeleteRequestSnafu, JoinTaskSnafu,
    MissingTimeIndexColumnSnafu, RequestDeletesSnafu, Result, TableNotFoundSnafu,
};
use crate::region_req_factory::RegionRequestFactory;
use crate::req_convert::delete::{ColumnToRow, RowToRegion, TableToRegion};

pub struct Deleter {
    catalog_manager: CatalogManagerRef,
    partition_manager: PartitionRuleManagerRef,
    node_manager: NodeManagerRef,
}

pub type DeleterRef = Arc<Deleter>;

impl Deleter {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        partition_manager: PartitionRuleManagerRef,
        node_manager: NodeManagerRef,
    ) -> Self {
        Self {
            catalog_manager,
            partition_manager,
            node_manager,
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
        validate_column_count_match(&requests)?;

        let requests = self.trim_columns(requests, &ctx).await?;
        let deletes = RowToRegion::new(
            self.catalog_manager.as_ref(),
            self.partition_manager.as_ref(),
            &ctx,
        )
        .convert(requests)
        .await?;

        let affected_rows = self.do_request(deletes, &ctx).await?;

        Ok(Output::new_with_affected_rows(affected_rows))
    }

    pub async fn handle_table_delete(
        &self,
        request: TableDeleteRequest,
        ctx: QueryContextRef,
    ) -> Result<AffectedRows> {
        let catalog = request.catalog_name.as_str();
        let schema = request.schema_name.as_str();
        let table = request.table_name.as_str();
        let table = self.get_table(catalog, schema, table).await?;
        let table_info = table.table_info();

        let deletes = TableToRegion::new(&table_info, &self.partition_manager)
            .convert(request)
            .await?;

        let affected_rows = self.do_request(deletes, &ctx).await?;
        Ok(affected_rows as _)
    }
}

impl Deleter {
    async fn do_request(
        &self,
        requests: RegionDeleteRequests,
        ctx: &QueryContextRef,
    ) -> Result<AffectedRows> {
        let request_factory = RegionRequestFactory::new(RegionRequestHeader {
            tracing_context: TracingContext::from_current_span().to_w3c(),
            dbname: ctx.get_db_string(),
            ..Default::default()
        });

        let tasks = self
            .group_requests_by_peer(requests)
            .await?
            .into_iter()
            .map(|(peer, deletes)| {
                let request = request_factory.build_delete(deletes);
                let node_manager = self.node_manager.clone();
                common_runtime::spawn_global(async move {
                    node_manager
                        .datanode(&peer)
                        .await
                        .handle(request)
                        .await
                        .context(RequestDeletesSnafu)
                })
            });
        let results = future::try_join_all(tasks).await.context(JoinTaskSnafu)?;

        let affected_rows = results
            .into_iter()
            .map(|resp| resp.map(|r| r.affected_rows))
            .sum::<Result<AffectedRows>>()?;
        crate::metrics::DIST_DELETE_ROW_COUNT.inc_by(affected_rows as u64);
        Ok(affected_rows)
    }

    async fn group_requests_by_peer(
        &self,
        requests: RegionDeleteRequests,
    ) -> Result<HashMap<Peer, RegionDeleteRequests>> {
        let mut deletes: HashMap<Peer, RegionDeleteRequests> = HashMap::new();

        for req in requests.requests {
            let peer = self
                .partition_manager
                .find_region_leader(req.region_id.into())
                .await
                .context(FindRegionLeaderSnafu)?;
            deletes.entry(peer).or_default().requests.push(req);
        }

        Ok(deletes)
    }

    async fn trim_columns(
        &self,
        mut requests: RowDeleteRequests,
        ctx: &QueryContextRef,
    ) -> Result<RowDeleteRequests> {
        for req in &mut requests.deletes {
            let catalog = ctx.current_catalog();
            let schema = ctx.current_schema();
            let table = self.get_table(catalog, &schema, &req.table_name).await?;
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

    async fn get_table(&self, catalog: &str, schema: &str, table: &str) -> Result<TableRef> {
        self.catalog_manager
            .table(catalog, schema, table)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: common_catalog::format_full_table_name(catalog, schema, table),
            })
    }
}

fn validate_column_count_match(requests: &RowDeleteRequests) -> Result<()> {
    for request in &requests.deletes {
        let rows = request.rows.as_ref().unwrap();
        let column_count = rows.schema.len();
        rows.rows.iter().try_for_each(|r| {
            ensure!(
                r.values.len() == column_count,
                InvalidDeleteRequestSnafu {
                    reason: format!(
                        "column count mismatch, columns: {}, values: {}",
                        column_count,
                        r.values.len()
                    )
                }
            );
            Ok(())
        })?;
    }
    Ok(())
}
