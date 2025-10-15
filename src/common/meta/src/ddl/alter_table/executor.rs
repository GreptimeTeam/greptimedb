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

use std::collections::HashMap;

use api::region::RegionResponse;
use api::v1::AlterTableExpr;
use api::v1::region::region_request::Body;
use api::v1::region::{AlterRequest, RegionRequest, RegionRequestHeader, alter_request};
use common_catalog::format_full_table_name;
use common_grpc_expr::alter_expr_to_request;
use common_telemetry::tracing_context::TracingContext;
use common_telemetry::{debug, info};
use futures::future;
use snafu::{ResultExt, ensure};
use store_api::metadata::ColumnMetadata;
use store_api::storage::{RegionId, TableId};
use table::metadata::{RawTableInfo, TableInfo};
use table::requests::AlterKind;
use table::table_name::TableName;

use crate::cache_invalidator::{CacheInvalidatorRef, Context};
use crate::ddl::utils::{add_peer_context_if_needed, raw_table_info};
use crate::error::{self, Result, UnexpectedSnafu};
use crate::instruction::CacheIdent;
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::{DeserializedValueWithBytes, RegionDistribution, TableMetadataManagerRef};
use crate::node_manager::NodeManagerRef;
use crate::rpc::router::{RegionRoute, find_leaders, region_distribution};

/// [AlterTableExecutor] performs:
/// - Alters the metadata of the table.
/// - Alters regions on the datanode nodes.
pub struct AlterTableExecutor {
    table: TableName,
    table_id: TableId,
    /// The new table name if the alter kind is rename table.
    new_table_name: Option<String>,
}

impl AlterTableExecutor {
    /// Creates a new [`AlterTableExecutor`].
    pub fn new(table: TableName, table_id: TableId, new_table_name: Option<String>) -> Self {
        Self {
            table,
            table_id,
            new_table_name,
        }
    }

    /// Prepares to alter the table.
    ///
    /// ## Checks:
    /// - The new table name doesn't exist (rename).
    /// - Table exists.
    pub(crate) async fn on_prepare(
        &self,
        table_metadata_manager: &TableMetadataManagerRef,
    ) -> Result<()> {
        let catalog = &self.table.catalog_name;
        let schema = &self.table.schema_name;
        let table_name = &self.table.table_name;

        let manager = table_metadata_manager;
        if let Some(new_table_name) = &self.new_table_name {
            let new_table_name_key = TableNameKey::new(catalog, schema, new_table_name);
            let exists = manager
                .table_name_manager()
                .exists(new_table_name_key)
                .await?;
            ensure!(
                !exists,
                error::TableAlreadyExistsSnafu {
                    table_name: format_full_table_name(catalog, schema, new_table_name),
                }
            )
        }

        let table_name_key = TableNameKey::new(catalog, schema, table_name);
        let exists = manager.table_name_manager().exists(table_name_key).await?;
        ensure!(
            exists,
            error::TableNotFoundSnafu {
                table_name: format_full_table_name(catalog, schema, table_name),
            }
        );

        Ok(())
    }

    /// Validates the alter table expression and builds the new table info.
    ///
    /// This validation is performed early to ensure the alteration is valid before
    /// proceeding to the `on_alter_metadata` state, where regions have already been altered.
    /// Building the new table info here allows us to catch any issues with the
    /// alteration before committing metadata changes.
    pub(crate) fn validate_alter_table_expr(
        table_info: &RawTableInfo,
        alter_table_expr: AlterTableExpr,
    ) -> Result<TableInfo> {
        build_new_table_info(table_info, alter_table_expr)
    }

    /// Updates table metadata for alter table operation.
    pub(crate) async fn on_alter_metadata(
        &self,
        table_metadata_manager: &TableMetadataManagerRef,
        current_table_info_value: &DeserializedValueWithBytes<TableInfoValue>,
        region_distribution: Option<&RegionDistribution>,
        mut raw_table_info: RawTableInfo,
        column_metadatas: &[ColumnMetadata],
    ) -> Result<()> {
        let table_ref = self.table.table_ref();
        let table_id = self.table_id;

        if let Some(new_table_name) = &self.new_table_name {
            debug!(
                "Starting update table: {} metadata, table_id: {}, new table info: {:?}, new table name: {}",
                table_ref, table_id, raw_table_info, new_table_name
            );

            table_metadata_manager
                .rename_table(current_table_info_value, new_table_name.clone())
                .await?;
        } else {
            debug!(
                "Starting update table: {} metadata, table_id: {}, new table info: {:?}",
                table_ref, table_id, raw_table_info
            );

            ensure!(
                region_distribution.is_some(),
                UnexpectedSnafu {
                    err_msg: "region distribution is not set when updating table metadata",
                }
            );

            if !column_metadatas.is_empty() {
                raw_table_info::update_table_info_column_ids(&mut raw_table_info, column_metadatas);
            }
            table_metadata_manager
                .update_table_info(
                    current_table_info_value,
                    region_distribution.cloned(),
                    raw_table_info,
                )
                .await?;
        }

        Ok(())
    }

    /// Alters regions on the datanode nodes.
    pub(crate) async fn on_alter_regions(
        &self,
        node_manager: &NodeManagerRef,
        region_routes: &[RegionRoute],
        kind: Option<alter_request::Kind>,
    ) -> Vec<Result<RegionResponse>> {
        let region_distribution = region_distribution(region_routes);
        let leaders = find_leaders(region_routes)
            .into_iter()
            .map(|p| (p.id, p))
            .collect::<HashMap<_, _>>();
        let total_num_region = region_distribution
            .values()
            .map(|r| r.leader_regions.len())
            .sum::<usize>();
        let mut alter_region_tasks = Vec::with_capacity(total_num_region);
        for (datanode_id, region_role_set) in region_distribution {
            if region_role_set.leader_regions.is_empty() {
                continue;
            }
            // Safety: must exists.
            let peer = leaders.get(&datanode_id).unwrap();
            let requester = node_manager.datanode(peer).await;

            for region_id in region_role_set.leader_regions {
                let region_id = RegionId::new(self.table_id, region_id);
                let request = make_alter_region_request(region_id, kind.clone());

                let requester = requester.clone();
                let peer = peer.clone();

                alter_region_tasks.push(async move {
                    requester
                        .handle(request)
                        .await
                        .map_err(add_peer_context_if_needed(peer))
                });
            }
        }

        future::join_all(alter_region_tasks)
            .await
            .into_iter()
            .collect::<Vec<_>>()
    }

    /// Invalidates cache for the table.
    pub(crate) async fn invalidate_table_cache(
        &self,
        cache_invalidator: &CacheInvalidatorRef,
    ) -> Result<()> {
        let ctx = Context {
            subject: Some(format!(
                "Invalidate table cache by altering table {}, table_id: {}",
                self.table.table_ref(),
                self.table_id,
            )),
        };

        cache_invalidator
            .invalidate(
                &ctx,
                &[
                    CacheIdent::TableName(self.table.clone()),
                    CacheIdent::TableId(self.table_id),
                ],
            )
            .await?;

        Ok(())
    }
}

/// Makes alter region request.
pub(crate) fn make_alter_region_request(
    region_id: RegionId,
    kind: Option<alter_request::Kind>,
) -> RegionRequest {
    RegionRequest {
        header: Some(RegionRequestHeader {
            tracing_context: TracingContext::from_current_span().to_w3c(),
            ..Default::default()
        }),
        body: Some(Body::Alter(AlterRequest {
            region_id: region_id.as_u64(),
            kind,
            ..Default::default()
        })),
    }
}

/// Builds new table info after alteration.
///
/// This function creates a new table info by applying the alter table expression
/// to the existing table info. For add column operations, it increments the
/// `next_column_id` by the number of columns being added, which may result in gaps
/// in the column id sequence.
fn build_new_table_info(
    table_info: &RawTableInfo,
    alter_table_expr: AlterTableExpr,
) -> Result<TableInfo> {
    let table_info =
        TableInfo::try_from(table_info.clone()).context(error::ConvertRawTableInfoSnafu)?;
    let schema_name = &table_info.schema_name;
    let catalog_name = &table_info.catalog_name;
    let table_name = &table_info.name;
    let table_id = table_info.ident.table_id;
    let request = alter_expr_to_request(table_id, alter_table_expr, Some(&table_info.meta))
        .context(error::ConvertAlterTableRequestSnafu)?;

    let new_meta = table_info
        .meta
        .builder_with_alter_kind(table_name, &request.alter_kind)
        .context(error::TableSnafu)?
        .build()
        .with_context(|_| error::BuildTableMetaSnafu {
            table_name: format_full_table_name(catalog_name, schema_name, table_name),
        })?;

    let mut new_info = table_info.clone();
    new_info.meta = new_meta;
    new_info.ident.version = table_info.ident.version + 1;
    match request.alter_kind {
        AlterKind::AddColumns { columns } => {
            // Bumps the column id for the new columns.
            // It may bump more than the actual number of columns added if there are
            // existing columns, but it's fine.
            new_info.meta.next_column_id += columns.len() as u32;
        }
        AlterKind::RenameTable { new_table_name } => {
            new_info.name = new_table_name.clone();
        }
        AlterKind::DropColumns { .. }
        | AlterKind::ModifyColumnTypes { .. }
        | AlterKind::SetTableOptions { .. }
        | AlterKind::UnsetTableOptions { .. }
        | AlterKind::SetIndexes { .. }
        | AlterKind::UnsetIndexes { .. }
        | AlterKind::DropDefaults { .. }
        | AlterKind::SetDefaults { .. } => {}
    }

    info!(
        "Built new table info: {:?} for table {}, table_id: {}",
        new_info.meta, table_name, table_id
    );
    Ok(new_info)
}
