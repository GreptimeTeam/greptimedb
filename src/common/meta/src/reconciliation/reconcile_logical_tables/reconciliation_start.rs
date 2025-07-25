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

use std::any::Any;

use common_procedure::{Context as ProcedureContext, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt};
use store_api::storage::TableId;
use table::table_name::TableName;

use crate::ddl::utils::region_metadata_lister::RegionMetadataLister;
use crate::ddl::utils::table_id::get_all_table_ids_by_names;
use crate::ddl::utils::table_info::all_logical_table_routes_have_same_physical_id;
use crate::error::{self, Result};
use crate::reconciliation::reconcile_logical_tables::resolve_table_metadatas::ResolveTableMetadatas;
use crate::reconciliation::reconcile_logical_tables::{ReconcileLogicalTablesContext, State};
use crate::reconciliation::utils::{check_column_metadatas_consistent, validate_table_id_and_name};

/// The start state of the reconciliation procedure.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReconciliationStart;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ReconciliationStart {
    async fn next(
        &mut self,
        ctx: &mut ReconcileLogicalTablesContext,
        _procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let table_id = ctx.table_id();
        let table_name = ctx.table_name();

        validate_table_id_and_name(
            ctx.table_metadata_manager.table_name_manager(),
            table_id,
            table_name,
        )
        .await?;

        let (physical_table_id, physical_table_route) = ctx
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await?;
        ensure!(
            physical_table_id == table_id,
            error::UnexpectedSnafu {
                err_msg: format!(
                    "Expected physical table: {}, but it's a logical table of table: {}",
                    table_name, physical_table_id
                ),
            }
        );

        info!(
            "Starting reconciliation for logical table: table_id: {}, table_name: {}",
            table_id, table_name
        );

        let region_metadata_lister = RegionMetadataLister::new(ctx.node_manager.clone());
        let region_metadatas = region_metadata_lister
            .list(physical_table_id, &physical_table_route.region_routes)
            .await?;

        ensure!(
            !region_metadatas.is_empty(),
            error::UnexpectedSnafu {
                err_msg: format!(
                    "No region metadata found for table: {}, table_id: {}",
                    table_name, table_id
                ),
            }
        );

        if region_metadatas.iter().any(|r| r.is_none()) {
            return error::UnexpectedSnafu {
                err_msg: format!(
                    "Some regions of the physical table are not open. Table: {}, table_id: {}",
                    table_name, table_id
                ),
            }
            .fail();
        }

        // Safety: must exist
        let region_metadatas = region_metadatas
            .into_iter()
            .map(|r| r.unwrap())
            .collect::<Vec<_>>();
        let _region_metadata = check_column_metadatas_consistent(&region_metadatas).context(
            error::UnexpectedSnafu {
                err_msg: format!(
                    "Column metadatas are not consistent for table: {}, table_id: {}",
                    table_name, table_id
                ),
            },
        )?;

        // TODO(weny): ensure all columns in region metadata can be found in table info.

        // Validates the logical tables.
        Self::validate_schema(&ctx.persistent_ctx.logical_tables)?;
        let table_refs = ctx
            .persistent_ctx
            .logical_tables
            .iter()
            .map(|t| t.table_ref())
            .collect::<Vec<_>>();
        let table_ids = get_all_table_ids_by_names(
            ctx.table_metadata_manager.table_name_manager(),
            &table_refs,
        )
        .await?;
        Self::validate_logical_table_routes(ctx, &table_ids).await?;

        ctx.persistent_ctx.physical_table_route = Some(physical_table_route);
        ctx.persistent_ctx.logical_table_ids = table_ids;
        Ok((Box::new(ResolveTableMetadatas), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReconciliationStart {
    /// Validates all the logical tables have the same catalog and schema.
    fn validate_schema(logical_tables: &[TableName]) -> Result<()> {
        let is_same_schema = logical_tables.windows(2).all(|pair| {
            pair[0].catalog_name == pair[1].catalog_name
                && pair[0].schema_name == pair[1].schema_name
        });

        ensure!(
            is_same_schema,
            error::UnexpectedSnafu {
                err_msg: "The logical tables have different schemas",
            }
        );

        Ok(())
    }

    async fn validate_logical_table_routes(
        ctx: &mut ReconcileLogicalTablesContext,
        table_ids: &[TableId],
    ) -> Result<()> {
        let all_logical_table_routes_have_same_physical_id =
            all_logical_table_routes_have_same_physical_id(
                ctx.table_metadata_manager.table_route_manager(),
                table_ids,
                ctx.table_id(),
            )
            .await?;

        ensure!(
            all_logical_table_routes_have_same_physical_id,
            error::UnexpectedSnafu {
                err_msg: "All the logical tables should have the same physical table id",
            }
        );

        Ok(())
    }
}
