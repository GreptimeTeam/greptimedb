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
use snafu::ensure;

use crate::ddl::utils::region_metadata_lister::RegionMetadataLister;
use crate::error::{self, Result, UnexpectedSnafu};
use crate::reconciliation::reconcile_table::resolve_column_metadata::ResolveColumnMetadata;
use crate::reconciliation::reconcile_table::{ReconcileTableContext, State};
use crate::reconciliation::utils::validate_table_id_and_name;

/// The start state of the reconciliation procedure.
///
/// This state is used to prepare the table for reconciliation.
/// It will:
/// 1. Check the table id and table name consistency.
/// 2. Ensures the table is a physical table.
/// 3. List the region metadatas for the physical table.
#[derive(Debug, Serialize, Deserialize)]
pub struct ReconciliationStart;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ReconciliationStart {
    async fn next(
        &mut self,
        ctx: &mut ReconcileTableContext,
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
                    "Reconcile table only works for physical table, but got logical table: {}, table_id: {}",
                    table_name, table_id
                ),
            }
        );

        info!("Reconciling table: {}, table_id: {}", table_name, table_id);
        // TODO(weny): Repairs the table route if needed.
        let region_metadata_lister = RegionMetadataLister::new(ctx.node_manager.clone());
        // Always list region metadatas for the physical table.
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

        // If some regions are not opened, we should try to open them.
        if region_metadatas.iter().any(|r| r.is_none()) {
            // TODO(weny): Try our best to open regions before reconciliation.
            return UnexpectedSnafu {
                err_msg: format!(
                    "Some regions are not opened, table: {}, table_id: {}",
                    table_name, table_id
                ),
            }
            .fail();
        }

        // Persist the physical table route.
        // TODO(weny): refetch the physical table route if repair is needed.
        ctx.persistent_ctx.physical_table_route = Some(physical_table_route);
        let region_metadatas = region_metadatas.into_iter().map(|r| r.unwrap()).collect();
        Ok((
            Box::new(ResolveColumnMetadata::new(
                ctx.persistent_ctx.resolve_strategy,
                region_metadatas,
            )),
            // We don't persist the state of this step.
            Status::executing(false),
        ))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
