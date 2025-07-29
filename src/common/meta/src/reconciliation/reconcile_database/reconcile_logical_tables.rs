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
use std::collections::HashMap;

use common_procedure::{Context as ProcedureContext, ProcedureWithId, Status};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use table::metadata::TableId;
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::error::{Result, TableInfoNotFoundSnafu};
use crate::key::table_route::TableRouteValue;
use crate::reconciliation::reconcile_database::end::ReconcileDatabaseEnd;
use crate::reconciliation::reconcile_database::{ReconcileDatabaseContext, State};
use crate::reconciliation::reconcile_logical_tables::ReconcileLogicalTablesProcedure;
use crate::reconciliation::utils::Context;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReconcileLogicalTables;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ReconcileLogicalTables {
    async fn next(
        &mut self,
        ctx: &mut ReconcileDatabaseContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        // Waits for inflight subprocedures first.
        ctx.wait_for_inflight_subprocedures(procedure_ctx).await?;

        let catalog = &ctx.persistent_ctx.catalog;
        let schema = &ctx.persistent_ctx.schema;
        let parallelism = ctx.persistent_ctx.parallelism;
        if ctx.volatile_ctx.tables.as_deref().is_none() {
            let tables = ctx
                .table_metadata_manager
                .table_name_manager()
                .tables(catalog, schema);
            ctx.volatile_ctx.tables = Some(tables);
        }

        let pending_logical_tables = &mut ctx.volatile_ctx.pending_logical_tables;
        let mut pending_procedures = Vec::with_capacity(parallelism);
        let context = Context {
            node_manager: ctx.node_manager.clone(),
            table_metadata_manager: ctx.table_metadata_manager.clone(),
            cache_invalidator: ctx.cache_invalidator.clone(),
        };
        // Safety: must exists.
        while let Some((table_name, table_name_value)) =
            ctx.volatile_ctx.tables.as_mut().unwrap().try_next().await?
        {
            let table_id = table_name_value.table_id();
            let Some(table_route) = ctx
                .table_metadata_manager
                .table_route_manager()
                .table_route_storage()
                .get(table_id)
                .await?
            else {
                continue;
            };

            let table_ref = TableReference::full(catalog, schema, &table_name);
            Self::enqueue_logical_table(pending_logical_tables, table_id, table_ref, table_route);
            // Try to build reconcile logical tables procedure.
            if let Some(procedure) = Self::try_build_reconcile_logical_tables_procedure(
                &context,
                pending_logical_tables,
                parallelism,
            )
            .await?
            {
                pending_procedures.push(procedure);
            }
            // Schedule reconcile logical tables procedures if the number of pending procedures
            // is greater than or equal to parallelism.
            if Self::should_schedule_reconcile_logical_tables(&pending_procedures, parallelism) {
                return Self::schedule_reconcile_logical_tables(ctx, &mut pending_procedures);
            }
        }

        // Build remaining procedures.
        Self::build_remaining_procedures(
            &context,
            pending_logical_tables,
            &mut pending_procedures,
            parallelism,
        )
        .await?;
        // If there are remaining procedures, schedule reconcile logical tables procedures.
        if !pending_procedures.is_empty() {
            return Self::schedule_reconcile_logical_tables(ctx, &mut pending_procedures);
        }

        ctx.volatile_ctx.tables.take();
        Ok((Box::new(ReconcileDatabaseEnd), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReconcileLogicalTables {
    fn schedule_reconcile_logical_tables(
        ctx: &mut ReconcileDatabaseContext,
        buffer: &mut Vec<ProcedureWithId>,
    ) -> Result<(Box<dyn State>, Status)> {
        let procedures = std::mem::take(buffer);
        ctx.volatile_ctx
            .inflight_subprocedures
            .extend(procedures.iter().map(|p| p.id));

        Ok((
            Box::new(ReconcileLogicalTables),
            Status::suspended(procedures, false),
        ))
    }

    fn should_schedule_reconcile_logical_tables(
        buffer: &[ProcedureWithId],
        parallelism: usize,
    ) -> bool {
        buffer.len() >= parallelism
    }

    async fn try_build_reconcile_logical_tables_procedure(
        ctx: &Context,
        pending_logical_tables: &mut HashMap<TableId, Vec<(TableId, TableName)>>,
        parallelism: usize,
    ) -> Result<Option<ProcedureWithId>> {
        let mut physical_table_id = None;
        for (table_id, tables) in pending_logical_tables.iter() {
            if tables.len() >= parallelism {
                physical_table_id = Some(*table_id);
                break;
            }
        }

        if let Some(physical_table_id) = physical_table_id {
            // Safety: The key  exists.
            let tables = pending_logical_tables.remove(&physical_table_id).unwrap();
            return Ok(Some(
                Self::build_reconcile_logical_tables_procedure(ctx, physical_table_id, tables)
                    .await?,
            ));
        }

        Ok(None)
    }

    async fn build_remaining_procedures(
        ctx: &Context,
        pending_logical_tables: &mut HashMap<TableId, Vec<(TableId, TableName)>>,
        pending_procedures: &mut Vec<ProcedureWithId>,
        parallelism: usize,
    ) -> Result<()> {
        if pending_logical_tables.is_empty() {
            return Ok(());
        }

        while let Some(physical_table_id) = pending_logical_tables.keys().next().cloned() {
            if pending_procedures.len() >= parallelism {
                return Ok(());
            }

            // Safety: must exist.
            let tables = pending_logical_tables.remove(&physical_table_id).unwrap();
            pending_procedures.push(
                Self::build_reconcile_logical_tables_procedure(ctx, physical_table_id, tables)
                    .await?,
            );
        }

        Ok(())
    }

    async fn build_reconcile_logical_tables_procedure(
        ctx: &Context,
        physical_table_id: TableId,
        logical_tables: Vec<(TableId, TableName)>,
    ) -> Result<ProcedureWithId> {
        let table_info = ctx
            .table_metadata_manager
            .table_info_manager()
            .get(physical_table_id)
            .await?
            .context(TableInfoNotFoundSnafu {
                table: format!("table_id: {}", physical_table_id),
            })?;

        let physical_table_name = table_info.table_name();
        let procedure = ReconcileLogicalTablesProcedure::new(
            ctx.clone(),
            physical_table_id,
            physical_table_name,
            logical_tables,
            true,
        );

        Ok(ProcedureWithId::with_random_id(Box::new(procedure)))
    }

    fn enqueue_logical_table(
        tables: &mut HashMap<TableId, Vec<(TableId, TableName)>>,
        table_id: TableId,
        table_ref: TableReference<'_>,
        table_route: TableRouteValue,
    ) {
        if !table_route.is_physical() {
            let logical_table_route = table_route.into_logical_table_route();
            let physical_table_id = logical_table_route.physical_table_id();
            tables
                .entry(physical_table_id)
                .or_default()
                .push((table_id, table_ref.into()));
        }
    }
}
