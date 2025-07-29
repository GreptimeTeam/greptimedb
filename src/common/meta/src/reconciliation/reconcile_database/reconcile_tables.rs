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

use common_procedure::{Context as ProcedureContext, ProcedureWithId, Status};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use store_api::storage::TableId;
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::error::Result;
use crate::key::table_route::TableRouteValue;
use crate::reconciliation::reconcile_database::reconcile_logical_tables::ReconcileLogicalTables;
use crate::reconciliation::reconcile_database::{ReconcileDatabaseContext, State};
use crate::reconciliation::reconcile_table::ReconcileTableProcedure;
use crate::reconciliation::utils::Context;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReconcileTables;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ReconcileTables {
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

        let pending_tables = &mut ctx.volatile_ctx.pending_tables;
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
            // Enqueue table.
            Self::enqueue_table(pending_tables, table_id, table_ref, table_route);
            // Schedule reconcile table procedures if the number of pending procedures
            // is greater than or equal to parallelism.
            if Self::should_schedule_reconcile_tables(pending_tables, parallelism) {
                return Self::schedule_reconcile_tables(ctx);
            }
        }

        // If there are remaining tables, schedule reconcile table procedures.
        if !pending_tables.is_empty() {
            return Self::schedule_reconcile_tables(ctx);
        }
        ctx.volatile_ctx.tables.take();
        Ok((Box::new(ReconcileLogicalTables), Status::executing(true)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReconcileTables {
    fn schedule_reconcile_tables(
        ctx: &mut ReconcileDatabaseContext,
    ) -> Result<(Box<dyn State>, Status)> {
        let tables = std::mem::take(&mut ctx.volatile_ctx.pending_tables);
        let subprocedures = Self::build_reconcile_table_procedures(ctx, tables);
        ctx.volatile_ctx
            .inflight_subprocedures
            .extend(subprocedures.iter().map(|p| p.id));

        Ok((
            Box::new(ReconcileTables),
            Status::suspended(subprocedures, false),
        ))
    }

    fn should_schedule_reconcile_tables(
        pending_tables: &[(TableId, TableName)],
        parallelism: usize,
    ) -> bool {
        pending_tables.len() >= parallelism
    }

    fn build_reconcile_table_procedures(
        ctx: &ReconcileDatabaseContext,
        tables: Vec<(TableId, TableName)>,
    ) -> Vec<ProcedureWithId> {
        let mut procedures = Vec::with_capacity(tables.len());
        for (table_id, table_name) in tables {
            let context = Context {
                node_manager: ctx.node_manager.clone(),
                table_metadata_manager: ctx.table_metadata_manager.clone(),
                cache_invalidator: ctx.cache_invalidator.clone(),
            };
            let procedure = ReconcileTableProcedure::new(
                context,
                table_id,
                table_name,
                ctx.persistent_ctx.resolve_strategy,
                true,
            );
            procedures.push(ProcedureWithId::with_random_id(Box::new(procedure)));
        }

        procedures
    }

    fn enqueue_table(
        tables: &mut Vec<(TableId, TableName)>,
        table_id: TableId,
        table_ref: TableReference<'_>,
        table_route: TableRouteValue,
    ) {
        if table_route.is_physical() {
            tables.push((table_id, table_ref.into()));
        }
    }
}
