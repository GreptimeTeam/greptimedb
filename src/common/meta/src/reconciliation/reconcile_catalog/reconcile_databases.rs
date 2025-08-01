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

use crate::error::Result;
use crate::reconciliation::reconcile_catalog::end::ReconcileCatalogEnd;
use crate::reconciliation::reconcile_catalog::{ReconcileCatalogContext, State};
use crate::reconciliation::reconcile_database::{ReconcileDatabaseProcedure, DEFAULT_PARALLELISM};
use crate::reconciliation::utils::Context;

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ReconcileDatabases;

#[async_trait::async_trait]
#[typetag::serde]
impl State for ReconcileDatabases {
    async fn next(
        &mut self,
        ctx: &mut ReconcileCatalogContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)> {
        // Waits for inflight subprocedure first.
        ctx.wait_for_inflight_subprocedure(procedure_ctx).await?;

        if ctx.volatile_ctx.schemas.as_deref().is_none() {
            let schemas = ctx
                .table_metadata_manager
                .schema_manager()
                .schema_names(&ctx.persistent_ctx.catalog);
            ctx.volatile_ctx.schemas = Some(schemas);
        }

        if let Some(catalog) = ctx
            .volatile_ctx
            .schemas
            .as_mut()
            .unwrap()
            .try_next()
            .await?
        {
            return Self::schedule_reconcile_database(ctx, catalog);
        }

        Ok((Box::new(ReconcileCatalogEnd), Status::executing(false)))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ReconcileDatabases {
    fn schedule_reconcile_database(
        ctx: &mut ReconcileCatalogContext,
        schema: String,
    ) -> Result<(Box<dyn State>, Status)> {
        let context = Context {
            node_manager: ctx.node_manager.clone(),
            table_metadata_manager: ctx.table_metadata_manager.clone(),
            cache_invalidator: ctx.cache_invalidator.clone(),
        };
        let procedure = ReconcileDatabaseProcedure::new(
            context,
            ctx.persistent_ctx.catalog.clone(),
            schema,
            ctx.persistent_ctx.fast_fail,
            DEFAULT_PARALLELISM,
            ctx.persistent_ctx.resolve_strategy,
            true,
        );
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));

        Ok((
            Box::new(ReconcileDatabases),
            Status::suspended(vec![procedure_with_id], false),
        ))
    }
}
