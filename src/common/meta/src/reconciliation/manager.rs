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

use std::sync::Arc;

use common_procedure::{
    BoxedProcedure, ProcedureId, ProcedureManagerRef, ProcedureWithId, watcher,
};
use common_telemetry::{error, info, warn};
use snafu::{OptionExt, ResultExt};
use store_api::storage::TableId;
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::{self, Result, TableNotFoundSnafu};
use crate::key::TableMetadataManagerRef;
use crate::key::table_name::TableNameKey;
use crate::reconciliation::reconcile_catalog::ReconcileCatalogProcedure;
use crate::reconciliation::reconcile_database::{DEFAULT_PARALLELISM, ReconcileDatabaseProcedure};
use crate::reconciliation::reconcile_logical_tables::ReconcileLogicalTablesProcedure;
use crate::reconciliation::reconcile_table::ReconcileTableProcedure;
use crate::reconciliation::reconcile_table::resolve_column_metadata::ResolveStrategy;
use crate::reconciliation::utils::Context;
use crate::region_rpc::RegionRpcRef;

pub type ReconciliationManagerRef = Arc<ReconciliationManager>;

/// The manager for reconciliation procedures.
pub struct ReconciliationManager {
    procedure_manager: ProcedureManagerRef,
    context: Context,
}

macro_rules! register_reconcile_loader {
    ($self:ident, $procedure:ty) => {{
        let context = $self.context.clone();
        $self
            .procedure_manager
            .register_loader(
                <$procedure>::TYPE_NAME,
                Box::new(move |json| {
                    let context = context.clone();
                    let procedure = <$procedure>::from_json(context, json)?;
                    Ok(Box::new(procedure))
                }),
            )
            .context(error::RegisterProcedureLoaderSnafu {
                type_name: <$procedure>::TYPE_NAME,
            })?;
    }};
}

impl ReconciliationManager {
    pub fn new(
        region_rpc: RegionRpcRef,
        table_metadata_manager: TableMetadataManagerRef,
        cache_invalidator: CacheInvalidatorRef,
        procedure_manager: ProcedureManagerRef,
    ) -> Self {
        Self {
            procedure_manager,
            context: Context {
                region_rpc,
                table_metadata_manager,
                cache_invalidator,
            },
        }
    }

    /// Try to start the reconciliation manager.
    ///
    /// This function will register the procedure loaders for the reconciliation procedures.
    /// Returns an error if the procedure loaders are already registered.
    pub fn try_start(&self) -> Result<()> {
        register_reconcile_loader!(self, ReconcileLogicalTablesProcedure);
        register_reconcile_loader!(self, ReconcileTableProcedure);
        register_reconcile_loader!(self, ReconcileDatabaseProcedure);
        register_reconcile_loader!(self, ReconcileCatalogProcedure);

        Ok(())
    }

    /// Reconcile a table.
    ///
    /// Returns the procedure id of the reconciliation procedure.
    pub async fn reconcile_table(
        &self,
        table_ref: TableReference<'_>,
        resolve_strategy: ResolveStrategy,
    ) -> Result<ProcedureId> {
        let table_name_key =
            TableNameKey::new(table_ref.catalog, table_ref.schema, table_ref.table);
        let table_metadata_manager = &self.context.table_metadata_manager;
        let table_id = table_metadata_manager
            .table_name_manager()
            .get(table_name_key)
            .await?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?
            .table_id();
        let (physical_table_id, _) = table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await?;

        if physical_table_id == table_id {
            self.reconcile_physical_table(table_id, table_ref.into(), resolve_strategy)
                .await
        } else {
            let physical_table_info = table_metadata_manager
                .table_info_manager()
                .get(physical_table_id)
                .await?
                .with_context(|| TableNotFoundSnafu {
                    table_name: format!("table_id: {}", physical_table_id),
                })?;

            self.reconcile_logical_tables(
                physical_table_id,
                physical_table_info.table_name(),
                vec![(table_id, table_ref.into())],
            )
            .await
        }
    }

    /// Reconcile a database.
    ///
    /// Returns the procedure id of the reconciliation procedure.
    pub async fn reconcile_database(
        &self,
        catalog: String,
        schema: String,
        resolve_strategy: ResolveStrategy,
        parallelism: usize,
    ) -> Result<ProcedureId> {
        let parallelism = normalize_parallelism(parallelism);
        let procedure = ReconcileDatabaseProcedure::new(
            self.context.clone(),
            catalog,
            schema,
            false,
            parallelism,
            resolve_strategy,
            false,
        );
        self.spawn_procedure(Box::new(procedure)).await
    }

    async fn reconcile_physical_table(
        &self,
        table_id: TableId,
        table_name: TableName,
        resolve_strategy: ResolveStrategy,
    ) -> Result<ProcedureId> {
        let procedure = ReconcileTableProcedure::new(
            self.context.clone(),
            table_id,
            table_name,
            resolve_strategy,
            false,
        );
        self.spawn_procedure(Box::new(procedure)).await
    }

    async fn reconcile_logical_tables(
        &self,
        physical_table_id: TableId,
        physical_table_name: TableName,
        logical_tables: Vec<(TableId, TableName)>,
    ) -> Result<ProcedureId> {
        let procedure = ReconcileLogicalTablesProcedure::new(
            self.context.clone(),
            physical_table_id,
            physical_table_name,
            logical_tables,
            false,
        );
        self.spawn_procedure(Box::new(procedure)).await
    }

    /// Reconcile a catalog.
    ///
    /// Returns the procedure id of the reconciliation procedure.
    pub async fn reconcile_catalog(
        &self,
        catalog: String,
        resolve_strategy: ResolveStrategy,
        parallelism: usize,
    ) -> Result<ProcedureId> {
        let parallelism = normalize_parallelism(parallelism);
        let procedure = ReconcileCatalogProcedure::new(
            self.context.clone(),
            catalog,
            false,
            resolve_strategy,
            parallelism,
        );
        self.spawn_procedure(Box::new(procedure)).await
    }

    async fn spawn_procedure(&self, procedure: BoxedProcedure) -> Result<ProcedureId> {
        let procedure_manager = self.procedure_manager.clone();
        let procedure_with_id = ProcedureWithId::with_random_id(procedure);
        let procedure_id = procedure_with_id.id;
        let mut watcher = procedure_manager
            .submit(procedure_with_id)
            .await
            .context(error::SubmitProcedureSnafu)?;
        common_runtime::spawn_global(async move {
            if let Err(e) = watcher::wait(&mut watcher).await {
                error!(e; "Failed to wait reconciliation procedure {procedure_id}");
                return;
            }

            info!("Reconciliation procedure {procedure_id} is finished successfully!");
        });
        Ok(procedure_id)
    }
}

fn normalize_parallelism(parallelism: usize) -> usize {
    if parallelism == 0 {
        warn!(
            "Parallelism is 0, using default parallelism: {}",
            DEFAULT_PARALLELISM
        );
        DEFAULT_PARALLELISM
    } else {
        parallelism
    }
}
