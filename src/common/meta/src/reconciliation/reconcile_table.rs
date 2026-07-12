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

pub(crate) mod reconcile_regions;
pub(crate) mod reconciliation_end;
pub(crate) mod reconciliation_start;
pub(crate) mod resolve_column_metadata;
pub(crate) mod update_table_info;

use std::any::Any;
use std::fmt::Debug;

use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::metadata::ColumnMetadata;
use store_api::storage::TableId;
use table::metadata::TableMeta;
use table::table_name::TableName;
use tonic::async_trait;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::Result;
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::PhysicalTableRouteValue;
use crate::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use crate::lock_key::{CatalogLock, SchemaLock, TableNameLock};
use crate::metrics;
use crate::node_manager::NodeManagerRef;
use crate::reconciliation::reconcile_table::reconciliation_start::ReconciliationStart;
use crate::reconciliation::reconcile_table::resolve_column_metadata::ResolveStrategy;
use crate::reconciliation::utils::{
    Context, ReconcileTableMetrics, build_table_meta_from_column_metadatas,
};

pub struct ReconcileTableContext {
    pub node_manager: NodeManagerRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    pub persistent_ctx: PersistentContext,
    pub volatile_ctx: VolatileContext,
}

impl ReconcileTableContext {
    /// Creates a new [`ReconcileTableContext`] with the given [`Context`] and [`PersistentContext`].
    pub fn new(ctx: Context, persistent_ctx: PersistentContext) -> Self {
        Self {
            node_manager: ctx.node_manager,
            table_metadata_manager: ctx.table_metadata_manager,
            cache_invalidator: ctx.cache_invalidator,
            persistent_ctx,
            volatile_ctx: VolatileContext::default(),
        }
    }

    /// Returns the physical table name.
    pub(crate) fn table_name(&self) -> &TableName {
        &self.persistent_ctx.table_name
    }

    /// Returns the physical table id.
    pub(crate) fn table_id(&self) -> TableId {
        self.persistent_ctx.table_id
    }

    /// Builds a [`TableMeta`] from the provided [`ColumnMetadata`]s.
    pub(crate) fn build_table_meta(
        &self,
        column_metadatas: &[ColumnMetadata],
    ) -> Result<TableMeta> {
        // Safety: The table info value is set in `ReconciliationStart` state.
        let table_info_value = self.persistent_ctx.table_info_value.as_ref().unwrap();
        let table_id = self.table_id();
        let table_ref = self.table_name().table_ref();
        let name_to_ids = table_info_value.table_info.name_to_ids();
        let table_meta = build_table_meta_from_column_metadatas(
            table_id,
            table_ref,
            &table_info_value.table_info.meta,
            name_to_ids,
            column_metadatas,
        )?;

        Ok(table_meta)
    }

    /// Returns a mutable reference to the metrics.
    pub(crate) fn mut_metrics(&mut self) -> &mut ReconcileTableMetrics {
        &mut self.volatile_ctx.metrics
    }

    /// Returns a reference to the metrics.
    pub(crate) fn metrics(&self) -> &ReconcileTableMetrics {
        &self.volatile_ctx.metrics
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistentContext {
    pub(crate) table_id: TableId,
    pub(crate) table_name: TableName,
    pub(crate) resolve_strategy: ResolveStrategy,
    /// The table info value.
    /// The value will be set in `ReconciliationStart` state.
    pub(crate) table_info_value: Option<DeserializedValueWithBytes<TableInfoValue>>,
    // The physical table route.
    // The value will be set in `ReconciliationStart` state.
    pub(crate) physical_table_route: Option<PhysicalTableRouteValue>,
    // Whether the procedure is a subprocedure.
    pub(crate) is_subprocedure: bool,
}

impl PersistentContext {
    pub(crate) fn new(
        table_id: TableId,
        table_name: TableName,
        resolve_strategy: ResolveStrategy,
        is_subprocedure: bool,
    ) -> Self {
        Self {
            table_id,
            table_name,
            resolve_strategy,
            table_info_value: None,
            physical_table_route: None,
            is_subprocedure,
        }
    }
}

#[derive(Default)]
pub(crate) struct VolatileContext {
    pub(crate) table_meta: Option<TableMeta>,
    pub(crate) metrics: ReconcileTableMetrics,
}

pub struct ReconcileTableProcedure {
    pub context: ReconcileTableContext,
    state: Box<dyn State>,
}

impl ReconcileTableProcedure {
    /// Creates a new [`ReconcileTableProcedure`] with the given [`Context`] and [`PersistentContext`].
    pub fn new(
        ctx: Context,
        table_id: TableId,
        table_name: TableName,
        resolve_strategy: ResolveStrategy,
        is_subprocedure: bool,
    ) -> Self {
        let persistent_ctx =
            PersistentContext::new(table_id, table_name, resolve_strategy, is_subprocedure);
        let context = ReconcileTableContext::new(ctx, persistent_ctx);
        let state = Box::new(ReconciliationStart);
        Self { context, state }
    }
}

impl ReconcileTableProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::ReconcileTable";

    pub(crate) fn from_json(ctx: Context, json: &str) -> ProcedureResult<Self> {
        let ProcedureDataOwned {
            state,
            persistent_ctx,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let context = ReconcileTableContext::new(ctx, persistent_ctx);
        Ok(Self { context, state })
    }
}

#[derive(Debug, Serialize)]
struct ProcedureData<'a> {
    state: &'a dyn State,
    persistent_ctx: &'a PersistentContext,
}

#[derive(Debug, Deserialize)]
struct ProcedureDataOwned {
    state: Box<dyn State>,
    persistent_ctx: PersistentContext,
}

#[async_trait]
impl Procedure for ReconcileTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &mut self.state;

        let procedure_name = Self::TYPE_NAME;
        let step = state.name();
        let _timer = metrics::METRIC_META_RECONCILIATION_PROCEDURE
            .with_label_values(&[procedure_name, step])
            .start_timer();
        match state.next(&mut self.context, _ctx).await {
            Ok((next, status)) => {
                *state = next;
                Ok(status)
            }
            Err(e) => {
                if e.is_retry_later() {
                    metrics::METRIC_META_RECONCILIATION_PROCEDURE_ERROR
                        .with_label_values(&[procedure_name, step, metrics::ERROR_TYPE_RETRYABLE])
                        .inc();
                    Err(ProcedureError::retry_later(e))
                } else {
                    metrics::METRIC_META_RECONCILIATION_PROCEDURE_ERROR
                        .with_label_values(&[procedure_name, step, metrics::ERROR_TYPE_EXTERNAL])
                        .inc();
                    Err(ProcedureError::external(e))
                }
            }
        }
    }

    fn dump(&self) -> ProcedureResult<String> {
        let data = ProcedureData {
            state: self.state.as_ref(),
            persistent_ctx: &self.context.persistent_ctx,
        };
        serde_json::to_string(&data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.context.table_name().table_ref();

        if self.context.persistent_ctx.is_subprocedure {
            // The catalog and schema are already locked by the parent procedure.
            // Only lock the table name.
            return LockKey::new(vec![
                TableNameLock::new(table_ref.catalog, table_ref.schema, table_ref.table).into(),
            ]);
        }

        LockKey::new(vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
            TableNameLock::new(table_ref.catalog, table_ref.schema, table_ref.table).into(),
        ])
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "reconcile_table_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    async fn next(
        &mut self,
        ctx: &mut ReconcileTableContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;

    fn as_any(&self) -> &dyn Any;
}
