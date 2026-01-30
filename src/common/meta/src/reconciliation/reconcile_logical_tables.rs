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
pub(crate) mod resolve_table_metadatas;
pub(crate) mod update_table_infos;

use std::any::Any;
use std::fmt::Debug;

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::metadata::ColumnMetadata;
use store_api::storage::TableId;
use table::metadata::RawTableInfo;
use table::table_name::TableName;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::Result;
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::PhysicalTableRouteValue;
use crate::key::{DeserializedValueWithBytes, TableMetadataManagerRef};
use crate::lock_key::{CatalogLock, SchemaLock, TableLock};
use crate::metrics;
use crate::reconciliation::reconcile_logical_tables::reconciliation_start::ReconciliationStart;
use crate::reconciliation::utils::{Context, ReconcileLogicalTableMetrics};
use crate::region_rpc::RegionRpcRef;

pub struct ReconcileLogicalTablesContext {
    pub region_rpc: RegionRpcRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    pub persistent_ctx: PersistentContext,
    pub volatile_ctx: VolatileContext,
}

impl ReconcileLogicalTablesContext {
    /// Creates a new [`ReconcileLogicalTablesContext`] with the given [`Context`] and [`PersistentContext`].
    pub fn new(ctx: Context, persistent_ctx: PersistentContext) -> Self {
        Self {
            region_rpc: ctx.region_rpc,
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

    /// Returns a mutable reference to the metrics.
    pub(crate) fn mut_metrics(&mut self) -> &mut ReconcileLogicalTableMetrics {
        &mut self.volatile_ctx.metrics
    }

    /// Returns a reference to the metrics.
    pub(crate) fn metrics(&self) -> &ReconcileLogicalTableMetrics {
        &self.volatile_ctx.metrics
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistentContext {
    pub(crate) table_id: TableId,
    pub(crate) table_name: TableName,
    // The logical tables need to be reconciled.
    // The logical tables belongs to the physical table.
    pub(crate) logical_tables: Vec<TableName>,
    // The logical table ids.
    // The value will be set in `ReconciliationStart` state.
    pub(crate) logical_table_ids: Vec<TableId>,
    /// The table info value.
    /// The value will be set in `ReconciliationStart` state.
    pub(crate) table_info_value: Option<DeserializedValueWithBytes<TableInfoValue>>,
    // The physical table route.
    // The value will be set in `ReconciliationStart` state.
    pub(crate) physical_table_route: Option<PhysicalTableRouteValue>,
    // The table infos to be updated.
    // The value will be set in `ResolveTableMetadatas` state.
    pub(crate) update_table_infos: Vec<(TableId, Vec<ColumnMetadata>)>,
    // The table infos to be created.
    // The value will be set in `ResolveTableMetadatas` state.
    pub(crate) create_tables: Vec<(TableId, RawTableInfo)>,
    // Whether the procedure is a subprocedure.
    pub(crate) is_subprocedure: bool,
}

impl PersistentContext {
    pub(crate) fn new(
        table_id: TableId,
        table_name: TableName,
        logical_tables: Vec<(TableId, TableName)>,
        is_subprocedure: bool,
    ) -> Self {
        let (logical_table_ids, logical_tables) = logical_tables.into_iter().unzip();

        Self {
            table_id,
            table_name,
            logical_tables,
            logical_table_ids,
            table_info_value: None,
            physical_table_route: None,
            update_table_infos: vec![],
            create_tables: vec![],
            is_subprocedure,
        }
    }
}

#[derive(Default)]
pub(crate) struct VolatileContext {
    pub(crate) metrics: ReconcileLogicalTableMetrics,
}

pub struct ReconcileLogicalTablesProcedure {
    pub context: ReconcileLogicalTablesContext,
    state: Box<dyn State>,
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

impl ReconcileLogicalTablesProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::ReconcileLogicalTables";

    pub fn new(
        ctx: Context,
        table_id: TableId,
        table_name: TableName,
        logical_tables: Vec<(TableId, TableName)>,
        is_subprocedure: bool,
    ) -> Self {
        let persistent_ctx =
            PersistentContext::new(table_id, table_name, logical_tables, is_subprocedure);
        let context = ReconcileLogicalTablesContext::new(ctx, persistent_ctx);
        let state = Box::new(ReconciliationStart);
        Self { context, state }
    }

    pub(crate) fn from_json(ctx: Context, json: &str) -> ProcedureResult<Self> {
        let ProcedureDataOwned {
            state,
            persistent_ctx,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let context = ReconcileLogicalTablesContext::new(ctx, persistent_ctx);
        Ok(Self { context, state })
    }
}

#[async_trait]
impl Procedure for ReconcileLogicalTablesProcedure {
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

        let mut table_ids = self
            .context
            .persistent_ctx
            .logical_table_ids
            .iter()
            .map(|t| TableLock::Write(*t).into())
            .collect::<Vec<_>>();
        table_ids.sort_unstable();
        table_ids.push(TableLock::Read(self.context.table_id()).into());
        if self.context.persistent_ctx.is_subprocedure {
            // The catalog and schema are already locked by the parent procedure.
            // Only lock the table name.
            return LockKey::new(table_ids);
        }
        let mut keys = vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
        ];
        keys.extend(table_ids);
        LockKey::new(keys)
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "reconcile_logical_tables_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    async fn next(
        &mut self,
        ctx: &mut ReconcileLogicalTablesContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;

    fn as_any(&self) -> &dyn Any;
}
