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

pub(crate) mod end;
pub(crate) mod reconcile_logical_tables;
pub(crate) mod reconcile_tables;
pub(crate) mod start;

use std::any::Any;
use std::collections::HashMap;
use std::fmt::Debug;
use std::time::Instant;

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::storage::TableId;
use table::table_name::TableName;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::Result;
use crate::key::TableMetadataManagerRef;
use crate::key::table_name::TableNameValue;
use crate::lock_key::{CatalogLock, SchemaLock};
use crate::metrics;
use crate::reconciliation::reconcile_database::start::ReconcileDatabaseStart;
use crate::reconciliation::reconcile_table::resolve_column_metadata::ResolveStrategy;
use crate::reconciliation::utils::{
    Context, ReconcileDatabaseMetrics, SubprocedureMeta, wait_for_inflight_subprocedures,
};
use crate::region_rpc::RegionRpcRef;
pub(crate) const DEFAULT_PARALLELISM: usize = 64;

pub(crate) struct ReconcileDatabaseContext {
    pub region_rpc: RegionRpcRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    persistent_ctx: PersistentContext,
    volatile_ctx: VolatileContext,
}

impl ReconcileDatabaseContext {
    pub fn new(ctx: Context, persistent_ctx: PersistentContext) -> Self {
        Self {
            region_rpc: ctx.region_rpc,
            table_metadata_manager: ctx.table_metadata_manager,
            cache_invalidator: ctx.cache_invalidator,
            persistent_ctx,
            volatile_ctx: VolatileContext::default(),
        }
    }

    /// Waits for inflight subprocedures to complete.
    pub(crate) async fn wait_for_inflight_subprocedures(
        &mut self,
        procedure_ctx: &ProcedureContext,
    ) -> Result<()> {
        if !self.volatile_ctx.inflight_subprocedures.is_empty() {
            let result = wait_for_inflight_subprocedures(
                procedure_ctx,
                &self.volatile_ctx.inflight_subprocedures,
                self.persistent_ctx.fail_fast,
            )
            .await?;

            // Collects result into metrics
            let metrics = result.into();
            self.volatile_ctx.inflight_subprocedures.clear();
            self.volatile_ctx.metrics += metrics;
        }

        Ok(())
    }

    /// Returns the immutable metrics.
    pub(crate) fn metrics(&self) -> &ReconcileDatabaseMetrics {
        &self.volatile_ctx.metrics
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistentContext {
    catalog: String,
    schema: String,
    fail_fast: bool,
    parallelism: usize,
    resolve_strategy: ResolveStrategy,
    is_subprocedure: bool,
}

impl PersistentContext {
    pub fn new(
        catalog: String,
        schema: String,
        fail_fast: bool,
        parallelism: usize,
        resolve_strategy: ResolveStrategy,
        is_subprocedure: bool,
    ) -> Self {
        Self {
            catalog,
            schema,
            fail_fast,
            parallelism,
            resolve_strategy,
            is_subprocedure,
        }
    }
}

pub(crate) struct VolatileContext {
    /// Stores pending physical tables.
    pending_tables: Vec<(TableId, TableName)>,
    /// Stores pending logical tables associated with each physical table.
    ///
    /// - Key: Table ID of the physical table.
    /// - Value: Vector of (TableId, TableName) tuples representing logical tables belonging to the physical table.
    pending_logical_tables: HashMap<TableId, Vec<(TableId, TableName)>>,
    /// Stores inflight subprocedures.
    inflight_subprocedures: Vec<SubprocedureMeta>,
    /// Stores the stream of tables.
    tables: Option<BoxStream<'static, Result<(String, TableNameValue)>>>,
    /// The metrics of reconciling database.
    metrics: ReconcileDatabaseMetrics,
    /// The start time of the reconciliation.
    start_time: Instant,
}

impl Default for VolatileContext {
    fn default() -> Self {
        Self {
            pending_tables: vec![],
            pending_logical_tables: HashMap::new(),
            inflight_subprocedures: vec![],
            tables: None,
            metrics: ReconcileDatabaseMetrics::default(),
            start_time: Instant::now(),
        }
    }
}

pub struct ReconcileDatabaseProcedure {
    pub context: ReconcileDatabaseContext,
    state: Box<dyn State>,
}

impl ReconcileDatabaseProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::ReconcileDatabase";

    pub fn new(
        ctx: Context,
        catalog: String,
        schema: String,
        fail_fast: bool,
        parallelism: usize,
        resolve_strategy: ResolveStrategy,
        is_subprocedure: bool,
    ) -> Self {
        let persistent_ctx = PersistentContext::new(
            catalog,
            schema,
            fail_fast,
            parallelism,
            resolve_strategy,
            is_subprocedure,
        );
        let context = ReconcileDatabaseContext::new(ctx, persistent_ctx);
        let state = Box::new(ReconcileDatabaseStart);
        Self { context, state }
    }

    pub(crate) fn from_json(ctx: Context, json: &str) -> ProcedureResult<Self> {
        let ProcedureDataOwned {
            state,
            persistent_ctx,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let context = ReconcileDatabaseContext::new(ctx, persistent_ctx);
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
impl Procedure for ReconcileDatabaseProcedure {
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
        let catalog = &self.context.persistent_ctx.catalog;
        let schema = &self.context.persistent_ctx.schema;
        // If the procedure is a subprocedure, only lock the schema.
        if self.context.persistent_ctx.is_subprocedure {
            return LockKey::new(vec![SchemaLock::write(catalog, schema).into()]);
        }

        LockKey::new(vec![
            CatalogLock::Read(catalog).into(),
            SchemaLock::write(catalog, schema).into(),
        ])
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "reconcile_database_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    async fn next(
        &mut self,
        ctx: &mut ReconcileDatabaseContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;

    fn as_any(&self) -> &dyn Any;
}
