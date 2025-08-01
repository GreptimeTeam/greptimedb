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
use std::fmt::Debug;

use common_procedure::error::FromJsonSnafu;
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure, ProcedureId,
    Result as ProcedureResult, Status,
};
use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

use crate::cache_invalidator::CacheInvalidatorRef;
use crate::error::Result;
use crate::key::TableMetadataManagerRef;
use crate::lock_key::CatalogLock;
use crate::node_manager::NodeManagerRef;
use crate::reconciliation::reconcile_catalog::start::ReconcileCatalogStart;
use crate::reconciliation::reconcile_database::utils::wait_for_inflight_subprocedures;
use crate::reconciliation::reconcile_table::resolve_column_metadata::ResolveStrategy;
use crate::reconciliation::utils::Context;

pub(crate) mod end;
pub(crate) mod reconcile_databases;
pub(crate) mod start;

pub(crate) struct ReconcileCatalogContext {
    pub node_manager: NodeManagerRef,
    pub table_metadata_manager: TableMetadataManagerRef,
    pub cache_invalidator: CacheInvalidatorRef,
    persistent_ctx: PersistentContext,
    volatile_ctx: VolatileContext,
}

impl ReconcileCatalogContext {
    pub fn new(ctx: Context, persistent_ctx: PersistentContext) -> Self {
        Self {
            node_manager: ctx.node_manager,
            table_metadata_manager: ctx.table_metadata_manager,
            cache_invalidator: ctx.cache_invalidator,
            persistent_ctx,
            volatile_ctx: VolatileContext::default(),
        }
    }

    pub(crate) async fn wait_for_inflight_subprocedure(
        &mut self,
        procedure_ctx: &ProcedureContext,
    ) -> Result<()> {
        if let Some(procedure_id) = self.volatile_ctx.inflight_subprocedure {
            wait_for_inflight_subprocedures(
                procedure_ctx,
                &[procedure_id],
                self.persistent_ctx.fast_fail,
            )
            .await?;
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PersistentContext {
    catalog: String,
    fast_fail: bool,
    resolve_strategy: ResolveStrategy,
}

impl PersistentContext {
    pub fn new(catalog: String, fast_fail: bool, resolve_strategy: ResolveStrategy) -> Self {
        Self {
            catalog,
            fast_fail,
            resolve_strategy,
        }
    }
}

#[derive(Default)]
pub(crate) struct VolatileContext {
    /// Stores the stream of catalogs.
    schemas: Option<BoxStream<'static, Result<String>>>,
    /// Stores the inflight subprocedure.
    inflight_subprocedure: Option<ProcedureId>,
}

pub struct ReconcileCatalogProcedure {
    pub context: ReconcileCatalogContext,
    state: Box<dyn State>,
}

impl ReconcileCatalogProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::ReconcileCatalog";

    pub fn new(
        ctx: Context,
        catalog: String,
        fast_fail: bool,
        resolve_strategy: ResolveStrategy,
    ) -> Self {
        let persistent_ctx = PersistentContext::new(catalog, fast_fail, resolve_strategy);
        let context = ReconcileCatalogContext::new(ctx, persistent_ctx);
        let state = Box::new(ReconcileCatalogStart);
        Self { context, state }
    }

    pub(crate) fn from_json(ctx: Context, json: &str) -> ProcedureResult<Self> {
        let ProcedureDataOwned {
            state,
            persistent_ctx,
        } = serde_json::from_str(json).context(FromJsonSnafu)?;
        let context = ReconcileCatalogContext::new(ctx, persistent_ctx);
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

#[async_trait::async_trait]
impl Procedure for ReconcileCatalogProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &mut self.state;

        match state.next(&mut self.context, _ctx).await {
            Ok((next, status)) => {
                *state = next;
                Ok(status)
            }
            Err(e) => {
                if e.is_retry_later() {
                    Err(ProcedureError::retry_later(e))
                } else {
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
        serde_json::to_string(&data).context(FromJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let catalog = &self.context.persistent_ctx.catalog;

        LockKey::new(vec![CatalogLock::Write(catalog).into()])
    }
}

#[async_trait::async_trait]
#[typetag::serde(tag = "reconcile_catalog_state")]
pub(crate) trait State: Sync + Send + Debug {
    fn name(&self) -> &'static str {
        let type_name = std::any::type_name::<Self>();
        // short name
        type_name.split("::").last().unwrap_or(type_name)
    }

    async fn next(
        &mut self,
        ctx: &mut ReconcileCatalogContext,
        procedure_ctx: &ProcedureContext,
    ) -> Result<(Box<dyn State>, Status)>;

    fn as_any(&self) -> &dyn Any;
}
