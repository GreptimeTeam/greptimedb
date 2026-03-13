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

use async_trait::async_trait;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{Context as ProcedureContext, LockKey, Procedure, Status};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use strum::AsRefStr;
use table::metadata::{TableId, TableInfo, TableType};
use table::table_reference::TableReference;

use crate::cache_invalidator::Context;
use crate::ddl::utils::map_to_procedure_error;
use crate::ddl::{DdlContext, TableMetadata};
use crate::error::{self, Result};
use crate::instruction::CacheIdent;
use crate::key::table_name::TableNameKey;
use crate::lock_key::{CatalogLock, SchemaLock, TableNameLock};
use crate::metrics;
use crate::rpc::ddl::CreateViewTask;

// The procedure to execute `[CreateViewTask]`.
pub struct CreateViewProcedure {
    pub context: DdlContext,
    pub data: CreateViewData,
}

impl CreateViewProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateView";

    pub fn new(task: CreateViewTask, context: DdlContext) -> Self {
        Self {
            context,
            data: CreateViewData {
                state: CreateViewState::Prepare,
                task,
                need_update: false,
            },
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(CreateViewProcedure { context, data })
    }

    fn view_info(&self) -> &TableInfo {
        &self.data.task.view_info
    }

    fn need_update(&self) -> bool {
        self.data.need_update
    }

    pub(crate) fn view_id(&self) -> TableId {
        self.view_info().ident.table_id
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_allocated_metadata(&mut self, view_id: TableId) {
        self.data.set_allocated_metadata(view_id, false)
    }

    /// On the prepare step, it performs:
    /// - Checks whether the view exists.
    /// - Allocates the view id.
    ///
    /// Abort(non-retry):
    /// - ViewName exists and `create_if_not_exists` is false.
    /// - Failed to allocate [ViewMetadata].
    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        let expr = &self.data.task.create_view;
        let view_name_value = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .get(TableNameKey::new(
                &expr.catalog_name,
                &expr.schema_name,
                &expr.view_name,
            ))
            .await?;

        // If `view_id` is None, creating the new view,
        // otherwise:
        // - replaces the exists one when `or_replace` is true.
        // - returns the exists one when `create_if_not_exists` is true.
        // - throws the `[ViewAlreadyExistsSnafu]` error.
        let mut view_id = None;

        if let Some(value) = view_name_value {
            ensure!(
                expr.create_if_not_exists || expr.or_replace,
                error::ViewAlreadyExistsSnafu {
                    view_name: self.data.table_ref().to_string(),
                }
            );

            let exists_view_id = value.table_id();

            if !expr.or_replace {
                return Ok(Status::done_with_output(exists_view_id));
            }
            view_id = Some(exists_view_id);
        }

        if let Some(view_id) = view_id {
            let view_info_value = self
                .context
                .table_metadata_manager
                .table_info_manager()
                .get(view_id)
                .await?
                .with_context(|| error::TableInfoNotFoundSnafu {
                    table: self.data.table_ref().to_string(),
                })?;

            // Ensure the exists one is view, we can't replace a table.
            ensure!(
                view_info_value.table_info.table_type == TableType::View,
                error::TableAlreadyExistsSnafu {
                    table_name: self.data.table_ref().to_string(),
                }
            );

            self.data.set_allocated_metadata(view_id, true);
        } else {
            // Allocate the new `view_id`.
            let TableMetadata { table_id, .. } = self
                .context
                .table_metadata_allocator
                .create_view(&None)
                .await?;
            self.data.set_allocated_metadata(table_id, false);
        }

        self.data.state = CreateViewState::CreateMetadata;

        Ok(Status::executing(true))
    }

    async fn invalidate_view_cache(&self) -> Result<()> {
        let cache_invalidator = &self.context.cache_invalidator;
        let ctx = Context {
            subject: Some("Invalidate view cache by creating view".to_string()),
        };

        cache_invalidator
            .invalidate(
                &ctx,
                &[
                    CacheIdent::TableName(self.data.table_ref().into()),
                    CacheIdent::TableId(self.view_id()),
                ],
            )
            .await?;

        Ok(())
    }

    /// Creates view metadata
    ///
    /// Abort(not-retry):
    /// - Failed to create view metadata.
    async fn on_create_metadata(&mut self, ctx: &ProcedureContext) -> Result<Status> {
        let view_id = self.view_id();
        let manager = &self.context.table_metadata_manager;

        if self.need_update() {
            // Retrieve the current view info and try to update it.
            let current_view_info = manager
                .view_info_manager()
                .get(view_id)
                .await?
                .with_context(|| error::ViewNotFoundSnafu {
                    view_name: self.data.table_ref().to_string(),
                })?;
            let new_logical_plan = self.data.task.raw_logical_plan().clone();
            let table_names = self.data.task.table_names();
            let columns = self.data.task.columns().clone();
            let plan_columns = self.data.task.plan_columns().clone();
            let new_view_definition = self.data.task.view_definition().to_string();

            manager
                .update_view_info(
                    view_id,
                    &current_view_info,
                    new_logical_plan,
                    table_names,
                    columns,
                    plan_columns,
                    new_view_definition,
                )
                .await?;

            info!("Updated view metadata for view {view_id}");
        } else {
            let raw_view_info = self.view_info().clone();
            manager
                .create_view_metadata(
                    raw_view_info,
                    self.data.task.raw_logical_plan().clone(),
                    self.data.task.table_names(),
                    self.data.task.columns().clone(),
                    self.data.task.plan_columns().clone(),
                    self.data.task.view_definition().to_string(),
                )
                .await?;

            info!(
                "Created view metadata for view {view_id} with procedure: {}",
                ctx.procedure_id
            );
        }
        self.invalidate_view_cache().await?;

        Ok(Status::done_with_output(view_id))
    }
}

#[async_trait]
impl Procedure for CreateViewProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;

        let _timer = metrics::METRIC_META_PROCEDURE_CREATE_VIEW
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match state {
            CreateViewState::Prepare => self.on_prepare().await,
            CreateViewState::CreateMetadata => self.on_create_metadata(ctx).await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.data.table_ref();

        LockKey::new(vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
            TableNameLock::new(table_ref.catalog, table_ref.schema, table_ref.table).into(),
        ])
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, AsRefStr, PartialEq)]
pub enum CreateViewState {
    /// Prepares to create the table
    Prepare,
    /// Creates metadata
    CreateMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateViewData {
    pub state: CreateViewState,
    pub task: CreateViewTask,
    /// Whether to update the view info.
    pub need_update: bool,
}

impl CreateViewData {
    fn set_allocated_metadata(&mut self, view_id: TableId, need_update: bool) {
        self.task.view_info.ident.table_id = view_id;
        self.need_update = need_update;
    }

    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }
}
