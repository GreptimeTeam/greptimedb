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
use snafu::{ensure, OptionExt, ResultExt};
use strum::AsRefStr;
use table::metadata::{RawTableInfo, TableId};
use table::table_reference::TableReference;

use crate::ddl::utils::handle_retry_error;
use crate::ddl::{DdlContext, TableMetadata, TableMetadataAllocatorContext};
use crate::error::{self, Result};
use crate::key::table_name::TableNameKey;
use crate::lock_key::{CatalogLock, SchemaLock, TableNameLock};
use crate::rpc::ddl::CreateViewTask;
use crate::{metrics, ClusterId};

// The proceudure to execute `[CreateViewTask]`.
pub struct CreateViewProcedure {
    pub context: DdlContext,
    pub creator: ViewCreator,
}

impl CreateViewProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::CreateView";

    pub fn new(cluster_id: ClusterId, task: CreateViewTask, context: DdlContext) -> Self {
        Self {
            context,
            creator: ViewCreator::new(cluster_id, task),
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        let creator = ViewCreator { data };

        Ok(CreateViewProcedure { context, creator })
    }

    fn view_info(&self) -> &RawTableInfo {
        &self.creator.data.task.view_info
    }

    fn need_update(&self) -> bool {
        self.creator.data.need_update
    }

    pub(crate) fn view_id(&self) -> TableId {
        self.view_info().ident.table_id
    }

    #[cfg(any(test, feature = "testing"))]
    pub fn set_allocated_metadata(&mut self, view_id: TableId) {
        self.creator.set_allocated_metadata(view_id, false)
    }

    /// On the prepare step, it performs:
    /// - Checks whether the view exists.
    /// - Allocates the view id.
    ///
    /// Abort(non-retry):
    /// - ViewName exists and `create_if_not_exists` is false.
    /// - Failed to allocate [ViewMetadata].
    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        let expr = &self.creator.data.task.create_view;
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

        // If view_id is None, creating the new view,
        // otherwise replacing the exists one.
        let mut view_id = None;

        if let Some(value) = view_name_value {
            ensure!(
                expr.create_if_not_exists || expr.or_replace,
                error::TableAlreadyExistsSnafu {
                    table_name: self.creator.data.table_ref().to_string(),
                }
            );

            let exists_view_id = value.table_id();

            if !expr.or_replace {
                return Ok(Status::done_with_output(exists_view_id));
            }
            view_id = Some(exists_view_id);
        }

        if let Some(view_id) = view_id {
            self.creator.set_allocated_metadata(view_id, true);
        } else {
            // Allocate the new `view_id`.
            self.creator.data.state = CreateViewState::CreateMetadata;
            let TableMetadata { table_id, .. } = self
                .context
                .table_metadata_allocator
                .create_view(
                    &TableMetadataAllocatorContext {
                        cluster_id: self.creator.data.cluster_id,
                    },
                    &None,
                )
                .await?;
            self.creator.set_allocated_metadata(table_id, false);
        }

        Ok(Status::executing(true))
    }

    /// Creates view metadata
    ///
    /// Abort(not-retry):
    /// - Failed to create view metadata.
    async fn on_create_metadata(&mut self) -> Result<Status> {
        let view_id = self.view_id();
        let manager = &self.context.table_metadata_manager;

        if self.need_update() {
            let current_view_info = manager
                .view_info_manager()
                .get(view_id)
                .await?
                .with_context(|| error::TableNotFoundSnafu {
                    //FIXME(dennis): view name
                    table_name: "",
                })?;

            let new_logical_plan = self.creator.data.task.raw_logical_plan().clone();
            manager
                .update_view_info(view_id, &current_view_info, new_logical_plan)
                .await?;

            info!("Updated view metadata for view {view_id}");
        } else {
            let raw_view_info = self.view_info().clone();
            manager
                .create_view_metadata(raw_view_info, self.creator.data.task.raw_logical_plan())
                .await?;

            info!("Created view metadata for view {view_id}");
        }

        Ok(Status::done_with_output(view_id))
    }
}

#[async_trait]
impl Procedure for CreateViewProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.creator.data.state;

        let _timer = metrics::METRIC_META_PROCEDURE_CREATE_VIEW
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match state {
            CreateViewState::Prepare => self.on_prepare().await,
            CreateViewState::CreateMetadata => self.on_create_metadata().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.creator.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.creator.data.table_ref();

        LockKey::new(vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
            TableNameLock::new(table_ref.catalog, table_ref.schema, table_ref.table).into(),
        ])
    }
}

pub struct ViewCreator {
    /// The serializable data.
    pub data: CreateViewData,
}

impl ViewCreator {
    pub fn new(cluster_id: u64, task: CreateViewTask) -> Self {
        Self {
            data: CreateViewData {
                state: CreateViewState::Prepare,
                cluster_id,
                task,
                need_update: false,
            },
        }
    }

    fn set_allocated_metadata(&mut self, view_id: TableId, need_update: bool) {
        self.data.task.view_info.ident.table_id = view_id;
        self.data.need_update = need_update;
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
    pub cluster_id: ClusterId,
    /// Whether to update the view info.
    pub need_update: bool,
}

impl CreateViewData {
    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }
}
