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
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::info;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use strum::AsRefStr;
use table::metadata::{TableId, TableInfo, TableType};
use table::table_reference::TableReference;

use crate::cache_invalidator::Context;
use crate::ddl::DdlContext;
use crate::ddl::utils::map_to_procedure_error;
use crate::error::{self, Result};
use crate::instruction::CacheIdent;
use crate::key::table_name::TableNameKey;
use crate::lock_key::{CatalogLock, SchemaLock, TableLock};
use crate::metrics;
use crate::rpc::ddl::DropViewTask;

/// The procedure for dropping a view.
pub struct DropViewProcedure {
    /// The context of procedure runtime.
    pub(crate) context: DdlContext,
    /// The serializable data.
    pub(crate) data: DropViewData,
}

impl DropViewProcedure {
    pub const TYPE_NAME: &'static str = "metasrv-procedure::DropView";

    pub fn new(task: DropViewTask, context: DdlContext) -> Self {
        Self {
            context,
            data: DropViewData {
                state: DropViewState::Prepare,
                task,
            },
        }
    }

    pub fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data: DropViewData = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(Self { context, data })
    }

    #[cfg(test)]
    pub(crate) fn state(&self) -> DropViewState {
        self.data.state
    }

    /// Checks whether view exists.
    /// - Early returns if view not exists and `drop_if_exists` is `true`.
    /// - Throws an error if view not exists and `drop_if_exists` is `false`.
    pub(crate) async fn on_prepare(&mut self) -> Result<Status> {
        let table_ref = self.data.table_ref();

        let exists = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .exists(TableNameKey::new(
                table_ref.catalog,
                table_ref.schema,
                table_ref.table,
            ))
            .await?;

        if !exists && self.data.task.drop_if_exists {
            return Ok(Status::done());
        }

        ensure!(
            exists,
            error::ViewNotFoundSnafu {
                view_name: table_ref.to_string(),
            }
        );

        self.check_view_metadata().await?;
        self.data.state = DropViewState::DeleteMetadata;

        Ok(Status::executing(true))
    }

    async fn check_view_metadata(&mut self) -> Result<()> {
        let view_id = self.data.view_id();
        let table_info_value = self
            .context
            .table_metadata_manager
            .table_info_manager()
            .get(view_id)
            .await?
            .with_context(|| error::TableInfoNotFoundSnafu {
                table: self.data.table_ref().to_string(),
            })?;

        self.ensure_is_view(&table_info_value.table_info)?;
        self.ensure_view_info_exists(view_id).await?;

        Ok(())
    }

    fn ensure_is_view(&self, table_info: &TableInfo) -> Result<()> {
        ensure!(
            table_info.table_type == TableType::View,
            error::InvalidViewInfoSnafu {
                err_msg: format!("{} is not a view", self.data.table_ref()),
            }
        );
        Ok(())
    }

    async fn ensure_view_info_exists(&self, view_id: TableId) -> Result<()> {
        self.context
            .table_metadata_manager
            .view_info_manager()
            .get(view_id)
            .await?
            .with_context(|| error::ViewNotFoundSnafu {
                view_name: self.data.table_ref().to_string(),
            })?;
        Ok(())
    }

    async fn on_delete_metadata(&mut self) -> Result<Status> {
        let view_id = self.data.view_id();
        self.context
            .table_metadata_manager
            .destroy_view_info(view_id, &self.data.table_ref().into())
            .await?;

        info!("Deleted view metadata for view {view_id}");

        self.data.state = DropViewState::InvalidateViewCache;
        Ok(Status::executing(true))
    }

    async fn on_broadcast(&mut self) -> Result<Status> {
        let view_id = self.data.view_id();
        let ctx = Context {
            subject: Some("Invalidate view cache by dropping view".to_string()),
        };

        self.context
            .cache_invalidator
            .invalidate(
                &ctx,
                &[
                    CacheIdent::TableId(view_id),
                    CacheIdent::TableName(self.data.table_ref().into()),
                ],
            )
            .await?;

        Ok(Status::done())
    }
}

#[async_trait]
impl Procedure for DropViewProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let state = &self.data.state;
        let _timer = metrics::METRIC_META_PROCEDURE_DROP_VIEW
            .with_label_values(&[state.as_ref()])
            .start_timer();

        match self.data.state {
            DropViewState::Prepare => self.on_prepare().await,
            DropViewState::DeleteMetadata => self.on_delete_metadata().await,
            DropViewState::InvalidateViewCache => self.on_broadcast().await,
        }
        .map_err(map_to_procedure_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.data.table_ref();
        let view_id = self.data.view_id();
        let lock_key = vec![
            CatalogLock::Read(table_ref.catalog).into(),
            SchemaLock::read(table_ref.catalog, table_ref.schema).into(),
            TableLock::Write(view_id).into(),
        ];

        LockKey::new(lock_key)
    }
}

/// The serializable data
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DropViewData {
    state: DropViewState,
    task: DropViewTask,
}

impl DropViewData {
    fn table_ref(&self) -> TableReference<'_> {
        self.task.table_ref()
    }

    fn view_id(&self) -> TableId {
        self.task.view_id
    }
}

/// The state of drop view
#[derive(Debug, Serialize, Deserialize, AsRefStr, PartialEq, Clone, Copy)]
pub(crate) enum DropViewState {
    /// Prepares to drop the view
    Prepare,
    /// Deletes metadata
    DeleteMetadata,
    /// Invalidate view cache
    InvalidateViewCache,
}
