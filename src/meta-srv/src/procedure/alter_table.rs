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

use std::vec;

use async_trait::async_trait;
use client::Database;
use common_meta::cache_invalidator::Context;
use common_meta::ident::TableIdent;
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::table_route::TableRouteValue;
use common_meta::rpc::ddl::AlterTableTask;
use common_meta::rpc::router::{find_leaders, RegionRoute};
use common_meta::table_name::TableName;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure, Status,
};
use common_telemetry::{debug, info};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId, TableInfo};
use table::requests::{AlterKind, AlterTableRequest};

use crate::ddl::DdlContext;
use crate::error::{self, Result, TableMetadataManagerSnafu};
use crate::procedure::utils::handle_request_datanode_error;

// TODO(weny): removes in following PRs.
#[allow(dead_code)]
pub struct AlterTableProcedure {
    context: DdlContext,
    data: AlterTableData,
}

// TODO(weny): removes in following PRs.
#[allow(dead_code)]
impl AlterTableProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::AlterTable";

    pub(crate) fn new(
        cluster_id: u64,
        task: AlterTableTask,
        alter_table_request: AlterTableRequest,
        table_info_value: TableInfoValue,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: AlterTableData::new(task, alter_table_request, table_info_value, cluster_id),
        }
    }

    pub(crate) fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(AlterTableProcedure { context, data })
    }

    // Checks whether the table exists.
    async fn on_prepare(&mut self) -> Result<Status> {
        let request = &self.data.alter_table_request;

        let manager = &self.context.table_metadata_manager;

        if let AlterKind::RenameTable { new_table_name } = &request.alter_kind {
            let exist = manager
                .table_name_manager()
                .exists(TableNameKey::new(
                    &request.catalog_name,
                    &request.schema_name,
                    new_table_name,
                ))
                .await
                .context(TableMetadataManagerSnafu)?;

            ensure!(
                !exist,
                error::TableAlreadyExistsSnafu {
                    table_name: common_catalog::format_full_table_name(
                        &request.catalog_name,
                        &request.schema_name,
                        new_table_name,
                    ),
                }
            )
        }

        let exist = manager
            .table_name_manager()
            .exists(TableNameKey::new(
                &request.catalog_name,
                &request.schema_name,
                &request.table_name,
            ))
            .await
            .context(TableMetadataManagerSnafu)?;

        ensure!(
            exist,
            error::TableNotFoundSnafu {
                name: request.table_ref().to_string()
            }
        );

        self.data.state = AlterTableState::UpdateMetadata;

        Ok(Status::executing(true))
    }

    /// Alters table on datanode.
    async fn on_datanode_alter_table(&mut self) -> Result<Status> {
        let region_routes = self
            .data
            .region_routes
            .as_ref()
            .context(error::UnexpectedSnafu {
                violated: "expected table_route",
            })?;

        let table_ref = self.data.table_ref();

        let clients = self.context.datanode_clients.clone();
        let leaders = find_leaders(region_routes);
        let mut joins = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            let client = clients.get_client(&datanode).await;
            let client = Database::new(table_ref.catalog, table_ref.schema, client);
            let expr = self.data.task.alter_table.clone();
            joins.push(common_runtime::spawn_bg(async move {
                debug!("Sending {:?} to {:?}", expr, client);
                client
                    .alter(expr)
                    .await
                    .map_err(handle_request_datanode_error(datanode))
            }));
        }

        let _ = join_all(joins)
            .await
            .into_iter()
            .map(|e| e.context(error::JoinSnafu).flatten())
            .collect::<Result<Vec<_>>>()?;

        Ok(Status::Done)
    }

    /// Update table metadata for rename table operation.
    async fn on_update_metadata_for_rename(&self, new_table_name: String) -> Result<()> {
        let table_metadata_manager = &self.context.table_metadata_manager;

        let current_table_info_value = self.data.table_info_value.clone();

        table_metadata_manager
            .rename_table(current_table_info_value, new_table_name)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        Ok(())
    }

    async fn on_update_metadata_for_alter(&self, new_table_info: RawTableInfo) -> Result<()> {
        let table_metadata_manager = &self.context.table_metadata_manager;
        let current_table_info_value = self.data.table_info_value.clone();

        table_metadata_manager
            .update_table_info(current_table_info_value, new_table_info)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        Ok(())
    }

    fn build_new_table_info(&self) -> Result<TableInfo> {
        // Builds new_meta
        let table_info = TableInfo::try_from(self.data.table_info().clone())
            .context(error::ConvertRawTableInfoSnafu)?;

        let table_ref = self.data.table_ref();

        let request = &self.data.alter_table_request;

        let new_meta = table_info
            .meta
            .builder_with_alter_kind(table_ref.table, &request.alter_kind)
            .context(error::TableSnafu)?
            .build()
            .with_context(|_| error::BuildTableMetaSnafu {
                table_name: table_ref.table,
            })?;

        let mut new_info = table_info.clone();
        new_info.ident.version = table_info.ident.version + 1;
        new_info.meta = new_meta;

        if let AlterKind::RenameTable { new_table_name } = &request.alter_kind {
            new_info.name = new_table_name.to_string();
        }

        Ok(new_info)
    }

    /// Update table metadata.
    async fn on_update_metadata(&mut self) -> Result<Status> {
        let request = &self.data.alter_table_request;
        let table_id = self.data.table_id();
        let table_ref = self.data.table_ref();
        let new_info = self.build_new_table_info()?;
        let table_metadata_manager = &self.context.table_metadata_manager;

        debug!(
            "starting update table: {} metadata, new table info {:?}",
            table_ref.to_string(),
            new_info
        );

        if let AlterKind::RenameTable { new_table_name } = &request.alter_kind {
            self.on_update_metadata_for_rename(new_table_name.to_string())
                .await?;
        } else {
            self.on_update_metadata_for_alter(new_info.into()).await?;
        }

        info!("Updated table metadata for table {table_id}");

        let TableRouteValue { region_routes, .. } = table_metadata_manager
            .table_route_manager()
            .get(table_id)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .with_context(|| error::TableRouteNotFoundSnafu {
                table_name: table_ref.to_string(),
            })?;

        self.data.region_routes = Some(region_routes);
        self.data.state = AlterTableState::InvalidateTableCache;
        Ok(Status::executing(true))
    }

    /// Broadcasts the invalidating table cache instructions.
    async fn on_broadcast(&mut self) -> Result<Status> {
        let table_name = self.data.table_name();

        let table_ident = TableIdent {
            catalog: table_name.catalog_name,
            schema: table_name.schema_name,
            table: table_name.table_name,
            table_id: self.data.table_id(),
            engine: self.data.table_info().meta.engine.to_string(),
        };

        self.context
            .cache_invalidator
            .invalidate_table(
                &Context {
                    subject: Some("Invalidate table cache by alter table procedure".to_string()),
                },
                table_ident,
            )
            .await
            .context(error::InvalidateTableCacheSnafu)?;

        self.data.state = AlterTableState::DatanodeAlterTable;
        Ok(Status::executing(true))
    }

    fn lock_key_inner(&self) -> Vec<String> {
        let table_ref = self.data.table_ref();
        let table_key = common_catalog::format_full_table_name(
            table_ref.catalog,
            table_ref.schema,
            table_ref.table,
        );
        let mut lock_key = vec![table_key];

        if let AlterKind::RenameTable { new_table_name } = &self.data.alter_table_request.alter_kind
        {
            lock_key.push(common_catalog::format_full_table_name(
                table_ref.catalog,
                table_ref.schema,
                new_table_name,
            ))
        }

        lock_key
    }
}

#[async_trait]
impl Procedure for AlterTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        let error_handler = |e| {
            if matches!(e, error::Error::RetryLater { .. }) {
                ProcedureError::retry_later(e)
            } else {
                ProcedureError::external(e)
            }
        };

        match self.data.state {
            AlterTableState::Prepare => self.on_prepare().await,
            AlterTableState::UpdateMetadata => self.on_update_metadata().await,
            AlterTableState::InvalidateTableCache => self.on_broadcast().await,
            AlterTableState::DatanodeAlterTable => self.on_datanode_alter_table().await,
        }
        .map_err(error_handler)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let key = self.lock_key_inner();

        LockKey::new(key)
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum AlterTableState {
    /// Prepares to alter the table
    Prepare,
    /// Updates table metadata.
    UpdateMetadata,
    /// Broadcasts the invalidating table cache instruction.
    InvalidateTableCache,
    /// Datanode alters the table.
    DatanodeAlterTable,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlterTableData {
    state: AlterTableState,
    task: AlterTableTask,
    alter_table_request: AlterTableRequest,
    region_routes: Option<Vec<RegionRoute>>,
    table_info_value: TableInfoValue,
    cluster_id: u64,
}

impl AlterTableData {
    pub fn new(
        task: AlterTableTask,
        alter_table_request: AlterTableRequest,
        table_info_value: TableInfoValue,
        cluster_id: u64,
    ) -> Self {
        Self {
            state: AlterTableState::Prepare,
            task,
            alter_table_request,
            region_routes: None,
            table_info_value,
            cluster_id,
        }
    }

    fn table_ref(&self) -> TableReference {
        self.task.table_ref()
    }

    fn table_name(&self) -> TableName {
        self.task.table_name()
    }

    fn table_id(&self) -> TableId {
        self.table_info().ident.table_id
    }

    fn table_info(&self) -> &RawTableInfo {
        &self.table_info_value.table_info
    }
}
