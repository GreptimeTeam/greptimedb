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

use api::v1::meta::MailboxMessage;
use async_trait::async_trait;
use client::Database;
use common_meta::ident::TableIdent;
use common_meta::instruction::Instruction;
use common_meta::key::table_info::TableInfoValue;
use common_meta::key::table_name::TableNameKey;
use common_meta::key::TableRouteKey;
use common_meta::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use common_meta::rpc::ddl::AlterTableTask;
use common_meta::rpc::router::TableRoute;
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
use crate::error::{self, Result, TableMetadataManagerSnafu, UnexpectedSnafu};
use crate::procedure::utils::handle_request_datanode_error;
use crate::service::mailbox::BroadcastChannel;
use crate::table_routes::fetch_table;

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
        table_info: RawTableInfo,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: AlterTableData::new(task, alter_table_request, table_info, cluster_id),
        }
    }

    pub(crate) fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(AlterTableProcedure { context, data })
    }

    /// Alters table on datanode.
    async fn on_datanode_alter_table(&mut self) -> Result<Status> {
        let table_route = self
            .data
            .table_route
            .as_ref()
            .context(error::UnexpectedSnafu {
                violated: "expected table_route",
            })?;

        let table_ref = self.data.table_ref();

        let clients = self.context.datanode_clients.clone();
        let leaders = table_route.find_leaders();
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

    async fn update_table_info_value(
        &self,
        table_id: TableId,
        table_info_value: &TableInfoValue,
        new_table_info: RawTableInfo,
    ) -> Result<()> {
        self.context.table_metadata_manager
            .table_info_manager()
            .compare_and_put(table_id, Some(table_info_value.clone()), new_table_info)
            .await
            .context(TableMetadataManagerSnafu)?
            .map_err(|curr| {
                // The table info metadata should be guarded by procedure locks.
                UnexpectedSnafu {
                    violated: format!(
                        "TableInfoValue for table {table_id} is changed during table alternation, expected: '{table_info_value:?}', actual: '{curr:?}'",
                    )
                }.build()
            })
    }

    /// Update table metadata for rename table operation.
    async fn on_update_metadata_for_rename(&self, new_table_info: TableInfo) -> Result<TableRoute> {
        let table_metadata_manager = &self.context.table_metadata_manager;

        let table_ref = self.data.table_ref();
        let new_table_name = new_table_info.name.clone();
        let table_id = self.data.table_info.ident.table_id;

        if let Some((table_info_value, table_route_value)) =
            fetch_table(&self.context.kv_store, table_metadata_manager, table_id).await?
        {
            self.update_table_info_value(table_id, &table_info_value, new_table_info.into())
                .await?;
            info!("Updated TableInfoValue for table {table_id} with new table name '{new_table_name}'");

            table_metadata_manager
                .table_name_manager()
                .rename(
                    TableNameKey::new(table_ref.catalog, table_ref.schema, table_ref.table),
                    table_id,
                    &new_table_name,
                )
                .await
                .context(TableMetadataManagerSnafu)?;
            info!("Renamed TableNameKey to new table name '{new_table_name}' for table {table_id}");

            let table_route = table_route_value
                .clone()
                .try_into()
                .context(error::TableRouteConversionSnafu)?;

            let table_route_key = TableRouteKey {
                table_id,
                catalog_name: table_ref.catalog,
                schema_name: table_ref.schema,
                table_name: table_ref.table,
            };
            let new_table_route_key = TableRouteKey {
                table_name: &new_table_name,
                ..table_route_key
            };

            let txn = Txn::new()
                .when(vec![Compare::with_value(
                    table_route_key.to_string().into_bytes(),
                    CompareOp::Equal,
                    table_route_value.clone().into(),
                )])
                .and_then(vec![
                    TxnOp::Delete(table_route_key.to_string().into_bytes()),
                    TxnOp::Put(
                        new_table_route_key.to_string().into_bytes(),
                        table_route_value.into(),
                    ),
                ]);

            let resp = self.context.kv_store.txn(txn).await?;

            ensure!(
                resp.succeeded,
                error::TxnSnafu {
                    msg: "table metadata changed"
                }
            );
            info!("Updated TableRouteValue for table {table_id} with new table name '{new_table_name}'");
            return Ok(table_route);
        }

        error::TableNotFoundSnafu {
            name: table_ref.to_string(),
        }
        .fail()
    }

    fn build_new_table_info(&self) -> Result<TableInfo> {
        // Builds new_meta
        let table_info = TableInfo::try_from(self.data.table_info.clone())
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
        let table_id = self.data.table_info.ident.table_id;
        let table_ref = self.data.table_ref();
        let new_info = self.build_new_table_info()?;
        debug!(
            "starting update table: {} metadata, new table info {:?}",
            table_ref.to_string(),
            new_info
        );

        if matches!(request.alter_kind, AlterKind::RenameTable { .. }) {
            let table_route = self.on_update_metadata_for_rename(new_info).await?;

            self.data.state = AlterTableState::InvalidateTableCache;
            self.data.table_route = Some(table_route);
            return Ok(Status::executing(true));
        }

        if let Some((table_info_value, table_route_value)) = fetch_table(
            &self.context.kv_store,
            &self.context.table_metadata_manager,
            table_id,
        )
        .await?
        {
            let table_route = table_route_value
                .clone()
                .try_into()
                .context(error::TableRouteConversionSnafu)?;
            let new_raw_info: RawTableInfo = new_info.into();

            self.update_table_info_value(table_id, &table_info_value, new_raw_info)
                .await?;
            info!("Updated TableInfoValue for table {table_id} when altering table");

            self.data.state = AlterTableState::InvalidateTableCache;
            self.data.table_route = Some(table_route);

            Ok(Status::executing(true))
        } else {
            error::TableNotFoundSnafu {
                name: table_ref.to_string(),
            }
            .fail()
        }
    }

    /// Broadcasts the invalidating table cache instructions.
    async fn on_broadcast(&mut self) -> Result<Status> {
        let table_name = self.data.table_name();

        let table_ident = TableIdent {
            catalog: table_name.catalog_name,
            schema: table_name.schema_name,
            table: table_name.table_name,
            table_id: self.data.table_info.ident.table_id,
            engine: self.data.table_info.meta.engine.to_string(),
        };
        let instruction = Instruction::InvalidateTableCache(table_ident);

        let msg = &MailboxMessage::json_message(
            "Invalidate table cache by alter table procedure",
            &format!("Metasrv@{}", self.context.server_addr),
            "Frontend broadcast",
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        self.context
            .mailbox
            .broadcast(&BroadcastChannel::Frontend, msg)
            .await?;
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
    table_route: Option<TableRoute>,
    table_info: RawTableInfo,
    cluster_id: u64,
}

impl AlterTableData {
    pub fn new(
        task: AlterTableTask,
        alter_table_request: AlterTableRequest,
        table_info: RawTableInfo,
        cluster_id: u64,
    ) -> Self {
        Self {
            state: AlterTableState::UpdateMetadata,
            task,
            alter_table_request,
            table_route: None,
            table_info,
            cluster_id,
        }
    }

    fn table_ref(&self) -> TableReference {
        self.task.table_ref()
    }

    fn table_name(&self) -> TableName {
        self.task.table_name()
    }
}
