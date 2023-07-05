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
use catalog::helper::TableGlobalKey;
use client::Database;
use common_meta::ident::TableIdent;
use common_meta::instruction::Instruction;
use common_meta::rpc::ddl::AlterTableTask;
use common_meta::rpc::router::TableRoute;
use common_meta::table_name::TableName;
use common_procedure::error::{FromJsonSnafu, Result as ProcedureResult, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure, Status,
};
use common_telemetry::debug;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableInfo};
use table::requests::{AlterKind, AlterTableRequest};

use super::utils::{build_table_metadata, build_table_metadata_key, TableMetadata};
use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::service::mailbox::BroadcastChannel;
use crate::service::store::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::table_routes::get_table_global_value;

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
        table_route: TableRoute,
        table_info: RawTableInfo,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: AlterTableData::new(
                task,
                alter_table_request,
                table_route,
                table_info,
                cluster_id,
            ),
        }
    }

    pub(crate) fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;

        Ok(AlterTableProcedure { context, data })
    }

    /// Alters table on datanode.
    async fn on_datanode_alter_table(&mut self) -> Result<Status> {
        let table_route = &self.data.table_route;

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
                    .context(error::RequestDatanodeSnafu { peer: datanode })
            }));
        }

        let _ = join_all(joins)
            .await
            .into_iter()
            .map(|e| e.context(error::JoinSnafu).flatten())
            .collect::<Result<Vec<_>>>()
            .map_err(|err| {
                error::RetryLaterSnafu {
                    reason: format!("Failed to execute drop table on datanode, source: {}", err),
                }
                .build()
            })?;

        self.data.state = AlterTableState::UpdateMetadata;

        Ok(Status::executing(true))
    }

    /// Update table metadata.
    async fn on_update_metadata(&mut self) -> Result<Status> {
        // Builds new_meta
        let table_info = TableInfo::try_from(self.data.table_info.clone())
            .context(error::ConvertRawTableInfoSnafu)?;

        let table_id = table_info.ident.table_id;

        let table_ref = self.data.table_ref();

        let request = self.data.alter_table_request.clone();

        let new_meta = table_info
            .meta
            .builder_with_alter_kind(table_ref.table, &request.alter_kind)
            .context(error::TableSnafu)?
            .build()
            .with_context(|_| error::BuildTableMetaSnafu {
                table_name: table_ref.table.clone(),
            })?;

        let mut new_info = table_info.clone();
        new_info.ident.version = table_info.ident.version + 1;
        new_info.meta = new_meta;
        let new_raw_info = new_info.into();

        let table_global_key = TableGlobalKey {
            catalog_name: table_ref.catalog.to_string(),
            schema_name: table_ref.schema.to_string(),
            table_name: table_ref.table.to_string(),
        };

        let fetched_table_global_value =
            get_table_global_value(&self.context.kv_store, &table_global_key)
                .await?
                .with_context(|| error::TableNotFoundSnafu {
                    name: table_ref.to_string(),
                })?;

        // If the metadata already updated.
        if fetched_table_global_value.table_info == new_raw_info {
            self.data.state = AlterTableState::Broadcast;
            return Ok(Status::executing(true));
        }

        // Builds txn
        let table_ref = self.data.table_ref();

        let TableMetadata {
            table_global_key,
            mut table_global_value,
            table_route_key,
            table_route_value,
        } = build_table_metadata(
            table_ref,
            self.data.table_route.clone(),
            self.data.table_info.clone(),
        )?;

        let mut txn = Txn::new().when(vec![
            Compare::with_value(
                table_route_key.to_string().into_bytes(),
                CompareOp::Equal,
                table_route_value.clone().into(),
            ),
            Compare::with_value(
                table_global_key.to_string().into_bytes(),
                CompareOp::Equal,
                table_global_value
                    .clone()
                    .as_bytes()
                    .context(error::InvalidCatalogValueSnafu)?,
            ),
        ]);

        table_global_value.table_info = new_raw_info;

        // If needs to remove a table
        // - Removes original global value and route value. Put new global value and route value.
        // Else
        // - Updates original global value
        txn = if let AlterKind::RenameTable { new_table_name } =
            &self.data.alter_table_request.alter_kind
        {
            let new_table_ref = TableReference {
                catalog: table_ref.catalog,
                schema: table_ref.schema,
                table: new_table_name,
            };

            let (new_table_global_key, new_table_route_key) =
                build_table_metadata_key(new_table_ref, table_id);

            txn.and_then(vec![
                TxnOp::Delete(table_global_key.to_string().into_bytes()),
                TxnOp::Delete(table_route_key.to_string().into_bytes()),
                TxnOp::Put(
                    new_table_global_key.to_string().into_bytes(),
                    table_global_value
                        .clone()
                        .as_bytes()
                        .context(error::InvalidCatalogValueSnafu)?,
                ),
                TxnOp::Put(
                    new_table_route_key.to_string().into_bytes(),
                    table_route_value.into(),
                ),
            ])
        } else {
            txn.and_then(vec![TxnOp::Put(
                table_global_key.to_string().into_bytes(),
                table_global_value
                    .clone()
                    .as_bytes()
                    .context(error::InvalidCatalogValueSnafu)?,
            )])
        };

        let resp = self.context.kv_store.txn(txn).await?;

        ensure!(
            resp.succeeded,
            error::TxnSnafu {
                msg: "table metadata changed"
            }
        );

        self.data.state = AlterTableState::Broadcast;

        Ok(Status::executing(true))
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
            "Invalidate Table Cache",
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

        Ok(Status::Done)
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
            AlterTableState::DatanodeAlterTable => {
                self.on_datanode_alter_table().await.map_err(error_handler)
            }
            AlterTableState::UpdateMetadata => {
                self.on_update_metadata().await.map_err(error_handler)
            }
            AlterTableState::Broadcast => self.on_broadcast().await.map_err(error_handler),
        }
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
    /// Datanode alters the table.
    DatanodeAlterTable,
    /// Updates table metadata.
    UpdateMetadata,
    /// Broadcasts the invalidating table cache instruction.
    Broadcast,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AlterTableData {
    state: AlterTableState,
    task: AlterTableTask,
    alter_table_request: AlterTableRequest,
    table_route: TableRoute,
    table_info: RawTableInfo,
    cluster_id: u64,
}

impl AlterTableData {
    pub fn new(
        task: AlterTableTask,
        alter_table_request: AlterTableRequest,
        table_route: TableRoute,
        table_info: RawTableInfo,
        cluster_id: u64,
    ) -> Self {
        Self {
            state: AlterTableState::DatanodeAlterTable,
            task,
            alter_table_request,
            table_route,
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
