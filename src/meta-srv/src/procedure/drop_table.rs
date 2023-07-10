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

use api::v1::meta::MailboxMessage;
use api::v1::{DropTableExpr, TableId};
use async_trait::async_trait;
use client::Database;
use common_catalog::consts::MITO_ENGINE;
use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_meta::ident::TableIdent;
use common_meta::instruction::Instruction;
use common_meta::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use common_meta::rpc::ddl::DropTableTask;
use common_meta::rpc::router::TableRoute;
use common_meta::table_name::TableName;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, LockKey, Procedure, Result as ProcedureResult, Status,
};
use common_telemetry::debug;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use table::engine::TableReference;

use super::utils::{build_table_metadata_key, handle_retry_error};
use crate::ddl::DdlContext;
use crate::error;
use crate::error::Result;
use crate::procedure::utils::{build_table_route_value, handle_request_datanode_error};
use crate::service::mailbox::BroadcastChannel;
use crate::table_routes::fetch_table;
pub struct DropTableProcedure {
    context: DdlContext,
    data: DropTableData,
}

// TODO(weny): removes in following PRs.
#[allow(unused)]
impl DropTableProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::DropTable";

    pub(crate) fn new(
        cluster_id: u64,
        task: DropTableTask,
        table_route: TableRoute,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: DropTableData::new(cluster_id, task, table_route),
        }
    }

    pub(crate) fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    /// Removes the table metadata.
    async fn on_remove_metadata(&mut self) -> Result<Status> {
        let table_ref = self.data.table_ref();

        // If metadata not exists (might have already been removed).
        if fetch_table(&self.context.kv_store, table_ref)
            .await?
            .is_none()
        {
            self.data.state = DropTableState::InvalidateTableCache;

            return Ok(Status::executing(true));
        }

        let table_ref = self.data.table_ref();
        let table_id = self.data.task.table_id;

        let (table_global_key, table_route_key) = build_table_metadata_key(table_ref, table_id);
        let table_route_value = build_table_route_value(self.data.table_route.clone())?;

        // To protect the potential resource leak issues.
        // We must compare the table route value, before deleting.
        let txn = Txn::new()
            .when(vec![Compare::with_value(
                table_route_key.to_string().into_bytes(),
                CompareOp::Equal,
                table_route_value.into(),
            )])
            .and_then(vec![
                TxnOp::Delete(table_route_key.to_string().into_bytes()),
                TxnOp::Delete(table_global_key.to_string().into_bytes()),
            ]);
        let resp = self.context.kv_store.txn(txn).await?;

        ensure!(
            resp.succeeded,
            error::TxnSnafu {
                msg: "table_route_value changed"
            }
        );

        self.data.state = DropTableState::InvalidateTableCache;

        Ok(Status::executing(true))
    }

    /// Broadcasts invalidate table cache instruction.
    async fn on_broadcast(&mut self) -> Result<Status> {
        let table_name = self.data.table_name();

        let table_ident = TableIdent {
            catalog: table_name.catalog_name,
            schema: table_name.schema_name,
            table: table_name.table_name,
            table_id: self.data.task.table_id,
            // TODO(weny): retrieves the engine from the upper.
            engine: MITO_ENGINE.to_string(),
        };
        let instruction = Instruction::InvalidateTableCache(table_ident);

        let msg = &MailboxMessage::json_message(
            "Invalidate Table Cache by dropping table procedure",
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

        self.data.state = DropTableState::DatanodeDropTable;

        Ok(Status::executing(true))
    }

    /// Executes drop table instruction on datanode.
    async fn on_datanode_drop_table(&mut self) -> Result<Status> {
        let table_route = &self.data.table_route;

        let table_ref = self.data.table_ref();
        let table_id = self.data.task.table_id;

        let clients = self.context.datanode_clients.clone();
        let leaders = table_route.find_leaders();
        let mut joins = Vec::with_capacity(leaders.len());

        let expr = DropTableExpr {
            catalog_name: table_ref.catalog.to_string(),
            schema_name: table_ref.schema.to_string(),
            table_name: table_ref.table.to_string(),
            table_id: Some(TableId { id: table_id }),
        };

        for datanode in leaders {
            debug!("Dropping table {table_ref} on Datanode {datanode:?}");

            let client = clients.get_client(&datanode).await;
            let client = Database::new(table_ref.catalog, table_ref.schema, client);
            let expr = expr.clone();
            joins.push(common_runtime::spawn_bg(async move {
                if let Err(err) = client.drop_table(expr).await {
                    // TODO(weny): add tests for `TableNotFound`
                    if err.status_code() != StatusCode::TableNotFound {
                        return Err(handle_request_datanode_error(datanode)(err));
                    }
                }
                Ok(())
            }));
        }

        let _r = join_all(joins)
            .await
            .into_iter()
            .map(|e| e.context(error::JoinSnafu).flatten())
            .collect::<Result<Vec<_>>>()?;

        Ok(Status::Done)
    }
}

#[async_trait]
impl Procedure for DropTableProcedure {
    fn type_name(&self) -> &str {
        Self::TYPE_NAME
    }

    async fn execute(&mut self, _ctx: &ProcedureContext) -> ProcedureResult<Status> {
        match self.data.state {
            DropTableState::RemoveMetadata => self.on_remove_metadata().await,
            DropTableState::InvalidateTableCache => self.on_broadcast().await,
            DropTableState::DatanodeDropTable => self.on_datanode_drop_table().await,
        }
        .map_err(handle_retry_error)
    }

    fn dump(&self) -> ProcedureResult<String> {
        serde_json::to_string(&self.data).context(ToJsonSnafu)
    }

    fn lock_key(&self) -> LockKey {
        let table_ref = &self.data.table_ref();
        let key = common_catalog::format_full_table_name(
            table_ref.catalog,
            table_ref.schema,
            table_ref.table,
        );

        LockKey::single(key)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DropTableData {
    state: DropTableState,
    cluster_id: u64,
    task: DropTableTask,
    table_route: TableRoute,
}

impl DropTableData {
    pub fn new(cluster_id: u64, task: DropTableTask, table_route: TableRoute) -> Self {
        Self {
            state: DropTableState::RemoveMetadata,
            cluster_id,
            task,
            table_route,
        }
    }

    fn table_ref(&self) -> TableReference {
        self.task.table_ref()
    }

    fn table_name(&self) -> TableName {
        self.task.table_name()
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum DropTableState {
    /// Removes metadata
    RemoveMetadata,
    /// Invalidates Table Cache
    InvalidateTableCache,
    /// Datanode drops the table
    DatanodeDropTable,
}
