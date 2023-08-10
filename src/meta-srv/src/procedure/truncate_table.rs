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
use client::Database;
use common_meta::rpc::ddl::TruncateTableTask;
use common_meta::rpc::router::{RegionRoute, find_leaders};
use common_meta::table_name::TableName;
use common_procedure::error::{FromJsonSnafu, ToJsonSnafu};
use common_procedure::{
    Context as ProcedureContext, Error as ProcedureError, LockKey, Procedure,
    Result as ProcedureResult, Status,
};
use common_telemetry::debug;
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use table::engine::TableReference;

use crate::ddl::DdlContext;
use crate::error::{self, Result};
use crate::procedure::utils::handle_request_datanode_error;

pub struct TruncateTableProcedure {
    context: DdlContext,
    data: TruncateTableData,
}

#[async_trait]
impl Procedure for TruncateTableProcedure {
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
            TruncateTableState::DatanodeTruncateTable => self.on_datanode_truncate_table().await,
        }
        .map_err(error_handler)
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

impl TruncateTableProcedure {
    pub(crate) const TYPE_NAME: &'static str = "metasrv-procedure::TruncateTableProcedure";

    pub(crate) fn new(
        cluster_id: u64,
        task: TruncateTableTask,
        region_routes: Vec<RegionRoute>,
        context: DdlContext,
    ) -> Self {
        Self {
            context,
            data: TruncateTableData::new(cluster_id, task, region_routes),
        }
    }

    pub(crate) fn from_json(json: &str, context: DdlContext) -> ProcedureResult<Self> {
        let data = serde_json::from_str(json).context(FromJsonSnafu)?;
        Ok(Self { context, data })
    }

    async fn on_datanode_truncate_table(&mut self) -> Result<Status> {
        let region_routes = &self.data.region_routes;

        let table_ref = self.data.table_ref();

        let clients = self.context.datanode_clients.clone();
        let leaders = find_leaders(region_routes);

        let mut joins = Vec::with_capacity(leaders.len());

        for datanode in leaders {
            debug!("Truncating table {}, on Datanode {:?}", table_ref, datanode);

            let client = clients.get_client(&datanode).await;
            let client = Database::new(table_ref.catalog, table_ref.schema, client);
            let expr = self.data.task.truncate_table.clone();
            joins.push(common_runtime::spawn_bg(async move {
                debug!("Sending {:?} to {:?}", expr, client);
                client
                    .truncate_table(expr)
                    .await
                    .map_err(handle_request_datanode_error(datanode))
            }));
        }
        join_all(joins)
            .await
            .into_iter()
            .map(|e| e.context(error::JoinSnafu))
            .collect::<Result<Vec<_>>>()?;
        Ok(Status::Done)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TruncateTableData {
    state: TruncateTableState,
    cluster_id: u64,
    task: TruncateTableTask,
    region_routes: Vec<RegionRoute>,
}

impl TruncateTableData {
    pub fn new(cluster_id: u64, task: TruncateTableTask, region_routes: Vec<RegionRoute>) -> Self {
        Self {
            state: TruncateTableState::DatanodeTruncateTable,
            cluster_id,
            task,
            region_routes,
        }
    }

    pub fn table_ref(&self) -> TableReference {
        self.task.table_ref()
    }

    pub fn table_name(&self) -> TableName {
        self.task.table_name()
    }
}

#[derive(Debug, Serialize, Deserialize)]
enum TruncateTableState {
    /// Datanode truncates the table
    DatanodeTruncateTable,
}
