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

use catalog::process_manager::ProcessManagerRef;
use common_frontend::DisplayProcessId;
use common_query::Output;
use common_telemetry::error;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::statements::kill::Kill;

use crate::error;
use crate::statement::StatementExecutor;

impl StatementExecutor {
    pub async fn execute_kill(
        &self,
        query_ctx: QueryContextRef,
        kill: Kill,
    ) -> error::Result<Output> {
        let Some(process_manager) = self.process_manager.as_ref() else {
            error!("Process manager is not initialized");
            return error::ProcessManagerMissingSnafu.fail();
        };

        let succ = match kill {
            Kill::ProcessId(process_id) => {
                self.kill_process_id(process_manager, query_ctx, process_id)
                    .await?
            }
            Kill::ConnectionId(conn_id) => {
                self.kill_connection_id(process_manager, query_ctx, conn_id)
                    .await?
            }
        };
        Ok(Output::new_with_affected_rows(if succ { 1 } else { 0 }))
    }

    /// Handles `KILL <PROCESS_ID>` statements.
    async fn kill_process_id(
        &self,
        pm: &ProcessManagerRef,
        query_ctx: QueryContextRef,
        process_id: String,
    ) -> error::Result<bool> {
        let display_id = DisplayProcessId::try_from(process_id.as_str())
            .map_err(|_| error::InvalidProcessIdSnafu { id: process_id }.build())?;

        let current_user_catalog = query_ctx.current_catalog().to_string();
        pm.kill_process(display_id.server_addr, current_user_catalog, display_id.id)
            .await
            .context(error::CatalogSnafu)
    }

    /// Handles MySQL `KILL QUERY <CONNECTION_ID>` statements.
    pub async fn kill_connection_id(
        &self,
        pm: &ProcessManagerRef,
        query_ctx: QueryContextRef,
        connection_id: u32,
    ) -> error::Result<bool> {
        pm.kill_local_process(query_ctx.current_catalog().to_string(), connection_id)
            .context(error::CatalogSnafu)
    }
}
