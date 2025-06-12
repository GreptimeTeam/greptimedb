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

use common_frontend::DisplayProcessId;
use common_query::Output;
use common_telemetry::error;
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::error;
use crate::statement::StatementExecutor;

impl StatementExecutor {
    pub async fn execute_kill(
        &self,
        query_ctx: QueryContextRef,
        process_id: String,
    ) -> crate::error::Result<Output> {
        let Some(process_manager) = self.process_manager.as_ref() else {
            error!("Process manager is not initialized");
            return Ok(Output::new_with_affected_rows(0));
        };

        let display_id = DisplayProcessId::try_from(process_id.as_str())
            .map_err(|_| error::InvalidProcessIdSnafu { id: process_id }.build())?;

        let current_user_catalog = query_ctx.current_catalog().to_string();
        process_manager
            .kill_process(display_id.server_addr, current_user_catalog, display_id.id)
            .await
            .context(error::CatalogSnafu)?;

        Ok(Output::new_with_affected_rows(0))
    }
}
