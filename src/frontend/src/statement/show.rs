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

use common_query::Output;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::statements::show::{ShowDatabases, ShowTables};

use crate::error::{ExecuteStatementSnafu, Result};
use crate::statement::StatementExecutor;

impl StatementExecutor {
    pub(super) fn show_databases(&self, stmt: ShowDatabases) -> Result<Output> {
        query::sql::show_databases(stmt, self.catalog_manager.clone())
            .context(ExecuteStatementSnafu)
    }

    pub(super) async fn show_tables(
        &self,
        stmt: ShowTables,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_tables(stmt, self.catalog_manager.clone(), query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }
}
