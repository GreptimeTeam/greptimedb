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
use common_telemetry::tracing;
use query::parser::QueryStatement;
use session::context::QueryContextRef;
use sql::statements::insert::Insert;
use sql::statements::statement::Statement;

use super::StatementExecutor;
use crate::error::Result;

impl StatementExecutor {
    #[tracing::instrument(skip_all)]
    pub async fn insert(&self, insert: Box<Insert>, query_ctx: QueryContextRef) -> Result<Output> {
        if insert.can_extract_values() {
            // Fast path: plain insert ("insert with literal values") is executed directly
            self.inserter
                .handle_statement_insert(insert.as_ref(), &query_ctx)
                .await
        } else {
            // Slow path: insert with subquery. Execute using query engine.
            let statement = QueryStatement::Sql(Statement::Insert(insert));
            self.plan_exec(statement, query_ctx).await
        }
    }
}
