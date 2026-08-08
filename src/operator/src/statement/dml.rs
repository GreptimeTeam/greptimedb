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

use common_catalog::consts::is_ddl_reserved_table;
use common_error::ext::BoxedError;
use common_query::Output;
use common_telemetry::tracing;
use query::parser::QueryStatement;
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::ResultExt;
use sql::statements::insert::Insert;
use sql::statements::statement::Statement;

use crate::error::{ExternalSnafu, ParseSqlSnafu, Result};
use crate::statement::StatementExecutor;

impl StatementExecutor {
    #[tracing::instrument(skip_all)]
    pub async fn insert(&self, insert: Box<Insert>, query_ctx: QueryContextRef) -> Result<Output> {
        self.create_ddl_reserved_target_on_demand(&insert, &query_ctx)
            .await?;
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

    /// A DDL-reserved table (the declared-edge table of the entity graph) is
    /// defined by the system, not the user: its first INSERT creates it here
    /// with the canonical schema.
    async fn create_ddl_reserved_target_on_demand(
        &self,
        insert: &Insert,
        query_ctx: &QueryContextRef,
    ) -> Result<()> {
        let table_name = insert.table_name().context(ParseSqlSnafu)?;
        let (catalog, schema, table) = table_idents_to_full_name(table_name, query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        if !is_ddl_reserved_table(&schema, &table) {
            return Ok(());
        }
        let exists = self
            .catalog_manager
            .table_exists(&catalog, &schema, &table, Some(query_ctx))
            .await
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        if !exists {
            self.create_declared_relationships_table(&catalog, query_ctx.clone())
                .await?;
        }
        Ok(())
    }
}
