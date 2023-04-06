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

mod describe;
mod show;
mod tql;

use catalog::CatalogManagerRef;
use common_query::Output;
use common_recordbatch::RecordBatches;
use query::parser::QueryStatement;
use query::query_engine::StatementExecutorExtRef;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{ensure, ResultExt};
use sql::statements::statement::Statement;

use crate::error::{
    CatalogSnafu, ExecLogicalPlanSnafu, ExecuteStatementSnafu, PlanStatementSnafu, Result,
    SchemaNotFoundSnafu,
};

#[derive(Clone)]
pub(crate) struct StatementExecutor {
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    executor_ext: StatementExecutorExtRef,
}

impl StatementExecutor {
    pub(crate) fn new(
        catalog_manager: CatalogManagerRef,
        query_engine: QueryEngineRef,
        executor_ext: StatementExecutorExtRef,
    ) -> Self {
        Self {
            catalog_manager,
            query_engine,
            executor_ext,
        }
    }

    pub(crate) async fn execute_sql(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        match stmt {
            Statement::Query(_) | Statement::Explain(_) | Statement::Delete(_) => {
                self.plan_exec(QueryStatement::Sql(stmt), query_ctx).await
            }

            // For performance consideration, only "insert with select" is executed by query engine.
            // Plain insert ("insert with values") is still executed directly in statement.
            Statement::Insert(ref insert) if insert.is_insert_select() => {
                self.plan_exec(QueryStatement::Sql(stmt), query_ctx).await
            }

            Statement::Tql(tql) => self.execute_tql(tql, query_ctx).await,

            Statement::DescribeTable(stmt) => self.describe_table(stmt, query_ctx).await,

            Statement::Use(db) => self.handle_use(db, query_ctx),

            Statement::ShowDatabases(stmt) => self.show_databases(stmt),

            Statement::ShowTables(stmt) => self.show_tables(stmt, query_ctx),

            Statement::CreateDatabase(_)
            | Statement::CreateTable(_)
            | Statement::CreateExternalTable(_)
            | Statement::Insert(_)
            | Statement::Alter(_)
            | Statement::DropTable(_)
            | Statement::Copy(_)
            | Statement::ShowCreateTable(_) => self
                .executor_ext
                .execute_sql(stmt, query_ctx)
                .await
                .context(ExecuteStatementSnafu),
        }
    }

    pub(crate) async fn plan_exec(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let planner = self.query_engine.planner();
        let plan = planner
            .plan(stmt, query_ctx.clone())
            .await
            .context(PlanStatementSnafu)?;
        self.query_engine
            .execute(plan, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)
    }

    fn handle_use(&self, db: String, query_ctx: QueryContextRef) -> Result<Output> {
        let catalog = &query_ctx.current_catalog();
        ensure!(
            self.catalog_manager
                .schema(catalog, &db)
                .context(CatalogSnafu)?
                .is_some(),
            SchemaNotFoundSnafu { schema_info: &db }
        );

        query_ctx.set_current_schema(&db);

        Ok(Output::RecordBatches(RecordBatches::empty()))
    }
}
