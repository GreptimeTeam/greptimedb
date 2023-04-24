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

use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_query::Output;
use common_telemetry::logging::info;
use common_telemetry::timer;
use query::error::QueryExecutionSnafu;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::query_engine::SqlStatementExecutor;
use session::context::QueryContextRef;
use snafu::prelude::*;
use sql::ast::ObjectName;
use sql::statements::statement::Statement;
use table::engine::TableReference;
use table::requests::{CreateDatabaseRequest, DropTableRequest};

use crate::error::{
    self, BumpTableIdSnafu, ExecuteSqlSnafu, ExecuteStatementSnafu, NotSupportSqlSnafu,
    PlanStatementSnafu, Result, TableIdProviderNotFoundSnafu,
};
use crate::instance::Instance;
use crate::metrics;
use crate::sql::{SqlHandler, SqlRequest};

impl Instance {
    async fn do_execute_sql(&self, stmt: Statement, query_ctx: QueryContextRef) -> Result<Output> {
        match stmt {
            Statement::Insert(insert) => {
                let request =
                    SqlHandler::insert_to_request(self.catalog_manager.clone(), *insert, query_ctx)
                        .await?;
                self.sql_handler.insert(request).await
            }
            Statement::CreateDatabase(create_database) => {
                let request = CreateDatabaseRequest {
                    db_name: create_database.name.to_string(),
                    create_if_not_exists: create_database.if_not_exists,
                };

                info!("Creating a new database: {}", request.db_name);

                self.sql_handler
                    .execute(SqlRequest::CreateDatabase(request), query_ctx)
                    .await
            }

            Statement::CreateTable(create_table) => {
                let table_id = self
                    .table_id_provider
                    .as_ref()
                    .context(TableIdProviderNotFoundSnafu)?
                    .next_table_id()
                    .await
                    .context(BumpTableIdSnafu)?;
                let _engine_name = create_table.engine.clone();
                // TODO(hl): Select table engine by engine_name

                let name = create_table.name.clone();
                let (catalog, schema, table) = table_idents_to_full_name(&name, query_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let request = SqlHandler::create_to_request(table_id, create_table, &table_ref)?;
                let table_id = request.id;
                info!("Creating table: {table_ref}, table id = {table_id}",);

                self.sql_handler
                    .execute(SqlRequest::CreateTable(request), query_ctx)
                    .await
            }
            Statement::CreateExternalTable(create_external_table) => {
                let table_id = self
                    .table_id_provider
                    .as_ref()
                    .context(TableIdProviderNotFoundSnafu)?
                    .next_table_id()
                    .await
                    .context(BumpTableIdSnafu)?;
                let name = create_external_table.name.clone();
                let (catalog, schema, table) = table_idents_to_full_name(&name, query_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let request = self
                    .sql_handler
                    .create_external_to_request(table_id, create_external_table, &table_ref)
                    .await?;
                let table_id = request.id;
                info!("Creating external table: {table_ref}, table id = {table_id}",);
                self.sql_handler
                    .execute(SqlRequest::CreateTable(request), query_ctx)
                    .await
            }
            Statement::Alter(alter_table) => {
                let name = alter_table.table_name().clone();
                let (catalog, schema, table) = table_idents_to_full_name(&name, query_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let req = SqlHandler::alter_to_request(alter_table, table_ref)?;
                self.sql_handler
                    .execute(SqlRequest::Alter(req), query_ctx)
                    .await
            }
            Statement::DropTable(drop_table) => {
                let (catalog_name, schema_name, table_name) =
                    table_idents_to_full_name(drop_table.table_name(), query_ctx.clone())?;
                let req = DropTableRequest {
                    catalog_name,
                    schema_name,
                    table_name,
                };
                self.sql_handler
                    .execute(SqlRequest::DropTable(req), query_ctx)
                    .await
            }
            Statement::ShowCreateTable(show) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(&show.table_name, query_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let table = self.sql_handler.get_table(&table_ref).await?;

                query::sql::show_create_table(table, None).context(ExecuteStatementSnafu)
            }
            _ => NotSupportSqlSnafu {
                msg: format!("not supported to execute {stmt:?}"),
            }
            .fail(),
        }
    }

    pub async fn execute_promql(
        &self,
        promql: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let _timer = timer!(metrics::HANDLE_PROMQL_ELAPSED);

        let stmt = QueryLanguageParser::parse_promql(promql).context(ExecuteSqlSnafu)?;

        let engine = self.query_engine();
        let plan = engine
            .planner()
            .plan(stmt, query_ctx.clone())
            .await
            .context(PlanStatementSnafu)?;
        engine
            .execute(plan, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    // TODO(ruihang): merge this and `execute_promql` after #951 landed
    pub async fn execute_promql_statement(
        &self,
        promql: &str,
        start: SystemTime,
        end: SystemTime,
        interval: Duration,
        lookback: Duration,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let query = PromQuery {
            query: promql.to_string(),
            start: "0".to_string(),
            end: "0".to_string(),
            step: "5m".to_string(),
        };
        let mut stmt = QueryLanguageParser::parse_promql(&query).context(ExecuteSqlSnafu)?;
        match &mut stmt {
            QueryStatement::Sql(_) => unreachable!(),
            QueryStatement::Promql(eval_stmt) => {
                eval_stmt.start = start;
                eval_stmt.end = end;
                eval_stmt.interval = interval;
                eval_stmt.lookback_delta = lookback
            }
        }

        let engine = self.query_engine();
        let plan = engine
            .planner()
            .plan(stmt, query_ctx.clone())
            .await
            .context(PlanStatementSnafu)?;
        engine
            .execute(plan, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }
}

// TODO(LFC): Refactor consideration: move this function to some helper mod,
// could be done together or after `TableReference`'s refactoring, when issue #559 is resolved.
/// Converts maybe fully-qualified table name (`<catalog>.<schema>.<table>`) to tuple.
pub fn table_idents_to_full_name(
    obj_name: &ObjectName,
    query_ctx: QueryContextRef,
) -> Result<(String, String, String)> {
    match &obj_name.0[..] {
        [table] => Ok((
            query_ctx.current_catalog(),
            query_ctx.current_schema(),
            table.value.clone(),
        )),
        [schema, table] => Ok((
            query_ctx.current_catalog(),
            schema.value.clone(),
            table.value.clone(),
        )),
        [catalog, schema, table] => Ok((
            catalog.value.clone(),
            schema.value.clone(),
            table.value.clone(),
        )),
        _ => error::InvalidSqlSnafu {
            msg: format!(
                "expect table name to be <catalog>.<schema>.<table>, <schema>.<table> or <table>, actual: {obj_name}",
            ),
        }.fail(),
    }
}

#[async_trait]
impl SqlStatementExecutor for Instance {
    async fn execute_sql(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> query::error::Result<Output> {
        self.do_execute_sql(stmt, query_ctx)
            .await
            .map_err(BoxedError::new)
            .context(QueryExecutionSnafu)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use session::context::QueryContext;

    use super::*;

    #[test]
    fn test_table_idents_to_full_name() {
        let my_catalog = "my_catalog";
        let my_schema = "my_schema";
        let my_table = "my_table";

        let full = ObjectName(vec![my_catalog.into(), my_schema.into(), my_table.into()]);
        let partial = ObjectName(vec![my_schema.into(), my_table.into()]);
        let bare = ObjectName(vec![my_table.into()]);

        let using_schema = "foo";
        let query_ctx = Arc::new(QueryContext::with(DEFAULT_CATALOG_NAME, using_schema));
        let empty_ctx = Arc::new(QueryContext::new());

        assert_eq!(
            table_idents_to_full_name(&full, query_ctx.clone()).unwrap(),
            (
                my_catalog.to_string(),
                my_schema.to_string(),
                my_table.to_string()
            )
        );
        assert_eq!(
            table_idents_to_full_name(&full, empty_ctx.clone()).unwrap(),
            (
                my_catalog.to_string(),
                my_schema.to_string(),
                my_table.to_string()
            )
        );

        assert_eq!(
            table_idents_to_full_name(&partial, query_ctx.clone()).unwrap(),
            (
                DEFAULT_CATALOG_NAME.to_string(),
                my_schema.to_string(),
                my_table.to_string()
            )
        );
        assert_eq!(
            table_idents_to_full_name(&partial, empty_ctx.clone()).unwrap(),
            (
                DEFAULT_CATALOG_NAME.to_string(),
                my_schema.to_string(),
                my_table.to_string()
            )
        );

        assert_eq!(
            table_idents_to_full_name(&bare, query_ctx).unwrap(),
            (
                DEFAULT_CATALOG_NAME.to_string(),
                using_schema.to_string(),
                my_table.to_string()
            )
        );
        assert_eq!(
            table_idents_to_full_name(&bare, empty_ctx).unwrap(),
            (
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_SCHEMA_NAME.to_string(),
                my_table.to_string()
            )
        );
    }
}
