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
use common_error::prelude::BoxedError;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::logging::info;
use common_telemetry::timer;
use query::parser::{QueryLanguageParser, QueryStatement};
use servers::error as server_error;
use servers::promql::PromqlHandler;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use snafu::prelude::*;
use sql::ast::ObjectName;
use sql::statements::statement::Statement;
use table::engine::TableReference;
use table::requests::{CreateDatabaseRequest, DropTableRequest};

use crate::error::{self, BumpTableIdSnafu, ExecuteSqlSnafu, Result, TableIdProviderNotFoundSnafu};
use crate::instance::Instance;
use crate::metric;
use crate::sql::SqlRequest;

impl Instance {
    pub async fn execute_stmt(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        match stmt {
            QueryStatement::Sql(Statement::Query(_)) | QueryStatement::Promql(_) => {
                let logical_plan = self
                    .query_engine
                    .statement_to_plan(stmt, query_ctx)
                    .context(ExecuteSqlSnafu)?;

                self.query_engine
                    .execute(&logical_plan)
                    .await
                    .context(ExecuteSqlSnafu)
            }
            QueryStatement::Sql(Statement::Insert(i)) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(i.table_name(), query_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let request = self.sql_handler.insert_to_request(
                    self.catalog_manager.clone(),
                    *i,
                    table_ref,
                )?;
                self.sql_handler.execute(request, query_ctx).await
            }
            QueryStatement::Sql(Statement::Delete(d)) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(d.table_name(), query_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let request = self.sql_handler.delete_to_request(table_ref, *d)?;
                self.sql_handler.execute(request, query_ctx).await
            }

            QueryStatement::Sql(Statement::CreateDatabase(c)) => {
                let request = CreateDatabaseRequest {
                    db_name: c.name.to_string(),
                    create_if_not_exists: c.if_not_exists,
                };

                info!("Creating a new database: {}", request.db_name);

                self.sql_handler
                    .execute(SqlRequest::CreateDatabase(request), query_ctx)
                    .await
            }

            QueryStatement::Sql(Statement::CreateTable(c)) => {
                let table_id = self
                    .table_id_provider
                    .as_ref()
                    .context(TableIdProviderNotFoundSnafu)?
                    .next_table_id()
                    .await
                    .context(BumpTableIdSnafu)?;
                let _engine_name = c.engine.clone();
                // TODO(hl): Select table engine by engine_name

                let name = c.name.clone();
                let (catalog, schema, table) = table_idents_to_full_name(&name, query_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let request = self
                    .sql_handler
                    .create_to_request(table_id, c, &table_ref)?;
                let table_id = request.id;
                info!("Creating table: {table_ref}, table id = {table_id}",);

                self.sql_handler
                    .execute(SqlRequest::CreateTable(request), query_ctx)
                    .await
            }
            QueryStatement::Sql(Statement::Alter(alter_table)) => {
                let name = alter_table.table_name().clone();
                let (catalog, schema, table) = table_idents_to_full_name(&name, query_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let req = self.sql_handler.alter_to_request(alter_table, table_ref)?;
                self.sql_handler
                    .execute(SqlRequest::Alter(req), query_ctx)
                    .await
            }
            QueryStatement::Sql(Statement::DropTable(drop_table)) => {
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
            QueryStatement::Sql(Statement::ShowDatabases(stmt)) => {
                self.sql_handler
                    .execute(SqlRequest::ShowDatabases(stmt), query_ctx)
                    .await
            }
            QueryStatement::Sql(Statement::ShowTables(stmt)) => {
                self.sql_handler
                    .execute(SqlRequest::ShowTables(stmt), query_ctx)
                    .await
            }
            QueryStatement::Sql(Statement::Explain(stmt)) => {
                self.sql_handler
                    .execute(SqlRequest::Explain(Box::new(stmt)), query_ctx)
                    .await
            }
            QueryStatement::Sql(Statement::DescribeTable(stmt)) => {
                self.sql_handler
                    .execute(SqlRequest::DescribeTable(stmt), query_ctx)
                    .await
            }
            QueryStatement::Sql(Statement::ShowCreateTable(_stmt)) => {
                unimplemented!("SHOW CREATE TABLE is unimplemented yet");
            }
            QueryStatement::Sql(Statement::Use(ref schema)) => {
                let catalog = &query_ctx.current_catalog();
                ensure!(
                    self.is_valid_schema(catalog, schema)?,
                    error::DatabaseNotFoundSnafu { catalog, schema }
                );

                query_ctx.set_current_schema(schema);

                Ok(Output::RecordBatches(RecordBatches::empty()))
            }
        }
    }

    pub async fn execute_sql(&self, sql: &str, query_ctx: QueryContextRef) -> Result<Output> {
        let stmt = QueryLanguageParser::parse_sql(sql).context(ExecuteSqlSnafu)?;
        self.execute_stmt(stmt, query_ctx).await
    }

    pub async fn execute_promql(&self, sql: &str, query_ctx: QueryContextRef) -> Result<Output> {
        let stmt = QueryLanguageParser::parse_promql(sql).context(ExecuteSqlSnafu)?;
        self.execute_stmt(stmt, query_ctx).await
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
impl SqlQueryHandler for Instance {
    type Error = error::Error;

    async fn do_query(&self, query: &str, query_ctx: QueryContextRef) -> Vec<Result<Output>> {
        let _timer = timer!(metric::METRIC_HANDLE_SQL_ELAPSED);
        // we assume sql string has only 1 statement in datanode
        let result = self.execute_sql(query, query_ctx).await;
        vec![result]
    }

    async fn do_promql_query(
        &self,
        query: &str,
        query_ctx: QueryContextRef,
    ) -> Vec<Result<Output>> {
        let _timer = timer!(metric::METRIC_HANDLE_PROMQL_ELAPSED);
        let result = self.execute_promql(query, query_ctx).await;
        vec![result]
    }

    async fn do_statement_query(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let _timer = timer!(metric::METRIC_HANDLE_SQL_ELAPSED);
        self.execute_stmt(QueryStatement::Sql(stmt), query_ctx)
            .await
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.catalog_manager
            .schema(catalog, schema)
            .map(|s| s.is_some())
            .context(error::CatalogSnafu)
    }
}

#[async_trait]
impl PromqlHandler for Instance {
    async fn do_query(&self, query: &str) -> server_error::Result<Output> {
        let _timer = timer!(metric::METRIC_HANDLE_PROMQL_ELAPSED);
        self.execute_promql(query, QueryContext::arc())
            .await
            .map_err(BoxedError::new)
            .with_context(|_| server_error::ExecuteQuerySnafu { query })
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
