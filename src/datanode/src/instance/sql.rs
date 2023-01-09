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
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::prelude::BoxedError;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_telemetry::logging::{error, info};
use common_telemetry::timer;
use query::parser::{QueryLanguageParser, QueryStatement};
use servers::query_handler::SqlQueryHandler;
use session::context::QueryContextRef;
use snafu::prelude::*;
use sql::ast::ObjectName;
use sql::statements::statement::Statement;
use table::engine::TableReference;
use table::requests::CreateDatabaseRequest;

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
            QueryStatement::SQL(Statement::Query(_)) | QueryStatement::PromQL(_) => {
                let logical_plan = self
                    .query_engine
                    .statement_to_plan(stmt, query_ctx)
                    .context(ExecuteSqlSnafu)?;

                self.query_engine
                    .execute(&logical_plan)
                    .await
                    .context(ExecuteSqlSnafu)
            }
            QueryStatement::SQL(Statement::Insert(i)) => {
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

            QueryStatement::SQL(Statement::CreateDatabase(c)) => {
                let request = CreateDatabaseRequest {
                    db_name: c.name.to_string(),
                };

                info!("Creating a new database: {}", request.db_name);

                self.sql_handler
                    .execute(SqlRequest::CreateDatabase(request), query_ctx)
                    .await
            }

            QueryStatement::SQL(Statement::CreateTable(c)) => {
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
                let request = self.sql_handler.create_to_request(table_id, c, table_ref)?;
                let table_id = request.id;
                info!(
                    "Creating table, catalog: {:?}, schema: {:?}, table name: {:?}, table id: {}",
                    catalog, schema, table, table_id
                );

                self.sql_handler
                    .execute(SqlRequest::CreateTable(request), query_ctx)
                    .await
            }
            QueryStatement::SQL(Statement::Alter(alter_table)) => {
                let name = alter_table.table_name().clone();
                let (catalog, schema, table) = table_idents_to_full_name(&name, query_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let req = self.sql_handler.alter_to_request(alter_table, table_ref)?;
                self.sql_handler
                    .execute(SqlRequest::Alter(req), query_ctx)
                    .await
            }
            QueryStatement::SQL(Statement::DropTable(drop_table)) => {
                let req = self.sql_handler.drop_table_to_request(drop_table);
                self.sql_handler
                    .execute(SqlRequest::DropTable(req), query_ctx)
                    .await
            }
            QueryStatement::SQL(Statement::ShowDatabases(stmt)) => {
                self.sql_handler
                    .execute(SqlRequest::ShowDatabases(stmt), query_ctx)
                    .await
            }
            QueryStatement::SQL(Statement::ShowTables(stmt)) => {
                self.sql_handler
                    .execute(SqlRequest::ShowTables(stmt), query_ctx)
                    .await
            }
            QueryStatement::SQL(Statement::Explain(stmt)) => {
                self.sql_handler
                    .execute(SqlRequest::Explain(Box::new(stmt)), query_ctx)
                    .await
            }
            QueryStatement::SQL(Statement::DescribeTable(stmt)) => {
                self.sql_handler
                    .execute(SqlRequest::DescribeTable(stmt), query_ctx)
                    .await
            }
            QueryStatement::SQL(Statement::ShowCreateTable(_stmt)) => {
                unimplemented!("SHOW CREATE TABLE is unimplemented yet");
            }
            QueryStatement::SQL(Statement::Use(db)) => {
                ensure!(
                    self.catalog_manager
                        .schema(DEFAULT_CATALOG_NAME, &db)
                        .context(error::CatalogSnafu)?
                        .is_some(),
                    error::SchemaNotFoundSnafu { name: &db }
                );

                query_ctx.set_current_schema(&db);

                Ok(Output::RecordBatches(RecordBatches::empty()))
            }
        }
    }

    pub async fn execute_sql(&self, sql: &str, query_ctx: QueryContextRef) -> Result<Output> {
        let stmt = QueryLanguageParser::parse_sql(sql).context(ExecuteSqlSnafu)?;
        self.execute_stmt(stmt, query_ctx).await
    }
}

// TODO(LFC): Refactor consideration: move this function to some helper mod,
// could be done together or after `TableReference`'s refactoring, when issue #559 is resolved.
/// Converts maybe fully-qualified table name (`<catalog>.<schema>.<table>`) to tuple.
fn table_idents_to_full_name(
    obj_name: &ObjectName,
    query_ctx: QueryContextRef,
) -> Result<(String, String, String)> {
    match &obj_name.0[..] {
        [table] => Ok((
            DEFAULT_CATALOG_NAME.to_string(),
            query_ctx.current_schema().unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string()),
            table.value.clone(),
        )),
        [schema, table] => Ok((
            DEFAULT_CATALOG_NAME.to_string(),
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
    async fn do_query(
        &self,
        query: &str,
        query_ctx: QueryContextRef,
    ) -> Vec<servers::error::Result<Output>> {
        let _timer = timer!(metric::METRIC_HANDLE_SQL_ELAPSED);
        // we assume sql string has only 1 statement in datanode
        let result = self
            .execute_sql(query, query_ctx)
            .await
            .map_err(|e| {
                error!(e; "Instance failed to execute sql");
                BoxedError::new(e)
            })
            .context(servers::error::ExecuteQuerySnafu { query });
        vec![result]
    }

    async fn do_statement_query(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> servers::error::Result<Output> {
        let _timer = timer!(metric::METRIC_HANDLE_SQL_ELAPSED);
        self.execute_stmt(QueryStatement::SQL(stmt), query_ctx)
            .await
            .map_err(|e| {
                error!(e; "Instance failed to execute sql");
                BoxedError::new(e)
            })
            .context(servers::error::ExecuteStatementSnafu)
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> servers::error::Result<bool> {
        self.catalog_manager
            .schema(catalog, schema)
            .map(|s| s.is_some())
            .context(servers::error::CatalogSnafu)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

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
        let query_ctx = Arc::new(QueryContext::with_current_schema(using_schema.to_string()));
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
