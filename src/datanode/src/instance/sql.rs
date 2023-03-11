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
use common_recordbatch::RecordBatches;
use common_telemetry::logging::info;
use common_telemetry::timer;
use datatypes::schema::Schema;
use futures::StreamExt;
use query::parser::{PromQuery, QueryLanguageParser, QueryStatement};
use query::plan::LogicalPlan;
use servers::error as server_error;
use servers::prom::PromHandler;
use servers::query_handler::sql::SqlQueryHandler;
use session::context::{QueryContext, QueryContextRef};
use snafu::prelude::*;
use sql::ast::ObjectName;
use sql::statements::copy::CopyTable;
use sql::statements::statement::Statement;
use sql::statements::tql::Tql;
use table::engine::TableReference;
use table::requests::{
    CopyTableFromRequest, CopyTableRequest, CreateDatabaseRequest, DropTableRequest,
};

use crate::error::{self, BumpTableIdSnafu, ExecuteSqlSnafu, Result, TableIdProviderNotFoundSnafu};
use crate::instance::Instance;
use crate::metric;
use crate::sql::insert::InsertRequests;
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
                    .await
                    .context(ExecuteSqlSnafu)?;

                self.query_engine
                    .execute(&logical_plan)
                    .await
                    .context(ExecuteSqlSnafu)
            }
            QueryStatement::Sql(Statement::Insert(insert)) => {
                let requests = self
                    .sql_handler
                    .insert_to_requests(self.catalog_manager.clone(), *insert, query_ctx.clone())
                    .await?;

                match requests {
                    InsertRequests::Request(request) => {
                        self.sql_handler.execute(request, query_ctx.clone()).await
                    }

                    InsertRequests::Stream(mut s) => {
                        let mut rows = 0;
                        while let Some(request) = s.next().await {
                            match self
                                .sql_handler
                                .execute(request?, query_ctx.clone())
                                .await?
                            {
                                Output::AffectedRows(n) => {
                                    rows += n;
                                }
                                _ => unreachable!(),
                            }
                        }
                        Ok(Output::AffectedRows(rows))
                    }
                }
            }
            QueryStatement::Sql(Statement::Delete(delete)) => {
                let request = SqlRequest::Delete(*delete);
                self.sql_handler.execute(request, query_ctx).await
            }
            QueryStatement::Sql(Statement::CreateDatabase(create_database)) => {
                let request = CreateDatabaseRequest {
                    db_name: create_database.name.to_string(),
                    create_if_not_exists: create_database.if_not_exists,
                };

                info!("Creating a new database: {}", request.db_name);

                self.sql_handler
                    .execute(SqlRequest::CreateDatabase(request), query_ctx)
                    .await
            }

            QueryStatement::Sql(Statement::CreateTable(create_table)) => {
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
                let request =
                    self.sql_handler
                        .create_to_request(table_id, create_table, &table_ref)?;
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
            QueryStatement::Sql(Statement::ShowDatabases(show_databases)) => {
                self.sql_handler
                    .execute(SqlRequest::ShowDatabases(show_databases), query_ctx)
                    .await
            }
            QueryStatement::Sql(Statement::ShowTables(show_tables)) => {
                self.sql_handler
                    .execute(SqlRequest::ShowTables(show_tables), query_ctx)
                    .await
            }
            QueryStatement::Sql(Statement::Explain(explain)) => {
                self.sql_handler
                    .execute(SqlRequest::Explain(Box::new(explain)), query_ctx)
                    .await
            }
            QueryStatement::Sql(Statement::DescribeTable(describe_table)) => {
                self.sql_handler
                    .execute(SqlRequest::DescribeTable(describe_table), query_ctx)
                    .await
            }
            QueryStatement::Sql(Statement::ShowCreateTable(_show_create_table)) => {
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
            QueryStatement::Sql(Statement::Copy(copy_table)) => match copy_table {
                CopyTable::To(copy_table) => {
                    let (catalog_name, schema_name, table_name) =
                        table_idents_to_full_name(copy_table.table_name(), query_ctx.clone())?;
                    let file_name = copy_table.file_name().to_string();

                    let req = CopyTableRequest {
                        catalog_name,
                        schema_name,
                        table_name,
                        file_name,
                    };

                    self.sql_handler
                        .execute(SqlRequest::CopyTable(req), query_ctx)
                        .await
                }
                CopyTable::From(copy_table) => {
                    let (catalog_name, schema_name, table_name) =
                        table_idents_to_full_name(&copy_table.table_name, query_ctx.clone())?;
                    let req = CopyTableFromRequest {
                        catalog_name,
                        schema_name,
                        table_name,
                        connection: copy_table.connection,
                        pattern: copy_table.pattern,
                        from: copy_table.from,
                    };
                    self.sql_handler
                        .execute(SqlRequest::CopyTableFrom(req), query_ctx)
                        .await
                }
            },
            QueryStatement::Sql(Statement::Tql(tql)) => self.execute_tql(tql, query_ctx).await,
        }
    }

    pub(crate) async fn execute_tql(&self, tql: Tql, query_ctx: QueryContextRef) -> Result<Output> {
        match tql {
            Tql::Eval(eval) => {
                let promql = PromQuery {
                    start: eval.start,
                    end: eval.end,
                    step: eval.step,
                    query: eval.query,
                };
                let stmt = QueryLanguageParser::parse_promql(&promql).context(ExecuteSqlSnafu)?;
                let logical_plan = self
                    .query_engine
                    .statement_to_plan(stmt, query_ctx)
                    .await
                    .context(ExecuteSqlSnafu)?;

                self.query_engine
                    .execute(&logical_plan)
                    .await
                    .context(ExecuteSqlSnafu)
            }
            Tql::Explain(_explain) => {
                todo!("waiting for promql-parser ast adding a explain node")
            }
        }
    }

    pub async fn execute_sql(&self, sql: &str, query_ctx: QueryContextRef) -> Result<Output> {
        let stmt = QueryLanguageParser::parse_sql(sql).context(ExecuteSqlSnafu)?;
        self.execute_stmt(stmt, query_ctx).await
    }

    pub async fn execute_promql(
        &self,
        promql: &PromQuery,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let stmt = QueryLanguageParser::parse_promql(promql).context(ExecuteSqlSnafu)?;
        self.execute_stmt(stmt, query_ctx).await
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
        query: &PromQuery,
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

    async fn do_describe(
        &self,
        stmt: Statement,
        query_ctx: QueryContextRef,
    ) -> Result<Option<(Schema, LogicalPlan)>> {
        if let Statement::Query(_) = stmt {
            self.query_engine
                .describe(QueryStatement::Sql(stmt), query_ctx)
                .await
                .map(Some)
                .context(error::DescribeStatementSnafu)
        } else {
            Ok(None)
        }
    }

    fn is_valid_schema(&self, catalog: &str, schema: &str) -> Result<bool> {
        self.catalog_manager
            .schema(catalog, schema)
            .map(|s| s.is_some())
            .context(error::CatalogSnafu)
    }
}

#[async_trait]
impl PromHandler for Instance {
    async fn do_query(&self, query: &PromQuery) -> server_error::Result<Output> {
        let _timer = timer!(metric::METRIC_HANDLE_PROMQL_ELAPSED);

        self.execute_promql(query, QueryContext::arc())
            .await
            .map_err(BoxedError::new)
            .with_context(|_| {
                let query_literal = format!("{query:?}");
                server_error::ExecuteQuerySnafu {
                    query: query_literal,
                }
            })
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
