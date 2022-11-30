// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use async_trait::async_trait;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::prelude::BoxedError;
use common_query::Output;
use common_telemetry::logging::{error, info};
use common_telemetry::timer;
use servers::query_handler::SqlQueryHandler;
use session::context::SessionContext;
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
    pub async fn execute_sql(&self, sql: &str, session_ctx: Arc<SessionContext>) -> Result<Output> {
        let stmt = self
            .query_engine
            .sql_to_statement(sql)
            .context(ExecuteSqlSnafu)?;

        match stmt {
            Statement::Query(_) => {
                let logical_plan = self
                    .query_engine
                    .statement_to_plan(stmt, session_ctx)
                    .context(ExecuteSqlSnafu)?;

                self.query_engine
                    .execute(&logical_plan)
                    .await
                    .context(ExecuteSqlSnafu)
            }
            Statement::Insert(i) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(i.table_name(), session_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let request = self.sql_handler.insert_to_request(
                    self.catalog_manager.clone(),
                    *i,
                    table_ref,
                )?;
                self.sql_handler.execute(request, session_ctx).await
            }

            Statement::CreateDatabase(c) => {
                let request = CreateDatabaseRequest {
                    db_name: c.name.to_string(),
                };

                info!("Creating a new database: {}", request.db_name);

                self.sql_handler
                    .execute(SqlRequest::CreateDatabase(request), session_ctx)
                    .await
            }

            Statement::CreateTable(c) => {
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
                let (catalog, schema, table) =
                    table_idents_to_full_name(&name, session_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let request = self.sql_handler.create_to_request(table_id, c, table_ref)?;
                let table_id = request.id;
                info!(
                    "Creating table, catalog: {:?}, schema: {:?}, table name: {:?}, table id: {}",
                    catalog, schema, table, table_id
                );

                self.sql_handler
                    .execute(SqlRequest::CreateTable(request), session_ctx)
                    .await
            }
            Statement::Alter(alter_table) => {
                let name = alter_table.table_name().clone();
                let (catalog, schema, table) =
                    table_idents_to_full_name(&name, session_ctx.clone())?;
                let table_ref = TableReference::full(&catalog, &schema, &table);
                let req = self.sql_handler.alter_to_request(alter_table, table_ref)?;
                self.sql_handler
                    .execute(SqlRequest::Alter(req), session_ctx)
                    .await
            }
            Statement::DropTable(drop_table) => {
                let req = self.sql_handler.drop_table_to_request(drop_table);
                self.sql_handler
                    .execute(SqlRequest::DropTable(req), session_ctx)
                    .await
            }
            Statement::ShowDatabases(stmt) => {
                self.sql_handler
                    .execute(SqlRequest::ShowDatabases(stmt), session_ctx)
                    .await
            }
            Statement::ShowTables(stmt) => {
                self.sql_handler
                    .execute(SqlRequest::ShowTables(stmt), session_ctx)
                    .await
            }
            Statement::Explain(stmt) => {
                self.sql_handler
                    .execute(SqlRequest::Explain(Box::new(stmt)), session_ctx)
                    .await
            }
            Statement::DescribeTable(stmt) => {
                self.sql_handler
                    .execute(SqlRequest::DescribeTable(stmt), session_ctx)
                    .await
            }
            Statement::ShowCreateTable(_stmt) => {
                unimplemented!("SHOW CREATE TABLE is unimplemented yet");
            }
        }
    }
}

/// Converts maybe fully-qualified table name (`<catalog>.<schema>.<table>`) to tuple.
fn table_idents_to_full_name(
    obj_name: &ObjectName,
    session_ctx: Arc<SessionContext>,
) -> Result<(String, String, String)> {
    match &obj_name.0[..] {
        [table] => Ok((
            DEFAULT_CATALOG_NAME.to_string(),
            session_ctx.current_schema().unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string()),
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
                "expect table name to be <catalog>.<schema>.<table>, <schema>.<table> or <table>, actual: {}",
                obj_name
            ),
        }.fail(),
    }
}

#[async_trait]
impl SqlQueryHandler for Instance {
    async fn do_query(
        &self,
        query: &str,
        session_ctx: Arc<SessionContext>,
    ) -> servers::error::Result<Output> {
        let _timer = timer!(metric::METRIC_HANDLE_SQL_ELAPSED);
        self.execute_sql(query, session_ctx)
            .await
            .map_err(|e| {
                error!(e; "Instance failed to execute sql");
                BoxedError::new(e)
            })
            .context(servers::error::ExecuteQuerySnafu { query })
    }
}
