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

use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_query::Output;
use common_telemetry::logging::{error, info};
use common_telemetry::timer;
use servers::query_handler::SqlQueryHandler;
use snafu::prelude::*;
use sql::statements::statement::Statement;
use table::requests::CreateDatabaseRequest;

use crate::error::{
    BumpTableIdSnafu, CatalogNotFoundSnafu, CatalogSnafu, ExecuteSqlSnafu, ParseSqlSnafu, Result,
    SchemaNotFoundSnafu, TableIdProviderNotFoundSnafu,
};
use crate::instance::Instance;
use crate::metric;
use crate::sql::SqlRequest;

impl Instance {
    pub async fn execute_sql(&self, sql: &str) -> Result<Output> {
        let stmt = self
            .query_engine
            .sql_to_statement(sql)
            .context(ExecuteSqlSnafu)?;

        match stmt {
            Statement::Query(_) => {
                let logical_plan = self
                    .query_engine
                    .statement_to_plan(stmt)
                    .context(ExecuteSqlSnafu)?;

                self.query_engine
                    .execute(&logical_plan)
                    .await
                    .context(ExecuteSqlSnafu)
            }
            Statement::Insert(i) => {
                let (catalog_name, schema_name, _table_name) =
                    i.full_table_name().context(ParseSqlSnafu)?;

                let schema_provider = self
                    .catalog_manager
                    .catalog(&catalog_name)
                    .context(CatalogSnafu)?
                    .context(CatalogNotFoundSnafu { name: catalog_name })?
                    .schema(&schema_name)
                    .context(CatalogSnafu)?
                    .context(SchemaNotFoundSnafu { name: schema_name })?;

                let request = self.sql_handler.insert_to_request(schema_provider, *i)?;
                self.sql_handler.execute(request).await
            }

            Statement::CreateDatabase(c) => {
                let request = CreateDatabaseRequest {
                    db_name: c.name.to_string(),
                };

                info!("Creating a new database: {}", request.db_name);

                self.sql_handler
                    .execute(SqlRequest::CreateDatabase(request))
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

                let request = self.sql_handler.create_to_request(table_id, c)?;
                let catalog_name = &request.catalog_name;
                let schema_name = &request.schema_name;
                let table_name = &request.table_name;
                let table_id = request.id;
                info!(
                    "Creating table, catalog: {:?}, schema: {:?}, table name: {:?}, table id: {}",
                    catalog_name, schema_name, table_name, table_id
                );

                self.sql_handler
                    .execute(SqlRequest::CreateTable(request))
                    .await
            }
            Statement::Alter(alter_table) => {
                let req = self.sql_handler.alter_to_request(alter_table)?;
                self.sql_handler.execute(SqlRequest::Alter(req)).await
            }
            Statement::ShowDatabases(stmt) => {
                self.sql_handler
                    .execute(SqlRequest::ShowDatabases(stmt))
                    .await
            }
            Statement::ShowTables(stmt) => {
                self.sql_handler.execute(SqlRequest::ShowTables(stmt)).await
            }
            Statement::DescribeTable(stmt) => {
                self.sql_handler
                    .execute(SqlRequest::DescribeTable(stmt))
                    .await
            }
            Statement::ShowCreateTable(stmt) => {
                self.sql_handler
                    .execute(SqlRequest::ShowCreateTable(stmt))
                    .await
            }
        }
    }
}

#[async_trait]
impl SqlQueryHandler for Instance {
    async fn do_query(&self, query: &str) -> servers::error::Result<Output> {
        let _timer = timer!(metric::METRIC_HANDLE_SQL_ELAPSED);
        self.execute_sql(query)
            .await
            .map_err(|e| {
                error!(e; "Instance failed to execute sql");
                BoxedError::new(e)
            })
            .context(servers::error::ExecuteQuerySnafu { query })
    }
}
