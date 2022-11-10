use async_trait::async_trait;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_error::prelude::BoxedError;
use common_query::Output;
use common_telemetry::{
    logging::{error, info},
    timer,
};
use servers::query_handler::SqlQueryHandler;
use snafu::prelude::*;
use sql::statements::statement::Statement;

use crate::error::{CatalogSnafu, ExecuteSqlSnafu, Result};
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
                let schema_provider = self
                    .catalog_manager
                    .catalog(DEFAULT_CATALOG_NAME)
                    .expect("datafusion does not accept fallible catalog access")
                    .unwrap()
                    .schema(DEFAULT_SCHEMA_NAME)
                    .expect("datafusion does not accept fallible catalog access")
                    .unwrap();

                let request = self.sql_handler.insert_to_request(schema_provider, *i)?;
                self.sql_handler.execute(request).await
            }

            Statement::CreateDatabase(_) => {
                unimplemented!();
            }

            Statement::CreateTable(c) => {
                let table_id = self
                    .catalog_manager
                    .next_table_id()
                    .await
                    .context(CatalogSnafu)?;
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

                self.sql_handler.execute(SqlRequest::Create(request)).await
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
            Statement::ShowCreateTable(_stmt) => {
                unimplemented!("SHOW CREATE TABLE is unimplemented yet");
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

    async fn insert_script(&self, name: &str, script: &str) -> servers::error::Result<()> {
        self.script_executor.insert_script(name, script).await
    }

    async fn execute_script(&self, name: &str) -> servers::error::Result<Output> {
        self.script_executor.execute_script(name).await
    }
}
