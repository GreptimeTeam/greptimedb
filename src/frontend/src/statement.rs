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

mod backup;
mod copy_table_from;
mod copy_table_to;
mod describe;
mod show;
mod tql;

use std::collections::HashMap;
use std::str::FromStr;

use catalog::CatalogManagerRef;
use common_error::prelude::BoxedError;
use common_query::Output;
use common_recordbatch::RecordBatches;
use common_time::range::TimestampRange;
use common_time::Timestamp;
use datanode::instance::sql::{idents_to_full_database_name, table_idents_to_full_name};
use query::parser::QueryStatement;
use query::query_engine::SqlStatementExecutorRef;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::copy::{CopyDatabaseArgument, CopyTable, CopyTableArgument};
use sql::statements::statement::Statement;
use table::engine::TableReference;
use table::requests::{CopyDatabaseRequest, CopyDirection, CopyTableRequest};
use table::TableRef;

use crate::error;
use crate::error::{
    CatalogSnafu, ExecLogicalPlanSnafu, ExecuteStatementSnafu, ExternalSnafu, PlanStatementSnafu,
    Result, SchemaNotFoundSnafu, TableNotFoundSnafu,
};
use crate::statement::backup::{COPY_DATABASE_TIME_END_KEY, COPY_DATABASE_TIME_START_KEY};

#[derive(Clone)]
pub struct StatementExecutor {
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    sql_stmt_executor: SqlStatementExecutorRef,
}

impl StatementExecutor {
    pub(crate) fn new(
        catalog_manager: CatalogManagerRef,
        query_engine: QueryEngineRef,
        sql_stmt_executor: SqlStatementExecutorRef,
    ) -> Self {
        Self {
            catalog_manager,
            query_engine,
            sql_stmt_executor,
        }
    }

    pub async fn execute_stmt(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        match stmt {
            QueryStatement::Sql(stmt) => self.execute_sql(stmt, query_ctx).await,
            QueryStatement::Promql(_) => self.plan_exec(stmt, query_ctx).await,
        }
    }

    pub async fn execute_sql(&self, stmt: Statement, query_ctx: QueryContextRef) -> Result<Output> {
        match stmt {
            Statement::Query(_) | Statement::Explain(_) | Statement::Delete(_) => {
                self.plan_exec(QueryStatement::Sql(stmt), query_ctx).await
            }

            // For performance consideration, only requests that can't extract values is executed by query engine.
            // Plain insert ("insert with literal values") is still executed directly in statement.
            Statement::Insert(insert) => {
                if insert.can_extract_values() {
                    self.sql_stmt_executor
                        .execute_sql(Statement::Insert(insert), query_ctx)
                        .await
                        .context(ExecuteStatementSnafu)
                } else {
                    self.plan_exec(QueryStatement::Sql(Statement::Insert(insert)), query_ctx)
                        .await
                }
            }

            Statement::Tql(tql) => self.execute_tql(tql, query_ctx).await,

            Statement::DescribeTable(stmt) => self.describe_table(stmt, query_ctx).await,

            Statement::Use(db) => self.handle_use(db, query_ctx).await,

            Statement::ShowDatabases(stmt) => self.show_databases(stmt, query_ctx).await,

            Statement::ShowTables(stmt) => self.show_tables(stmt, query_ctx).await,

            Statement::Copy(sql::statements::copy::Copy::CopyTable(stmt)) => {
                let req = to_copy_table_request(stmt, query_ctx)?;
                match req.direction {
                    CopyDirection::Export => {
                        self.copy_table_to(req).await.map(Output::AffectedRows)
                    }
                    CopyDirection::Import => {
                        self.copy_table_from(req).await.map(Output::AffectedRows)
                    }
                }
            }

            Statement::Copy(sql::statements::copy::Copy::CopyDatabase(arg)) => {
                self.copy_database(to_copy_database_request(arg, &query_ctx)?)
                    .await
            }

            Statement::CreateDatabase(_)
            | Statement::CreateTable(_)
            | Statement::CreateExternalTable(_)
            | Statement::Alter(_)
            | Statement::DropTable(_)
            | Statement::TruncateTable(_)
            | Statement::ShowCreateTable(_) => self
                .sql_stmt_executor
                .execute_sql(stmt, query_ctx)
                .await
                .context(ExecuteStatementSnafu),
        }
    }

    async fn plan_exec(&self, stmt: QueryStatement, query_ctx: QueryContextRef) -> Result<Output> {
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

    async fn handle_use(&self, db: String, query_ctx: QueryContextRef) -> Result<Output> {
        let catalog = &query_ctx.current_catalog();
        ensure!(
            self.catalog_manager
                .schema_exist(catalog, &db)
                .await
                .context(CatalogSnafu)?,
            SchemaNotFoundSnafu { schema_info: &db }
        );

        query_ctx.set_current_schema(&db);

        Ok(Output::RecordBatches(RecordBatches::empty()))
    }

    async fn get_table(&self, table_ref: &TableReference<'_>) -> Result<TableRef> {
        let TableReference {
            catalog,
            schema,
            table,
        } = table_ref;
        self.catalog_manager
            .table(catalog, schema, table)
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })
    }
}

fn to_copy_table_request(stmt: CopyTable, query_ctx: QueryContextRef) -> Result<CopyTableRequest> {
    let direction = match stmt {
        CopyTable::To(_) => CopyDirection::Export,
        CopyTable::From(_) => CopyDirection::Import,
    };

    let CopyTableArgument {
        location,
        connection,
        with,
        table_name,
        ..
    } = match stmt {
        CopyTable::To(arg) => arg,
        CopyTable::From(arg) => arg,
    };
    let (catalog_name, schema_name, table_name) = table_idents_to_full_name(&table_name, query_ctx)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    let pattern = with
        .get(common_datasource::file_format::FILE_PATTERN)
        .cloned();

    Ok(CopyTableRequest {
        catalog_name,
        schema_name,
        table_name,
        location,
        with,
        connection,
        pattern,
        direction,
        // we copy the whole table by default.
        timestamp_range: None,
    })
}

/// Converts [CopyDatabaseArgument] to [CopyDatabaseRequest].
/// This function extracts the necessary info including catalog/database name, time range, etc.
fn to_copy_database_request(
    arg: CopyDatabaseArgument,
    query_ctx: &QueryContextRef,
) -> Result<CopyDatabaseRequest> {
    let (catalog_name, database_name) = idents_to_full_database_name(&arg.database_name, query_ctx)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    let start_timestamp = extract_timestamp(&arg.with, COPY_DATABASE_TIME_START_KEY)?;
    let end_timestamp = extract_timestamp(&arg.with, COPY_DATABASE_TIME_END_KEY)?;

    let time_range = match (start_timestamp, end_timestamp) {
        (Some(start), Some(end)) => TimestampRange::new(start, end),
        (Some(start), None) => Some(TimestampRange::from_start(start)),
        (None, Some(end)) => Some(TimestampRange::until_end(end, false)), // exclusive end
        (None, None) => None,
    };

    Ok(CopyDatabaseRequest {
        catalog_name,
        schema_name: database_name,
        location: arg.location,
        with: arg.with,
        connection: arg.connection,
        time_range,
    })
}

/// Extracts timestamp from a [HashMap<String, String>] with given key.
fn extract_timestamp(map: &HashMap<String, String>, key: &str) -> Result<Option<Timestamp>> {
    map.get(key)
        .map(|v| {
            Timestamp::from_str(v)
                .map_err(|_| error::InvalidCopyParameterSnafu { key, value: v }.build())
        })
        .transpose()
}
