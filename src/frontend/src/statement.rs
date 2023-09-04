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
mod dml;
mod show;
mod tql;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use api::v1::region::{region_request, InsertRequests};
use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_query::Output;
use common_time::range::TimestampRange;
use common_time::Timestamp;
use datanode::instance::sql::{idents_to_full_database_name, table_idents_to_full_name};
use query::parser::QueryStatement;
use query::plan::LogicalPlan;
use query::query_engine::SqlStatementExecutorRef;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::statements::copy::{CopyDatabaseArgument, CopyTable, CopyTableArgument};
use sql::statements::statement::Statement;
use table::engine::TableReference;
use table::error::TableOperationSnafu;
use table::requests::{
    CopyDatabaseRequest, CopyDirection, CopyTableRequest, DeleteRequest, InsertRequest,
};
use table::TableRef;

use crate::catalog::FrontendCatalogManager;
use crate::error::{
    self, CatalogSnafu, ExecLogicalPlanSnafu, ExecuteStatementSnafu, ExternalSnafu, InsertSnafu,
    PlanStatementSnafu, Result, TableNotFoundSnafu,
};
use crate::instance::distributed::deleter::DistDeleter;
use crate::instance::region_handler::RegionRequestHandlerRef;
use crate::statement::backup::{COPY_DATABASE_TIME_END_KEY, COPY_DATABASE_TIME_START_KEY};
use crate::table::insert::insert_request_table_to_region;

#[derive(Clone)]
pub struct StatementExecutor {
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    sql_stmt_executor: SqlStatementExecutorRef,
    region_request_handler: RegionRequestHandlerRef,
}

impl StatementExecutor {
    pub(crate) fn new(
        catalog_manager: CatalogManagerRef,
        query_engine: QueryEngineRef,
        sql_stmt_executor: SqlStatementExecutorRef,
        region_request_handler: RegionRequestHandlerRef,
    ) -> Self {
        Self {
            catalog_manager,
            query_engine,
            sql_stmt_executor,
            region_request_handler,
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
            Statement::Query(_) | Statement::Explain(_) => {
                self.plan_exec(QueryStatement::Sql(stmt), query_ctx).await
            }

            Statement::Insert(insert) => self.insert(insert, query_ctx).await,

            Statement::Delete(delete) => self.delete(delete, query_ctx).await,

            Statement::Tql(tql) => self.execute_tql(tql, query_ctx).await,

            Statement::DescribeTable(stmt) => self.describe_table(stmt, query_ctx).await,

            Statement::ShowDatabases(stmt) => self.show_databases(stmt, query_ctx).await,

            Statement::ShowTables(stmt) => self.show_tables(stmt, query_ctx).await,

            Statement::Copy(sql::statements::copy::Copy::CopyTable(stmt)) => {
                let req = to_copy_table_request(stmt, query_ctx.clone())?;
                match req.direction {
                    CopyDirection::Export => self
                        .copy_table_to(req, query_ctx)
                        .await
                        .map(Output::AffectedRows),
                    CopyDirection::Import => self
                        .copy_table_from(req, query_ctx)
                        .await
                        .map(Output::AffectedRows),
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

    async fn plan(&self, stmt: QueryStatement, query_ctx: QueryContextRef) -> Result<LogicalPlan> {
        self.query_engine
            .planner()
            .plan(stmt, query_ctx)
            .await
            .context(PlanStatementSnafu)
    }

    async fn plan_exec(&self, stmt: QueryStatement, query_ctx: QueryContextRef) -> Result<Output> {
        let plan = self.plan(stmt, query_ctx.clone()).await?;
        self.query_engine
            .execute(plan, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)
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

    async fn handle_table_insert_request(
        &self,
        request: InsertRequest,
        query_ctx: QueryContextRef,
    ) -> Result<usize> {
        let table_ref = TableReference::full(
            &request.catalog_name,
            &request.schema_name,
            &request.table_name,
        );
        let table = self.get_table(&table_ref).await?;
        let table_info = table.table_info();

        let region_request = insert_request_table_to_region(&table_info, request)?;
        let body = region_request::Body::Inserts(InsertRequests {
            requests: vec![region_request],
        });
        let region_response = self.region_request_handler.handle(body, query_ctx).await?;

        Ok(region_response.affected_rows as _)
    }

    // TODO(zhongzc): A middle state that eliminates calls to table.delete,
    // For DistTable, its delete is not invoked; for MitoTable, it is still called but eventually eliminated.
    async fn send_delete_request(&self, request: DeleteRequest) -> Result<usize> {
        let frontend_catalog_manager = self
            .catalog_manager
            .as_any()
            .downcast_ref::<FrontendCatalogManager>();

        let table_name = request.table_name.clone();
        match frontend_catalog_manager {
            Some(frontend_catalog_manager) => {
                let inserter = DistDeleter::new(
                    request.catalog_name.clone(),
                    request.schema_name.clone(),
                    Arc::new(frontend_catalog_manager.clone()),
                );
                let affected_rows = inserter
                    .delete(vec![request])
                    .await
                    .map_err(BoxedError::new)
                    .context(TableOperationSnafu)
                    .context(InsertSnafu { table_name })?;
                Ok(affected_rows)
            }
            None => {
                let table_ref = TableReference::full(
                    &request.catalog_name,
                    &request.schema_name,
                    &request.table_name,
                );
                let affected_rows = self
                    .get_table(&table_ref)
                    .await?
                    .delete(request)
                    .await
                    .context(InsertSnafu { table_name })?;
                Ok(affected_rows)
            }
        }
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
