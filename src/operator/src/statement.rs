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

mod copy_database;
mod copy_table_from;
mod copy_table_to;
mod ddl;
mod describe;
mod dml;
mod show;
mod tql;

use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::ddl::ProcedureExecutorRef;
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
use common_meta::table_name::TableName;
use common_query::Output;
use common_telemetry::tracing;
use common_time::range::TimestampRange;
use common_time::{Timestamp, Timezone};
use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
use query::parser::QueryStatement;
use query::plan::LogicalPlan;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::copy::{CopyDatabase, CopyDatabaseArgument, CopyTable, CopyTableArgument};
use sql::statements::set_variables::SetVariables;
use sql::statements::statement::Statement;
use sql::statements::OptionMap;
use sql::util::format_raw_object_name;
use sqlparser::ast::{Expr, Ident, ObjectName, Value};
use table::requests::{CopyDatabaseRequest, CopyDirection, CopyTableRequest};
use table::table_reference::TableReference;
use table::TableRef;

use crate::error::{
    self, CatalogSnafu, ExecLogicalPlanSnafu, ExternalSnafu, InvalidSqlSnafu, NotSupportedSnafu,
    PlanStatementSnafu, Result, TableNotFoundSnafu,
};
use crate::insert::InserterRef;
use crate::statement::copy_database::{COPY_DATABASE_TIME_END_KEY, COPY_DATABASE_TIME_START_KEY};

#[derive(Clone)]
pub struct StatementExecutor {
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    procedure_executor: ProcedureExecutorRef,
    table_metadata_manager: TableMetadataManagerRef,
    partition_manager: PartitionRuleManagerRef,
    cache_invalidator: CacheInvalidatorRef,
    inserter: InserterRef,
}

impl StatementExecutor {
    pub fn new(
        catalog_manager: CatalogManagerRef,
        query_engine: QueryEngineRef,
        procedure_executor: ProcedureExecutorRef,
        kv_backend: KvBackendRef,
        cache_invalidator: CacheInvalidatorRef,
        inserter: InserterRef,
    ) -> Self {
        Self {
            catalog_manager,
            query_engine,
            procedure_executor,
            table_metadata_manager: Arc::new(TableMetadataManager::new(kv_backend.clone())),
            partition_manager: Arc::new(PartitionRuleManager::new(kv_backend)),
            cache_invalidator,
            inserter,
        }
    }

    #[tracing::instrument(skip_all)]
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

            Statement::Insert(insert) => self.insert(insert, query_ctx).await,

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
                        .map(Output::new_with_affected_rows),
                    CopyDirection::Import => self.copy_table_from(req, query_ctx).await,
                }
            }

            Statement::Copy(sql::statements::copy::Copy::CopyDatabase(copy_database)) => {
                match copy_database {
                    CopyDatabase::To(arg) => {
                        self.copy_database_to(
                            to_copy_database_request(arg, &query_ctx)?,
                            query_ctx.clone(),
                        )
                        .await
                    }
                    CopyDatabase::From(arg) => {
                        self.copy_database_from(
                            to_copy_database_request(arg, &query_ctx)?,
                            query_ctx,
                        )
                        .await
                    }
                }
            }

            Statement::CreateTable(stmt) => {
                let _ = self.create_table(stmt, query_ctx).await?;
                Ok(Output::new_with_affected_rows(0))
            }
            Statement::CreateTableLike(stmt) => {
                let _ = self.create_table_like(stmt, query_ctx).await?;
                Ok(Output::new_with_affected_rows(0))
            }
            Statement::CreateExternalTable(stmt) => {
                let _ = self.create_external_table(stmt, query_ctx).await?;
                Ok(Output::new_with_affected_rows(0))
            }
            Statement::Alter(alter_table) => self.alter_table(alter_table, query_ctx).await,
            Statement::DropTable(stmt) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(stmt.table_name(), &query_ctx)
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;
                let table_name = TableName::new(catalog, schema, table);
                self.drop_table(table_name, stmt.drop_if_exists()).await
            }
            Statement::DropDatabase(stmt) => {
                self.drop_database(
                    query_ctx.current_catalog().to_string(),
                    format_raw_object_name(stmt.name()),
                    stmt.drop_if_exists(),
                )
                .await
            }
            Statement::TruncateTable(stmt) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(stmt.table_name(), &query_ctx)
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;
                let table_name = TableName::new(catalog, schema, table);
                self.truncate_table(table_name).await
            }
            Statement::CreateDatabase(stmt) => {
                self.create_database(
                    query_ctx.current_catalog(),
                    &format_raw_object_name(&stmt.name),
                    stmt.if_not_exists,
                )
                .await
            }
            Statement::ShowCreateTable(show) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(&show.table_name, &query_ctx)
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;

                let table_ref = self
                    .catalog_manager
                    .table(&catalog, &schema, &table)
                    .await
                    .context(error::CatalogSnafu)?
                    .context(error::TableNotFoundSnafu { table_name: &table })?;
                let table_name = TableName::new(catalog, schema, table);

                self.show_create_table(table_name, table_ref, query_ctx)
                    .await
            }
            Statement::SetVariables(set_var) => {
                let var_name = set_var.variable.to_string().to_uppercase();
                match var_name.as_str() {
                    "TIMEZONE" | "TIME_ZONE" => set_timezone(set_var.value, query_ctx)?,

                    // Some postgresql client app may submit a "SET bytea_output" stmt upon connection.
                    // However, currently we lack the support for it (tracked in https://github.com/GreptimeTeam/greptimedb/issues/3438),
                    // so we just ignore it here instead of returning an error to break the connection.
                    // Since the "bytea_output" only determines the output format of binary values,
                    // it won't cause much trouble if we do so.
                    // TODO(#3438): Remove this temporary workaround after the feature is implemented.
                    "BYTEA_OUTPUT" => (),

                    // Same as "bytea_output", we just ignore it here.
                    // Not harmful since it only relates to how date is viewed in client app's output.
                    // The tracked issue is https://github.com/GreptimeTeam/greptimedb/issues/3442.
                    // TODO(#3442): Remove this temporary workaround after the feature is implemented.
                    "DATESTYLE" => (),

                    "CLIENT_ENCODING" => validate_client_encoding(set_var)?,
                    _ => {
                        return NotSupportedSnafu {
                            feat: format!("Unsupported set variable {}", var_name),
                        }
                        .fail()
                    }
                }
                Ok(Output::new_with_affected_rows(0))
            }
            Statement::ShowVariables(show_variable) => self.show_variable(show_variable, query_ctx),
            Statement::ShowColumns(show_columns) => {
                self.show_columns(show_columns, query_ctx).await
            }
            Statement::ShowIndex(show_index) => self.show_index(show_index, query_ctx).await,
        }
    }

    pub async fn plan(
        &self,
        stmt: QueryStatement,
        query_ctx: QueryContextRef,
    ) -> Result<LogicalPlan> {
        self.query_engine
            .planner()
            .plan(stmt, query_ctx)
            .await
            .context(PlanStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
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
}

fn validate_client_encoding(set: SetVariables) -> Result<()> {
    let Some((encoding, [])) = set.value.split_first() else {
        return InvalidSqlSnafu {
            err_msg: "must provide one and only one client encoding value",
        }
        .fail();
    };
    let encoding = match encoding {
        Expr::Value(Value::SingleQuotedString(x))
        | Expr::Identifier(Ident {
            value: x,
            quote_style: _,
        }) => x.to_uppercase(),
        _ => {
            return InvalidSqlSnafu {
                err_msg: format!("client encoding must be a string, actual: {:?}", encoding),
            }
            .fail();
        }
    };
    // For the sake of simplicity, we only support "UTF8" ("UNICODE" is the alias for it,
    // see https://www.postgresql.org/docs/current/multibyte.html#MULTIBYTE-CHARSET-SUPPORTED).
    // "UTF8" is universal and sufficient for almost all cases.
    // GreptimeDB itself is always using "UTF8" as the internal encoding.
    ensure!(
        encoding == "UTF8" || encoding == "UNICODE",
        NotSupportedSnafu {
            feat: format!("client encoding of '{}'", encoding)
        }
    );
    Ok(())
}

fn set_timezone(exprs: Vec<Expr>, ctx: QueryContextRef) -> Result<()> {
    let tz_expr = exprs.first().context(NotSupportedSnafu {
        feat: "No timezone find in set variable statement",
    })?;
    match tz_expr {
        Expr::Value(Value::SingleQuotedString(tz)) | Expr::Value(Value::DoubleQuotedString(tz)) => {
            match Timezone::from_tz_string(tz.as_str()) {
                Ok(timezone) => ctx.set_timezone(timezone),
                Err(_) => {
                    return NotSupportedSnafu {
                        feat: format!("Invalid timezone expr {} in set variable statement", tz),
                    }
                    .fail()
                }
            }
            Ok(())
        }
        expr => NotSupportedSnafu {
            feat: format!(
                "Unsupported timezone expr {} in set variable statement",
                expr
            ),
        }
        .fail(),
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
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&table_name, &query_ctx)
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
        with: with.map,
        connection: connection.map,
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

    let start_timestamp = extract_timestamp(&arg.with, COPY_DATABASE_TIME_START_KEY, query_ctx)?;
    let end_timestamp = extract_timestamp(&arg.with, COPY_DATABASE_TIME_END_KEY, query_ctx)?;

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
        with: arg.with.map,
        connection: arg.connection.map,
        time_range,
    })
}

/// Extracts timestamp from a [HashMap<String, String>] with given key.
fn extract_timestamp(
    map: &OptionMap,
    key: &str,
    query_ctx: &QueryContextRef,
) -> Result<Option<Timestamp>> {
    map.get(key)
        .map(|v| {
            Timestamp::from_str(v, Some(&query_ctx.timezone()))
                .map_err(|_| error::InvalidCopyParameterSnafu { key, value: v }.build())
        })
        .transpose()
}

fn idents_to_full_database_name(
    obj_name: &ObjectName,
    query_ctx: &QueryContextRef,
) -> Result<(String, String)> {
    match &obj_name.0[..] {
        [database] => Ok((
            query_ctx.current_catalog().to_owned(),
            database.value.clone(),
        )),
        [catalog, database] => Ok((catalog.value.clone(), database.value.clone())),
        _ => InvalidSqlSnafu {
            err_msg: format!(
                "expect database name to be <catalog>.<database>, <database>, found: {obj_name}",
            ),
        }
        .fail(),
    }
}
