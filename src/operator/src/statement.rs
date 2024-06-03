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
mod set;
mod show;
mod tql;

use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_meta::cache::TableRouteCacheRef;
use common_meta::cache_invalidator::CacheInvalidatorRef;
use common_meta::ddl::ProcedureExecutorRef;
use common_meta::key::flow::{FlowMetadataManager, FlowMetadataManagerRef};
use common_meta::key::{TableMetadataManager, TableMetadataManagerRef};
use common_meta::kv_backend::KvBackendRef;
use common_query::Output;
use common_telemetry::tracing;
use common_time::range::TimestampRange;
use common_time::Timestamp;
use partition::manager::{PartitionRuleManager, PartitionRuleManagerRef};
use query::parser::QueryStatement;
use query::plan::LogicalPlan;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::{OptionExt, ResultExt};
use sql::statements::copy::{CopyDatabase, CopyDatabaseArgument, CopyTable, CopyTableArgument};
use sql::statements::statement::Statement;
use sql::statements::OptionMap;
use sql::util::format_raw_object_name;
use sqlparser::ast::ObjectName;
use table::requests::{CopyDatabaseRequest, CopyDirection, CopyTableRequest};
use table::table_name::TableName;
use table::table_reference::TableReference;
use table::TableRef;

use self::set::{set_bytea_output, set_datestyle, set_timezone, validate_client_encoding};
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
    flow_metadata_manager: FlowMetadataManagerRef,
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
        table_route_cache: TableRouteCacheRef,
    ) -> Self {
        Self {
            catalog_manager,
            query_engine,
            procedure_executor,
            table_metadata_manager: Arc::new(TableMetadataManager::new(kv_backend.clone())),
            flow_metadata_manager: Arc::new(FlowMetadataManager::new(kv_backend.clone())),
            partition_manager: Arc::new(PartitionRuleManager::new(kv_backend, table_route_cache)),
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

            Statement::ShowCollation(kind) => self.show_collation(kind, query_ctx).await,

            Statement::ShowCharset(kind) => self.show_charset(kind, query_ctx).await,

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
            Statement::CreateFlow(stmt) => self.create_flow(stmt, query_ctx).await,
            Statement::DropFlow(stmt) => {
                self.drop_flow(
                    query_ctx.current_catalog().to_string(),
                    format_raw_object_name(stmt.flow_name()),
                    stmt.drop_if_exists(),
                    query_ctx,
                )
                .await
            }
            Statement::CreateView(stmt) => {
                let _ = self.create_view(stmt, query_ctx).await?;
                Ok(Output::new_with_affected_rows(0))
            }
            Statement::Alter(alter_table) => self.alter_table(alter_table, query_ctx).await,
            Statement::DropTable(stmt) => {
                let mut table_names = Vec::new();
                for table_name_stmt in stmt.table_names() {
                    let (catalog, schema, table) =
                        table_idents_to_full_name(table_name_stmt, &query_ctx)
                            .map_err(BoxedError::new)
                            .context(error::ExternalSnafu)?;
                    table_names.push(TableName::new(catalog, schema, table));
                }
                self.drop_tables(table_names, stmt.drop_if_exists(), query_ctx.clone())
                    .await
            }
            Statement::DropDatabase(stmt) => {
                self.drop_database(
                    query_ctx.current_catalog().to_string(),
                    format_raw_object_name(stmt.name()),
                    stmt.drop_if_exists(),
                    query_ctx,
                )
                .await
            }
            Statement::TruncateTable(stmt) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(stmt.table_name(), &query_ctx)
                        .map_err(BoxedError::new)
                        .context(error::ExternalSnafu)?;
                let table_name = TableName::new(catalog, schema, table);
                self.truncate_table(table_name, query_ctx).await
            }
            Statement::CreateDatabase(stmt) => {
                self.create_database(
                    &format_raw_object_name(&stmt.name),
                    stmt.if_not_exists,
                    stmt.options.into_map(),
                    query_ctx,
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

                    "BYTEA_OUTPUT" => set_bytea_output(set_var.value, query_ctx)?,

                    // Same as "bytea_output", we just ignore it here.
                    // Not harmful since it only relates to how date is viewed in client app's output.
                    // The tracked issue is https://github.com/GreptimeTeam/greptimedb/issues/3442.
                    "DATESTYLE" => set_datestyle(set_var.value, query_ctx)?,

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
            Statement::ShowStatus(_) => self.show_status(query_ctx).await,
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

    pub fn optimize_logical_plan(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.query_engine
            .planner()
            .optimize(plan)
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
        limit,
        ..
    } = match stmt {
        CopyTable::To(arg) => arg,
        CopyTable::From(arg) => arg,
    };
    let (catalog_name, schema_name, table_name) =
        table_idents_to_full_name(&table_name, &query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    let timestamp_range = timestamp_range_from_option_map(&with, &query_ctx)?;

    let pattern = with
        .get(common_datasource::file_format::FILE_PATTERN)
        .cloned();

    Ok(CopyTableRequest {
        catalog_name,
        schema_name,
        table_name,
        location,
        with: with.into_map(),
        connection: connection.into_map(),
        pattern,
        direction,
        timestamp_range,
        limit,
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
    let time_range = timestamp_range_from_option_map(&arg.with, query_ctx)?;

    Ok(CopyDatabaseRequest {
        catalog_name,
        schema_name: database_name,
        location: arg.location,
        with: arg.with.into_map(),
        connection: arg.connection.into_map(),
        time_range,
    })
}

/// Extracts timestamp range from OptionMap with keys `start_time` and `end_time`.
/// The timestamp ranges should be a valid timestamp string as defined in [Timestamp::from_str].
/// The timezone used for conversion will respect that inside `query_ctx`.
fn timestamp_range_from_option_map(
    options: &OptionMap,
    query_ctx: &QueryContextRef,
) -> Result<Option<TimestampRange>> {
    let start_timestamp = extract_timestamp(options, COPY_DATABASE_TIME_START_KEY, query_ctx)?;
    let end_timestamp = extract_timestamp(options, COPY_DATABASE_TIME_END_KEY, query_ctx)?;
    let time_range = match (start_timestamp, end_timestamp) {
        (Some(start), Some(end)) => Some(TimestampRange::new(start, end).with_context(|| {
            error::InvalidTimestampRangeSnafu {
                start: start.to_iso8601_string(),
                end: end.to_iso8601_string(),
            }
        })?),
        (Some(start), None) => Some(TimestampRange::from_start(start)),
        (None, Some(end)) => Some(TimestampRange::until_end(end, false)), // exclusive end
        (None, None) => None,
    };
    Ok(time_range)
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

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_time::range::TimestampRange;
    use common_time::{Timestamp, Timezone};
    use session::context::QueryContextBuilder;
    use sql::statements::OptionMap;

    use crate::error;
    use crate::statement::copy_database::{
        COPY_DATABASE_TIME_END_KEY, COPY_DATABASE_TIME_START_KEY,
    };
    use crate::statement::timestamp_range_from_option_map;

    fn check_timestamp_range((start, end): (&str, &str)) -> error::Result<Option<TimestampRange>> {
        let query_ctx = QueryContextBuilder::default()
            .timezone(Arc::new(Timezone::from_tz_string("Asia/Shanghai").unwrap()))
            .build()
            .into();
        let map = OptionMap::from(
            [
                (COPY_DATABASE_TIME_START_KEY.to_string(), start.to_string()),
                (COPY_DATABASE_TIME_END_KEY.to_string(), end.to_string()),
            ]
            .into_iter()
            .collect::<HashMap<_, _>>(),
        );
        timestamp_range_from_option_map(&map, &query_ctx)
    }

    #[test]
    fn test_timestamp_range_from_option_map() {
        assert_eq!(
            Some(
                TimestampRange::new(
                    Timestamp::new_second(1649635200),
                    Timestamp::new_second(1649664000),
                )
                .unwrap(),
            ),
            check_timestamp_range(("2022-04-11 08:00:00", "2022-04-11 16:00:00"),).unwrap()
        );

        assert_matches!(
            check_timestamp_range(("2022-04-11 08:00:00", "2022-04-11 07:00:00")).unwrap_err(),
            error::Error::InvalidTimestampRange { .. }
        );
    }
}
