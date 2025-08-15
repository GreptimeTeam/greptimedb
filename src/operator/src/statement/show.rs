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

use common_error::ext::BoxedError;
use common_meta::key::schema_name::SchemaNameKey;
use common_query::Output;
use common_telemetry::tracing;
use partition::manager::PartitionInfo;
use session::context::QueryContextRef;
use session::table_name::table_idents_to_full_name;
use snafu::{OptionExt, ResultExt};
use sql::ast::ObjectNamePartExt;
use sql::statements::create::Partitions;
use sql::statements::show::{
    ShowColumns, ShowCreateFlow, ShowCreateView, ShowDatabases, ShowFlows, ShowIndex, ShowKind,
    ShowProcessList, ShowRegion, ShowTableStatus, ShowTables, ShowVariables, ShowViews,
};
use sql::statements::OptionMap;
use table::metadata::{TableInfo, TableType};
use table::table_name::TableName;
use table::TableRef;

use crate::error::{
    self, CatalogSnafu, ExecLogicalPlanSnafu, ExecuteStatementSnafu, ExternalSnafu,
    FindViewInfoSnafu, InvalidSqlSnafu, Result, TableMetadataManagerSnafu, ViewInfoNotFoundSnafu,
    ViewNotFoundSnafu,
};
use crate::statement::StatementExecutor;

impl StatementExecutor {
    #[tracing::instrument(skip_all)]
    pub(super) async fn show_databases(
        &self,
        stmt: ShowDatabases,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_databases(stmt, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn show_tables(
        &self,
        stmt: ShowTables,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_tables(stmt, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn show_table_status(
        &self,
        stmt: ShowTableStatus,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_table_status(stmt, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn show_columns(
        &self,
        stmt: ShowColumns,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_columns(stmt, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn show_index(
        &self,
        stmt: ShowIndex,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_index(stmt, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    pub(super) async fn show_region(
        &self,
        stmt: ShowRegion,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_region(stmt, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn show_create_database(
        &self,
        database_name: &str,
        opts: OptionMap,
    ) -> Result<Output> {
        query::sql::show_create_database(database_name, opts).context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn show_create_table(
        &self,
        table_name: TableName,
        table: TableRef,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let table_info = table.table_info();
        if table_info.table_type != TableType::Base {
            return error::ShowCreateTableBaseOnlySnafu {
                table_name: table_name.to_string(),
                table_type: table_info.table_type,
            }
            .fail();
        }

        let schema_options = self
            .table_metadata_manager
            .schema_manager()
            .get(SchemaNameKey {
                catalog: &table_name.catalog_name,
                schema: &table_name.schema_name,
            })
            .await
            .context(TableMetadataManagerSnafu)?
            .map(|v| v.into_inner());

        let partitions = self
            .partition_manager
            .find_table_partitions(table_info.table_id())
            .await
            .context(error::FindTablePartitionRuleSnafu {
                table_name: &table_name.table_name,
            })?;

        let partitions = create_partitions_stmt(&table_info, partitions)?;

        query::sql::show_create_table(table, schema_options, partitions, query_ctx)
            .context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn show_create_table_for_pg(
        &self,
        table_name: TableName,
        table: TableRef,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let table_info = table.table_info();
        if table_info.table_type != TableType::Base {
            return error::ShowCreateTableBaseOnlySnafu {
                table_name: table_name.to_string(),
                table_type: table_info.table_type,
            }
            .fail();
        }

        query::sql::show_create_foreign_table_for_pg(table, query_ctx)
            .context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn show_create_view(
        &self,
        show: ShowCreateView,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let (catalog, schema, view) = table_idents_to_full_name(&show.view_name, &query_ctx)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

        let table_ref = self
            .catalog_manager
            .table(&catalog, &schema, &view, Some(&query_ctx))
            .await
            .context(CatalogSnafu)?
            .context(ViewNotFoundSnafu { view_name: &view })?;

        let view_id = table_ref.table_info().ident.table_id;

        let view_info = self
            .view_info_manager
            .get(view_id)
            .await
            .context(FindViewInfoSnafu { view_name: &view })?
            .context(ViewInfoNotFoundSnafu { view_name: &view })?;

        query::sql::show_create_view(show.view_name, &view_info.definition, query_ctx)
            .context(error::ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn show_views(
        &self,
        stmt: ShowViews,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_views(stmt, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn show_flows(
        &self,
        stmt: ShowFlows,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_flows(stmt, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    #[cfg(feature = "enterprise")]
    #[tracing::instrument(skip_all)]
    pub(super) async fn show_triggers(
        &self,
        stmt: sql::statements::show::trigger::ShowTriggers,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_triggers(stmt, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn show_create_flow(
        &self,
        show: ShowCreateFlow,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let obj_name = &show.flow_name;
        let (catalog_name, flow_name) = match &obj_name.0[..] {
            [table] => (query_ctx.current_catalog().to_string(), table.to_string_unquoted()),
            [catalog, table] => (catalog.to_string_unquoted(), table.to_string_unquoted()),
            _ => {
                return InvalidSqlSnafu {
                    err_msg: format!(
                        "expect flow name to be <catalog>.<flow_name> or <flow_name>, actual: {obj_name}",
                    ),
                }
                .fail()
            }
        };

        let flow_name_val = self
            .flow_metadata_manager
            .flow_name_manager()
            .get(&catalog_name, &flow_name)
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::FlowNotFoundSnafu {
                flow_name: &flow_name,
            })?;

        let flow_val = self
            .flow_metadata_manager
            .flow_info_manager()
            .get(flow_name_val.flow_id())
            .await
            .context(error::TableMetadataManagerSnafu)?
            .context(error::FlowNotFoundSnafu {
                flow_name: &flow_name,
            })?;

        query::sql::show_create_flow(obj_name.clone(), flow_val, query_ctx)
            .context(error::ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub fn show_variable(&self, stmt: ShowVariables, query_ctx: QueryContextRef) -> Result<Output> {
        query::sql::show_variable(stmt, query_ctx).context(error::ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn show_collation(
        &self,
        kind: ShowKind,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_collations(kind, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(error::ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn show_charset(&self, kind: ShowKind, query_ctx: QueryContextRef) -> Result<Output> {
        query::sql::show_charsets(kind, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(error::ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn show_status(&self, query_ctx: QueryContextRef) -> Result<Output> {
        query::sql::show_status(query_ctx)
            .await
            .context(error::ExecuteStatementSnafu)
    }
    pub async fn show_search_path(&self, query_ctx: QueryContextRef) -> Result<Output> {
        query::sql::show_search_path(query_ctx)
            .await
            .context(error::ExecuteStatementSnafu)
    }

    pub async fn show_processlist(
        &self,
        stmt: ShowProcessList,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_processlist(stmt, &self.query_engine, &self.catalog_manager, query_ctx)
            .await
            .context(ExecLogicalPlanSnafu)
    }
}

pub(crate) fn create_partitions_stmt(
    table_info: &TableInfo,
    partitions: Vec<PartitionInfo>,
) -> Result<Option<Partitions>> {
    if partitions.is_empty() {
        return Ok(None);
    }

    let column_list = table_info
        .meta
        .partition_column_names()
        .map(|name| name[..].into())
        .collect();

    let exprs = partitions
        .iter()
        .filter_map(|partition| {
            partition
                .partition_expr
                .as_ref()
                .map(|expr| expr.to_parser_expr())
        })
        .collect();

    Ok(Some(Partitions { column_list, exprs }))
}
