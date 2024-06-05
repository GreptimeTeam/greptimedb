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

use common_query::Output;
use common_telemetry::tracing;
use partition::manager::PartitionInfo;
use partition::partition::PartitionBound;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::ast::Ident;
use sql::statements::create::Partitions;
use sql::statements::show::{
    ShowColumns, ShowDatabases, ShowIndex, ShowKind, ShowTables, ShowVariables,
};
use table::metadata::TableType;
use table::table_name::TableName;
use table::TableRef;

use crate::error::{self, ExecuteStatementSnafu, Result};
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

        let partitions = self
            .partition_manager
            .find_table_partitions(table.table_info().table_id())
            .await
            .context(error::FindTablePartitionRuleSnafu {
                table_name: &table_name.table_name,
            })?;

        let partitions = create_partitions_stmt(partitions)?;

        query::sql::show_create_table(table, partitions, query_ctx)
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
}

pub(crate) fn create_partitions_stmt(partitions: Vec<PartitionInfo>) -> Result<Option<Partitions>> {
    if partitions.is_empty() {
        return Ok(None);
    }

    let column_list: Vec<Ident> = partitions[0]
        .partition
        .partition_columns()
        .iter()
        .map(|name| name[..].into())
        .collect();

    let exprs = partitions
        .iter()
        .filter_map(|partition| {
            partition
                .partition
                .partition_bounds()
                .first()
                .and_then(|bound| {
                    if let PartitionBound::Expr(expr) = bound {
                        Some(expr.to_parser_expr())
                    } else {
                        None
                    }
                })
        })
        .collect();

    Ok(Some(Partitions { column_list, exprs }))
}
