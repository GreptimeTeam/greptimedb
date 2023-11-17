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

use common_meta::table_name::TableName;
use common_query::Output;
use common_telemetry::tracing;
use partition::manager::PartitionInfo;
use partition::partition::PartitionBound;
use session::context::QueryContextRef;
use snafu::ResultExt;
use sql::ast::{Ident, Value as SqlValue};
use sql::statements::create::{PartitionEntry, Partitions};
use sql::statements::show::{ShowDatabases, ShowTables};
use sql::{statements, MAXVALUE};
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
        query::sql::show_databases(stmt, self.catalog_manager.clone(), query_ctx)
            .await
            .context(ExecuteStatementSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub(super) async fn show_tables(
        &self,
        stmt: ShowTables,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        query::sql::show_tables(stmt, self.catalog_manager.clone(), query_ctx)
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
}

fn create_partitions_stmt(partitions: Vec<PartitionInfo>) -> Result<Option<Partitions>> {
    if partitions.is_empty() {
        return Ok(None);
    }

    let column_list: Vec<Ident> = partitions[0]
        .partition
        .partition_columns()
        .iter()
        .map(|name| name[..].into())
        .collect();

    let entries = partitions
        .into_iter()
        .map(|info| {
            // Generated the partition name from id
            let name = &format!("r{}", info.id.region_number());
            let bounds = info.partition.partition_bounds();
            let value_list = bounds
                .iter()
                .map(|b| match b {
                    PartitionBound::Value(v) => statements::value_to_sql_value(v)
                        .with_context(|_| error::ConvertSqlValueSnafu { value: v.clone() }),
                    PartitionBound::MaxValue => Ok(SqlValue::Number(MAXVALUE.to_string(), false)),
                })
                .collect::<Result<Vec<_>>>()?;

            Ok(PartitionEntry {
                name: name[..].into(),
                value_list,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(Partitions {
        column_list,
        entries,
    }))
}
