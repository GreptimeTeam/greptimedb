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

use common_grpc_expr::alter_expr_to_request;
use snafu::ResultExt;
use table::metadata::{RawTableInfo, TableInfo};
use table::requests::AlterKind;

use crate::ddl::alter_table::AlterTableProcedure;
use crate::error::{self, Result};
use crate::key::table_info::TableInfoValue;
use crate::key::{DeserializedValueWithBytes, RegionDistribution};

impl AlterTableProcedure {
    /// Builds new table info after alteration.
    /// It bumps the column id of the table by the number of the add column requests.
    /// So there may be holes in the column id sequence.
    pub(crate) fn build_new_table_info(&self, table_info: &RawTableInfo) -> Result<TableInfo> {
        let table_info =
            TableInfo::try_from(table_info.clone()).context(error::ConvertRawTableInfoSnafu)?;
        let table_ref = self.data.table_ref();
        let alter_expr = self.data.task.alter_table.clone();
        let request = alter_expr_to_request(self.data.table_id(), alter_expr)
            .context(error::ConvertAlterTableRequestSnafu)?;

        let new_meta = table_info
            .meta
            .builder_with_alter_kind(table_ref.table, &request.alter_kind)
            .context(error::TableSnafu)?
            .build()
            .with_context(|_| error::BuildTableMetaSnafu {
                table_name: table_ref.table,
            })?;

        let mut new_info = table_info.clone();
        new_info.meta = new_meta;
        new_info.ident.version = table_info.ident.version + 1;
        match request.alter_kind {
            AlterKind::AddColumns { columns } => {
                // Bumps the column id for the new columns.
                // It may bump more than the actual number of columns added if there are
                // existing columns, but it's fine.
                new_info.meta.next_column_id += columns.len() as u32;
            }
            AlterKind::RenameTable { new_table_name } => {
                new_info.name = new_table_name.to_string();
            }
            AlterKind::DropColumns { .. }
            | AlterKind::ModifyColumnTypes { .. }
            | AlterKind::SetTableOptions { .. }
            | AlterKind::UnsetTableOptions { .. }
            | AlterKind::SetColumnFulltext { .. }
            | AlterKind::UnsetColumnFulltext { .. } => {}
        }

        Ok(new_info)
    }

    /// Updates table metadata for rename table operation.
    pub(crate) async fn on_update_metadata_for_rename(
        &self,
        new_table_name: String,
        current_table_info_value: &DeserializedValueWithBytes<TableInfoValue>,
    ) -> Result<()> {
        let table_metadata_manager = &self.context.table_metadata_manager;
        table_metadata_manager
            .rename_table(current_table_info_value, new_table_name)
            .await?;

        Ok(())
    }

    /// Updates table metadata for alter table operation.
    pub(crate) async fn on_update_metadata_for_alter(
        &self,
        new_table_info: RawTableInfo,
        region_distribution: RegionDistribution,
        current_table_info_value: &DeserializedValueWithBytes<TableInfoValue>,
    ) -> Result<()> {
        let table_metadata_manager = &self.context.table_metadata_manager;
        table_metadata_manager
            .update_table_info(
                current_table_info_value,
                Some(region_distribution),
                new_table_info,
            )
            .await?;

        Ok(())
    }
}
