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

use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::error;
use crate::error::{ConvertAlterTableRequestSnafu, Result};
use crate::key::table_info::TableInfoValue;
use crate::rpc::ddl::AlterTableTask;

impl AlterLogicalTablesProcedure {
    pub(crate) fn build_update_metadata(&self) -> Result<Vec<(TableInfoValue, RawTableInfo)>> {
        let mut table_info_values_to_update = Vec::with_capacity(self.data.tasks.len());
        for (task, table) in self
            .data
            .tasks
            .iter()
            .zip(self.data.table_info_values.iter())
        {
            table_info_values_to_update.push(self.build_new_table_info(task, table)?);
        }

        Ok(table_info_values_to_update)
    }

    fn build_new_table_info(
        &self,
        task: &AlterTableTask,
        table: &TableInfoValue,
    ) -> Result<(TableInfoValue, RawTableInfo)> {
        // Builds new_meta
        let table_info = TableInfo::try_from(table.table_info.clone())
            .context(error::ConvertRawTableInfoSnafu)?;
        let table_ref = task.table_ref();
        let request =
            alter_expr_to_request(table.table_info.ident.table_id, task.alter_table.clone())
                .context(ConvertAlterTableRequestSnafu)?;
        let new_meta = table_info
            .meta
            .builder_with_alter_kind(table_ref.table, &request.alter_kind, true)
            .context(error::TableSnafu)?
            .build()
            .with_context(|_| error::BuildTableMetaSnafu {
                table_name: table_ref.table,
            })?;
        let version = table_info.ident.version + 1;
        let mut new_table = table_info;
        new_table.meta = new_meta;
        new_table.ident.version = version;

        let mut raw_table_info = RawTableInfo::from(new_table);
        raw_table_info.sort_columns();

        Ok((table.clone(), raw_table_info))
    }
}
