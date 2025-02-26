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
use common_telemetry::warn;
use itertools::Itertools;
use snafu::ResultExt;
use table::metadata::{RawTableInfo, TableInfo};

use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::ddl::physical_table_metadata;
use crate::error;
use crate::error::{ConvertAlterTableRequestSnafu, Result};
use crate::key::table_info::TableInfoValue;
use crate::key::DeserializedValueWithBytes;
use crate::rpc::ddl::AlterTableTask;

impl AlterLogicalTablesProcedure {
    pub(crate) async fn update_physical_table_metadata(&mut self) -> Result<()> {
        if self.data.physical_columns.is_empty() {
            warn!("No physical columns found, leaving the physical table's schema unchanged when altering logical tables");
            return Ok(());
        }

        // Safety: must exist.
        let physical_table_info = self.data.physical_table_info.as_ref().unwrap();

        // Generates new table info
        let old_raw_table_info = physical_table_info.table_info.clone();
        let new_raw_table_info = physical_table_metadata::build_new_physical_table_info(
            old_raw_table_info,
            &self.data.physical_columns,
        );

        // Updates physical table's metadata, and we don't need to touch per-region settings.
        self.context
            .table_metadata_manager
            .update_table_info(physical_table_info, None, new_raw_table_info)
            .await?;

        Ok(())
    }

    pub(crate) async fn update_logical_tables_metadata(&mut self) -> Result<()> {
        let table_info_values = self.build_update_metadata()?;
        let manager = &self.context.table_metadata_manager;
        let chunk_size = manager.batch_update_table_info_value_chunk_size();
        if table_info_values.len() > chunk_size {
            let chunks = table_info_values
                .into_iter()
                .chunks(chunk_size)
                .into_iter()
                .map(|check| check.collect::<Vec<_>>())
                .collect::<Vec<_>>();
            for chunk in chunks {
                manager.batch_update_table_info_values(chunk).await?;
            }
        } else {
            manager
                .batch_update_table_info_values(table_info_values)
                .await?;
        }

        Ok(())
    }

    pub(crate) fn build_update_metadata(
        &self,
    ) -> Result<Vec<(DeserializedValueWithBytes<TableInfoValue>, RawTableInfo)>> {
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
        table: &DeserializedValueWithBytes<TableInfoValue>,
    ) -> Result<(DeserializedValueWithBytes<TableInfoValue>, RawTableInfo)> {
        // Builds new_meta
        let table_info = TableInfo::try_from(table.table_info.clone())
            .context(error::ConvertRawTableInfoSnafu)?;
        let table_ref = task.table_ref();
        let request =
            alter_expr_to_request(table.table_info.ident.table_id, task.alter_table.clone())
                .context(ConvertAlterTableRequestSnafu)?;
        let new_meta = table_info
            .meta
            .builder_with_alter_kind(table_ref.table, &request.alter_kind)
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
