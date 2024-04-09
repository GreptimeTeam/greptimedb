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

use common_catalog::format_full_table_name;
use snafu::OptionExt;
use table::metadata::TableId;

use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::error::{
    AlterLogicalTablesInvalidArgumentsSnafu, Result, TableInfoNotFoundSnafu, TableNotFoundSnafu,
    TableRouteNotFoundSnafu,
};
use crate::key::table_info::TableInfoValue;
use crate::key::table_name::TableNameKey;
use crate::key::table_route::TableRouteValue;
use crate::key::DeserializedValueWithBytes;
use crate::rpc::ddl::AlterTableTask;

impl AlterLogicalTablesProcedure {
    pub(crate) fn filter_task(&mut self, finished_tasks: &[bool]) -> Result<()> {
        debug_assert_eq!(finished_tasks.len(), self.data.tasks.len());
        debug_assert_eq!(finished_tasks.len(), self.data.table_info_values.len());
        self.data.tasks = self
            .data
            .tasks
            .drain(..)
            .zip(finished_tasks.iter())
            .filter_map(|(task, finished)| if *finished { None } else { Some(task) })
            .collect();
        self.data.table_info_values = self
            .data
            .table_info_values
            .drain(..)
            .zip(finished_tasks.iter())
            .filter_map(|(table_info_value, finished)| {
                if *finished {
                    None
                } else {
                    Some(table_info_value)
                }
            })
            .collect();

        Ok(())
    }

    pub(crate) async fn fill_physical_table_info(&mut self) -> Result<()> {
        let (physical_table_info, physical_table_route) = self
            .context
            .table_metadata_manager
            .get_full_table_info(self.data.physical_table_id)
            .await?;

        let physical_table_info = physical_table_info.with_context(|| TableInfoNotFoundSnafu {
            table: format!("table id - {}", self.data.physical_table_id),
        })?;
        let physical_table_route = physical_table_route
            .context(TableRouteNotFoundSnafu {
                table_id: self.data.physical_table_id,
            })?
            .into_inner();

        self.data.physical_table_info = Some(physical_table_info);
        let TableRouteValue::Physical(physical_table_route) = physical_table_route else {
            return AlterLogicalTablesInvalidArgumentsSnafu {
                err_msg: format!(
                    "expected a physical table but got a logical table: {:?}",
                    self.data.physical_table_id
                ),
            }
            .fail();
        };
        self.data.physical_table_route = Some(physical_table_route);

        Ok(())
    }

    pub(crate) async fn fill_table_info_values(&mut self) -> Result<()> {
        let table_ids = self.get_all_table_ids().await?;
        let table_info_values = self.get_all_table_info_values(&table_ids).await?;
        debug_assert_eq!(table_info_values.len(), self.data.tasks.len());
        self.data.table_info_values = table_info_values;

        Ok(())
    }

    async fn get_all_table_info_values(
        &self,
        table_ids: &[TableId],
    ) -> Result<Vec<DeserializedValueWithBytes<TableInfoValue>>> {
        let table_info_manager = self.context.table_metadata_manager.table_info_manager();
        let mut table_info_map = table_info_manager.batch_get_raw(table_ids).await?;
        let mut table_info_values = Vec::with_capacity(table_ids.len());
        for (table_id, task) in table_ids.iter().zip(self.data.tasks.iter()) {
            let table_info_value =
                table_info_map
                    .remove(table_id)
                    .with_context(|| TableInfoNotFoundSnafu {
                        table: extract_table_name(task),
                    })?;
            table_info_values.push(table_info_value);
        }

        Ok(table_info_values)
    }

    async fn get_all_table_ids(&self) -> Result<Vec<TableId>> {
        let table_name_manager = self.context.table_metadata_manager.table_name_manager();
        let table_name_keys = self
            .data
            .tasks
            .iter()
            .map(|task| extract_table_name_key(task))
            .collect();

        let table_name_values = table_name_manager.batch_get(table_name_keys).await?;
        let mut table_ids = Vec::with_capacity(table_name_values.len());
        for (value, task) in table_name_values.into_iter().zip(self.data.tasks.iter()) {
            let table_id = value
                .with_context(|| TableNotFoundSnafu {
                    table_name: extract_table_name(task),
                })?
                .table_id();
            table_ids.push(table_id);
        }

        Ok(table_ids)
    }
}

#[inline]
fn extract_table_name(task: &AlterTableTask) -> String {
    format_full_table_name(
        &task.alter_table.catalog_name,
        &task.alter_table.schema_name,
        &task.alter_table.table_name,
    )
}

#[inline]
fn extract_table_name_key(task: &AlterTableTask) -> TableNameKey {
    TableNameKey::new(
        &task.alter_table.catalog_name,
        &task.alter_table.schema_name,
        &task.alter_table.table_name,
    )
}
