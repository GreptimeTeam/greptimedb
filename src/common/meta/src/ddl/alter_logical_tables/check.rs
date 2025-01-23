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

use std::collections::HashSet;

use api::v1::alter_table_expr::Kind;
use snafu::{ensure, OptionExt};

use crate::ddl::alter_logical_tables::AlterLogicalTablesProcedure;
use crate::error::{AlterLogicalTablesInvalidArgumentsSnafu, Result};
use crate::key::table_info::TableInfoValue;
use crate::key::table_route::TableRouteValue;
use crate::rpc::ddl::AlterTableTask;

impl AlterLogicalTablesProcedure {
    pub(crate) fn check_input_tasks(&self) -> Result<()> {
        self.check_schema()?;
        self.check_alter_kind()?;
        Ok(())
    }

    pub(crate) async fn check_physical_table(&self) -> Result<()> {
        let table_route_manager = self.context.table_metadata_manager.table_route_manager();
        let table_ids = self
            .data
            .table_info_values
            .iter()
            .map(|v| v.table_info.ident.table_id)
            .collect::<Vec<_>>();
        let table_routes = table_route_manager
            .table_route_storage()
            .batch_get(&table_ids)
            .await?;
        let physical_table_id = self.data.physical_table_id;
        let is_same_physical_table = table_routes.iter().all(|r| {
            if let Some(TableRouteValue::Logical(r)) = r {
                r.physical_table_id() == physical_table_id
            } else {
                false
            }
        });

        ensure!(
            is_same_physical_table,
            AlterLogicalTablesInvalidArgumentsSnafu {
                err_msg: "All the tasks should have the same physical table id"
            }
        );

        Ok(())
    }

    pub(crate) fn check_finished_tasks(&self) -> Result<Vec<bool>> {
        let task = &self.data.tasks;
        let table_info_values = &self.data.table_info_values;

        Ok(task
            .iter()
            .zip(table_info_values.iter())
            .map(|(task, table)| Self::check_finished_task(task, table))
            .collect())
    }

    // Checks if the schemas of the tasks are the same
    fn check_schema(&self) -> Result<()> {
        let is_same_schema = self.data.tasks.windows(2).all(|pair| {
            pair[0].alter_table.catalog_name == pair[1].alter_table.catalog_name
                && pair[0].alter_table.schema_name == pair[1].alter_table.schema_name
        });

        ensure!(
            is_same_schema,
            AlterLogicalTablesInvalidArgumentsSnafu {
                err_msg: "Schemas of the tasks are not the same"
            }
        );

        Ok(())
    }

    fn check_alter_kind(&self) -> Result<()> {
        for task in &self.data.tasks {
            let kind = task.alter_table.kind.as_ref().context(
                AlterLogicalTablesInvalidArgumentsSnafu {
                    err_msg: "Alter kind is missing",
                },
            )?;
            let Kind::AddColumns(_) = kind else {
                return AlterLogicalTablesInvalidArgumentsSnafu {
                    err_msg: "Only support add columns operation",
                }
                .fail();
            };
        }

        Ok(())
    }

    fn check_finished_task(task: &AlterTableTask, table: &TableInfoValue) -> bool {
        let columns = table
            .table_info
            .meta
            .schema
            .column_schemas
            .iter()
            .map(|c| &c.name)
            .collect::<HashSet<_>>();

        let Some(kind) = task.alter_table.kind.as_ref() else {
            return true; // Never get here since we have checked it in `check_alter_kind`
        };
        let Kind::AddColumns(add_columns) = kind else {
            return true; // Never get here since we have checked it in `check_alter_kind`
        };

        // We only check that all columns have been finished. That is to say,
        // if one part is finished but another part is not, it will be considered
        // unfinished.
        add_columns
            .add_columns
            .iter()
            .map(|add_column| add_column.column_def.as_ref().map(|c| &c.name))
            .all(|column| column.map(|c| columns.contains(c)).unwrap_or(false))
    }
}
