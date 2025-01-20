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

use snafu::ensure;

use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::error::{CreateLogicalTablesInvalidArgumentsSnafu, Result, TableAlreadyExistsSnafu};
use crate::key::table_name::TableNameKey;

impl CreateLogicalTablesProcedure {
    pub(crate) fn check_input_tasks(&self) -> Result<()> {
        self.check_schema()?;

        Ok(())
    }

    pub async fn check_tables_already_exist(&mut self) -> Result<()> {
        let table_name_keys = self
            .data
            .all_create_table_exprs()
            .iter()
            .map(|expr| TableNameKey::new(&expr.catalog_name, &expr.schema_name, &expr.table_name))
            .collect::<Vec<_>>();
        let table_ids_already_exists = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .batch_get(table_name_keys)
            .await?
            .iter()
            .map(|x| x.map(|x| x.table_id()))
            .collect::<Vec<_>>();

        self.data.table_ids_already_exists = table_ids_already_exists;

        // Validates the tasks
        let tasks = &mut self.data.tasks;
        for (task, table_id) in tasks.iter().zip(self.data.table_ids_already_exists.iter()) {
            if table_id.is_some() {
                // If a table already exists, we just ignore it.
                ensure!(
                    task.create_table.create_if_not_exists,
                    TableAlreadyExistsSnafu {
                        table_name: task.create_table.table_name.to_string(),
                    }
                );
                continue;
            }
        }

        Ok(())
    }

    // Checks if the schemas of the tasks are the same
    fn check_schema(&self) -> Result<()> {
        let is_same_schema = self.data.tasks.windows(2).all(|pair| {
            pair[0].create_table.catalog_name == pair[1].create_table.catalog_name
                && pair[0].create_table.schema_name == pair[1].create_table.schema_name
        });

        ensure!(
            is_same_schema,
            CreateLogicalTablesInvalidArgumentsSnafu {
                err_msg: "Schemas of the tasks are not the same"
            }
        );

        Ok(())
    }
}
