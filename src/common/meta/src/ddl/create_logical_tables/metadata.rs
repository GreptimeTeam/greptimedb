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
use std::sync::Arc;

use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use snafu::OptionExt;

use crate::ddl::create_logical_tables::CreateLogicalTablesProcedure;
use crate::error::Result;
use crate::key::table_route::TableRouteValue;

impl CreateLogicalTablesProcedure {
    pub(crate) async fn fill_physical_table_info(&mut self) -> Result<()> {
        let physical_region_numbers = self
            .context
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(self.data.physical_table_id)
            .await
            .map(|(_, route)| TableRouteValue::Physical(route).region_numbers())?;

        self.data.physical_region_numbers = physical_region_numbers;

        // Extract partition column names from the physical table
        let physical_table_info = self
            .context
            .table_metadata_manager
            .table_info_manager()
            .get(self.data.physical_table_id)
            .await?
            .with_context(|| crate::error::TableInfoNotFoundSnafu {
                table: format!("physical table {}", self.data.physical_table_id),
            })?;

        let physical_partition_columns: Vec<String> = physical_table_info
            .table_info
            .meta
            .partition_key_indices
            .iter()
            .map(|&idx| {
                physical_table_info.table_info.meta.schema.column_schemas()[idx]
                    .name
                    .clone()
            })
            .collect();

        self.data.physical_partition_columns = physical_partition_columns;

        Ok(())
    }

    pub(crate) fn merge_partition_columns_into_logical_tables(&mut self) -> Result<()> {
        let partition_columns = &self.data.physical_partition_columns;

        // Skip if no partition columns to add
        if partition_columns.is_empty() {
            return Ok(());
        }

        for task in &mut self.data.tasks {
            // Get existing column names in the logical table
            let column_schemas = task.table_info.meta.schema.column_schemas();
            let existing_column_names: HashSet<_> =
                column_schemas.iter().map(|c| &c.name).collect();

            let mut new_columns = Vec::new();
            let mut new_primary_key_indices = task.table_info.meta.primary_key_indices.clone();

            // Add missing partition columns
            for partition_column in partition_columns {
                if !existing_column_names.contains(partition_column) {
                    let new_column_index = column_schemas.len() + new_columns.len();

                    // Create new column schema for the partition column
                    let column_schema = ColumnSchema::new(
                        partition_column.clone(),
                        ConcreteDataType::string_datatype(),
                        true,
                    );
                    new_columns.push(column_schema);

                    // Add to primary key indices (partition columns are part of primary key)
                    new_primary_key_indices.push(new_column_index);
                }
            }

            // If we added new columns, update the table info
            if !new_columns.is_empty() {
                let mut updated_columns = column_schemas.to_vec();
                updated_columns.extend(new_columns);

                // Create new schema with updated columns
                let new_schema = Arc::new(Schema::new(updated_columns));

                // Update the table info
                task.table_info.meta.schema = new_schema;
                task.table_info.meta.primary_key_indices = new_primary_key_indices;
            }
        }

        Ok(())
    }

    pub(crate) async fn allocate_table_ids(&mut self) -> Result<()> {
        for (task, table_id) in self
            .data
            .tasks
            .iter_mut()
            .zip(self.data.table_ids_already_exists.iter())
        {
            let table_id = if let Some(table_id) = table_id {
                *table_id
            } else {
                self.context
                    .table_metadata_allocator
                    .allocate_table_id(&task.create_table.table_id)
                    .await?
            };
            task.set_table_id(table_id);

            // sort columns in task
            task.sort_columns();
        }

        Ok(())
    }
}
