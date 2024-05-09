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
