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

use crate::ddl::create_flow::CreateFlowProcedure;
use crate::error::{self, Result};
use crate::key::table_name::TableNameKey;

impl CreateFlowProcedure {
    /// Checks:
    /// - The new task name doesn't exist.
    /// - The sink table doesn't exist.
    pub(crate) async fn check_creation(&self) -> Result<()> {
        let catalog_name = &self.data.task.catalog_name;
        let task_name = &self.data.task.task_name;
        let sink_table_name = &self.data.task.sink_table_name;

        // Ensures the task name doesn't exist.
        let exists = self
            .context
            .flow_task_metadata_manager
            .flow_task_name_manager()
            .exists(catalog_name, task_name)
            .await?;
        ensure!(
            !exists,
            error::TaskAlreadyExistsSnafu {
                task_name: format!("{}.{}", catalog_name, task_name),
            }
        );

        //  Ensures sink table doesn't exist.
        let exists = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .exists(TableNameKey::new(
                &sink_table_name.catalog_name,
                &sink_table_name.schema_name,
                &sink_table_name.table_name,
            ))
            .await?;
        ensure!(
            !exists,
            error::TableAlreadyExistsSnafu {
                table_name: sink_table_name.to_string(),
            }
        );

        Ok(())
    }
}
