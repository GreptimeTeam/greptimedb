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

use crate::ddl::create_flow::CreateFlowProcedure;
use crate::error::{self, Result};
use crate::key::table_name::TableNameKey;

impl CreateFlowProcedure {
    /// Allocates the [FlowTaskId].
    pub(crate) async fn allocate_flow_task_id(&mut self) -> Result<()> {
        //TODO(weny, ruihang): We doesn't support the partitions. It's always be 1, now.
        let partitions = 1;
        let (flow_task_id, peers) = self
            .context
            .flow_task_metadata_allocator
            .create(partitions)
            .await?;
        self.data.flow_task_id = Some(flow_task_id);
        self.data.peers = peers;

        Ok(())
    }

    /// Collects source table ids
    pub(crate) async fn collect_source_tables(&mut self) -> Result<()> {
        // Ensures all source tables exist.
        let mut source_table_ids = Vec::with_capacity(self.data.task.source_table_names.len());

        for name in &self.data.task.source_table_names {
            let key = TableNameKey::new(&name.catalog_name, &name.schema_name, &name.table_name);
            match self
                .context
                .table_metadata_manager
                .table_name_manager()
                .get(key)
                .await?
            {
                Some(value) => source_table_ids.push(value.table_id()),
                None => {
                    return error::TableNotFoundSnafu {
                        table_name: name.to_string(),
                    }
                    .fail();
                }
            }
        }

        self.data.source_table_ids = source_table_ids;
        Ok(())
    }
}
