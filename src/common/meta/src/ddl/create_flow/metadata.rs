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
use crate::error::Result;
use crate::key::table_name::TableNameKey;

impl CreateFlowProcedure {
    /// Allocates the [FlowId].
    pub(crate) async fn allocate_flow_id(&mut self) -> Result<()> {
        // TODO(weny, ruihang): We don't support the partitions. It's always be 1, now.
        let partitions = 1;
        let (flow_id, peers) = self
            .context
            .flow_metadata_allocator
            .create(partitions)
            .await?;
        self.data.flow_id = Some(flow_id);
        self.data.peers = peers;

        Ok(())
    }

    /// Ensures all source tables exist and collects source table ids
    pub(crate) async fn collect_source_tables(&mut self) -> Result<()> {
        let keys = self
            .data
            .task
            .source_table_names
            .iter()
            .map(|name| TableNameKey::new(&name.catalog_name, &name.schema_name, &name.table_name))
            .collect::<Vec<_>>();

        let source_table_ids = self
            .context
            .table_metadata_manager
            .table_name_manager()
            .batch_get(keys)
            .await?;

        let mut resolved = Vec::with_capacity(self.data.task.source_table_names.len());
        let mut unresolved = Vec::new();

        for (name, table_id) in self
            .data
            .task
            .source_table_names
            .iter()
            .zip(source_table_ids)
        {
            match table_id {
                Some(table_id) => resolved.push(table_id.table_id()),
                None => unresolved.push(name.clone()),
            }
        }

        self.data.source_table_ids = resolved;
        self.data.unresolved_source_table_names = unresolved;
        self.data.last_activation_error = None;
        Ok(())
    }
}
