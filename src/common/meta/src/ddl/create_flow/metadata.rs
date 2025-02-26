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

use snafu::OptionExt;

use crate::ddl::create_flow::CreateFlowProcedure;
use crate::error::{self, Result};
use crate::key::table_name::TableNameKey;

impl CreateFlowProcedure {
    /// Allocates the [FlowId].
    pub(crate) async fn allocate_flow_id(&mut self) -> Result<()> {
        //TODO(weny, ruihang): We doesn't support the partitions. It's always be 1, now.
        let partitions = 1;
        let cluster_id = self.data.cluster_id;
        let (flow_id, peers) = self
            .context
            .flow_metadata_allocator
            .create(cluster_id, partitions)
            .await?;
        self.data.flow_id = Some(flow_id);
        self.data.peers = peers;

        Ok(())
    }

    /// Ensures all source tables exist and collects source table ids
    pub(crate) async fn collect_source_tables(&mut self) -> Result<()> {
        // Ensures all source tables exist.
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

        let source_table_ids = self
            .data
            .task
            .source_table_names
            .iter()
            .zip(source_table_ids)
            .map(|(name, table_id)| {
                Ok(table_id
                    .with_context(|| error::TableNotFoundSnafu {
                        table_name: name.to_string(),
                    })?
                    .table_id())
            })
            .collect::<Result<Vec<_>>>()?;

        self.data.source_table_ids = source_table_ids;
        Ok(())
    }
}
