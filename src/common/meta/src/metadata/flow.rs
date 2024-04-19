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

use snafu::{ensure, OptionExt};

use crate::ensure_values;
use crate::error::{self, Result};
use crate::key::flow_task::{FlowTaskManager, FlowTaskValue};
use crate::key::flownode_task::FlownodeTaskManager;
use crate::key::table_task::TableTaskManager;
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::TaskId;
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;

/// The manager of metadata, provides ability to:
/// - Create metadata of the task.
/// - Retrieve metadata of the task.
/// - Delete metadata of the task.
pub struct FlowMetadataManager {
    flow_task_manager: FlowTaskManager,
    flownode_task_manager: FlownodeTaskManager,
    table_task_manager: TableTaskManager,
    kv_backend: KvBackendRef,
}

impl FlowMetadataManager {
    /// Returns a new [FlowMetadataManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            flow_task_manager: FlowTaskManager::new(kv_backend.clone()),
            flownode_task_manager: FlownodeTaskManager::new(kv_backend.clone()),
            table_task_manager: TableTaskManager::new(kv_backend.clone()),
            kv_backend,
        }
    }

    /// Returns the [FlowTaskManager].
    pub fn flow_task_manager(&self) -> &FlowTaskManager {
        &self.flow_task_manager
    }

    /// Returns the [FlownodeTaskManager].
    pub fn flownode_task_manager(&self) -> &FlownodeTaskManager {
        &self.flownode_task_manager
    }

    /// Returns the [TableTaskManager].
    pub fn table_task_manager(&self) -> &TableTaskManager {
        &self.table_task_manager
    }

    /// Creates metadata for task and returns an error if different metadata exists.
    pub async fn create_flow_metadata(
        &self,
        task_id: TaskId,
        flow_task_value: FlowTaskValue,
    ) -> Result<()> {
        let (create_flow_task_txn, on_create_flow_task_failure) = self
            .flow_task_manager
            .build_create_txn(task_id, &flow_task_value)?;

        let create_flownode_task_txn = self
            .flownode_task_manager
            .build_create_txn(task_id, flow_task_value.flownode_id());

        let create_table_task_txn = self.table_task_manager.build_create_txn(
            task_id,
            flow_task_value.flownode_id(),
            flow_task_value.source_table_ids(),
        );

        let txn = Txn::merge_all(vec![
            create_flow_task_txn,
            create_flownode_task_txn,
            create_table_task_txn,
        ]);

        let mut resp = self.kv_backend.txn(txn).await?;
        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let remote_flow_task = on_create_flow_task_failure(&mut set)?
                .context(error::UnexpectedSnafu {
                    err_msg: format!(
                    "Reads the empty flow task during the creating flow task, task_id: {task_id}"
                ),
                })?
                .into_inner();
            let op_name = "creating flow task";
            ensure_values!(remote_flow_task, flow_task_value, op_name);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use futures::TryStreamExt;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_create_flow_metadata() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv);
        let task_id = 10;
        let flow_task_value = FlowTaskValue::new(vec![1024, 1025, 1026], vec![2048], 1);
        flow_metadata_manager
            .create_flow_metadata(task_id, flow_task_value.clone())
            .await
            .unwrap();
        // Creates again.
        flow_metadata_manager
            .create_flow_metadata(task_id, flow_task_value.clone())
            .await
            .unwrap();
        let got = flow_metadata_manager
            .flow_task_manager()
            .get(task_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got, flow_task_value);
        let tasks = flow_metadata_manager
            .flownode_task_manager()
            .tasks(1)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(tasks, vec![task_id]);
        for table_id in [1024, 1025, 1026] {
            let nodes = flow_metadata_manager
                .table_task_manager()
                .nodes(table_id)
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(nodes, vec![(1, task_id)])
        }
    }

    #[tokio::test]
    async fn test_create_table_metadata_err() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let flow_metadata_manager = FlowMetadataManager::new(mem_kv);
        let task_id = 10;
        let flow_task_value = FlowTaskValue::new(vec![1024, 1025, 1026], vec![2048], 1);
        flow_metadata_manager
            .create_flow_metadata(task_id, flow_task_value.clone())
            .await
            .unwrap();
        // Creates again.
        let flow_task_value = FlowTaskValue::new(vec![1024, 1025, 1026], vec![2049], 1);
        let err = flow_metadata_manager
            .create_flow_metadata(task_id, flow_task_value)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Reads the different value"));
    }
}
