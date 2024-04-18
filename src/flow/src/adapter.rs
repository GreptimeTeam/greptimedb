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

//! for getting data from source and sending results to sink
//! and communicating with other parts of the database

// allow unused code for now since we are just defining the structure
#![allow(unused)]

pub(crate) mod error;
use common_meta::error::Result as MetaResult;
use common_meta::kv_backend::KvBackendRef;
use serde::{Deserialize, Serialize};
use table::metadata::TableId;

pub struct FlowTaskManager {
    /// table_id: TableId to a list of task ids(u64)
    mirror_write_manager: TaskRouteManager,
    /// task_id: u64 -> TaskMetadata
    task_info: TaskMetadataManager,
}

impl FlowTaskManager {
    pub async fn create_flow_task(&self, sql: &str) -> MetaResult<TaskId> {
        // TODO: send sql to flow node, which will try to parse it into a flow task
        // Then it will return task metadata including source table used
        todo!()
    }
    pub async fn remove_flow_task(id: TaskId) -> MetaResult<()> {
        Ok(())
    }
}

/// Manage the routing of write request from a table to a list of tasks
pub struct TaskRouteManager {
    /// table_id: TableId to a list of task ids(u64)
    source_table_to_tasks: KvBackendRef,
    flow_node_id_to_metadata: KvBackendRef,
}

impl TaskRouteManager {
    /// Get all task ids that are reading from this table
    pub async fn get_tasks(&self, table_id: TableId) -> MetaResult<Vec<TaskId>> {
        // get all task ids that are writing to this table
        todo!()
    }

    /// Set a task to read from this table
    pub async fn set_tasks(&self, table_id: TableId, task_ids: Vec<TaskId>) -> MetaResult<()> {
        // set a task id to write to this table
        todo!()
    }

    /// Get flow node metadata by node id
    pub async fn get_node_metadata(&self, node_id: FlowNodeId) -> MetaResult<FlowNodeMetadata> {
        // get node metadata by node id
        todo!()
    }

    pub async fn set_node_metadata(
        &self,
        node_id: FlowNodeId,
        metadata: FlowNodeMetadata,
    ) -> MetaResult<()> {
        // set node metadata by node id
        todo!()
    }
}

pub struct FlowNodeMetadata {
    tasks: Vec<TaskId>,
    source_tables: Vec<TableId>,
}
pub struct TaskMetadataManager {
    task_id_to_metadata: KvBackendRef,
}

impl TaskMetadataManager {
    /// Get task metadata by task id
    pub async fn get(&self, task_id: TaskId) -> MetaResult<TaskMetadata> {
        // get task metadata by task id
        todo!()
    }

    /// Set task metadata by task id
    pub async fn set(&self, task_id: TaskId, metadata: TaskMetadata) -> MetaResult<TaskMetadata> {
        // set task metadata by task id
        todo!()
    }
}

pub struct TaskMetadata {
    /// The source tables used by this task
    pub source_tables: Vec<TableId>,
    /// The sink tables used by this task
    pub sink_tables: Vec<TableId>,
    /// Which flow node this task is running on
    pub flow_node: FlowNodeId,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TaskId(pub usize);

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct FlowNodeId(pub usize);
