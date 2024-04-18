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

pub(crate) mod error;
use common_meta::error::Result as MetaResult;
use common_meta::kv_backend::KvBackendRef;
use serde::{Deserialize, Serialize};

pub struct FlowTaskMetadataManager {
    /// table_name: Vec<String> to a list of task ids(u64)
    source_table_to_tasks: KvBackendRef,
    /// task_id: u64 -> flow node id: u64
    task_to_node: KvBackendRef,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct TaskId(pub usize);

impl FlowTaskMetadataManager {
    pub async fn create_flow_task(&self, sql: &str) -> MetaResult<TaskId> {
        // TODO: send sql to flow node, which will try to parse it into a flow task
        // Then it will return task metadata including source table used
        todo!()
    }
    pub async fn remove_flow_task(id: TaskId) -> MetaResult<()> {
        Ok(())
    }
}
