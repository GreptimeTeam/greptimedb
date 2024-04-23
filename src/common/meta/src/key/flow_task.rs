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

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use table::metadata::TableId;

use super::txn_helper::TxnOpGetResponseSet;
use super::{DeserializedValueWithBytes, PartitionId};
use crate::error::Result;
use crate::key::{txn_helper, TableMetaKey, TableMetaValue, TaskId, FLOW_TASK_KEY_PREFIX};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;
use crate::FlownodeId;

/// The key of flow task metadata.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FlowTaskKey {
    task_id: TaskId,
}

impl FlowTaskKey {
    /// Returns a [FlowTaskKey] with the specified `task_id`.
    pub fn new(task_id: TaskId) -> FlowTaskKey {
        FlowTaskKey { task_id }
    }
}

impl TableMetaKey for FlowTaskKey {
    fn as_raw_key(&self) -> Vec<u8> {
        format!("{FLOW_TASK_KEY_PREFIX}/{}", self.task_id).into_bytes()
    }
}

// The metadata of the flow task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlowTaskValue {
    /// The source tables used by the task.
    pub(crate) source_tables: Vec<TableId>,
    /// The sink table used by the task.
    pub(crate) sink_table: TableId,
    /// Which flow nodes this task is running on.
    pub(crate) flownode_ids: BTreeMap<PartitionId, FlownodeId>,
    /// The metadata
    pub(crate) catalog_name: String,
    pub(crate) task_name: String,
    pub(crate) raw_sql: String,
    pub(crate) expire_when: String,
    pub(crate) comment: String,
    pub(crate) options: HashMap<String, String>,
}

impl FlowTaskValue {
    /// Returns the `flownode_id`.
    pub fn flownode_ids(&self) -> &BTreeMap<PartitionId, FlownodeId> {
        &self.flownode_ids
    }

    /// Returns the `source_table`.
    pub fn source_table_ids(&self) -> &[TableId] {
        &self.source_tables
    }
}

/// The manager of [FlowTaskKey].
pub struct FlowTaskManager {
    kv_backend: KvBackendRef,
}

impl FlowTaskManager {
    /// Returns a new [FlowTaskManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Returns the [FlowTaskValue] of specified `task_id`.
    pub async fn get(&self, task_id: TaskId) -> Result<Option<FlowTaskValue>> {
        let key = FlowTaskKey::new(task_id);
        let raw_key = key.as_raw_key();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| FlowTaskValue::try_from_raw_value(&x.value))
            .transpose()
    }

    /// Builds a create flow task transaction, it is expected that the `__flow_task/{task_id}` wasn't occupied.
    /// Otherwise, the transaction will retrieve existing value.
    pub(crate) fn build_create_txn(
        &self,
        task_id: TaskId,
        flow_task_value: &FlowTaskValue,
    ) -> Result<(
        Txn,
        impl FnOnce(
            &mut TxnOpGetResponseSet,
        ) -> Result<Option<DeserializedValueWithBytes<FlowTaskValue>>>,
    )> {
        let key = FlowTaskKey::new(task_id);
        let raw_key = key.as_raw_key();
        let txn = txn_helper::build_put_if_absent_txn(
            raw_key.clone(),
            flow_task_value.try_as_raw_value()?,
        );

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_serialization() {
        let flow_task = FlowTaskKey::new(2);
        assert_eq!(b"__flow_task/2".to_vec(), flow_task.as_raw_key());
    }
}
