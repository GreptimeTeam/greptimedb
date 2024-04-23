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

use serde::{Deserialize, Serialize};

use crate::error::Result;
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    txn_helper, DeserializedValueWithBytes, TableMetaKey, TableMetaValue, TaskId,
    FLOW_TASK_NAME_KEY_PREFIX,
};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;

/// The key of mapping name to [TaskId]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlowTaskNameKey<'a> {
    pub catalog: &'a str,
    pub task: &'a str,
}

impl<'a> TableMetaKey for FlowTaskNameKey<'a> {
    fn as_raw_key(&self) -> Vec<u8> {
        format!("{FLOW_TASK_NAME_KEY_PREFIX}/{}/{}", self.catalog, self.task).into_bytes()
    }
}

impl<'a> FlowTaskNameKey<'a> {
    /// Returns a [FlowTaskNameKey].
    pub fn new(catalog: &'a str, task: &'a str) -> Self {
        Self { catalog, task }
    }
}

/// The value of [FlowTaskNameKey].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlowTaskNameValue {
    task_id: TaskId,
}

impl FlowTaskNameValue {
    /// Returns a [FlowTaskNameValue] with specified [TaskId].
    pub fn new(task_id: TaskId) -> Self {
        Self { task_id }
    }

    /// Returns the [TaskId]
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }
}

/// The manager of [FlowTaskNameKey].
pub struct FlowTaskNameManager {
    kv_backend: KvBackendRef,
}

impl FlowTaskNameManager {
    /// Returns a new [FlowTaskNameManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Returns the [FlowTaskNameValue] of specified `catalog.task`.
    pub async fn get(&self, catalog: &str, task: &str) -> Result<Option<FlowTaskNameValue>> {
        let key = FlowTaskNameKey::new(catalog, task);
        let raw_key = key.as_raw_key();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| FlowTaskNameValue::try_from_raw_value(&x.value))
            .transpose()
    }

    /// Builds a create flow task name transaction, it is expected that the `__flow_task_name/{catalog}/{name}` wasn't occupied.
    /// Otherwise, the transaction will retrieve existing value.
    pub fn build_create_txn(
        &self,
        catalog: &str,
        name: &str,
        task_id: TaskId,
    ) -> Result<(
        Txn,
        impl FnOnce(
            &mut TxnOpGetResponseSet,
        ) -> Result<Option<DeserializedValueWithBytes<FlowTaskNameValue>>>,
    )> {
        let key = FlowTaskNameKey::new(catalog, name);
        let raw_key = key.as_raw_key();
        let flow_task_name_value = FlowTaskNameValue::new(task_id);
        let txn = txn_helper::build_put_if_absent_txn(
            raw_key.clone(),
            flow_task_name_value.try_as_raw_value()?,
        );

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }
}
