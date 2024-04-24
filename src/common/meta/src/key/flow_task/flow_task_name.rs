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

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::{self, Result};
use crate::key::flow_task::FlowTaskScoped;
use crate::key::scope::{CatalogScoped, MetaKey};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    txn_helper, DeserializedValueWithBytes, FlowTaskId, TableMetaValue, NAME_PATTERN,
};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;

const FLOW_TASK_NAME_KEY_PREFIX: &str = "name";

lazy_static! {
    static ref FLOW_TASK_NAME_KEY_PATTERN: Regex =
        Regex::new(&format!("^{FLOW_TASK_NAME_KEY_PREFIX}/({NAME_PATTERN})$")).unwrap();
}

/// The key of mapping {task_name} to [FlowTaskId].
///
/// The layout: `__flow_task/{catalog}/name/{task_name}`.
pub struct FlowTaskNameKey(FlowTaskScoped<CatalogScoped<FlowTaskNameKeyInner>>);

impl FlowTaskNameKey {
    /// Returns the [FlowTaskNameKey]
    pub fn new(catalog: String, task_name: String) -> FlowTaskNameKey {
        let inner = FlowTaskNameKeyInner::new(task_name);
        FlowTaskNameKey(FlowTaskScoped::new(CatalogScoped::new(catalog, inner)))
    }

    /// Returns the catalog.
    pub fn catalog(&self) -> &str {
        self.0.catalog()
    }

    /// Return the `task_name`
    pub fn task_name(&self) -> &str {
        &self.0.task_name
    }
}

impl MetaKey<FlowTaskNameKey> for FlowTaskNameKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlowTaskNameKey> {
        Ok(FlowTaskNameKey(FlowTaskScoped::<
            CatalogScoped<FlowTaskNameKeyInner>,
        >::from_bytes(bytes)?))
    }
}

/// The key of mapping name to [FlowTaskId]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowTaskNameKeyInner {
    pub task_name: String,
}

impl MetaKey<FlowTaskNameKeyInner> for FlowTaskNameKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!("{FLOW_TASK_NAME_KEY_PREFIX}/{}", self.task_name).into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlowTaskNameKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidTableMetadataSnafu {
                err_msg: format!(
                    "FlowTaskNameKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            FLOW_TASK_NAME_KEY_PATTERN
                .captures(key)
                .context(error::InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid FlowTaskNameKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let task = captures[1].to_string();
        Ok(FlowTaskNameKeyInner { task_name: task })
    }
}

impl FlowTaskNameKeyInner {
    /// Returns a [FlowTaskNameKeyInner].
    pub fn new(task: String) -> Self {
        Self { task_name: task }
    }
}

/// The value of [FlowTaskNameKey].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlowTaskNameValue {
    flow_task_id: FlowTaskId,
}

impl FlowTaskNameValue {
    /// Returns a [FlowTaskNameValue] with specified [TaskId].
    pub fn new(flow_task_id: FlowTaskId) -> Self {
        Self { flow_task_id }
    }

    /// Returns the [FlowTaskId]
    pub fn flow_task_id(&self) -> FlowTaskId {
        self.flow_task_id
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
        let key = FlowTaskNameKey::new(catalog.to_string(), task.to_string());
        let raw_key = key.to_bytes();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| FlowTaskNameValue::try_from_raw_value(&x.value))
            .transpose()
    }

    /// Builds a create flow task name transaction.
    /// It's expected that the `__flow_task_name/{catalog}/{name}` wasn't occupied.
    /// Otherwise, the transaction will retrieve existing value.
    pub fn build_create_txn(
        &self,
        catalog: &str,
        name: &str,
        flow_task_id: FlowTaskId,
    ) -> Result<(
        Txn,
        impl FnOnce(
            &mut TxnOpGetResponseSet,
        ) -> Result<Option<DeserializedValueWithBytes<FlowTaskNameValue>>>,
    )> {
        let key = FlowTaskNameKey::new(catalog.to_string(), name.to_string());
        let raw_key = key.to_bytes();
        let flow_task_name_value = FlowTaskNameValue::new(flow_task_id);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_serialization() {
        let table_task_key = FlowTaskNameKey::new("my_catalog".to_string(), "my_task".to_string());
        assert_eq!(
            b"__flow_task/my_catalog/name/my_task".to_vec(),
            table_task_key.to_bytes(),
        );
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow_task/my_catalog/name/my_task".to_vec();
        let key = FlowTaskNameKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.catalog(), "my_catalog");
        assert_eq!(key.task_name(), "my_task");
    }
}
