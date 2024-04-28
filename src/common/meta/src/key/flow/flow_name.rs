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
use crate::key::flow::FlowTaskScoped;
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
pub struct FlowNameKey(FlowTaskScoped<CatalogScoped<FlowNameKeyInner>>);

impl FlowNameKey {
    /// Returns the [FlowNameKey]
    pub fn new(catalog: String, task_name: String) -> FlowNameKey {
        let inner = FlowNameKeyInner::new(task_name);
        FlowNameKey(FlowTaskScoped::new(CatalogScoped::new(catalog, inner)))
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

impl MetaKey<FlowNameKey> for FlowNameKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlowNameKey> {
        Ok(FlowNameKey(FlowTaskScoped::<
            CatalogScoped<FlowNameKeyInner>,
        >::from_bytes(bytes)?))
    }
}

/// The key of mapping name to [FlowTaskId]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowNameKeyInner {
    pub task_name: String,
}

impl MetaKey<FlowNameKeyInner> for FlowNameKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!("{FLOW_TASK_NAME_KEY_PREFIX}/{}", self.task_name).into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlowNameKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidTableMetadataSnafu {
                err_msg: format!(
                    "FlowNameKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            FLOW_TASK_NAME_KEY_PATTERN
                .captures(key)
                .context(error::InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid FlowNameKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let task = captures[1].to_string();
        Ok(FlowNameKeyInner { task_name: task })
    }
}

impl FlowNameKeyInner {
    /// Returns a [FlowNameKeyInner].
    pub fn new(task: String) -> Self {
        Self { task_name: task }
    }
}

/// The value of [FlowNameKey].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlowNameValue {
    flow_task_id: FlowTaskId,
}

impl FlowNameValue {
    /// Returns a [FlowNameValue] with specified [FlowTaskId].
    pub fn new(flow_task_id: FlowTaskId) -> Self {
        Self { flow_task_id }
    }

    /// Returns the [FlowTaskId]
    pub fn flow_task_id(&self) -> FlowTaskId {
        self.flow_task_id
    }
}

/// The manager of [FlowNameKey].
pub struct FlowNameManager {
    kv_backend: KvBackendRef,
}

impl FlowNameManager {
    /// Returns a new [FlowNameManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Returns the [FlowNameValue] of specified `catalog.task`.
    pub async fn get(&self, catalog: &str, task: &str) -> Result<Option<FlowNameValue>> {
        let key = FlowNameKey::new(catalog.to_string(), task.to_string());
        let raw_key = key.to_bytes();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| FlowNameValue::try_from_raw_value(&x.value))
            .transpose()
    }

    /// Returns true if the `task` exists.
    pub async fn exists(&self, catalog: &str, task: &str) -> Result<bool> {
        let key = FlowNameKey::new(catalog.to_string(), task.to_string());
        let raw_key = key.to_bytes();
        self.kv_backend.exists(&raw_key).await
    }

    /// Builds a create flow task name transaction.
    /// It's expected that the `__flow_task/{catalog}/name/{task_name}` wasn't occupied.
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
        ) -> Result<Option<DeserializedValueWithBytes<FlowNameValue>>>,
    )> {
        let key = FlowNameKey::new(catalog.to_string(), name.to_string());
        let raw_key = key.to_bytes();
        let flow_task_name_value = FlowNameValue::new(flow_task_id);
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
        let table_task_key = FlowNameKey::new("my_catalog".to_string(), "my_task".to_string());
        assert_eq!(
            b"__flow_task/my_catalog/name/my_task".to_vec(),
            table_task_key.to_bytes(),
        );
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow_task/my_catalog/name/my_task".to_vec();
        let key = FlowNameKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.catalog(), "my_catalog");
        assert_eq!(key.task_name(), "my_task");
    }
}
