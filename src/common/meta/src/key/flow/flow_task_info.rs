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

use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use table::metadata::TableId;

use crate::error::{self, Result};
use crate::key::flow::FlowTaskScoped;
use crate::key::scope::{CatalogScoped, MetaKey};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    txn_helper, DeserializedValueWithBytes, FlowTaskId, FlowTaskPartitionId, TableMetaValue,
};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;
use crate::table_name::TableName;
use crate::FlownodeId;

const FLOW_TASK_INFO_KEY_PREFIX: &str = "info";

lazy_static! {
    static ref FLOW_TASK_INFO_KEY_PATTERN: Regex =
        Regex::new(&format!("^{FLOW_TASK_INFO_KEY_PREFIX}/([0-9]+)$")).unwrap();
}

/// The key stores the metadata of the task.
///
/// The layout: `__flow_task/{catalog}/info/{flow_task_id}`.
pub struct FlowTaskInfoKey(FlowTaskScoped<CatalogScoped<FlowTaskInfoKeyInner>>);

impl MetaKey<FlowTaskInfoKey> for FlowTaskInfoKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlowTaskInfoKey> {
        Ok(FlowTaskInfoKey(FlowTaskScoped::<
            CatalogScoped<FlowTaskInfoKeyInner>,
        >::from_bytes(bytes)?))
    }
}

impl FlowTaskInfoKey {
    /// Returns the [FlowTaskInfoKey].
    pub fn new(catalog: String, flow_task_id: FlowTaskId) -> FlowTaskInfoKey {
        let inner = FlowTaskInfoKeyInner::new(flow_task_id);
        FlowTaskInfoKey(FlowTaskScoped::new(CatalogScoped::new(catalog, inner)))
    }

    /// Returns the catalog.
    pub fn catalog(&self) -> &str {
        self.0.catalog()
    }

    /// Returns the [FlowTaskId].
    pub fn flow_task_id(&self) -> FlowTaskId {
        self.0.flow_task_id
    }
}

/// The key of flow task metadata.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FlowTaskInfoKeyInner {
    flow_task_id: FlowTaskId,
}

impl FlowTaskInfoKeyInner {
    /// Returns a [FlowTaskInfoKey] with the specified `flow_task_id`.
    pub fn new(flow_task_id: FlowTaskId) -> FlowTaskInfoKeyInner {
        FlowTaskInfoKeyInner { flow_task_id }
    }
}

impl MetaKey<FlowTaskInfoKeyInner> for FlowTaskInfoKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!("{FLOW_TASK_INFO_KEY_PREFIX}/{}", self.flow_task_id).into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlowTaskInfoKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidTableMetadataSnafu {
                err_msg: format!(
                    "FlowTaskInfoKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            FLOW_TASK_INFO_KEY_PATTERN
                .captures(key)
                .context(error::InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid FlowTaskInfoKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let flow_task_id = captures[1].parse::<FlowTaskId>().unwrap();
        Ok(FlowTaskInfoKeyInner { flow_task_id })
    }
}

// The metadata of the flow task.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlowTaskInfoValue {
    /// The source tables used by the task.
    pub(crate) source_table_ids: Vec<TableId>,
    /// The sink table used by the task.
    pub(crate) sink_table_name: TableName,
    /// Which flow nodes this task is running on.
    pub(crate) flownode_ids: BTreeMap<FlowTaskPartitionId, FlownodeId>,
    /// The catalog name.
    pub(crate) catalog_name: String,
    /// The task name.
    pub(crate) task_name: String,
    /// The raw sql.
    pub(crate) raw_sql: String,
    /// The expr of expire.
    pub(crate) expire_when: String,
    /// The comment.
    pub(crate) comment: String,
    /// The options.
    pub(crate) options: HashMap<String, String>,
}

impl FlowTaskInfoValue {
    /// Returns the `flownode_id`.
    pub fn flownode_ids(&self) -> &BTreeMap<FlowTaskPartitionId, FlownodeId> {
        &self.flownode_ids
    }

    /// Returns the `source_table`.
    pub fn source_table_ids(&self) -> &[TableId] {
        &self.source_table_ids
    }
}

/// The manager of [FlowTaskInfoKey].
pub struct FlowTaskInfoManager {
    kv_backend: KvBackendRef,
}

impl FlowTaskInfoManager {
    /// Returns a new [FlowTaskInfoManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Returns the [FlowTaskInfoValue] of specified `flow_task_id`.
    pub async fn get(
        &self,
        catalog: &str,
        flow_task_id: FlowTaskId,
    ) -> Result<Option<FlowTaskInfoValue>> {
        let key = FlowTaskInfoKey::new(catalog.to_string(), flow_task_id).to_bytes();
        self.kv_backend
            .get(&key)
            .await?
            .map(|x| FlowTaskInfoValue::try_from_raw_value(&x.value))
            .transpose()
    }

    /// Builds a create flow task transaction.
    /// It is expected that the `__flow_task/{catalog}/info/{flow_task_id}` wasn't occupied.
    /// Otherwise, the transaction will retrieve existing value.
    pub(crate) fn build_create_txn(
        &self,
        catalog: &str,
        flow_task_id: FlowTaskId,
        flow_task_value: &FlowTaskInfoValue,
    ) -> Result<(
        Txn,
        impl FnOnce(
            &mut TxnOpGetResponseSet,
        ) -> Result<Option<DeserializedValueWithBytes<FlowTaskInfoValue>>>,
    )> {
        let key = FlowTaskInfoKey::new(catalog.to_string(), flow_task_id).to_bytes();
        let txn =
            txn_helper::build_put_if_absent_txn(key.clone(), flow_task_value.try_as_raw_value()?);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(key)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_serialization() {
        let flow_task = FlowTaskInfoKey::new("my_catalog".to_string(), 2);
        assert_eq!(
            b"__flow_task/my_catalog/info/2".to_vec(),
            flow_task.to_bytes()
        );
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow_task/my_catalog/info/2".to_vec();
        let key = FlowTaskInfoKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.catalog(), "my_catalog");
        assert_eq!(key.flow_task_id(), 2);
    }
}
