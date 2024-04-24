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
use crate::key::scope::{CatalogScoped, FlowTaskScoped, MetaKey};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{txn_helper, DeserializedValueWithBytes, FlowTaskId, PartitionId, TableMetaValue};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;
use crate::FlownodeId;

const FLOW_TASK_KEY_PREFIX: &str = "id";

lazy_static! {
    static ref FLOW_TASK_KEY_PATTERN: Regex =
        Regex::new(&format!("^{FLOW_TASK_KEY_PREFIX}/([0-9]*)$")).unwrap();
}

/// The key stores the metadata of the task.
///
/// The layout: `__flow_task/{catalog}/id/{flow_task_id}`.
pub struct FlowTaskKey(FlowTaskScoped<CatalogScoped<FlowTaskKeyInner>>);

impl MetaKey<FlowTaskKey> for FlowTaskKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlowTaskKey> {
        Ok(FlowTaskKey(FlowTaskScoped::<
            CatalogScoped<FlowTaskKeyInner>,
        >::from_bytes(bytes)?))
    }
}

impl FlowTaskKey {
    /// Returns the [FlowTaskKey].
    pub fn new(catalog: String, flow_task_id: FlowTaskId) -> FlowTaskKey {
        let inner = FlowTaskKeyInner::new(flow_task_id);
        FlowTaskKey(FlowTaskScoped::new(CatalogScoped::new(catalog, inner)))
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
struct FlowTaskKeyInner {
    flow_task_id: FlowTaskId,
}

impl FlowTaskKeyInner {
    /// Returns a [FlowTaskKey] with the specified `flow_task_id`.
    pub fn new(flow_task_id: FlowTaskId) -> FlowTaskKeyInner {
        FlowTaskKeyInner { flow_task_id }
    }
}

impl MetaKey<FlowTaskKeyInner> for FlowTaskKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!("{FLOW_TASK_KEY_PREFIX}/{}", self.flow_task_id).into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<FlowTaskKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidTableMetadataSnafu {
                err_msg: format!(
                    "FlowTaskKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            FLOW_TASK_KEY_PATTERN
                .captures(key)
                .context(error::InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid FlowTaskKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let flow_task_id = captures[1].parse::<FlowTaskId>().unwrap();
        Ok(FlowTaskKeyInner { flow_task_id })
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

    /// Returns the [FlowTaskValue] of specified `flow_task_id`.
    pub async fn get(
        &self,
        catalog: &str,
        flow_task_id: FlowTaskId,
    ) -> Result<Option<FlowTaskValue>> {
        let key = FlowTaskKey::new(catalog.to_string(), flow_task_id).to_bytes();
        self.kv_backend
            .get(&key)
            .await?
            .map(|x| FlowTaskValue::try_from_raw_value(&x.value))
            .transpose()
    }

    /// Builds a create flow task transaction.
    /// It is expected that the `__flow_task/{flow_task_id}` wasn't occupied.
    /// Otherwise, the transaction will retrieve existing value.
    pub(crate) fn build_create_txn(
        &self,
        catalog: &str,
        flow_task_id: FlowTaskId,
        flow_task_value: &FlowTaskValue,
    ) -> Result<(
        Txn,
        impl FnOnce(
            &mut TxnOpGetResponseSet,
        ) -> Result<Option<DeserializedValueWithBytes<FlowTaskValue>>>,
    )> {
        let key = FlowTaskKey::new(catalog.to_string(), flow_task_id).to_bytes();
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
        let flow_task = FlowTaskKey::new("my_catalog".to_string(), 2);
        assert_eq!(
            b"__flow_task/my_catalog/id/2".to_vec(),
            flow_task.to_bytes()
        );
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow_task/my_catalog/id/2".to_vec();
        let key = FlowTaskKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.catalog(), "my_catalog");
        assert_eq!(key.flow_task_id(), 2);
    }
}
