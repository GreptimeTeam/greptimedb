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
use std::sync::Arc;

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use table::metadata::TableId;
use table::table_name::TableName;

use crate::error::{self, Result};
use crate::key::flow::FlowScoped;
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{DeserializedValueWithBytes, FlowId, FlowPartitionId, MetadataKey, MetadataValue};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::FlownodeId;

const FLOW_INFO_KEY_PREFIX: &str = "info";

lazy_static! {
    static ref FLOW_INFO_KEY_PATTERN: Regex =
        Regex::new(&format!("^{FLOW_INFO_KEY_PREFIX}/([0-9]+)$")).unwrap();
}

/// The key stores the metadata of the flow.
///
/// The layout: `__flow/info/{flow_id}`.
pub struct FlowInfoKey(FlowScoped<FlowInfoKeyInner>);

pub type FlowInfoDecodeResult = Result<Option<DeserializedValueWithBytes<FlowInfoValue>>>;

impl<'a> MetadataKey<'a, FlowInfoKey> for FlowInfoKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowInfoKey> {
        Ok(FlowInfoKey(FlowScoped::<FlowInfoKeyInner>::from_bytes(
            bytes,
        )?))
    }
}

impl FlowInfoKey {
    /// Returns the [FlowInfoKey].
    pub fn new(flow_id: FlowId) -> FlowInfoKey {
        let inner = FlowInfoKeyInner::new(flow_id);
        FlowInfoKey(FlowScoped::new(inner))
    }

    /// Returns the [FlowId].
    pub fn flow_id(&self) -> FlowId {
        self.0.flow_id
    }
}

/// The key of flow metadata.
#[derive(Debug, Clone, Copy, PartialEq)]
struct FlowInfoKeyInner {
    flow_id: FlowId,
}

impl FlowInfoKeyInner {
    /// Returns a [FlowInfoKey] with the specified `flow_id`.
    pub fn new(flow_id: FlowId) -> FlowInfoKeyInner {
        FlowInfoKeyInner { flow_id }
    }
}

impl<'a> MetadataKey<'a, FlowInfoKeyInner> for FlowInfoKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!("{FLOW_INFO_KEY_PREFIX}/{}", self.flow_id).into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowInfoKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidMetadataSnafu {
                err_msg: format!(
                    "FlowInfoKeyInner '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures =
            FLOW_INFO_KEY_PATTERN
                .captures(key)
                .context(error::InvalidMetadataSnafu {
                    err_msg: format!("Invalid FlowInfoKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let flow_id = captures[1].parse::<FlowId>().unwrap();
        Ok(FlowInfoKeyInner { flow_id })
    }
}

// The metadata of the flow.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlowInfoValue {
    /// The source tables used by the flow.
    pub(crate) source_table_ids: Vec<TableId>,
    /// The sink table used by the flow.
    pub(crate) sink_table_name: TableName,
    /// Which flow nodes this flow is running on.
    pub(crate) flownode_ids: BTreeMap<FlowPartitionId, FlownodeId>,
    /// The catalog name.
    pub(crate) catalog_name: String,
    /// The query context used when create flow.
    /// Although flow doesn't belong to any schema, this query_context is needed to remember
    /// the query context when `create_flow` is executed
    /// for recovering flow using the same sql&query_context after db restart.
    /// if none, should use default query context
    #[serde(default)]
    pub(crate) query_context: Option<crate::rpc::ddl::QueryContext>,
    /// The flow name.
    pub(crate) flow_name: String,
    /// The raw sql.
    pub(crate) raw_sql: String,
    /// The expr of expire.
    /// Duration in seconds as `i64`.
    pub(crate) expire_after: Option<i64>,
    /// The comment.
    pub(crate) comment: String,
    /// The options.
    pub(crate) options: HashMap<String, String>,
    /// The created time
    #[serde(default)]
    pub(crate) created_time: DateTime<Utc>,
    /// The updated time.
    #[serde(default)]
    pub(crate) updated_time: DateTime<Utc>,
}

impl FlowInfoValue {
    /// Returns the `flownode_id`.
    pub fn flownode_ids(&self) -> &BTreeMap<FlowPartitionId, FlownodeId> {
        &self.flownode_ids
    }

    /// Returns the `source_table`.
    pub fn source_table_ids(&self) -> &[TableId] {
        &self.source_table_ids
    }

    pub fn catalog_name(&self) -> &String {
        &self.catalog_name
    }

    pub fn query_context(&self) -> &Option<crate::rpc::ddl::QueryContext> {
        &self.query_context
    }

    pub fn flow_name(&self) -> &String {
        &self.flow_name
    }

    pub fn sink_table_name(&self) -> &TableName {
        &self.sink_table_name
    }

    pub fn raw_sql(&self) -> &String {
        &self.raw_sql
    }

    pub fn expire_after(&self) -> Option<i64> {
        self.expire_after
    }

    pub fn comment(&self) -> &String {
        &self.comment
    }

    pub fn options(&self) -> &HashMap<String, String> {
        &self.options
    }

    pub fn created_time(&self) -> &DateTime<Utc> {
        &self.created_time
    }

    pub fn updated_time(&self) -> &DateTime<Utc> {
        &self.updated_time
    }
}

pub type FlowInfoManagerRef = Arc<FlowInfoManager>;

/// The manager of [FlowInfoKey].
pub struct FlowInfoManager {
    kv_backend: KvBackendRef,
}

impl FlowInfoManager {
    /// Returns a new [FlowInfoManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Returns the [FlowInfoValue] of specified `flow_id`.
    pub async fn get(&self, flow_id: FlowId) -> Result<Option<FlowInfoValue>> {
        let key = FlowInfoKey::new(flow_id).to_bytes();
        self.kv_backend
            .get(&key)
            .await?
            .map(|x| FlowInfoValue::try_from_raw_value(&x.value))
            .transpose()
    }

    /// Returns the [FlowInfoValue] with original bytes of specified `flow_id`.
    pub async fn get_raw(
        &self,
        flow_id: FlowId,
    ) -> Result<Option<DeserializedValueWithBytes<FlowInfoValue>>> {
        let key = FlowInfoKey::new(flow_id).to_bytes();
        self.kv_backend
            .get(&key)
            .await?
            .map(|x| DeserializedValueWithBytes::from_inner_slice(&x.value))
            .transpose()
    }

    /// Builds a create flow transaction.
    /// It is expected that the `__flow/info/{flow_id}` wasn't occupied.
    /// Otherwise, the transaction will retrieve existing value.
    pub(crate) fn build_create_txn(
        &self,
        flow_id: FlowId,
        flow_value: &FlowInfoValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> FlowInfoDecodeResult,
    )> {
        let key = FlowInfoKey::new(flow_id).to_bytes();
        let txn = Txn::put_if_not_exists(key.clone(), flow_value.try_as_raw_value()?);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(key)),
        ))
    }

    /// Builds a update flow transaction.
    /// It is expected that the `__flow/info/{flow_id}` IS ALREADY occupied and equal to `prev_flow_value`,
    /// but the new value can be the same, so to allow replace operation to happen even when the value is the same.
    /// Otherwise, the transaction will retrieve existing value and fail.
    pub(crate) fn build_update_txn(
        &self,
        flow_id: FlowId,
        current_flow_value: &DeserializedValueWithBytes<FlowInfoValue>,
        new_flow_value: &FlowInfoValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> FlowInfoDecodeResult,
    )> {
        let key = FlowInfoKey::new(flow_id).to_bytes();
        let raw_value = new_flow_value.try_as_raw_value()?;
        let prev_value = current_flow_value.get_raw_bytes();
        let txn = Txn::new()
            .when(vec![Compare::new(
                key.clone(),
                CompareOp::Equal,
                Some(prev_value),
            )])
            .and_then(vec![TxnOp::Put(key.clone(), raw_value)])
            .or_else(vec![TxnOp::Get(key.clone())]);

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
        let flow_info = FlowInfoKey::new(2);
        assert_eq!(b"__flow/info/2".to_vec(), flow_info.to_bytes());
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow/info/2".to_vec();
        let key = FlowInfoKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.flow_id(), 2);
    }
}
