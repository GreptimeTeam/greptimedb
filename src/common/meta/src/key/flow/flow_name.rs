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

use api::v1::flow::flow_server::Flow;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::{self, Result};
use crate::key::flow::FlowScoped;
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    txn_helper, DeserializedValueWithBytes, FlowId, MetaKey, TableMetaValue, NAME_PATTERN,
};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;

const FLOW_NAME_KEY_PREFIX: &str = "name";

lazy_static! {
    static ref FLOW_NAME_KEY_PATTERN: Regex = Regex::new(&format!(
        "^{FLOW_NAME_KEY_PREFIX}/({NAME_PATTERN})/({NAME_PATTERN})$"
    ))
    .unwrap();
}

/// The key of mapping {flow_name} to [FlowId].
///
/// The layout: `__flow/name/{catalog_name}/{flow_name}`.
pub struct FlowNameKey(FlowScoped<FlowNameKeyInner>);

impl FlowNameKey {
    /// Returns the [FlowNameKey]
    pub fn new(catalog: String, flow_name: String) -> FlowNameKey {
        let inner = FlowNameKeyInner::new(catalog, flow_name);
        FlowNameKey(FlowScoped::new(inner))
    }

    /// Returns the catalog.
    pub fn catalog(&self) -> &str {
        &self.0.catalog_name
    }

    /// Return the `flow_name`
    pub fn flow_name(&self) -> &str {
        &self.0.flow_name
    }
}

impl<'a> MetaKey<'a, FlowNameKey> for FlowNameKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowNameKey> {
        Ok(FlowNameKey(FlowScoped::<FlowNameKeyInner>::from_bytes(
            bytes,
        )?))
    }
}

/// The key of mapping name to [FlowId]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowNameKeyInner {
    pub catalog_name: String,
    pub flow_name: String,
}

impl<'a> MetaKey<'a, FlowNameKeyInner> for FlowNameKeyInner {
    fn to_bytes(&self) -> Vec<u8> {
        format!(
            "{FLOW_NAME_KEY_PREFIX}/{}/{}",
            self.catalog_name, self.flow_name
        )
        .into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowNameKeyInner> {
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
            FLOW_NAME_KEY_PATTERN
                .captures(key)
                .context(error::InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid FlowNameKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let catalog_name = captures[1].to_string();
        let flow_name = captures[2].to_string();
        Ok(FlowNameKeyInner {
            catalog_name,
            flow_name,
        })
    }
}

impl FlowNameKeyInner {
    /// Returns a [FlowNameKeyInner].
    pub fn new(catalog_name: String, flow_name: String) -> Self {
        Self {
            catalog_name,
            flow_name,
        }
    }
}

/// The value of [FlowNameKey].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlowNameValue {
    flow_id: FlowId,
}

impl FlowNameValue {
    /// Returns a [FlowNameValue] with specified [FlowId].
    pub fn new(flow_id: FlowId) -> Self {
        Self { flow_id }
    }

    /// Returns the [FlowId]
    pub fn flow_id(&self) -> FlowId {
        self.flow_id
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

    /// Returns the [FlowNameValue] of specified `catalog.flow`.
    pub async fn get(&self, catalog: &str, flow: &str) -> Result<Option<FlowNameValue>> {
        let key = FlowNameKey::new(catalog.to_string(), flow.to_string());
        let raw_key = key.to_bytes();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| FlowNameValue::try_from_raw_value(&x.value))
            .transpose()
    }

    /// Returns true if the `flow` exists.
    pub async fn exists(&self, catalog: &str, flow: &str) -> Result<bool> {
        let key = FlowNameKey::new(catalog.to_string(), flow.to_string());
        let raw_key = key.to_bytes();
        self.kv_backend.exists(&raw_key).await
    }

    /// Builds a create flow name transaction.
    /// It's expected that the `__flow/name/{catalog}/{flow_name}` wasn't occupied.
    /// Otherwise, the transaction will retrieve existing value.
    pub fn build_create_txn(
        &self,
        catalog_name: &str,
        flow_name: &str,
        flow_id: FlowId,
    ) -> Result<(
        Txn,
        impl FnOnce(
            &mut TxnOpGetResponseSet,
        ) -> Result<Option<DeserializedValueWithBytes<FlowNameValue>>>,
    )> {
        let key = FlowNameKey::new(catalog_name.to_string(), flow_name.to_string());
        let raw_key = key.to_bytes();
        let flow_flow_name_value = FlowNameValue::new(flow_id);
        let txn = txn_helper::build_put_if_absent_txn(
            raw_key.clone(),
            flow_flow_name_value.try_as_raw_value()?,
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
        let key = FlowNameKey::new("my_catalog".to_string(), "my_task".to_string());
        assert_eq!(b"__flow/name/my_catalog/my_task".to_vec(), key.to_bytes(),);
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow/name/my_catalog/my_task".to_vec();
        let key = FlowNameKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.catalog(), "my_catalog");
        assert_eq!(key.flow_name(), "my_task");
    }
}
