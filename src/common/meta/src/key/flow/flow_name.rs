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

use std::sync::Arc;

use futures::stream::BoxStream;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;

use crate::error::{self, Result};
use crate::key::flow::FlowScoped;
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    BytesAdapter, DeserializedValueWithBytes, FlowId, MetadataKey, MetadataValue, NAME_PATTERN,
};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;

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
pub struct FlowNameKey<'a>(FlowScoped<FlowNameKeyInner<'a>>);

#[allow(dead_code)]
impl<'a> FlowNameKey<'a> {
    /// Returns the [FlowNameKey]
    pub fn new(catalog: &'a str, flow_name: &'a str) -> FlowNameKey<'a> {
        let inner = FlowNameKeyInner::new(catalog, flow_name);
        FlowNameKey(FlowScoped::new(inner))
    }

    pub fn range_start_key(catalog: &str) -> Vec<u8> {
        let inner = BytesAdapter::from(Self::prefix(catalog).into_bytes());

        FlowScoped::new(inner).to_bytes()
    }

    /// Return `name/{catalog}/` as prefix
    pub fn prefix(catalog: &str) -> String {
        format!("{}/{}/", FLOW_NAME_KEY_PREFIX, catalog)
    }

    /// Returns the catalog.
    pub fn catalog(&self) -> &str {
        self.0.catalog_name
    }

    /// Return the `flow_name`
    pub fn flow_name(&self) -> &str {
        self.0.flow_name
    }
}

impl<'a> MetadataKey<'a, FlowNameKey<'a>> for FlowNameKey<'a> {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowNameKey<'a>> {
        Ok(FlowNameKey(FlowScoped::<FlowNameKeyInner>::from_bytes(
            bytes,
        )?))
    }
}

/// The key of mapping name to [FlowId]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlowNameKeyInner<'a> {
    pub catalog_name: &'a str,
    pub flow_name: &'a str,
}

impl<'a> MetadataKey<'a, FlowNameKeyInner<'a>> for FlowNameKeyInner<'_> {
    fn to_bytes(&self) -> Vec<u8> {
        format!(
            "{FLOW_NAME_KEY_PREFIX}/{}/{}",
            self.catalog_name, self.flow_name
        )
        .into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<FlowNameKeyInner> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            error::InvalidMetadataSnafu {
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
                .context(error::InvalidMetadataSnafu {
                    err_msg: format!("Invalid FlowNameKeyInner '{key}'"),
                })?;
        // Safety: pass the regex check above
        let catalog_name = captures.get(1).unwrap().as_str();
        let flow_name = captures.get(2).unwrap().as_str();
        Ok(FlowNameKeyInner {
            catalog_name,
            flow_name,
        })
    }
}

impl<'a> FlowNameKeyInner<'a> {
    /// Returns a [FlowNameKeyInner].
    pub fn new(catalog_name: &'a str, flow_name: &'a str) -> Self {
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

pub fn flow_name_decoder(kv: KeyValue) -> Result<(String, FlowNameValue)> {
    let flow_name = FlowNameKey::from_bytes(&kv.key)?;
    let flow_id = FlowNameValue::try_from_raw_value(&kv.value)?;
    Ok((flow_name.flow_name().to_string(), flow_id))
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
        let key = FlowNameKey::new(catalog, flow);
        let raw_key = key.to_bytes();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| FlowNameValue::try_from_raw_value(&x.value))
            .transpose()
    }

    /// Return all flows' names and ids
    pub async fn flow_names(
        &self,
        catalog: &str,
    ) -> BoxStream<'static, Result<(String, FlowNameValue)>> {
        let start_key = FlowNameKey::range_start_key(catalog);
        common_telemetry::debug!("flow_names: start_key: {:?}", start_key);
        let req = RangeRequest::new().with_prefix(start_key);

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(flow_name_decoder),
        )
        .into_stream();

        Box::pin(stream)
    }

    /// Returns true if the `flow` exists.
    pub async fn exists(&self, catalog: &str, flow: &str) -> Result<bool> {
        let key = FlowNameKey::new(catalog, flow);
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
        let key = FlowNameKey::new(catalog_name, flow_name);
        let raw_key = key.to_bytes();
        let flow_flow_name_value = FlowNameValue::new(flow_id);
        let txn = Txn::put_if_not_exists(raw_key.clone(), flow_flow_name_value.try_as_raw_value()?);

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
        let key = FlowNameKey::new("my_catalog", "my_task");
        assert_eq!(b"__flow/name/my_catalog/my_task".to_vec(), key.to_bytes(),);
    }

    #[test]
    fn test_key_deserialization() {
        let bytes = b"__flow/name/my_catalog/my_task".to_vec();
        let key = FlowNameKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.catalog(), "my_catalog");
        assert_eq!(key.flow_name(), "my_task");
    }
    #[test]
    fn test_key_start_range() {
        assert_eq!(
            b"__flow/name/greptime/".to_vec(),
            FlowNameKey::range_start_key("greptime")
        );
    }
}
