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

use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use table::metadata::TableId;
use table::table_name::TableName;

use super::VIEW_INFO_KEY_PATTERN;
use crate::error::{InvalidViewInfoSnafu, Result};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{DeserializedValueWithBytes, MetaKey, TableMetaValue, VIEW_INFO_KEY_PREFIX};
use crate::kv_backend::txn::Txn;
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::BatchGetRequest;

/// The VIEW logical plan encoded bytes
type RawViewLogicalPlan = Vec<u8>;

/// The key stores the metadata of the view.
///
/// The layout: `__view_info/{view_id}`.
#[derive(Debug, PartialEq)]
pub struct ViewInfoKey {
    view_id: TableId,
}

impl ViewInfoKey {
    /// Returns a new `[ViewInfoKey]`.
    pub fn new(view_id: TableId) -> Self {
        Self { view_id }
    }
}

impl Display for ViewInfoKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", VIEW_INFO_KEY_PREFIX, self.view_id)
    }
}

impl<'a> MetaKey<'a, ViewInfoKey> for ViewInfoKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<ViewInfoKey> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidViewInfoSnafu {
                err_msg: format!(
                    "ViewInfoKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures = VIEW_INFO_KEY_PATTERN
            .captures(key)
            .context(InvalidViewInfoSnafu {
                err_msg: format!("Invalid ViewInfoKey '{key}'"),
            })?;
        // Safety: pass the regex check above
        let view_id = captures[1].parse::<TableId>().unwrap();
        Ok(ViewInfoKey { view_id })
    }
}

/// The VIEW info value that keeps the metadata.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ViewInfoValue {
    /// The encoded logical plan
    pub view_info: RawViewLogicalPlan,
    /// The resolved fully table names in logical plan
    pub table_names: HashSet<TableName>,
    version: u64,
}

impl ViewInfoValue {
    pub fn new(view_info: &RawViewLogicalPlan, table_names: HashSet<TableName>) -> Self {
        Self {
            view_info: view_info.clone(),
            table_names,
            version: 0,
        }
    }

    pub(crate) fn update(
        &self,
        new_view_info: RawViewLogicalPlan,
        table_names: HashSet<TableName>,
    ) -> Self {
        Self {
            view_info: new_view_info,
            table_names,
            version: self.version + 1,
        }
    }
}

/// The `[ViewInfo]` manager
pub struct ViewInfoManager {
    kv_backend: KvBackendRef,
}

pub type ViewInfoManagerRef = Arc<ViewInfoManager>;

impl ViewInfoManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Builds a create view info transaction, it expected the `__view_info/{view_id}` wasn't occupied.
    pub(crate) fn build_create_txn(
        &self,
        view_id: TableId,
        view_info_value: &ViewInfoValue,
    ) -> Result<(
        Txn,
        impl FnOnce(
            &mut TxnOpGetResponseSet,
        ) -> Result<Option<DeserializedValueWithBytes<ViewInfoValue>>>,
    )> {
        let key = ViewInfoKey::new(view_id);
        let raw_key = key.to_bytes();

        let txn = Txn::put_if_not_exists(raw_key.clone(), view_info_value.try_as_raw_value()?);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Builds a update view info transaction, it expected the remote value equals the `current_current_view_info_value`.
    /// It retrieves the latest value if the comparing failed.
    pub(crate) fn build_update_txn(
        &self,
        view_id: TableId,
        current_view_info_value: &DeserializedValueWithBytes<ViewInfoValue>,
        new_view_info_value: &ViewInfoValue,
    ) -> Result<(
        Txn,
        impl FnOnce(
            &mut TxnOpGetResponseSet,
        ) -> Result<Option<DeserializedValueWithBytes<ViewInfoValue>>>,
    )> {
        let key = ViewInfoKey::new(view_id);
        let raw_key = key.to_bytes();
        let raw_value = current_view_info_value.get_raw_bytes();
        let new_raw_value: Vec<u8> = new_view_info_value.try_as_raw_value()?;

        let txn = Txn::compare_and_put(raw_key.clone(), raw_value, new_raw_value);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Get the `[ViewInfoValue]` by the view id
    pub async fn get(
        &self,
        view_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<ViewInfoValue>>> {
        let key = ViewInfoKey::new(view_id);
        let raw_key = key.to_bytes();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| DeserializedValueWithBytes::from_inner_slice(&x.value))
            .transpose()
    }

    /// Get the `[ViewInfoValue]` by the view id slice in batch
    pub async fn batch_get(&self, view_ids: &[TableId]) -> Result<HashMap<TableId, ViewInfoValue>> {
        let lookup_table = view_ids
            .iter()
            .map(|id| (ViewInfoKey::new(*id).to_bytes(), id))
            .collect::<HashMap<_, _>>();

        let resp = self
            .kv_backend
            .batch_get(BatchGetRequest {
                keys: lookup_table.keys().cloned().collect::<Vec<_>>(),
            })
            .await?;

        let values = resp
            .kvs
            .iter()
            .map(|kv| {
                Ok((
                    // Safety: must exist.
                    **lookup_table.get(kv.key()).unwrap(),
                    ViewInfoValue::try_from_raw_value(&kv.value)?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(values)
    }

    /// Returns batch of `DeserializedValueWithBytes<ViewInfoValue>`.
    pub async fn batch_get_raw(
        &self,
        view_ids: &[TableId],
    ) -> Result<HashMap<TableId, DeserializedValueWithBytes<ViewInfoValue>>> {
        let lookup_table = view_ids
            .iter()
            .map(|id| (ViewInfoKey::new(*id).to_bytes(), id))
            .collect::<HashMap<_, _>>();

        let resp = self
            .kv_backend
            .batch_get(BatchGetRequest {
                keys: lookup_table.keys().cloned().collect::<Vec<_>>(),
            })
            .await?;

        let values = resp
            .kvs
            .iter()
            .map(|kv| {
                Ok((
                    // Safety: must exist.
                    **lookup_table.get(kv.key()).unwrap(),
                    DeserializedValueWithBytes::from_inner_slice(&kv.value)?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_serialization() {
        let key = ViewInfoKey::new(42);
        let raw_key = key.to_bytes();
        assert_eq!(raw_key, b"__view_info/42");
    }

    #[test]
    fn test_key_deserialization() {
        let expected = ViewInfoKey::new(42);
        let key = ViewInfoKey::from_bytes(b"__view_info/42").unwrap();
        assert_eq!(key, expected);
    }

    #[test]
    fn test_value_serialization() {
        let value = ViewInfoValue {
            view_info: vec![1, 2, 3],
            version: 1,
        };
        let serialized = value.try_as_raw_value().unwrap();
        let deserialized = ViewInfoValue::try_from_raw_value(&serialized).unwrap();
        assert_eq!(value, deserialized);
    }
}
