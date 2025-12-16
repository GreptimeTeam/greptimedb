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

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;

use serde::{Deserialize, Serialize};
use snafu::{OptionExt as _, ResultExt, ensure};
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::error::{InvalidMetadataSnafu, Result, SerdeJsonSnafu};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    DeserializedValueWithBytes, MetadataKey, MetadataValue, TABLE_PART_KEY_PATTERN,
    TABLE_PART_PREFIX,
};
use crate::kv_backend::KvBackendRef;
use crate::kv_backend::txn::Txn;
use crate::rpc::store::BatchGetRequest;

/// The key stores table repartition relation information.
/// i.e. the src/dst region after repartition, which means dst region may still hold
/// some files from src region, this should be updated after repartition is done.
/// And gc scheduler will use this information to clean up those files(and this mapping if all files from src region are cleaned).
///
/// The layout: `__table_part/{table_id}`.
#[derive(Debug, PartialEq)]
pub struct TablePartKey {
    pub table_id: TableId,
}

impl TablePartKey {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }

    /// Returns the range prefix of the table partition key.
    pub fn range_prefix() -> Vec<u8> {
        format!("{}/", TABLE_PART_PREFIX).into_bytes()
    }
}

impl MetadataKey<'_, TablePartKey> for TablePartKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<TablePartKey> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "TablePartKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures = TABLE_PART_KEY_PATTERN
            .captures(key)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid TablePartKey '{key}'"),
            })?;
        // Safety: pass the regex check above
        let table_id = captures[1].parse::<TableId>().unwrap();
        Ok(TablePartKey { table_id })
    }
}

impl Display for TablePartKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", TABLE_PART_PREFIX, self.table_id)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TablePartValue {
    /// Mapping from src region to dst regions after repartition.
    pub src_to_dst: BTreeMap<RegionId, BTreeSet<RegionId>>,
}

impl TablePartValue {
    /// Update mapping from src region to dst regions. Should be called once repartition is done.
    pub fn update_mappings(&mut self, src: RegionId, dst: &[RegionId]) {
        self.src_to_dst.entry(src).or_default().extend(dst);
    }

    /// Remove mappings from src region to dst regions. Should be called once files from src region are cleaned up in dst regions.
    pub fn remove_mappings(&mut self, src: RegionId, dsts: &[RegionId]) {
        if let Some(dst_set) = self.src_to_dst.get_mut(&src) {
            for dst in dsts {
                dst_set.remove(dst);
            }
            if dst_set.is_empty() {
                self.src_to_dst.remove(&src);
            }
        }
    }
}

impl MetadataValue for TablePartValue {
    fn try_from_raw_value(raw_value: &[u8]) -> Result<Self> {
        serde_json::from_slice::<TablePartValue>(raw_value).context(SerdeJsonSnafu)
    }

    fn try_as_raw_value(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).context(SerdeJsonSnafu)
    }
}

pub type TablePartValueDecodeResult = Result<Option<DeserializedValueWithBytes<TablePartValue>>>;

pub struct TablePartManager {
    kv_backend: KvBackendRef,
}

impl TablePartManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Builds a create table part transaction,
    /// it expected the `__table_part/{table_id}` wasn't occupied.
    pub fn build_create_txn(
        &self,
        table_id: TableId,
        table_part_value: &TablePartValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> TablePartValueDecodeResult + use<>,
    )> {
        let key = TablePartKey::new(table_id);
        let raw_key = key.to_bytes();

        let txn = Txn::put_if_not_exists(raw_key.clone(), table_part_value.try_as_raw_value()?);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Builds a update table part transaction,
    /// it expected the remote value equals the `current_table_part_value`.
    /// It retrieves the latest value if the comparing failed.
    pub fn build_update_txn(
        &self,
        table_id: TableId,
        current_table_part_value: &DeserializedValueWithBytes<TablePartValue>,
        new_table_part_value: &TablePartValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> TablePartValueDecodeResult + use<>,
    )> {
        let key = TablePartKey::new(table_id);
        let raw_key = key.to_bytes();
        let raw_value = current_table_part_value.get_raw_bytes();
        let new_raw_value: Vec<u8> = new_table_part_value.try_as_raw_value()?;

        let txn = Txn::compare_and_put(raw_key.clone(), raw_value, new_raw_value);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Returns the [`TablePartValue`].
    pub async fn get(&self, table_id: TableId) -> Result<Option<TablePartValue>> {
        self.get_inner(table_id).await
    }

    async fn get_inner(&self, table_id: TableId) -> Result<Option<TablePartValue>> {
        let key = TablePartKey::new(table_id);
        self.kv_backend
            .get(&key.to_bytes())
            .await?
            .map(|kv| TablePartValue::try_from_raw_value(&kv.value))
            .transpose()
    }

    /// Returns the [`TablePartValue`] wrapped with [`DeserializedValueWithBytes`].
    pub async fn get_with_raw_bytes(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TablePartValue>>> {
        self.get_with_raw_bytes_inner(table_id).await
    }

    async fn get_with_raw_bytes_inner(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TablePartValue>>> {
        let key = TablePartKey::new(table_id);
        self.kv_backend
            .get(&key.to_bytes())
            .await?
            .map(|kv| DeserializedValueWithBytes::from_inner_slice(&kv.value))
            .transpose()
    }

    /// Returns batch of [`TablePartValue`] that respects the order of `table_ids`.
    pub async fn batch_get(&self, table_ids: &[TableId]) -> Result<Vec<Option<TablePartValue>>> {
        let raw_table_parts = self.batch_get_inner(table_ids).await?;

        Ok(raw_table_parts
            .into_iter()
            .map(|v| v.map(|x| x.inner))
            .collect())
    }

    /// Returns batch of [`TablePartValue`] wrapped with [`DeserializedValueWithBytes`].
    pub async fn batch_get_with_raw_bytes(
        &self,
        table_ids: &[TableId],
    ) -> Result<Vec<Option<DeserializedValueWithBytes<TablePartValue>>>> {
        self.batch_get_inner(table_ids).await
    }

    async fn batch_get_inner(
        &self,
        table_ids: &[TableId],
    ) -> Result<Vec<Option<DeserializedValueWithBytes<TablePartValue>>>> {
        let keys = table_ids
            .iter()
            .map(|id| TablePartKey::new(*id).to_bytes())
            .collect::<Vec<_>>();
        let resp = self
            .kv_backend
            .batch_get(BatchGetRequest { keys: keys.clone() })
            .await?;

        let kvs = resp
            .kvs
            .into_iter()
            .map(|kv| (keys.binary_search(&kv.key).unwrap(), kv))
            .collect::<BTreeMap<_, _>>();

        let mut results = Vec::with_capacity(table_ids.len());
        for (i, _key) in keys.iter().enumerate() {
            results.push(
                kvs.get(&i)
                    .map(|kv| DeserializedValueWithBytes::from_inner_slice(&kv.value))
                    .transpose()?,
            );
        }

        Ok(results)
    }

    /// Updates mappings from src region to dst regions.
    /// Should be called once repartition is done.
    pub async fn update_mappings(&self, src: RegionId, dst: &[RegionId]) -> Result<()> {
        let table_id = src.table_id();

        // Get current table part with raw bytes for CAS operation
        let current_table_part = self
            .get_with_raw_bytes(table_id)
            .await?
            .context(crate::error::TablePartitionNotFoundSnafu { table_id })?;

        // Clone the current part value and update mappings
        let mut new_table_part_value = current_table_part.inner.clone();
        new_table_part_value.update_mappings(src, dst);

        // Execute atomic update
        let (txn, _) =
            self.build_update_txn(table_id, &current_table_part, &new_table_part_value)?;

        let result = self.kv_backend.txn(txn).await?;

        ensure!(
            result.succeeded,
            crate::error::MetadataCorruptionSnafu {
                err_msg: format!(
                    "Failed to update mappings for table {}: CAS operation failed",
                    table_id
                ),
            }
        );

        Ok(())
    }

    /// Removes mappings from src region to dst regions.
    /// Should be called once files from src region are cleaned up in dst regions.
    pub async fn remove_mappings(&self, src: RegionId, dsts: &[RegionId]) -> Result<()> {
        let table_id = src.table_id();

        // Get current table part with raw bytes for CAS operation
        let current_table_part = self
            .get_with_raw_bytes(table_id)
            .await?
            .context(crate::error::TablePartitionNotFoundSnafu { table_id })?;

        // Clone the current part value and remove mappings
        let mut new_table_part_value = current_table_part.inner.clone();
        new_table_part_value.remove_mappings(src, dsts);

        // Execute atomic update
        let (txn, _) =
            self.build_update_txn(table_id, &current_table_part, &new_table_part_value)?;

        let result = self.kv_backend.txn(txn).await?;

        ensure!(
            result.succeeded,
            crate::error::MetadataCorruptionSnafu {
                err_msg: format!(
                    "Failed to remove mappings for table {}: CAS operation failed",
                    table_id
                ),
            }
        );

        Ok(())
    }

    /// Returns the destination regions for a given source region.
    pub async fn get_dst_regions(
        &self,
        src_region: RegionId,
    ) -> Result<Option<BTreeSet<RegionId>>> {
        let table_id = src_region.table_id();
        let table_part = self.get(table_id).await?;
        Ok(table_part.and_then(|part| part.src_to_dst.get(&src_region).cloned()))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use super::*;
    use crate::kv_backend::TxnService;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[test]
    fn test_table_part_key_serialization() {
        let key = TablePartKey::new(42);
        let raw_key = key.to_bytes();
        assert_eq!(raw_key, b"__table_part/42");
    }

    #[test]
    fn test_table_part_key_deserialization() {
        let expected = TablePartKey::new(42);
        let key = TablePartKey::from_bytes(b"__table_part/42").unwrap();
        assert_eq!(key, expected);
    }

    #[test]
    fn test_table_part_key_deserialization_invalid_utf8() {
        let result = TablePartKey::from_bytes(b"__table_part/\xff");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not a valid UTF8 string")
        );
    }

    #[test]
    fn test_table_part_key_deserialization_invalid_format() {
        let result = TablePartKey::from_bytes(b"invalid_key_format");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid TablePartKey")
        );
    }

    #[test]
    fn test_table_part_value_serialization_deserialization() {
        let mut src_to_dst = BTreeMap::new();
        let src_region = RegionId::new(1, 1);
        let dst_regions = vec![RegionId::new(1, 2), RegionId::new(1, 3)];
        src_to_dst.insert(src_region, dst_regions.into_iter().collect());

        let value = TablePartValue { src_to_dst };
        let serialized = value.try_as_raw_value().unwrap();
        let deserialized = TablePartValue::try_from_raw_value(&serialized).unwrap();

        assert_eq!(value, deserialized);
    }

    #[test]
    fn test_table_part_value_update_mappings_new_src() {
        let mut value = TablePartValue {
            src_to_dst: BTreeMap::new(),
        };

        let src = RegionId::new(1, 1);
        let dst = vec![RegionId::new(1, 2), RegionId::new(1, 3)];

        value.update_mappings(src, &dst);

        assert_eq!(value.src_to_dst.len(), 1);
        assert!(value.src_to_dst.contains_key(&src));
        assert_eq!(value.src_to_dst.get(&src).unwrap().len(), 2);
        assert!(
            value
                .src_to_dst
                .get(&src)
                .unwrap()
                .contains(&RegionId::new(1, 2))
        );
        assert!(
            value
                .src_to_dst
                .get(&src)
                .unwrap()
                .contains(&RegionId::new(1, 3))
        );
    }

    #[test]
    fn test_table_part_value_update_mappings_existing_src() {
        let mut value = TablePartValue {
            src_to_dst: BTreeMap::new(),
        };

        let src = RegionId::new(1, 1);
        let initial_dst = vec![RegionId::new(1, 2)];
        let additional_dst = vec![RegionId::new(1, 3), RegionId::new(1, 4)];

        // Initial mapping
        value.update_mappings(src, &initial_dst);
        // Update with additional destinations
        value.update_mappings(src, &additional_dst);

        assert_eq!(value.src_to_dst.len(), 1);
        assert_eq!(value.src_to_dst.get(&src).unwrap().len(), 3);
        assert!(
            value
                .src_to_dst
                .get(&src)
                .unwrap()
                .contains(&RegionId::new(1, 2))
        );
        assert!(
            value
                .src_to_dst
                .get(&src)
                .unwrap()
                .contains(&RegionId::new(1, 3))
        );
        assert!(
            value
                .src_to_dst
                .get(&src)
                .unwrap()
                .contains(&RegionId::new(1, 4))
        );
    }

    #[test]
    fn test_table_part_value_remove_mappings_existing() {
        let mut value = TablePartValue {
            src_to_dst: BTreeMap::new(),
        };

        let src = RegionId::new(1, 1);
        let dst_regions = vec![
            RegionId::new(1, 2),
            RegionId::new(1, 3),
            RegionId::new(1, 4),
        ];
        value.update_mappings(src, &dst_regions);

        // Remove some mappings
        let to_remove = vec![RegionId::new(1, 2), RegionId::new(1, 3)];
        value.remove_mappings(src, &to_remove);

        assert_eq!(value.src_to_dst.len(), 1);
        assert_eq!(value.src_to_dst.get(&src).unwrap().len(), 1);
        assert!(
            value
                .src_to_dst
                .get(&src)
                .unwrap()
                .contains(&RegionId::new(1, 4))
        );
    }

    #[test]
    fn test_table_part_value_remove_mappings_all() {
        let mut value = TablePartValue {
            src_to_dst: BTreeMap::new(),
        };

        let src = RegionId::new(1, 1);
        let dst_regions = vec![RegionId::new(1, 2), RegionId::new(1, 3)];
        value.update_mappings(src, &dst_regions);

        // Remove all mappings
        value.remove_mappings(src, &dst_regions);

        assert_eq!(value.src_to_dst.len(), 0);
    }

    #[test]
    fn test_table_part_value_remove_mappings_nonexistent() {
        let mut value = TablePartValue {
            src_to_dst: BTreeMap::new(),
        };

        let src = RegionId::new(1, 1);
        let dst_regions = vec![RegionId::new(1, 2)];
        value.update_mappings(src, &dst_regions);

        // Try to remove non-existent mappings
        let nonexistent_dst = vec![RegionId::new(1, 3), RegionId::new(1, 4)];
        value.remove_mappings(src, &nonexistent_dst);

        // Should remain unchanged
        assert_eq!(value.src_to_dst.len(), 1);
        assert_eq!(value.src_to_dst.get(&src).unwrap().len(), 1);
        assert!(
            value
                .src_to_dst
                .get(&src)
                .unwrap()
                .contains(&RegionId::new(1, 2))
        );
    }

    #[test]
    fn test_table_part_value_remove_mappings_nonexistent_src() {
        let mut value = TablePartValue {
            src_to_dst: BTreeMap::new(),
        };

        let src = RegionId::new(1, 1);
        let dst_regions = vec![RegionId::new(1, 2)];

        // Try to remove mappings for non-existent source
        value.remove_mappings(src, &dst_regions);

        // Should remain empty
        assert_eq!(value.src_to_dst.len(), 0);
    }

    #[tokio::test]
    async fn test_table_part_manager_get_empty() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TablePartManager::new(kv);
        let result = manager.get(1024).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_table_part_manager_get_with_raw_bytes_empty() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TablePartManager::new(kv);
        let result = manager.get_with_raw_bytes(1024).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_table_part_manager_create_and_get() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TablePartManager::new(kv.clone());

        let mut src_to_dst = BTreeMap::new();
        let src_region = RegionId::new(1, 1);
        let dst_regions = vec![RegionId::new(1, 2), RegionId::new(1, 3)];
        src_to_dst.insert(src_region, dst_regions.into_iter().collect());

        let value = TablePartValue { src_to_dst };

        // Create the table part
        let (txn, _) = manager.build_create_txn(1024, &value).unwrap();
        let result = kv.txn(txn).await.unwrap();
        assert!(result.succeeded);

        // Get the table part
        let retrieved = manager.get(1024).await.unwrap().unwrap();
        assert_eq!(retrieved, value);
    }

    #[tokio::test]
    async fn test_table_part_manager_update_txn() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TablePartManager::new(kv.clone());

        let initial_value = TablePartValue {
            src_to_dst: BTreeMap::new(),
        };

        // Create initial table part
        let (create_txn, _) = manager.build_create_txn(1024, &initial_value).unwrap();
        let result = kv.txn(create_txn).await.unwrap();
        assert!(result.succeeded);

        // Get current value with raw bytes
        let current_value = manager.get_with_raw_bytes(1024).await.unwrap().unwrap();

        // Create updated value
        let mut updated_src_to_dst = BTreeMap::new();
        let src_region = RegionId::new(1, 1);
        let dst_regions = vec![RegionId::new(1, 2)];
        updated_src_to_dst.insert(src_region, dst_regions.into_iter().collect());
        let updated_value = TablePartValue {
            src_to_dst: updated_src_to_dst,
        };

        // Build update transaction
        let (update_txn, _) = manager
            .build_update_txn(1024, &current_value, &updated_value)
            .unwrap();
        let result = kv.txn(update_txn).await.unwrap();
        assert!(result.succeeded);

        // Verify update
        let retrieved = manager.get(1024).await.unwrap().unwrap();
        assert_eq!(retrieved, updated_value);
    }

    #[tokio::test]
    async fn test_table_part_manager_batch_get() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TablePartManager::new(kv.clone());

        // Create multiple table parts
        let table_parts = vec![
            (
                1024,
                TablePartValue {
                    src_to_dst: {
                        let mut map = BTreeMap::new();
                        map.insert(
                            RegionId::new(1, 1),
                            vec![RegionId::new(1, 2)].into_iter().collect(),
                        );
                        map
                    },
                },
            ),
            (
                1025,
                TablePartValue {
                    src_to_dst: {
                        let mut map = BTreeMap::new();
                        map.insert(
                            RegionId::new(2, 1),
                            vec![RegionId::new(2, 2), RegionId::new(2, 3)]
                                .into_iter()
                                .collect(),
                        );
                        map
                    },
                },
            ),
        ];

        for (table_id, value) in &table_parts {
            let (txn, _) = manager.build_create_txn(*table_id, value).unwrap();
            let result = kv.txn(txn).await.unwrap();
            assert!(result.succeeded);
        }

        // Batch get
        let results = manager.batch_get(&[1024, 1025, 1026]).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap(), &table_parts[0].1);
        assert_eq!(results[1].as_ref().unwrap(), &table_parts[1].1);
        assert!(results[2].is_none());
    }

    #[tokio::test]
    async fn test_table_part_manager_update_mappings() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TablePartManager::new(kv.clone());

        // Create initial table part
        let initial_value = TablePartValue {
            src_to_dst: BTreeMap::new(),
        };
        let (txn, _) = manager.build_create_txn(1024, &initial_value).unwrap();
        let result = kv.txn(txn).await.unwrap();
        assert!(result.succeeded);

        // Update mappings
        let src = RegionId::new(1024, 1);
        let dst = vec![RegionId::new(1024, 2), RegionId::new(1024, 3)];
        manager.update_mappings(src, &dst).await.unwrap();

        // Verify update
        let retrieved = manager.get(1024).await.unwrap().unwrap();
        assert_eq!(retrieved.src_to_dst.len(), 1);
        assert!(retrieved.src_to_dst.contains_key(&src));
        assert_eq!(retrieved.src_to_dst.get(&src).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_table_part_manager_remove_mappings() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TablePartManager::new(kv.clone());

        // Create initial table part with mappings
        let mut initial_src_to_dst = BTreeMap::new();
        let src = RegionId::new(1024, 1);
        let dst_regions = vec![
            RegionId::new(1024, 2),
            RegionId::new(1024, 3),
            RegionId::new(1024, 4),
        ];
        initial_src_to_dst.insert(src, dst_regions.into_iter().collect());

        let initial_value = TablePartValue {
            src_to_dst: initial_src_to_dst,
        };
        let (txn, _) = manager.build_create_txn(1024, &initial_value).unwrap();
        let result = kv.txn(txn).await.unwrap();
        assert!(result.succeeded);

        // Remove some mappings
        let to_remove = vec![RegionId::new(1024, 2), RegionId::new(1024, 3)];
        manager.remove_mappings(src, &to_remove).await.unwrap();

        // Verify removal
        let retrieved = manager.get(1024).await.unwrap().unwrap();
        assert_eq!(retrieved.src_to_dst.len(), 1);
        assert_eq!(retrieved.src_to_dst.get(&src).unwrap().len(), 1);
        assert!(
            retrieved
                .src_to_dst
                .get(&src)
                .unwrap()
                .contains(&RegionId::new(1024, 4))
        );
    }

    #[tokio::test]
    async fn test_table_part_manager_get_dst_regions() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TablePartManager::new(kv.clone());

        // Create initial table part with mappings
        let mut initial_src_to_dst = BTreeMap::new();
        let src = RegionId::new(1024, 1);
        let dst_regions = vec![RegionId::new(1024, 2), RegionId::new(1024, 3)];
        initial_src_to_dst.insert(src, dst_regions.into_iter().collect());

        let initial_value = TablePartValue {
            src_to_dst: initial_src_to_dst,
        };
        let (txn, _) = manager.build_create_txn(1024, &initial_value).unwrap();
        let result = kv.txn(txn).await.unwrap();
        assert!(result.succeeded);

        // Get destination regions
        let dst_regions = manager.get_dst_regions(src).await.unwrap();
        assert!(dst_regions.is_some());
        let dst_set = dst_regions.unwrap();
        assert_eq!(dst_set.len(), 2);
        assert!(dst_set.contains(&RegionId::new(1024, 2)));
        assert!(dst_set.contains(&RegionId::new(1024, 3)));

        // Test non-existent source region
        let nonexistent_src = RegionId::new(1024, 99);
        let result = manager.get_dst_regions(nonexistent_src).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_table_part_manager_operations_on_nonexistent_table() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TablePartManager::new(kv);

        let src = RegionId::new(1024, 1);
        let dst = vec![RegionId::new(1024, 2)];

        // Try to update mappings on non-existent table
        let result = manager.update_mappings(src, &dst).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Failed to find table partition for table id 1024"),
            "{err_msg}"
        );

        // Try to remove mappings on non-existent table
        let result = manager.remove_mappings(src, &dst).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Failed to find table partition for table id 1024"),
            "{err_msg}"
        );
    }

    #[tokio::test]
    async fn test_table_part_manager_batch_get_with_raw_bytes() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TablePartManager::new(kv.clone());

        // Create table part
        let value = TablePartValue {
            src_to_dst: {
                let mut map = BTreeMap::new();
                map.insert(
                    RegionId::new(1, 1),
                    vec![RegionId::new(1, 2)].into_iter().collect(),
                );
                map
            },
        };
        let (txn, _) = manager.build_create_txn(1024, &value).unwrap();
        let result = kv.txn(txn).await.unwrap();
        assert!(result.succeeded);

        // Batch get with raw bytes
        let results = manager
            .batch_get_with_raw_bytes(&[1024, 1025])
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert!(results[0].is_some());
        assert!(results[1].is_none());

        let retrieved = &results[0].as_ref().unwrap().inner;
        assert_eq!(retrieved, &value);
    }
}
