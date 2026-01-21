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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Display;

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt as _, ResultExt, ensure};
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::error::{InvalidMetadataSnafu, Result, SerdeJsonSnafu};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    DeserializedValueWithBytes, MetadataKey, MetadataValue, TABLE_REPART_KEY_PATTERN,
    TABLE_REPART_PREFIX,
};
use crate::kv_backend::KvBackendRef;
use crate::kv_backend::txn::Txn;
use crate::range_stream::{DEFAULT_PAGE_SIZE, PaginationStream};
use crate::rpc::KeyValue;
use crate::rpc::store::{BatchGetRequest, RangeRequest};

/// The key stores table repartition metadata.
/// Specifically, it records the relation between source and destination regions after a repartition operation is completed.
/// This is distinct from the initial partitioning scheme of the table.
/// For example, after repartition, a destination region may still hold files from a source region; this mapping should be updated once repartition is done.
/// The GC scheduler uses this information to clean up those files (and removes this mapping if all files from the source region are cleaned).
///
/// The layout: `__table_repart/{table_id}`.
#[derive(Debug, PartialEq)]
pub struct TableRepartKey {
    /// The unique identifier of the table whose re-partition information is stored in this key.
    pub table_id: TableId,
}

impl TableRepartKey {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }

    /// Returns the range prefix of the table repartition key.
    pub fn range_prefix() -> Vec<u8> {
        format!("{}/", TABLE_REPART_PREFIX).into_bytes()
    }
}

impl MetadataKey<'_, TableRepartKey> for TableRepartKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<TableRepartKey> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "TableRepartKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures = TABLE_REPART_KEY_PATTERN
            .captures(key)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid TableRepartKey '{key}'"),
            })?;
        // Safety: pass the regex check above
        let table_id = captures[1].parse::<TableId>().unwrap();
        Ok(TableRepartKey { table_id })
    }
}

impl Display for TableRepartKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", TABLE_REPART_PREFIX, self.table_id)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone, Default)]
pub struct TableRepartValue {
    /// A mapping from source region IDs to sets of destination region IDs after repartition.
    ///
    /// Each key in the map is a `RegionId` representing a source region that has been repartitioned.
    /// The corresponding value is a `BTreeSet<RegionId>` containing the IDs of destination regions
    /// that currently hold files originally from the source region. This mapping is updated after
    /// repartition and is used by the GC scheduler to track and clean up files that have been moved.
    pub src_to_dst: BTreeMap<RegionId, BTreeSet<RegionId>>,
}

impl TableRepartValue {
    /// Creates a new TableRepartValue with an empty src_to_dst map.
    pub fn new() -> Self {
        Default::default()
    }
    /// Update mapping from src region to dst regions. Should be called once repartition is done.
    ///
    /// If `dst` is empty, this method does nothing.
    pub fn update_mappings(&mut self, src: RegionId, dst: &[RegionId]) {
        if dst.is_empty() {
            return;
        }
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

impl MetadataValue for TableRepartValue {
    fn try_from_raw_value(raw_value: &[u8]) -> Result<Self> {
        serde_json::from_slice::<TableRepartValue>(raw_value).context(SerdeJsonSnafu)
    }

    fn try_as_raw_value(&self) -> Result<Vec<u8>> {
        serde_json::to_vec(self).context(SerdeJsonSnafu)
    }
}

pub type TableRepartValueDecodeResult =
    Result<Option<DeserializedValueWithBytes<TableRepartValue>>>;

/// Decodes `KeyValue` to [TableRepartKey] and [TableRepartValue].
pub fn table_repart_decoder(kv: KeyValue) -> Result<(TableRepartKey, TableRepartValue)> {
    let key = TableRepartKey::from_bytes(&kv.key)?;
    let value = TableRepartValue::try_from_raw_value(&kv.value)?;
    Ok((key, value))
}

pub struct TableRepartManager {
    kv_backend: KvBackendRef,
}

impl TableRepartManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Returns all table repartition entries.
    pub async fn table_reparts(&self) -> Result<Vec<(TableId, TableRepartValue)>> {
        let prefix = TableRepartKey::range_prefix();
        let req = RangeRequest::new().with_prefix(prefix);
        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            table_repart_decoder,
        )
        .into_stream();

        let res = stream.try_collect::<Vec<_>>().await?;
        Ok(res
            .into_iter()
            .map(|(key, value)| (key.table_id, value))
            .collect())
    }

    /// Builds a create table repart transaction,
    /// it expected the `__table_repart/{table_id}` wasn't occupied.
    pub fn build_create_txn(
        &self,
        table_id: TableId,
        table_repart_value: &TableRepartValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> TableRepartValueDecodeResult + use<>,
    )> {
        let key = TableRepartKey::new(table_id);
        let raw_key = key.to_bytes();

        let txn = Txn::put_if_not_exists(raw_key.clone(), table_repart_value.try_as_raw_value()?);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Builds a update table repart transaction,
    /// it expected the remote value equals the `current_table_repart_value`.
    /// It retrieves the latest value if the comparing failed.
    pub fn build_update_txn(
        &self,
        table_id: TableId,
        current_table_repart_value: &DeserializedValueWithBytes<TableRepartValue>,
        new_table_repart_value: &TableRepartValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> TableRepartValueDecodeResult + use<>,
    )> {
        let key = TableRepartKey::new(table_id);
        let raw_key = key.to_bytes();
        let raw_value = current_table_repart_value.get_raw_bytes();
        let new_raw_value: Vec<u8> = new_table_repart_value.try_as_raw_value()?;

        let txn = Txn::compare_and_put(raw_key.clone(), raw_value, new_raw_value);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Returns the [`TableRepartValue`].
    pub async fn get(&self, table_id: TableId) -> Result<Option<TableRepartValue>> {
        self.get_inner(table_id).await
    }

    async fn get_inner(&self, table_id: TableId) -> Result<Option<TableRepartValue>> {
        let key = TableRepartKey::new(table_id);
        self.kv_backend
            .get(&key.to_bytes())
            .await?
            .map(|kv| TableRepartValue::try_from_raw_value(&kv.value))
            .transpose()
    }

    /// Returns the [`TableRepartValue`] wrapped with [`DeserializedValueWithBytes`].
    pub async fn get_with_raw_bytes(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TableRepartValue>>> {
        self.get_with_raw_bytes_inner(table_id).await
    }

    async fn get_with_raw_bytes_inner(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TableRepartValue>>> {
        let key = TableRepartKey::new(table_id);
        self.kv_backend
            .get(&key.to_bytes())
            .await?
            .map(|kv| DeserializedValueWithBytes::from_inner_slice(&kv.value))
            .transpose()
    }

    /// Returns batch of [`TableRepartValue`] that respects the order of `table_ids`.
    pub async fn batch_get(&self, table_ids: &[TableId]) -> Result<Vec<Option<TableRepartValue>>> {
        let raw_table_reparts = self.batch_get_inner(table_ids).await?;

        Ok(raw_table_reparts
            .into_iter()
            .map(|v| v.map(|x| x.inner))
            .collect())
    }

    /// Returns batch of [`TableRepartValue`] wrapped with [`DeserializedValueWithBytes`].
    pub async fn batch_get_with_raw_bytes(
        &self,
        table_ids: &[TableId],
    ) -> Result<Vec<Option<DeserializedValueWithBytes<TableRepartValue>>>> {
        self.batch_get_inner(table_ids).await
    }

    async fn batch_get_inner(
        &self,
        table_ids: &[TableId],
    ) -> Result<Vec<Option<DeserializedValueWithBytes<TableRepartValue>>>> {
        let keys = table_ids
            .iter()
            .map(|id| TableRepartKey::new(*id).to_bytes())
            .collect::<Vec<_>>();
        let resp = self
            .kv_backend
            .batch_get(BatchGetRequest { keys: keys.clone() })
            .await?;

        let kvs = resp
            .kvs
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<HashMap<_, _>>();
        keys.into_iter()
            .map(|key| {
                if let Some(value) = kvs.get(&key) {
                    Ok(Some(DeserializedValueWithBytes::from_inner_slice(value)?))
                } else {
                    Ok(None)
                }
            })
            .collect()
    }

    /// Updates mappings from src region to dst regions.
    /// Should be called once repartition is done.
    pub async fn update_mappings(
        &self,
        table_id: TableId,
        region_mapping: &HashMap<RegionId, Vec<RegionId>>,
    ) -> Result<()> {
        // Get current table repart with raw bytes for CAS operation
        let Some(current_table_repart) = self.get_with_raw_bytes(table_id).await? else {
            let mut new_table_repart_value = TableRepartValue::new();
            for (src, dsts) in region_mapping.iter() {
                new_table_repart_value.update_mappings(*src, dsts);
            }

            let (txn, _) = self.build_create_txn(table_id, &new_table_repart_value)?;
            let result = self.kv_backend.txn(txn).await?;
            ensure!(
                result.succeeded,
                crate::error::MetadataCorruptionSnafu {
                    err_msg: format!(
                        "Failed to create table repart for table {}: CAS operation failed",
                        table_id
                    ),
                }
            );

            return Ok(());
        };

        // Clone the current repart value and update mappings
        let mut new_table_repart_value = current_table_repart.inner.clone();
        for (src, dsts) in region_mapping.iter() {
            new_table_repart_value.update_mappings(*src, dsts);
        }

        // Execute atomic update
        let (txn, _) =
            self.build_update_txn(table_id, &current_table_repart, &new_table_repart_value)?;

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
    pub async fn remove_mappings(
        &self,
        table_id: TableId,
        region_mapping: &HashMap<RegionId, Vec<RegionId>>,
    ) -> Result<()> {
        // Get current table repart with raw bytes for CAS operation
        let current_table_repart = self
            .get_with_raw_bytes(table_id)
            .await?
            .context(crate::error::TableRepartNotFoundSnafu { table_id })?;

        // Clone the current repart value and remove mappings
        let mut new_table_repart_value = current_table_repart.inner.clone();
        for (src, dsts) in region_mapping.iter() {
            new_table_repart_value.remove_mappings(*src, dsts);
        }

        // Execute atomic update
        let (txn, _) =
            self.build_update_txn(table_id, &current_table_repart, &new_table_repart_value)?;

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
        let table_repart = self.get(table_id).await?;
        Ok(table_repart.and_then(|repart| repart.src_to_dst.get(&src_region).cloned()))
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
    fn test_table_repart_key_serialization() {
        let key = TableRepartKey::new(42);
        let raw_key = key.to_bytes();
        assert_eq!(raw_key, b"__table_repart/42");
    }

    #[test]
    fn test_table_repart_key_deserialization() {
        let expected = TableRepartKey::new(42);
        let key = TableRepartKey::from_bytes(b"__table_repart/42").unwrap();
        assert_eq!(key, expected);
    }

    #[test]
    fn test_table_repart_key_deserialization_invalid_utf8() {
        let result = TableRepartKey::from_bytes(b"__table_repart/\xff");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("not a valid UTF8 string")
        );
    }

    #[test]
    fn test_table_repart_key_deserialization_invalid_format() {
        let result = TableRepartKey::from_bytes(b"invalid_key_format");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid TableRepartKey")
        );
    }

    #[test]
    fn test_table_repart_value_serialization_deserialization() {
        let mut src_to_dst = BTreeMap::new();
        let src_region = RegionId::new(1, 1);
        let dst_regions = vec![RegionId::new(1, 2), RegionId::new(1, 3)];
        src_to_dst.insert(src_region, dst_regions.into_iter().collect());

        let value = TableRepartValue { src_to_dst };
        let serialized = value.try_as_raw_value().unwrap();
        let deserialized = TableRepartValue::try_from_raw_value(&serialized).unwrap();

        assert_eq!(value, deserialized);
    }

    #[test]
    fn test_table_repart_value_update_mappings_new_src() {
        let mut value = TableRepartValue {
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
    fn test_table_repart_value_update_mappings_existing_src() {
        let mut value = TableRepartValue {
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
    fn test_table_repart_value_remove_mappings_existing() {
        let mut value = TableRepartValue {
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
    fn test_table_repart_value_remove_mappings_all() {
        let mut value = TableRepartValue {
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
    fn test_table_repart_value_remove_mappings_nonexistent() {
        let mut value = TableRepartValue {
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
    fn test_table_repart_value_remove_mappings_nonexistent_src() {
        let mut value = TableRepartValue {
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
    async fn test_table_repart_manager_get_empty() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRepartManager::new(kv);
        let result = manager.get(1024).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_table_repart_manager_get_with_raw_bytes_empty() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRepartManager::new(kv);
        let result = manager.get_with_raw_bytes(1024).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_table_repart_manager_create_and_get() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRepartManager::new(kv.clone());

        let mut src_to_dst = BTreeMap::new();
        let src_region = RegionId::new(1, 1);
        let dst_regions = vec![RegionId::new(1, 2), RegionId::new(1, 3)];
        src_to_dst.insert(src_region, dst_regions.into_iter().collect());

        let value = TableRepartValue { src_to_dst };

        // Create the table repart
        let (txn, _) = manager.build_create_txn(1024, &value).unwrap();
        let result = kv.txn(txn).await.unwrap();
        assert!(result.succeeded);

        // Get the table repart
        let retrieved = manager.get(1024).await.unwrap().unwrap();
        assert_eq!(retrieved, value);
    }

    #[tokio::test]
    async fn test_table_repart_manager_update_txn() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRepartManager::new(kv.clone());

        let initial_value = TableRepartValue {
            src_to_dst: BTreeMap::new(),
        };

        // Create initial table repart
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
        let updated_value = TableRepartValue {
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
    async fn test_table_repart_manager_batch_get() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRepartManager::new(kv.clone());

        // Create multiple table reparts
        let table_reparts = vec![
            (
                1024,
                TableRepartValue {
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
                TableRepartValue {
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

        for (table_id, value) in &table_reparts {
            let (txn, _) = manager.build_create_txn(*table_id, value).unwrap();
            let result = kv.txn(txn).await.unwrap();
            assert!(result.succeeded);
        }

        // Batch get
        let results = manager.batch_get(&[1024, 1025, 1026]).await.unwrap();
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].as_ref().unwrap(), &table_reparts[0].1);
        assert_eq!(results[1].as_ref().unwrap(), &table_reparts[1].1);
        assert!(results[2].is_none());
    }

    #[tokio::test]
    async fn test_table_repart_manager_update_mappings() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRepartManager::new(kv.clone());

        // Create initial table repart
        let initial_value = TableRepartValue {
            src_to_dst: BTreeMap::new(),
        };
        let (txn, _) = manager.build_create_txn(1024, &initial_value).unwrap();
        let result = kv.txn(txn).await.unwrap();
        assert!(result.succeeded);

        // Update mappings
        let src = RegionId::new(1024, 1);
        let dst = vec![RegionId::new(1024, 2), RegionId::new(1024, 3)];
        let region_mapping = HashMap::from([(src, dst)]);
        manager
            .update_mappings(1024, &region_mapping)
            .await
            .unwrap();

        // Verify update
        let retrieved = manager.get(1024).await.unwrap().unwrap();
        assert_eq!(retrieved.src_to_dst.len(), 1);
        assert!(retrieved.src_to_dst.contains_key(&src));
        assert_eq!(retrieved.src_to_dst.get(&src).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_table_repart_manager_remove_mappings() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRepartManager::new(kv.clone());

        // Create initial table repart with mappings
        let mut initial_src_to_dst = BTreeMap::new();
        let src = RegionId::new(1024, 1);
        let dst_regions = vec![
            RegionId::new(1024, 2),
            RegionId::new(1024, 3),
            RegionId::new(1024, 4),
        ];
        initial_src_to_dst.insert(src, dst_regions.into_iter().collect());

        let initial_value = TableRepartValue {
            src_to_dst: initial_src_to_dst,
        };
        let (txn, _) = manager.build_create_txn(1024, &initial_value).unwrap();
        let result = kv.txn(txn).await.unwrap();
        assert!(result.succeeded);

        // Remove some mappings
        let to_remove = vec![RegionId::new(1024, 2), RegionId::new(1024, 3)];
        let region_mapping = HashMap::from([(src, to_remove)]);
        manager
            .remove_mappings(1024, &region_mapping)
            .await
            .unwrap();

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
    async fn test_table_repart_manager_get_dst_regions() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRepartManager::new(kv.clone());

        // Create initial table repart with mappings
        let mut initial_src_to_dst = BTreeMap::new();
        let src = RegionId::new(1024, 1);
        let dst_regions = vec![RegionId::new(1024, 2), RegionId::new(1024, 3)];
        initial_src_to_dst.insert(src, dst_regions.into_iter().collect());

        let initial_value = TableRepartValue {
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
    async fn test_table_repart_manager_operations_on_nonexistent_table() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRepartManager::new(kv);

        let src = RegionId::new(1024, 1);
        let dst = vec![RegionId::new(1024, 2)];

        // Try to update mappings on non-existent table
        let region_mapping = HashMap::from([(src, dst.clone())]);
        manager
            .update_mappings(1024, &region_mapping)
            .await
            .unwrap();

        // Try to remove mappings on non-existent table
        let region_mapping = HashMap::from([(src, dst)]);
        let result = manager.remove_mappings(1024, &region_mapping).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Failed to find table repartition metadata for table id 1024"),
            "{err_msg}"
        );
    }

    #[tokio::test]
    async fn test_table_repart_manager_batch_get_with_raw_bytes() {
        let kv = Arc::new(MemoryKvBackend::default());
        let manager = TableRepartManager::new(kv.clone());

        // Create table repart
        let value = TableRepartValue {
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
