// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use futures::{StreamExt, TryStreamExt};
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use store_api::storage::RegionNumber;
use table::metadata::TableId;

use super::{DATANODE_TABLE_KEY_PATTERN, DATANODE_TABLE_KEY_PREFIX};
use crate::error::{ConcurrentModifyRegionsPlacementSnafu, InvalidTableMetadataSnafu, Result};
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::KvBackendRef;
use crate::DatanodeId;

struct DatanodeTableKey {
    datanode_id: DatanodeId,
    table_id: TableId,
}

impl DatanodeTableKey {
    fn new(datanode_id: DatanodeId, table_id: TableId) -> Self {
        Self {
            datanode_id,
            table_id,
        }
    }

    fn prefix(datanode_id: DatanodeId) -> String {
        format!("{}/{datanode_id}", DATANODE_TABLE_KEY_PREFIX)
    }

    #[allow(unused)]
    pub fn strip_table_id(raw_key: &[u8]) -> Result<TableId> {
        let key = String::from_utf8(raw_key.to_vec()).map_err(|e| {
            InvalidTableMetadataSnafu {
                err_msg: format!(
                    "DatanodeTableKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(raw_key)
                ),
            }
            .build()
        })?;
        let captures =
            DATANODE_TABLE_KEY_PATTERN
                .captures(&key)
                .context(InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid DatanodeTableKey '{key}'"),
                })?;
        // Safety: pass the regex check above
        let table_id = captures[2].parse::<TableId>().unwrap();
        Ok(table_id)
    }
}

impl TableMetaKey for DatanodeTableKey {
    fn as_raw_key(&self) -> Vec<u8> {
        format!("{}/{}", Self::prefix(self.datanode_id), self.table_id).into_bytes()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatanodeTableValue {
    pub table_id: TableId,
    pub regions: Vec<RegionNumber>,
    version: u64,
}

impl DatanodeTableValue {
    fn new(table_id: TableId, regions: Vec<RegionNumber>) -> Self {
        Self {
            table_id,
            regions,
            version: 0,
        }
    }
}

pub struct DatanodeTableManager {
    kv_backend: KvBackendRef,
}

impl DatanodeTableManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    async fn get(&self, key: &DatanodeTableKey) -> Result<Option<DatanodeTableValue>> {
        self.kv_backend
            .get(&key.as_raw_key())
            .await?
            .map(|kv| DatanodeTableValue::try_from_raw_value(kv.1))
            .transpose()
    }

    pub async fn create(
        &self,
        datanode_id: DatanodeId,
        table_id: TableId,
        regions: Vec<RegionNumber>,
    ) -> Result<()> {
        let key = DatanodeTableKey::new(datanode_id, table_id).as_raw_key();
        let val = DatanodeTableValue::new(table_id, regions).try_as_raw_value()?;
        self.kv_backend
            .compare_and_set(&key, &[], &val)
            .await?
            .map_err(|curr| {
                let curr = if let Some(curr) = curr {
                    DatanodeTableValue::try_from_raw_value(curr).map_or_else(
                        |e| format!("invalid DatanodeTableValue for Datanode {datanode_id}: {e}"),
                        |v| format!("{v:?}"),
                    )
                } else {
                    "empty".to_string()
                };
                ConcurrentModifyRegionsPlacementSnafu {
                    err_msg: format!("Datanode {datanode_id} already existed {curr}"),
                }
                .build()
            })
    }

    pub async fn remove(&self, datanode_id: DatanodeId, table_id: TableId) -> Result<()> {
        let key = DatanodeTableKey::new(datanode_id, table_id);
        let removed_key = to_removed_key(&String::from_utf8_lossy(&key.as_raw_key()));
        self.kv_backend
            .move_value(&key.as_raw_key(), removed_key.as_bytes())
            .await
    }

    // TODO(LFC): Use transaction to move region once the KvBackend and KvStore are merged into one.
    pub async fn move_region(
        &self,
        from_datanode: DatanodeId,
        to_datanode: DatanodeId,
        table_id: TableId,
        region: RegionNumber,
    ) -> Result<()> {
        let from_key = DatanodeTableKey::new(from_datanode, table_id);
        let from_value = self.get(&from_key).await?;
        if let Some(mut from_value) = from_value {
            from_value.regions.retain(|x| *x != region);
            from_value.version += 1;
            self.kv_backend
                .set(&from_key.as_raw_key(), &from_value.try_as_raw_value()?)
                .await?;
        }

        let to_key = DatanodeTableKey::new(to_datanode, table_id);
        let to_value = self.get(&to_key).await?;
        if let Some(mut to_value) = to_value {
            to_value.regions.push(region);
            to_value.version += 1;
            self.kv_backend
                .set(&to_key.as_raw_key(), &to_value.try_as_raw_value()?)
                .await?;
        }
        Ok(())
    }

    pub async fn tables(&self, datanode_id: DatanodeId) -> Result<Vec<DatanodeTableValue>> {
        let prefix = DatanodeTableKey::prefix(datanode_id);
        let table_ids = self
            .kv_backend
            .range(prefix.as_bytes())
            .map(|result| result.map(|kv| DatanodeTableValue::try_from_raw_value(kv.1)))
            .try_collect::<Vec<_>>()
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        Ok(table_ids)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::KvBackend;

    #[tokio::test]
    async fn test_move_region() {
        let manager = DatanodeTableManager::new(Arc::new(MemoryKvBackend::default()));

        assert!(manager.create(1, 1, vec![1, 2]).await.is_ok());
        assert!(manager.create(2, 1, vec![3, 4]).await.is_ok());

        assert!(manager.move_region(1, 2, 1, 1).await.is_ok());

        let value = manager
            .get(&DatanodeTableKey::new(1, 1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            value,
            DatanodeTableValue {
                table_id: 1,
                regions: vec![2],
                version: 1,
            }
        );

        let value = manager
            .get(&DatanodeTableKey::new(2, 1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            value,
            DatanodeTableValue {
                table_id: 1,
                regions: vec![3, 4, 1],
                version: 1,
            }
        );
    }

    #[tokio::test]
    async fn test_datanode_table_value_manager() {
        let backend = Arc::new(MemoryKvBackend::default());
        let manager = DatanodeTableManager::new(backend.clone());

        assert!(manager.create(1, 1, vec![1, 2, 3]).await.is_ok());
        assert!(manager.create(1, 2, vec![4, 5, 6]).await.is_ok());
        assert!(manager.create(2, 1, vec![4, 5, 6]).await.is_ok());
        assert!(manager.create(2, 2, vec![1, 2, 3]).await.is_ok());

        let err_msg = manager
            .create(1, 1, vec![4, 5, 6])
            .await
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains("Concurrent modify regions placement: Datanode 1 already existed DatanodeTableValue { table_id: 1, regions: [1, 2, 3], version: 0 }"));

        let to_be_removed_key = DatanodeTableKey::new(2, 1);
        let expected_value = DatanodeTableValue {
            table_id: 1,
            regions: vec![4, 5, 6],
            version: 0,
        };
        let value = manager.get(&to_be_removed_key).await.unwrap().unwrap();
        assert_eq!(value, expected_value);

        assert!(manager.remove(2, 1).await.is_ok());
        assert!(manager.get(&to_be_removed_key).await.unwrap().is_none());
        let kv = backend
            .get(b"__removed-__dn_table/2/1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"__removed-__dn_table/2/1", kv.0.as_slice());
        let value = DatanodeTableValue::try_from_raw_value(kv.1).unwrap();
        assert_eq!(value, expected_value);

        let values = manager.tables(1).await.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(
            values[0],
            DatanodeTableValue {
                table_id: 1,
                regions: vec![1, 2, 3],
                version: 0,
            }
        );
        assert_eq!(
            values[1],
            DatanodeTableValue {
                table_id: 2,
                regions: vec![4, 5, 6],
                version: 0,
            }
        );
    }

    #[test]
    fn test_serde() {
        let key = DatanodeTableKey {
            datanode_id: 1,
            table_id: 2,
        };
        let raw_key = key.as_raw_key();
        assert_eq!(raw_key, b"__dn_table/1/2");

        let value = DatanodeTableValue {
            table_id: 42,
            regions: vec![1, 2, 3],
            version: 1,
        };
        let literal = br#"{"table_id":42,"regions":[1,2,3],"version":1}"#;

        let raw_value = value.try_as_raw_value().unwrap();
        assert_eq!(raw_value, literal);

        let actual = DatanodeTableValue::try_from_raw_value(literal.to_vec()).unwrap();
        assert_eq!(actual, value);
    }

    #[test]
    fn test_strip_table_id() {
        fn test_err(raw_key: &[u8]) {
            let result = DatanodeTableKey::strip_table_id(raw_key);
            assert!(result.is_err());
        }

        test_err(b"");
        test_err(vec![0u8, 159, 146, 150].as_slice()); // invalid UTF8 string
        test_err(b"invalid_prefix/1/2");
        test_err(b"__dn_table/");
        test_err(b"__dn_table/invalid_len_1");
        test_err(b"__dn_table/invalid_len_3/1/2");
        test_err(b"__dn_table/invalid_node_id/2");
        test_err(b"__dn_table/1/invalid_table_id");

        let table_id = DatanodeTableKey::strip_table_id(b"__dn_table/1/2").unwrap();
        assert_eq!(table_id, 2);
    }
}
