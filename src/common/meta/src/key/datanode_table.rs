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

use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt};
use store_api::storage::RegionNumber;
use table::metadata::TableId;

use super::{DATANODE_TABLE_KEY_PATTERN, DATANODE_TABLE_KEY_PREFIX};
use crate::error::{InvalidTableMetadataSnafu, MoveRegionSnafu, Result, UnexpectedSnafu};
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{CompareAndPutRequest, MoveValueRequest, RangeRequest};
use crate::DatanodeId;

pub struct DatanodeTableKey {
    datanode_id: DatanodeId,
    table_id: TableId,
}

impl DatanodeTableKey {
    pub fn new(datanode_id: DatanodeId, table_id: TableId) -> Self {
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
    pub fn new(table_id: TableId, regions: Vec<RegionNumber>) -> Self {
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
            .map(|kv| DatanodeTableValue::try_from_raw_value(kv.value))
            .transpose()
    }

    /// Create DatanodeTable key and value. If the key already exists, check if the value is the same.
    pub async fn create(
        &self,
        datanode_id: DatanodeId,
        table_id: TableId,
        regions: Vec<RegionNumber>,
    ) -> Result<()> {
        let key = DatanodeTableKey::new(datanode_id, table_id);
        let val = DatanodeTableValue::new(table_id, regions.clone());
        let req = CompareAndPutRequest::new()
            .with_key(key.as_raw_key())
            .with_value(val.try_as_raw_value()?);

        let resp = self.kv_backend.compare_and_put(req).await?;
        if !resp.success {
            let Some(curr) = resp
                .prev_kv
                .map(|kv| DatanodeTableValue::try_from_raw_value(kv.value))
                .transpose()? else {
                return UnexpectedSnafu {
                    err_msg: format!("compare_and_put expect None but failed with current value None, key: {key}, val: {val:?}"),
                }.fail();
            };

            ensure!(
                curr.table_id == table_id && curr.regions == regions,
                UnexpectedSnafu {
                    err_msg: format!("current value '{curr:?}' already existed for key '{key}', {val:?} is not set"),
                }
            );
        }
        Ok(())
    }

    pub async fn remove(&self, datanode_id: DatanodeId, table_id: TableId) -> Result<()> {
        let key = DatanodeTableKey::new(datanode_id, table_id);
        let removed_key = to_removed_key(&String::from_utf8_lossy(&key.as_raw_key()));
        let req = MoveValueRequest::new(key.as_raw_key(), removed_key.as_bytes());
        let _ = self.kv_backend.move_value(req).await?;
        Ok(())
    }

    pub async fn move_region(
        &self,
        from_datanode: DatanodeId,
        to_datanode: DatanodeId,
        table_id: TableId,
        region: RegionNumber,
    ) -> Result<()> {
        let from_key = DatanodeTableKey::new(from_datanode, table_id);
        let mut from_value = self.get(&from_key).await?.context(MoveRegionSnafu {
            table_id,
            region,
            err_msg: format!("DatanodeTableKey not found for Datanode {from_datanode}"),
        })?;

        ensure!(
            from_value.regions.contains(&region),
            MoveRegionSnafu {
                table_id,
                region,
                err_msg: format!("target region not found in Datanode {from_datanode}"),
            }
        );

        let to_key = DatanodeTableKey::new(to_datanode, table_id);
        let to_value = self.get(&to_key).await?;

        if let Some(v) = to_value.as_ref() {
            ensure!(
                !v.regions.contains(&region),
                MoveRegionSnafu {
                    table_id,
                    region,
                    err_msg: format!("target region already existed in Datanode {to_datanode}"),
                }
            );
        }

        let compares = vec![
            Compare::with_value(
                from_key.as_raw_key(),
                CompareOp::Equal,
                from_value.try_as_raw_value()?,
            ),
            Compare::new(
                to_key.as_raw_key(),
                CompareOp::Equal,
                to_value
                    .as_ref()
                    .map(|x| x.try_as_raw_value())
                    .transpose()?,
            ),
        ];

        let mut operations = Vec::with_capacity(2);

        from_value.regions.retain(|x| *x != region);
        if from_value.regions.is_empty() {
            operations.push(TxnOp::Delete(from_key.as_raw_key()));
        } else {
            from_value.version += 1;
            operations.push(TxnOp::Put(
                from_key.as_raw_key(),
                from_value.try_as_raw_value()?,
            ));
        }

        if let Some(mut v) = to_value {
            v.regions.push(region);
            v.version += 1;
            operations.push(TxnOp::Put(to_key.as_raw_key(), v.try_as_raw_value()?));
        } else {
            let v = DatanodeTableValue::new(table_id, vec![region]);
            operations.push(TxnOp::Put(to_key.as_raw_key(), v.try_as_raw_value()?));
        }

        let txn = Txn::new().when(compares).and_then(operations);
        let resp = self.kv_backend.txn(txn).await?;
        ensure!(
            resp.succeeded,
            MoveRegionSnafu {
                table_id,
                region,
                err_msg: format!("txn failed with responses: {:?}", resp.responses),
            }
        );
        Ok(())
    }

    pub async fn tables(&self, datanode_id: DatanodeId) -> Result<Vec<DatanodeTableValue>> {
        let prefix = DatanodeTableKey::prefix(datanode_id);
        let req = RangeRequest::new().with_prefix(prefix.as_bytes());
        let resp = self.kv_backend.range(req).await?;
        let table_ids = resp
            .kvs
            .into_iter()
            .map(|kv| DatanodeTableValue::try_from_raw_value(kv.value))
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

        let result = manager.move_region(1, 2, 1, 1).await;
        assert!(result.unwrap_err().to_string().contains(
            "Failed to move region 1 in table 1, err: DatanodeTableKey not found for Datanode 1"
        ));

        assert!(manager.create(1, 1, vec![1, 2, 3]).await.is_ok());
        let result = manager.move_region(1, 2, 1, 100).await;
        assert!(result.unwrap_err().to_string().contains(
            "Failed to move region 100 in table 1, err: target region not found in Datanode 1"
        ));

        // Move region 1 from datanode 1 to datanode 2.
        // Note that the DatanodeTableValue is not existed for datanode 2 now.
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
                regions: vec![2, 3],
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
                regions: vec![1],
                version: 0,
            }
        );

        // Move region 2 from datanode 1 to datanode 2.
        assert!(manager.move_region(1, 2, 1, 2).await.is_ok());
        let value = manager
            .get(&DatanodeTableKey::new(1, 1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            value,
            DatanodeTableValue {
                table_id: 1,
                regions: vec![3],
                version: 2,
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
                regions: vec![1, 2],
                version: 1,
            }
        );

        // Move region 3 (the last region) from datanode 1 to datanode 2.
        assert!(manager.move_region(1, 2, 1, 3).await.is_ok());
        let value = manager.get(&DatanodeTableKey::new(1, 1)).await.unwrap();
        assert!(value.is_none());
        let value = manager
            .get(&DatanodeTableKey::new(2, 1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            value,
            DatanodeTableValue {
                table_id: 1,
                regions: vec![1, 2, 3],
                version: 2,
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

        // If the value is the same, "create" can be called again.
        assert!(manager.create(2, 2, vec![1, 2, 3]).await.is_ok());

        let err_msg = manager
            .create(1, 1, vec![4, 5, 6])
            .await
            .unwrap_err()
            .to_string();
        assert!(err_msg.contains("Unexpected: current value 'DatanodeTableValue { table_id: 1, regions: [1, 2, 3], version: 0 }' already existed for key '__dn_table/1/1', DatanodeTableValue { table_id: 1, regions: [4, 5, 6], version: 0 } is not set"));

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
        assert_eq!(b"__removed-__dn_table/2/1", kv.key());
        let value = DatanodeTableValue::try_from_raw_value(kv.value).unwrap();
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
