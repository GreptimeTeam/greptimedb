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

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use snafu::ensure;
use store_api::storage::RegionNumber;
use table::metadata::TableId;

use super::TABLE_REGION_KEY_PREFIX;
use crate::error::{Result, UnexpectedSnafu};
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{CompareAndPutRequest, MoveValueRequest};
use crate::DatanodeId;

pub type RegionDistribution = BTreeMap<DatanodeId, Vec<RegionNumber>>;

pub struct TableRegionKey {
    table_id: TableId,
}

impl TableRegionKey {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }
}

impl TableMetaKey for TableRegionKey {
    fn as_raw_key(&self) -> Vec<u8> {
        format!("{}/{}", TABLE_REGION_KEY_PREFIX, self.table_id).into_bytes()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableRegionValue {
    pub region_distribution: RegionDistribution,
    version: u64,
}

impl TableRegionValue {
    pub fn new(region_distribution: RegionDistribution) -> Self {
        Self {
            region_distribution,
            version: 0,
        }
    }
}

pub struct TableRegionManager {
    kv_backend: KvBackendRef,
}

impl TableRegionManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    pub async fn get(&self, table_id: TableId) -> Result<Option<TableRegionValue>> {
        let key = TableRegionKey::new(table_id);
        let raw_key = key.as_raw_key();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| TableRegionValue::try_from_raw_value(x.value))
            .transpose()
    }

    /// Create TableRegion key and value. If the key already exists, check if the value is the same.
    pub async fn create(
        &self,
        table_id: TableId,
        region_distribution: &RegionDistribution,
    ) -> Result<()> {
        let result = self
            .compare_and_put(table_id, None, region_distribution.clone())
            .await?;
        if let Err(curr) = result {
            let Some(curr) = curr else {
                return UnexpectedSnafu {
                    err_msg: format!("compare_and_put expect None but failed with current value None, table_id: {table_id}, region_distribution: {region_distribution:?}"),
                }.fail()
            };
            ensure!(
                &curr.region_distribution == region_distribution,
                UnexpectedSnafu {
                    err_msg: format!(
                        "TableRegionValue for table {table_id} is updated before it is created!"
                    )
                }
            )
        }
        Ok(())
    }

    /// Compare and put value of key. `expect` is the expected value, if backend's current value associated
    /// with key is the same as `expect`, the value will be updated to `val`.
    ///
    /// - If the compare-and-set operation successfully updated value, this method will return an `Ok(Ok())`
    /// - If associated value is not the same as `expect`, no value will be updated and an `Ok(Err(Vec<u8>))`
    /// will be returned, the `Err(Vec<u8>)` indicates the current associated value of key.
    /// - If any error happens during operation, an `Err(Error)` will be returned.
    pub async fn compare_and_put(
        &self,
        table_id: TableId,
        expect: Option<TableRegionValue>,
        region_distribution: RegionDistribution,
    ) -> Result<std::result::Result<(), Option<TableRegionValue>>> {
        let key = TableRegionKey::new(table_id);
        let raw_key = key.as_raw_key();

        let (expect, version) = if let Some(x) = expect {
            (x.try_as_raw_value()?, x.version + 1)
        } else {
            (vec![], 0)
        };

        let value = TableRegionValue {
            region_distribution,
            version,
        };
        let raw_value = value.try_as_raw_value()?;

        let req = CompareAndPutRequest::new()
            .with_key(raw_key)
            .with_expect(expect)
            .with_value(raw_value);
        let resp = self.kv_backend.compare_and_put(req).await?;
        Ok(if resp.success {
            Ok(())
        } else {
            Err(resp
                .prev_kv
                .map(|x| TableRegionValue::try_from_raw_value(x.value))
                .transpose()?)
        })
    }

    pub async fn remove(&self, table_id: TableId) -> Result<Option<TableRegionValue>> {
        let key = TableRegionKey::new(table_id).as_raw_key();
        let remove_key = to_removed_key(&String::from_utf8_lossy(&key));
        let req = MoveValueRequest::new(key, remove_key.as_bytes());

        let resp = self.kv_backend.move_value(req).await?;
        resp.0
            .map(|x| TableRegionValue::try_from_raw_value(x.value))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::KvBackend;

    #[tokio::test]
    async fn test_table_region_manager() {
        let backend = Arc::new(MemoryKvBackend::default());
        let manager = TableRegionManager::new(backend.clone());

        let region_distribution =
            RegionDistribution::from([(1, vec![1, 2, 3]), (2, vec![4, 5, 6])]);
        let new_region_distribution =
            RegionDistribution::from([(1, vec![4, 5, 6]), (2, vec![1, 2, 3])]);

        let result = manager
            .compare_and_put(1, None, region_distribution.clone())
            .await
            .unwrap();
        assert!(result.is_ok());

        let curr = manager
            .compare_and_put(1, None, new_region_distribution.clone())
            .await
            .unwrap()
            .unwrap_err()
            .unwrap();
        assert_eq!(
            curr,
            TableRegionValue {
                region_distribution: region_distribution.clone(),
                version: 0
            }
        );

        assert!(manager
            .compare_and_put(1, Some(curr), new_region_distribution.clone())
            .await
            .unwrap()
            .is_ok());

        assert!(manager.create(99, &region_distribution).await.is_ok());
        assert!(manager.create(99, &region_distribution).await.is_ok());

        let result = manager.create(99, &new_region_distribution).await;
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("TableRegionValue for table 99 is updated before it is created!"));

        let value = manager.get(1).await.unwrap().unwrap();
        assert_eq!(
            value,
            TableRegionValue {
                region_distribution: new_region_distribution.clone(),
                version: 1
            }
        );
        let value = manager.get(99).await.unwrap().unwrap();
        assert_eq!(
            value,
            TableRegionValue {
                region_distribution,
                version: 0
            }
        );
        assert!(manager.get(2).await.unwrap().is_none());

        let value = manager.remove(1).await.unwrap().unwrap();
        assert_eq!(
            value,
            TableRegionValue {
                region_distribution: new_region_distribution.clone(),
                version: 1
            }
        );
        assert!(manager.remove(123).await.unwrap().is_none());

        let kv = backend
            .get(b"__removed-__table_region/1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"__removed-__table_region/1", kv.key.as_slice());
        let value = TableRegionValue::try_from_raw_value(kv.value).unwrap();
        assert_eq!(value.region_distribution, new_region_distribution);
        assert_eq!(value.version, 1);
    }

    #[test]
    fn test_serde() {
        let key = TableRegionKey::new(1);
        let raw_key = key.as_raw_key();
        assert_eq!(raw_key, b"__table_region/1");

        let value = TableRegionValue {
            region_distribution: RegionDistribution::from([(1, vec![1, 2, 3]), (2, vec![4, 5, 6])]),
            version: 0,
        };
        let literal = br#"{"region_distribution":{"1":[1,2,3],"2":[4,5,6]},"version":0}"#;

        assert_eq!(value.try_as_raw_value().unwrap(), literal);
        assert_eq!(
            TableRegionValue::try_from_raw_value(literal.to_vec()).unwrap(),
            value,
        );
    }
}
