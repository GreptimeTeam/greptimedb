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
use store_api::storage::RegionNumber;
use table::metadata::TableId;

use super::TABLE_REGION_KEY_PREFIX;
use crate::error::Result;
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::KvBackendRef;
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
            .map(|x| TableRegionValue::try_from_raw_value(x.1))
            .transpose()
    }

    pub async fn compare_and_set(
        &self,
        table_id: TableId,
        expect: Option<TableRegionValue>,
        region_distribution: RegionDistribution,
    ) -> Result<std::result::Result<(), Option<Vec<u8>>>> {
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

        self.kv_backend
            .compare_and_set(&raw_key, &expect, &raw_value)
            .await
    }

    pub async fn remove(&self, table_id: TableId) -> Result<()> {
        let key = TableRegionKey::new(table_id);
        let remove_key = to_removed_key(&String::from_utf8_lossy(key.as_raw_key().as_slice()));
        self.kv_backend
            .move_value(&key.as_raw_key(), remove_key.as_bytes())
            .await
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
        let result = manager
            .compare_and_set(1, None, region_distribution.clone())
            .await
            .unwrap();
        assert!(result.is_ok());

        let new_region_distribution =
            RegionDistribution::from([(1, vec![4, 5, 6]), (2, vec![1, 2, 3])]);
        let curr = manager
            .compare_and_set(1, None, new_region_distribution.clone())
            .await
            .unwrap()
            .unwrap_err()
            .unwrap();
        let curr = TableRegionValue::try_from_raw_value(curr).unwrap();
        assert_eq!(
            curr,
            TableRegionValue {
                region_distribution,
                version: 0
            }
        );

        assert!(manager
            .compare_and_set(1, Some(curr), new_region_distribution.clone())
            .await
            .unwrap()
            .is_ok());

        let value = manager.get(1).await.unwrap().unwrap();
        assert_eq!(
            value,
            TableRegionValue {
                region_distribution: new_region_distribution.clone(),
                version: 1
            }
        );
        assert!(manager.get(2).await.unwrap().is_none());

        assert!(manager.remove(1).await.is_ok());

        let kv = backend
            .get(b"__removed-__table_region/1")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"__removed-__table_region/1", kv.0.as_slice());
        let value = TableRegionValue::try_from_raw_value(kv.1).unwrap();
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
