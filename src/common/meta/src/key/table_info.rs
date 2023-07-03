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
use table::metadata::{RawTableInfo, TableId};

use super::TABLE_INFO_KEY_PREFIX;
use crate::error::Result;
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::KvBackendRef;

pub struct TableInfoKey {
    table_id: TableId,
}

impl TableInfoKey {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }
}

impl TableMetaKey for TableInfoKey {
    fn as_raw_key(&self) -> Vec<u8> {
        format!("{}/{}", TABLE_INFO_KEY_PREFIX, self.table_id).into_bytes()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableInfoValue {
    pub table_info: RawTableInfo,
    version: u64,
}

pub struct TableInfoManager {
    kv_backend: KvBackendRef,
}

impl TableInfoManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    pub async fn get(&self, table_id: TableId) -> Result<Option<TableInfoValue>> {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.as_raw_key();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| TableInfoValue::try_from_raw_value(x.1))
            .transpose()
    }

    pub async fn compare_and_set(
        &self,
        table_id: TableId,
        expect: Option<TableInfoValue>,
        table_info: RawTableInfo,
    ) -> Result<std::result::Result<(), Option<Vec<u8>>>> {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.as_raw_key();

        let (expect, version) = if let Some(x) = expect {
            (x.try_as_raw_value()?, x.version + 1)
        } else {
            (vec![], 0)
        };

        let value = TableInfoValue {
            table_info,
            version,
        };
        let raw_value = value.try_as_raw_value()?;

        self.kv_backend
            .compare_and_set(&raw_key, &expect, &raw_value)
            .await
    }

    pub async fn remove(&self, table_id: TableId) -> Result<()> {
        let key = TableInfoKey::new(table_id);
        let removed_key = to_removed_key(&String::from_utf8_lossy(key.as_raw_key().as_slice()));
        self.kv_backend
            .move_value(&key.as_raw_key(), removed_key.as_bytes())
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, RawSchema, Schema};
    use table::metadata::{RawTableMeta, TableIdent, TableType};

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::KvBackend;

    #[tokio::test]
    async fn test_table_info_manager() {
        let backend = Arc::new(MemoryKvBackend::default());

        for i in 1..=3 {
            let key = TableInfoKey::new(i).as_raw_key();
            let val = TableInfoValue {
                table_info: new_table_info(i),
                version: 1,
            }
            .try_as_raw_value()
            .unwrap();
            backend.set(&key, &val).await.unwrap();
        }

        let manager = TableInfoManager::new(backend.clone());

        let val = manager.get(1).await.unwrap().unwrap();
        assert_eq!(
            val,
            TableInfoValue {
                table_info: new_table_info(1),
                version: 1,
            }
        );
        assert!(manager.get(4).await.unwrap().is_none());

        let table_info = new_table_info(4);
        let result = manager
            .compare_and_set(4, None, table_info.clone())
            .await
            .unwrap();
        assert!(result.is_ok());

        // test cas failed, the new table info is not set
        let new_table_info = new_table_info(4);
        let result = manager
            .compare_and_set(4, None, new_table_info.clone())
            .await
            .unwrap();
        let actual = TableInfoValue::try_from_raw_value(result.unwrap_err().unwrap()).unwrap();
        assert_eq!(
            actual,
            TableInfoValue {
                table_info: table_info.clone(),
                version: 0,
            }
        );

        // test cas success
        let result = manager
            .compare_and_set(4, Some(actual), new_table_info.clone())
            .await
            .unwrap();
        assert!(result.is_ok());

        assert!(manager.remove(4).await.is_ok());

        let kv = backend
            .get(b"__removed-__table_info/4")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"__removed-__table_info/4", kv.0.as_slice());
        let value = TableInfoValue::try_from_raw_value(kv.1).unwrap();
        assert_eq!(value.table_info, new_table_info);
        assert_eq!(value.version, 1);
    }

    #[test]
    fn test_key_serde() {
        let key = TableInfoKey::new(42);
        let raw_key = key.as_raw_key();
        assert_eq!(raw_key, b"__table_info/42");
    }

    #[test]
    fn test_value_serde() {
        let value = TableInfoValue {
            table_info: new_table_info(42),
            version: 1,
        };
        let serialized = value.try_as_raw_value().unwrap();
        let deserialized = TableInfoValue::try_from_raw_value(serialized).unwrap();
        assert_eq!(value, deserialized);
    }

    fn new_table_info(table_id: TableId) -> RawTableInfo {
        let schema = Schema::new(vec![ColumnSchema::new(
            "name",
            ConcreteDataType::string_datatype(),
            true,
        )]);

        let meta = RawTableMeta {
            schema: RawSchema::from(&schema),
            engine: "mito".to_string(),
            created_on: chrono::DateTime::default(),
            primary_key_indices: vec![0, 1],
            next_column_id: 3,
            engine_options: Default::default(),
            value_indices: vec![2, 3],
            options: Default::default(),
            region_numbers: vec![1],
        };

        RawTableInfo {
            ident: TableIdent {
                table_id,
                version: 1,
            },
            name: "table_1".to_string(),
            desc: Some("blah".to_string()),
            catalog_name: "catalog_1".to_string(),
            schema_name: "schema_1".to_string(),
            meta,
            table_type: TableType::Base,
        }
    }
}
