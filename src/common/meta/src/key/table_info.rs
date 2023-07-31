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
use snafu::ensure;
use table::metadata::{RawTableInfo, TableId};

use super::TABLE_INFO_KEY_PREFIX;
use crate::error::{Result, UnexpectedSnafu};
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{CompareAndPutRequest, MoveValueRequest};

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

impl TableInfoValue {
    pub fn new(table_info: RawTableInfo) -> Self {
        Self {
            table_info,
            version: 0,
        }
    }
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
            .map(|x| TableInfoValue::try_from_raw_value(x.value))
            .transpose()
    }

    /// Create TableInfo key and value. If the key already exists, check if the value is the same.
    pub async fn create(&self, table_id: TableId, table_info: &RawTableInfo) -> Result<()> {
        let result = self
            .compare_and_put(table_id, None, table_info.clone())
            .await?;
        if let Err(curr) = result {
            let Some(curr) = curr else {
                return UnexpectedSnafu {
                    err_msg: format!("compare_and_put expect None but failed with current value None, table_id: {table_id}, table_info: {table_info:?}"),
                }.fail()
            };
            ensure!(
                &curr.table_info == table_info,
                UnexpectedSnafu {
                    err_msg: format!(
                        "TableInfoValue for table {table_id} is updated before it is created!"
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
    /// - If associated value is not the same as `expect`, no value will be updated and an
    ///   `Ok(Err(Option<TableInfoValue>))` will be returned. The `Option<TableInfoValue>` indicates
    ///   the current associated value of key.
    /// - If any error happens during operation, an `Err(Error)` will be returned.
    pub async fn compare_and_put(
        &self,
        table_id: TableId,
        expect: Option<TableInfoValue>,
        table_info: RawTableInfo,
    ) -> Result<std::result::Result<(), Option<TableInfoValue>>> {
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
                .map(|x| TableInfoValue::try_from_raw_value(x.value))
                .transpose()?)
        })
    }

    pub async fn remove(&self, table_id: TableId) -> Result<()> {
        let key = TableInfoKey::new(table_id).as_raw_key();
        let removed_key = to_removed_key(&String::from_utf8_lossy(&key));
        let req = MoveValueRequest::new(key, removed_key.as_bytes());
        self.kv_backend.move_value(req).await?;
        Ok(())
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
    use crate::rpc::store::PutRequest;

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
            let req = PutRequest::new().with_key(key).with_value(val);
            backend.put(req).await.unwrap();
        }

        let manager = TableInfoManager::new(backend.clone());
        assert!(manager.create(99, &new_table_info(99)).await.is_ok());
        assert!(manager.create(99, &new_table_info(99)).await.is_ok());

        let result = manager.create(99, &new_table_info(88)).await;
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg
            .contains("Unexpected: TableInfoValue for table 99 is updated before it is created!"));

        let val = manager.get(1).await.unwrap().unwrap();
        assert_eq!(
            val,
            TableInfoValue {
                table_info: new_table_info(1),
                version: 1,
            }
        );
        assert!(manager.get(4).await.unwrap().is_none());

        // test cas failed, current value is not set
        let table_info = new_table_info(4);
        let result = manager
            .compare_and_put(
                4,
                Some(TableInfoValue {
                    table_info: table_info.clone(),
                    version: 0,
                }),
                table_info.clone(),
            )
            .await
            .unwrap();
        assert!(result.unwrap_err().is_none());

        let result = manager
            .compare_and_put(4, None, table_info.clone())
            .await
            .unwrap();
        assert!(result.is_ok());

        // test cas failed, the new table info is not set
        let new_table_info = new_table_info(4);
        let result = manager
            .compare_and_put(4, None, new_table_info.clone())
            .await
            .unwrap();
        let actual = result.unwrap_err().unwrap();
        assert_eq!(
            actual,
            TableInfoValue {
                table_info: table_info.clone(),
                version: 0,
            }
        );

        // test cas success
        let result = manager
            .compare_and_put(4, Some(actual), new_table_info.clone())
            .await
            .unwrap();
        assert!(result.is_ok());

        assert!(manager.remove(4).await.is_ok());

        let kv = backend
            .get(b"__removed-__table_info/4")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(b"__removed-__table_info/4", kv.key.as_slice());
        let value = TableInfoValue::try_from_raw_value(kv.value).unwrap();
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
