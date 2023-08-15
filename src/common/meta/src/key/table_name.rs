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

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt};
use table::metadata::TableId;

use super::{TABLE_NAME_KEY_PATTERN, TABLE_NAME_KEY_PREFIX};
use crate::error::{
    Error, InvalidTableMetadataSnafu, RenameTableSnafu, Result, TableAlreadyExistsSnafu,
    TableNotExistSnafu, UnexpectedSnafu,
};
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::memory::MemoryKvBackend;
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp, TxnRequest};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{CompareAndPutRequest, MoveValueRequest, RangeRequest};
use crate::table_name::TableName;

#[derive(Debug, Clone, Copy)]
pub struct TableNameKey<'a> {
    pub catalog: &'a str,
    pub schema: &'a str,
    pub table: &'a str,
}

impl<'a> TableNameKey<'a> {
    pub fn new(catalog: &'a str, schema: &'a str, table: &'a str) -> Self {
        Self {
            catalog,
            schema,
            table,
        }
    }

    pub fn prefix_to_table(catalog: &str, schema: &str) -> String {
        format!("{}/{}/{}", TABLE_NAME_KEY_PREFIX, catalog, schema)
    }

    fn strip_table_name(raw_key: &[u8]) -> Result<String> {
        let key = String::from_utf8(raw_key.to_vec()).map_err(|e| {
            InvalidTableMetadataSnafu {
                err_msg: format!(
                    "TableNameKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(raw_key)
                ),
            }
            .build()
        })?;
        let captures =
            TABLE_NAME_KEY_PATTERN
                .captures(&key)
                .context(InvalidTableMetadataSnafu {
                    err_msg: format!("Invalid TableNameKey '{key}'"),
                })?;
        // Safety: pass the regex check above
        Ok(captures[3].to_string())
    }
}

impl TableMetaKey for TableNameKey<'_> {
    fn as_raw_key(&self) -> Vec<u8> {
        format!(
            "{}/{}",
            Self::prefix_to_table(self.catalog, self.schema),
            self.table
        )
        .into_bytes()
    }
}

impl<'a> From<&'a TableName> for TableNameKey<'a> {
    fn from(value: &'a TableName) -> Self {
        Self {
            catalog: &value.catalog_name,
            schema: &value.schema_name,
            table: &value.table_name,
        }
    }
}

impl From<TableNameKey<'_>> for TableName {
    fn from(value: TableNameKey<'_>) -> Self {
        Self {
            catalog_name: value.catalog.to_string(),
            schema_name: value.schema.to_string(),
            table_name: value.table.to_string(),
        }
    }
}

impl<'a> TryFrom<&'a str> for TableNameKey<'a> {
    type Error = Error;

    fn try_from(s: &'a str) -> Result<Self> {
        let captures = TABLE_NAME_KEY_PATTERN
            .captures(s)
            .context(InvalidTableMetadataSnafu {
                err_msg: format!("Illegal TableNameKey format: '{s}'"),
            })?;
        // Safety: pass the regex check above
        Ok(Self {
            catalog: captures.get(1).unwrap().as_str(),
            schema: captures.get(2).unwrap().as_str(),
            table: captures.get(3).unwrap().as_str(),
        })
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct TableNameValue {
    table_id: TableId,
}

impl TableNameValue {
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id
    }
}

pub struct TableNameManager {
    kv_backend: KvBackendRef,
}

impl Default for TableNameManager {
    fn default() -> Self {
        Self::new(Arc::new(MemoryKvBackend::default()))
    }
}

impl TableNameManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Builds a create table name transaction. It only executes while the primary keys comparing successes.
    pub(crate) fn build_create_txn(
        &self,
        key: &TableNameKey<'_>,
        table_id: TableId,
    ) -> Result<TxnRequest> {
        let mut txn = TxnRequest::default();

        let raw_key = key.as_raw_key();
        let value = TableNameValue::new(table_id);
        let raw_value = value.try_as_raw_value()?;

        txn.success.push(TxnOp::Put(raw_key, raw_value));

        Ok(txn)
    }

    /// Builds a update table name transaction. It only executes while the primary keys comparing successes.
    pub(crate) fn build_update_txn(
        &self,
        key: &TableNameKey<'_>,
        new_key: &TableNameKey<'_>,
        table_id: TableId,
    ) -> Result<TxnRequest> {
        let mut txn = TxnRequest::default();
        let raw_key = key.as_raw_key();

        txn.success.push(TxnOp::Delete(raw_key));

        let new_raw_key = new_key.as_raw_key();
        let value = TableNameValue::new(table_id);
        let raw_value = value.try_as_raw_value()?;

        txn.success.push(TxnOp::Put(new_raw_key, raw_value));

        Ok(txn)
    }

    /// Builds a delete table name transaction. It only executes while the primary keys comparing successes.
    pub(crate) fn build_delete_txn(
        &self,
        key: &TableNameKey<'_>,
        table_id: TableId,
    ) -> Result<TxnRequest> {
        let mut txn = TxnRequest::default();
        let raw_key = key.as_raw_key();
        let value = TableNameValue::new(table_id);
        let raw_value = value.try_as_raw_value()?;
        let removed_key = to_removed_key(&String::from_utf8_lossy(&raw_key));

        txn.success.push(TxnOp::Delete(raw_key));
        txn.success
            .push(TxnOp::Put(removed_key.into_bytes(), raw_value));

        Ok(txn)
    }

    /// Create TableName key and value. If the key already exists, check if the value is the same.
    pub async fn create(&self, key: &TableNameKey<'_>, table_id: TableId) -> Result<()> {
        let raw_key = key.as_raw_key();
        let value = TableNameValue::new(table_id);
        let raw_value = value.try_as_raw_value()?;
        let req = CompareAndPutRequest::new()
            .with_key(raw_key)
            .with_value(raw_value);
        let result = self.kv_backend.compare_and_put(req).await?;
        if !result.success {
            let Some(curr) = result
                .prev_kv
                .map(|x| TableNameValue::try_from_raw_value(x.value))
                .transpose()?
            else {
                return UnexpectedSnafu {
                    err_msg: format!("compare_and_put expect None but failed with current value None, key: {key}, value: {value:?}"),
                }.fail();
            };
            ensure!(
                curr.table_id == table_id,
                TableAlreadyExistsSnafu {
                    table_id: curr.table_id
                }
            );
        }
        Ok(())
    }

    /// Rename a TableNameKey to a new table name. Will check whether the TableNameValue matches the
    /// `expected_table_id` first. Can be executed again if the first invocation is successful.
    pub async fn rename(
        &self,
        key: TableNameKey<'_>,
        expected_table_id: TableId,
        new_table_name: &str,
    ) -> Result<()> {
        let new_key = TableNameKey::new(key.catalog, key.schema, new_table_name);

        if let Some(value) = self.get(key).await? {
            ensure!(
                value.table_id == expected_table_id,
                RenameTableSnafu {
                    reason: format!(
                        "the input table name '{}' and id '{expected_table_id}' not match",
                        Into::<TableName>::into(key)
                    ),
                }
            );

            let txn = Txn::new()
                .when(vec![
                    Compare::with_value(
                        key.as_raw_key(),
                        CompareOp::Equal,
                        value.try_as_raw_value()?,
                    ),
                    Compare::with_not_exist_value(new_key.as_raw_key(), CompareOp::Equal),
                ])
                .and_then(vec![
                    TxnOp::Delete(key.as_raw_key()),
                    TxnOp::Put(new_key.as_raw_key(), value.try_as_raw_value()?),
                ]);

            let resp = self.kv_backend.txn(txn).await?;
            ensure!(
                resp.succeeded,
                RenameTableSnafu {
                    reason: format!("txn failed with response: {:?}", resp.responses)
                }
            );
        } else {
            let Some(value) = self.get(new_key).await? else {
                // If we can't get the table by its original name, nor can we get by its altered
                // name, then the table must not exist at the first place.
                return TableNotExistSnafu {
                    table_name: TableName::from(key).to_string(),
                }
                .fail();
            };

            ensure!(
                value.table_id == expected_table_id,
                TableAlreadyExistsSnafu {
                    table_id: value.table_id
                }
            );
        }
        Ok(())
    }

    pub async fn get(&self, key: TableNameKey<'_>) -> Result<Option<TableNameValue>> {
        let raw_key = key.as_raw_key();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| TableNameValue::try_from_raw_value(x.value))
            .transpose()
    }

    pub async fn tables(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Vec<(String, TableNameValue)>> {
        let key = TableNameKey::prefix_to_table(catalog, schema).into_bytes();
        let req = RangeRequest::new().with_prefix(key);
        let resp = self.kv_backend.range(req).await?;

        let mut res = Vec::with_capacity(resp.kvs.len());
        for kv in resp.kvs {
            res.push((
                TableNameKey::strip_table_name(kv.key())?,
                TableNameValue::try_from_raw_value(kv.value)?,
            ))
        }
        Ok(res)
    }

    pub async fn remove(&self, key: TableNameKey<'_>) -> Result<()> {
        let raw_key = key.as_raw_key();
        let removed_key = to_removed_key(&String::from_utf8_lossy(&raw_key));
        let req = MoveValueRequest::new(raw_key, removed_key.as_bytes());
        let _ = self.kv_backend.move_value(req).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::KvBackend;

    #[tokio::test]
    async fn test_table_name_manager() {
        let backend = Arc::new(MemoryKvBackend::default());
        let manager = TableNameManager::new(backend.clone());

        for i in 1..=3 {
            let table_name = format!("table_{}", i);
            let key = TableNameKey::new("my_catalog", "my_schema", &table_name);
            assert!(manager.create(&key, i).await.is_ok());
        }

        let key = TableNameKey::new("my_catalog", "my_schema", "my_table");
        assert!(manager.create(&key, 99).await.is_ok());
        assert!(manager.create(&key, 99).await.is_ok());

        let result = manager.create(&key, 9).await;
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Table already exists, table_id: 99"));

        let value = manager.get(key).await.unwrap().unwrap();
        assert_eq!(value.table_id(), 99);
        let not_existed = TableNameKey::new("x", "y", "z");
        assert!(manager.get(not_existed).await.unwrap().is_none());

        assert!(manager.remove(key).await.is_ok());
        let kv = backend
            .get(b"__removed-__table_name/my_catalog/my_schema/my_table")
            .await
            .unwrap()
            .unwrap();
        let value = TableNameValue::try_from_raw_value(kv.value).unwrap();
        assert_eq!(value.table_id(), 99);

        let key = TableNameKey::new("my_catalog", "my_schema", "table_1");
        assert!(manager.rename(key, 1, "table_1_new").await.is_ok());
        assert!(manager.rename(key, 1, "table_1_new").await.is_ok());

        let result = manager.rename(key, 2, "table_1_new").await;
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Table already exists, table_id: 1"));

        let result = manager
            .rename(
                TableNameKey::new("my_catalog", "my_schema", "table_2"),
                22,
                "table_2_new",
            )
            .await;
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Failed to rename table, reason: the input table name 'my_catalog.my_schema.table_2' and id '22' not match"));

        let result = manager.rename(not_existed, 1, "zz").await;
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Table does not exist, table_name: x.y.z"));

        let tables = manager.tables("my_catalog", "my_schema").await.unwrap();
        assert_eq!(tables.len(), 3);
        assert_eq!(
            tables,
            vec![
                ("table_1_new".to_string(), TableNameValue::new(1)),
                ("table_2".to_string(), TableNameValue::new(2)),
                ("table_3".to_string(), TableNameValue::new(3))
            ]
        )
    }

    #[test]
    fn test_strip_table_name() {
        fn test_err(raw_key: &[u8]) {
            assert!(TableNameKey::strip_table_name(raw_key).is_err());
        }

        test_err(b"");
        test_err(vec![0u8, 159, 146, 150].as_slice()); // invalid UTF8 string
        test_err(b"invalid_prefix/my_catalog/my_schema/my_table");
        test_err(b"__table_name/");
        test_err(b"__table_name/invalid_len_1");
        test_err(b"__table_name/invalid_len_2/x");
        test_err(b"__table_name/invalid_len_4/x/y/z");
        test_err(b"__table_name/000_invalid_catalog/y/z");
        test_err(b"__table_name/x/000_invalid_schema/z");
        test_err(b"__table_name/x/y/000_invalid_table");

        fn test_ok(table_name: &str) {
            assert_eq!(
                table_name,
                TableNameKey::strip_table_name(
                    format!("__table_name/my_catalog/my_schema/{}", table_name).as_bytes()
                )
                .unwrap()
            );
        }
        test_ok("my_table");
        test_ok("cpu:metrics");
        test_ok(":cpu:metrics");
    }

    #[test]
    fn test_serde() {
        let key = TableNameKey::new("my_catalog", "my_schema", "my_table");
        let raw_key = key.as_raw_key();
        assert_eq!(
            b"__table_name/my_catalog/my_schema/my_table",
            raw_key.as_slice()
        );

        let value = TableNameValue::new(1);
        let literal = br#"{"table_id":1}"#;

        assert_eq!(value.try_as_raw_value().unwrap(), literal);
        assert_eq!(
            TableNameValue::try_from_raw_value(literal.to_vec()).unwrap(),
            value
        );
    }
}
