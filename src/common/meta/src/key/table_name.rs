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
use snafu::OptionExt;
use table::metadata::TableId;

use super::{TABLE_NAME_KEY_PATTERN, TABLE_NAME_KEY_PREFIX};
use crate::error::{InvalidTableMetadataSnafu, Result};
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::memory::MemoryKvBackend;
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{CompareAndPutRequest, MoveValueRequest, RangeRequest};
use crate::table_name::TableName;

#[derive(Debug)]
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

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct TableNameValue {
    table_id: TableId,
}

impl TableNameValue {
    fn new(table_id: TableId) -> Self {
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

    pub async fn create(&self, key: &TableNameKey<'_>, table_id: TableId) -> Result<bool> {
        let raw_key = key.as_raw_key();
        let value = TableNameValue::new(table_id);
        let raw_value = value.try_as_raw_value()?;
        let req = CompareAndPutRequest::new()
            .with_key(raw_key)
            .with_value(raw_value);
        let result = self.kv_backend.compare_and_put(req).await?;
        Ok(result.success)
    }

    pub async fn get(&self, key: &TableNameKey<'_>) -> Result<Option<TableNameValue>> {
        let raw_key = key.as_raw_key();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| TableNameValue::try_from_raw_value(x.value))
            .transpose()
    }

    pub async fn tables(&self, catalog: &str, schema: &str) -> Result<Vec<String>> {
        let key = TableNameKey::prefix_to_table(catalog, schema).into_bytes();
        let req = RangeRequest::new().with_prefix(key);
        let resp = self.kv_backend.range(req).await?;
        let table_names = resp
            .kvs
            .into_iter()
            .map(|kv| TableNameKey::strip_table_name(kv.key()))
            .collect::<Result<Vec<_>>>()?;
        Ok(table_names)
    }

    pub async fn remove(&self, key: &TableNameKey<'_>) -> Result<()> {
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
            assert!(manager.create(&key, i).await.unwrap());
        }

        let key = TableNameKey::new("my_catalog", "my_schema", "my_table");
        assert!(manager.create(&key, 99).await.unwrap());
        assert!(!manager.create(&key, 99).await.unwrap());

        let value = manager.get(&key).await.unwrap().unwrap();
        assert_eq!(value.table_id(), 99);
        let not_existed = TableNameKey::new("x", "y", "z");
        assert!(manager.get(&not_existed).await.unwrap().is_none());

        assert!(manager.remove(&key).await.is_ok());
        let kv = backend
            .get(b"__removed-__table_name/my_catalog/my_schema/my_table")
            .await
            .unwrap()
            .unwrap();
        let value = TableNameValue::try_from_raw_value(kv.value).unwrap();
        assert_eq!(value.table_id(), 99);

        let tables = manager.tables("my_catalog", "my_schema").await.unwrap();
        assert_eq!(tables.len(), 3);
        assert_eq!(tables, vec!["table_1", "table_2", "table_3"]);
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
