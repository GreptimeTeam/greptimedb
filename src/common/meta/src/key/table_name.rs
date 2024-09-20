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

use std::collections::HashMap;
use std::fmt::Display;
use std::sync::Arc;

use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use table::metadata::TableId;
use table::table_name::TableName;

use super::{MetadataKey, MetadataValue, TABLE_NAME_KEY_PATTERN, TABLE_NAME_KEY_PREFIX};
use crate::error::{Error, InvalidMetadataSnafu, Result};
use crate::kv_backend::memory::MemoryKvBackend;
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::{BatchGetRequest, RangeRequest};
use crate::rpc::KeyValue;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
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
        format!("{}/{}/{}/", TABLE_NAME_KEY_PREFIX, catalog, schema)
    }
}

impl Display for TableNameKey<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}{}",
            Self::prefix_to_table(self.catalog, self.schema),
            self.table
        )
    }
}

impl<'a> MetadataKey<'a, TableNameKey<'a>> for TableNameKey<'_> {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<TableNameKey<'a>> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "TableNameKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures = TABLE_NAME_KEY_PATTERN
            .captures(key)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid TableNameKey '{key}'"),
            })?;
        let catalog = captures.get(1).unwrap().as_str();
        let schema = captures.get(2).unwrap().as_str();
        let table = captures.get(3).unwrap().as_str();
        Ok(TableNameKey {
            catalog,
            schema,
            table,
        })
    }
}

/// Decodes `KeyValue` to ({table_name}, TableNameValue)
pub fn table_decoder(kv: KeyValue) -> Result<(String, TableNameValue)> {
    let table_name_key = TableNameKey::from_bytes(&kv.key)?;
    let table_name_value = TableNameValue::try_from_raw_value(&kv.value)?;

    Ok((table_name_key.table.to_string(), table_name_value))
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
            .context(InvalidMetadataSnafu {
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

pub type TableNameManagerRef = Arc<TableNameManager>;

#[derive(Clone)]
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
    ) -> Result<Txn> {
        let raw_key = key.to_bytes();
        let value = TableNameValue::new(table_id);
        let raw_value = value.try_as_raw_value()?;

        let txn = Txn::new().and_then(vec![TxnOp::Put(raw_key, raw_value)]);

        Ok(txn)
    }

    /// Builds a update table name transaction. It only executes while the primary keys comparing successes.
    pub(crate) fn build_update_txn(
        &self,
        key: &TableNameKey<'_>,
        new_key: &TableNameKey<'_>,
        table_id: TableId,
    ) -> Result<Txn> {
        let raw_key = key.to_bytes();
        let new_raw_key = new_key.to_bytes();
        let value = TableNameValue::new(table_id);
        let raw_value = value.try_as_raw_value()?;

        let txn = Txn::new().and_then(vec![
            TxnOp::Delete(raw_key),
            TxnOp::Put(new_raw_key, raw_value),
        ]);
        Ok(txn)
    }

    pub async fn get(&self, key: TableNameKey<'_>) -> Result<Option<TableNameValue>> {
        let raw_key = key.to_bytes();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| TableNameValue::try_from_raw_value(&x.value))
            .transpose()
    }

    pub async fn batch_get(
        &self,
        keys: Vec<TableNameKey<'_>>,
    ) -> Result<Vec<Option<TableNameValue>>> {
        let raw_keys = keys
            .into_iter()
            .map(|key| key.to_bytes())
            .collect::<Vec<_>>();
        let req = BatchGetRequest::new().with_keys(raw_keys.clone());
        let res = self.kv_backend.batch_get(req).await?;
        let kvs = res
            .kvs
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<HashMap<_, _>>();
        let mut array = vec![None; raw_keys.len()];
        for (i, key) in raw_keys.into_iter().enumerate() {
            let v = kvs.get(&key);
            array[i] = v
                .map(|v| TableNameValue::try_from_raw_value(v))
                .transpose()?;
        }
        Ok(array)
    }

    pub async fn exists(&self, key: TableNameKey<'_>) -> Result<bool> {
        let raw_key = key.to_bytes();
        self.kv_backend.exists(&raw_key).await
    }

    pub fn tables(
        &self,
        catalog: &str,
        schema: &str,
    ) -> BoxStream<'static, Result<(String, TableNameValue)>> {
        let key = TableNameKey::prefix_to_table(catalog, schema).into_bytes();
        let req = RangeRequest::new().with_prefix(key);

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(table_decoder),
        )
        .into_stream();

        Box::pin(stream)
    }
}

#[cfg(test)]
mod tests {

    use futures::StreamExt;

    use super::*;
    use crate::kv_backend::KvBackend;
    use crate::rpc::store::PutRequest;

    #[test]
    fn test_strip_table_name() {
        fn test_err(bytes: &[u8]) {
            assert!(TableNameKey::from_bytes(bytes).is_err());
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
                TableNameKey::from_bytes(
                    format!("__table_name/my_catalog/my_schema/{}", table_name).as_bytes()
                )
                .unwrap()
                .table
            );
        }
        test_ok("my_table");
        test_ok("cpu:metrics");
        test_ok(":cpu:metrics");
        test_ok("sys.cpu.system");
        test_ok("foo-bar");
    }

    #[test]
    fn test_serialization() {
        let key = TableNameKey::new("my_catalog", "my_schema", "my_table");
        let raw_key = key.to_bytes();
        assert_eq!(
            b"__table_name/my_catalog/my_schema/my_table",
            raw_key.as_slice()
        );
        let table_name_key =
            TableNameKey::from_bytes(b"__table_name/my_catalog/my_schema/my_table").unwrap();
        assert_eq!(table_name_key.catalog, "my_catalog");
        assert_eq!(table_name_key.schema, "my_schema");
        assert_eq!(table_name_key.table, "my_table");

        let value = TableNameValue::new(1);
        let literal = br#"{"table_id":1}"#;

        assert_eq!(value.try_as_raw_value().unwrap(), literal);
        assert_eq!(TableNameValue::try_from_raw_value(literal).unwrap(), value);
    }

    #[tokio::test]
    async fn test_prefix_scan_tables() {
        let memory_kv = Arc::new(MemoryKvBackend::<crate::error::Error>::new());
        memory_kv
            .put(PutRequest {
                key: TableNameKey {
                    catalog: "greptime",
                    schema: "👉",
                    table: "t",
                }
                .to_bytes(),
                value: vec![],
                prev_kv: false,
            })
            .await
            .unwrap();
        memory_kv
            .put(PutRequest {
                key: TableNameKey {
                    catalog: "greptime",
                    schema: "👉👈",
                    table: "t",
                }
                .to_bytes(),
                value: vec![],
                prev_kv: false,
            })
            .await
            .unwrap();

        let manager = TableNameManager::new(memory_kv);
        let items = manager.tables("greptime", "👉").collect::<Vec<_>>().await;
        assert_eq!(items.len(), 1);
    }
}
