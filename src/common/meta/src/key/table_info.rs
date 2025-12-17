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

use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use table::metadata::{RawTableInfo, TableId};
use table::table_name::TableName;
use table::table_reference::TableReference;

use crate::ddl::utils::region_storage_path;
use crate::error::{InvalidMetadataSnafu, Result};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    DeserializedValueWithBytes, MetadataKey, MetadataValue, TABLE_INFO_KEY_PATTERN,
    TABLE_INFO_KEY_PREFIX,
};
use crate::kv_backend::KvBackendRef;
use crate::kv_backend::txn::Txn;
use crate::rpc::store::BatchGetRequest;

/// The key stores the metadata of the table.
///
/// The layout: `__table_info/{table_id}`.
#[derive(Debug, PartialEq)]
pub struct TableInfoKey {
    table_id: TableId,
}

impl TableInfoKey {
    /// Returns a new [TableInfoKey].
    pub fn new(table_id: TableId) -> Self {
        Self { table_id }
    }
}

impl Display for TableInfoKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", TABLE_INFO_KEY_PREFIX, self.table_id)
    }
}

impl MetadataKey<'_, TableInfoKey> for TableInfoKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<TableInfoKey> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "TableInfoKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures = TABLE_INFO_KEY_PATTERN
            .captures(key)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid TableInfoKey '{key}'"),
            })?;
        // Safety: pass the regex check above
        let table_id = captures[1].parse::<TableId>().unwrap();
        Ok(TableInfoKey { table_id })
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

    pub fn update(&self, new_table_info: RawTableInfo) -> Self {
        Self {
            table_info: new_table_info,
            version: self.version + 1,
        }
    }

    pub(crate) fn with_update<F>(&self, update: F) -> Self
    where
        F: FnOnce(&mut RawTableInfo),
    {
        let mut new_table_info = self.table_info.clone();
        update(&mut new_table_info);
        Self {
            table_info: new_table_info,
            version: self.version + 1,
        }
    }

    pub fn table_ref(&self) -> TableReference<'_> {
        TableReference::full(
            &self.table_info.catalog_name,
            &self.table_info.schema_name,
            &self.table_info.name,
        )
    }

    pub fn table_name(&self) -> TableName {
        TableName {
            catalog_name: self.table_info.catalog_name.clone(),
            schema_name: self.table_info.schema_name.clone(),
            table_name: self.table_info.name.clone(),
        }
    }

    /// Builds storage path for all regions in table.
    pub fn region_storage_path(&self) -> String {
        region_storage_path(&self.table_info.catalog_name, &self.table_info.schema_name)
    }
}

pub type TableInfoManagerRef = Arc<TableInfoManager>;

#[derive(Clone)]
pub struct TableInfoManager {
    kv_backend: KvBackendRef,
}
pub type TableInfoDecodeResult = Result<Option<DeserializedValueWithBytes<TableInfoValue>>>;

impl TableInfoManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Builds a create table info transaction, it expected the `__table_info/{table_id}` wasn't occupied.
    pub(crate) fn build_create_txn(
        &self,
        table_id: TableId,
        table_info_value: &TableInfoValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> TableInfoDecodeResult + use<>,
    )> {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.to_bytes();

        let txn = Txn::put_if_not_exists(raw_key.clone(), table_info_value.try_as_raw_value()?);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Builds a update table info transaction, it expected the remote value equals the `current_current_table_info_value`.
    /// It retrieves the latest value if the comparing failed.
    pub(crate) fn build_update_txn(
        &self,
        table_id: TableId,
        current_table_info_value: &DeserializedValueWithBytes<TableInfoValue>,
        new_table_info_value: &TableInfoValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> TableInfoDecodeResult + use<>,
    )> {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.to_bytes();
        let raw_value = current_table_info_value.get_raw_bytes();
        let new_raw_value: Vec<u8> = new_table_info_value.try_as_raw_value()?;

        let txn = Txn::compare_and_put(raw_key.clone(), raw_value, new_raw_value);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(raw_key)),
        ))
    }

    /// Checks if the table exists.
    pub async fn exists(&self, table_id: TableId) -> Result<bool> {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.to_bytes();
        self.kv_backend.exists(&raw_key).await
    }

    pub async fn get(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TableInfoValue>>> {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.to_bytes();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| DeserializedValueWithBytes::from_inner_slice(&x.value))
            .transpose()
    }

    pub async fn batch_get(
        &self,
        table_ids: &[TableId],
    ) -> Result<HashMap<TableId, TableInfoValue>> {
        let lookup_table = table_ids
            .iter()
            .map(|id| (TableInfoKey::new(*id).to_bytes(), id))
            .collect::<HashMap<_, _>>();

        let resp = self
            .kv_backend
            .batch_get(BatchGetRequest {
                keys: lookup_table.keys().cloned().collect::<Vec<_>>(),
            })
            .await?;

        let values = resp
            .kvs
            .iter()
            .map(|kv| {
                Ok((
                    // Safety: must exist.
                    **lookup_table.get(kv.key()).unwrap(),
                    TableInfoValue::try_from_raw_value(&kv.value)?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(values)
    }

    /// Returns batch of `DeserializedValueWithBytes<TableInfoValue>`.
    pub async fn batch_get_raw(
        &self,
        table_ids: &[TableId],
    ) -> Result<HashMap<TableId, DeserializedValueWithBytes<TableInfoValue>>> {
        let lookup_table = table_ids
            .iter()
            .map(|id| (TableInfoKey::new(*id).to_bytes(), id))
            .collect::<HashMap<_, _>>();

        let resp = self
            .kv_backend
            .batch_get(BatchGetRequest {
                keys: lookup_table.keys().cloned().collect::<Vec<_>>(),
            })
            .await?;

        let values = resp
            .kvs
            .iter()
            .map(|kv| {
                Ok((
                    // Safety: must exist.
                    **lookup_table.get(kv.key()).unwrap(),
                    DeserializedValueWithBytes::from_inner_slice(&kv.value)?,
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(values)
    }
}

#[cfg(test)]
mod tests {

    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, RawSchema, Schema};
    use table::metadata::{RawTableMeta, TableIdent, TableType};

    use super::*;

    #[test]
    fn test_deserialization_compatibility() {
        let old_fmt = r#"{"version":1,"table_info":{"ident":{"table_id":8714,"version":0},"name":"go_gc_duration_seconds","desc":"Created on insertion","catalog_name":"e87lehzy63d4cloud_docs_test","schema_name":"public","meta":{"schema":{"column_schemas":[{"name":"instance","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"job","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"quantile","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"greptime_timestamp","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}},{"name":"greptime_value","data_type":{"Float64":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}}],"timestamp_index":3,"version":0},"primary_key_indices":[0,1,2],"value_indices":[],"engine":"mito","next_column_id":5,"region_numbers":[],"engine_options":{},"options":{"write_buffer_size":null,"ttl":null,"extra_options":{}},"created_on":"1970-01-01T00:00:00Z"},"table_type":"Base"}}"#;
        let new_fmt = r#"{"version":1,"table_info":{"ident":{"table_id":8714,"version":0},"name":"go_gc_duration_seconds","desc":"Created on insertion","catalog_name":"e87lehzy63d4cloud_docs_test","schema_name":"public","meta":{"schema":{"column_schemas":[{"name":"instance","data_type":{"String":{"size_type":"Utf8"}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"job","data_type":{"String":{"size_type":"Utf8"}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"quantile","data_type":{"String":{"size_type":"Utf8"}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"greptime_timestamp","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}},{"name":"greptime_value","data_type":{"Float64":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}}],"timestamp_index":3,"version":0},"primary_key_indices":[0,1,2],"value_indices":[],"engine":"mito","next_column_id":5,"region_numbers":[],"engine_options":{},"options":{"write_buffer_size":null,"ttl":null,"extra_options":{}},"created_on":"1970-01-01T00:00:00Z"},"table_type":"Base"}}"#;

        let v = TableInfoValue::try_from_raw_value(old_fmt.as_bytes()).unwrap();
        let new_v = TableInfoValue::try_from_raw_value(new_fmt.as_bytes()).unwrap();
        assert_eq!(v, new_v);
        assert_eq!(v.table_info.meta.created_on, v.table_info.meta.updated_on);
        assert!(v.table_info.meta.partition_key_indices.is_empty());
    }

    #[test]
    fn test_key_serialization() {
        let key = TableInfoKey::new(42);
        let raw_key = key.to_bytes();
        assert_eq!(raw_key, b"__table_info/42");
    }

    #[test]
    fn test_key_deserialization() {
        let expected = TableInfoKey::new(42);
        let key = TableInfoKey::from_bytes(b"__table_info/42").unwrap();
        assert_eq!(key, expected);
    }

    #[test]
    fn test_value_serialization() {
        let value = TableInfoValue {
            table_info: new_table_info(42),
            version: 1,
        };
        let serialized = value.try_as_raw_value().unwrap();
        let deserialized = TableInfoValue::try_from_raw_value(&serialized).unwrap();
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
            updated_on: chrono::DateTime::default(),
            primary_key_indices: vec![0, 1],
            next_column_id: 3,
            value_indices: vec![2, 3],
            options: Default::default(),
            region_numbers: vec![1],
            partition_key_indices: vec![],
            column_ids: vec![],
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
