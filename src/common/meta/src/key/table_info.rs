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

use serde::{Deserialize, Serialize};
use table::engine::TableReference;
use table::metadata::{RawTableInfo, TableId};

use super::{DeserializedValueWithBytes, TableMetaValue, TABLE_INFO_KEY_PREFIX};
use crate::error::Result;
use crate::key::{to_removed_key, TableMetaKey};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp, TxnOpResponse};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::BatchGetRequest;
use crate::table_name::TableName;

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

    pub(crate) fn update(&self, new_table_info: RawTableInfo) -> Self {
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

    pub fn table_ref(&self) -> TableReference {
        TableReference::full(
            &self.table_info.catalog_name,
            &self.table_info.schema_name,
            &self.table_info.name,
        )
    }

    pub fn table_name(&self) -> TableName {
        TableName {
            catalog_name: self.table_info.catalog_name.to_string(),
            schema_name: self.table_info.schema_name.to_string(),
            table_name: self.table_info.name.to_string(),
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

    pub(crate) fn build_get_txn(
        &self,
        table_id: TableId,
    ) -> (
        Txn,
        impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<DeserializedValueWithBytes<TableInfoValue>>>,
    ) {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.as_raw_key();
        let txn = Txn::new().and_then(vec![TxnOp::Get(raw_key.clone())]);

        (txn, Self::build_decode_fn(raw_key))
    }

    /// Builds a create table info transaction, it expected the `__table_info/{table_id}` wasn't occupied.
    pub(crate) fn build_create_txn(
        &self,
        table_id: TableId,
        table_info_value: &TableInfoValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<DeserializedValueWithBytes<TableInfoValue>>>,
    )> {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.as_raw_key();

        let txn = Txn::new()
            .when(vec![Compare::with_not_exist_value(
                raw_key.clone(),
                CompareOp::Equal,
            )])
            .and_then(vec![TxnOp::Put(
                raw_key.clone(),
                table_info_value.try_as_raw_value()?,
            )])
            .or_else(vec![TxnOp::Get(raw_key.clone())]);

        Ok((txn, Self::build_decode_fn(raw_key)))
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
        impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<DeserializedValueWithBytes<TableInfoValue>>>,
    )> {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.as_raw_key();
        let raw_value = current_table_info_value.into_bytes();

        let txn = Txn::new()
            .when(vec![Compare::with_value(
                raw_key.clone(),
                CompareOp::Equal,
                raw_value,
            )])
            .and_then(vec![TxnOp::Put(
                raw_key.clone(),
                new_table_info_value.try_as_raw_value()?,
            )])
            .or_else(vec![TxnOp::Get(raw_key.clone())]);

        Ok((txn, Self::build_decode_fn(raw_key)))
    }

    /// Builds a delete table info transaction.
    pub(crate) fn build_delete_txn(
        &self,
        table_id: TableId,
        table_info_value: &DeserializedValueWithBytes<TableInfoValue>,
    ) -> Result<Txn> {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.as_raw_key();
        let raw_value = table_info_value.into_bytes();
        let removed_key = to_removed_key(&String::from_utf8_lossy(&raw_key));

        let txn = Txn::new().and_then(vec![
            TxnOp::Delete(raw_key),
            TxnOp::Put(removed_key.into_bytes(), raw_value),
        ]);

        Ok(txn)
    }

    fn build_decode_fn(
        raw_key: Vec<u8>,
    ) -> impl FnOnce(&Vec<TxnOpResponse>) -> Result<Option<DeserializedValueWithBytes<TableInfoValue>>>
    {
        move |kvs: &Vec<TxnOpResponse>| {
            kvs.iter()
                .filter_map(|resp| {
                    if let TxnOpResponse::ResponseGet(r) = resp {
                        Some(r)
                    } else {
                        None
                    }
                })
                .flat_map(|r| &r.kvs)
                .find(|kv| kv.key == raw_key)
                .map(|kv| DeserializedValueWithBytes::from_inner_slice(&kv.value))
                .transpose()
        }
    }

    #[cfg(test)]
    pub async fn get_removed(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TableInfoValue>>> {
        let key = TableInfoKey::new(table_id).to_string();
        let removed_key = to_removed_key(&key).into_bytes();
        self.kv_backend
            .get(&removed_key)
            .await?
            .map(|x| DeserializedValueWithBytes::from_inner_slice(&x.value))
            .transpose()
    }

    pub async fn get(
        &self,
        table_id: TableId,
    ) -> Result<Option<DeserializedValueWithBytes<TableInfoValue>>> {
        let key = TableInfoKey::new(table_id);
        let raw_key = key.as_raw_key();
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
            .map(|id| (TableInfoKey::new(*id).as_raw_key(), id))
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
}

#[cfg(test)]
mod tests {

    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, RawSchema, Schema};
    use table::metadata::{RawTableMeta, TableIdent, TableType};

    use super::*;

    #[test]
    fn test_deserialization_compatibility() {
        let s = r#"{"version":1,"table_info":{"ident":{"table_id":8714,"version":0},"name":"go_gc_duration_seconds","desc":"Created on insertion","catalog_name":"e87lehzy63d4cloud_docs_test","schema_name":"public","meta":{"schema":{"column_schemas":[{"name":"instance","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"job","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"quantile","data_type":{"String":null},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}},{"name":"greptime_timestamp","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}},{"name":"greptime_value","data_type":{"Float64":{}},"is_nullable":true,"is_time_index":false,"default_constraint":null,"metadata":{}}],"timestamp_index":3,"version":0},"primary_key_indices":[0,1,2],"value_indices":[],"engine":"mito","next_column_id":5,"region_numbers":[],"engine_options":{},"options":{"write_buffer_size":null,"ttl":null,"extra_options":{}},"created_on":"1970-01-01T00:00:00Z"},"table_type":"Base"}}"#;
        let v = TableInfoValue::try_from_raw_value(s.as_bytes()).unwrap();
        assert!(v.table_info.meta.partition_key_indices.is_empty());
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
            primary_key_indices: vec![0, 1],
            next_column_id: 3,
            value_indices: vec![2, 3],
            options: Default::default(),
            region_numbers: vec![1],
            partition_key_indices: vec![],
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
