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

use futures::stream::BoxStream;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use store_api::storage::RegionNumber;
use table::metadata::TableId;

use super::MetadataKey;
use crate::error::{InvalidMetadataSnafu, Result};
use crate::key::{
    MetadataValue, RegionDistribution, DATANODE_TABLE_KEY_PATTERN, DATANODE_TABLE_KEY_PREFIX,
};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;
use crate::DatanodeId;

#[serde_with::serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
/// RegionInfo
/// For compatible reason, DON'T modify the field name.
pub struct RegionInfo {
    #[serde(default)]
    /// The table engine, it SHOULD be immutable after created.
    pub engine: String,
    /// The region storage path, it SHOULD be immutable after created.
    #[serde(default)]
    pub region_storage_path: String,
    /// The region options.
    #[serde(default)]
    pub region_options: HashMap<String, String>,
    /// The per-region wal options.
    /// Key: region number. Value: the encoded wal options of the region.
    #[serde(default)]
    #[serde_as(as = "HashMap<serde_with::DisplayFromStr, _>")]
    pub region_wal_options: HashMap<RegionNumber, String>,
}

/// The key mapping {datanode_id} to {table_id}
///
/// The layout: `__dn_table/{datanode_id}/{table_id}`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct DatanodeTableKey {
    pub datanode_id: DatanodeId,
    pub table_id: TableId,
}

impl DatanodeTableKey {
    pub fn new(datanode_id: DatanodeId, table_id: TableId) -> Self {
        Self {
            datanode_id,
            table_id,
        }
    }

    pub fn prefix(datanode_id: DatanodeId) -> String {
        format!("{}/{datanode_id}/", DATANODE_TABLE_KEY_PREFIX)
    }
}

impl<'a> MetadataKey<'a, DatanodeTableKey> for DatanodeTableKey {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Result<DatanodeTableKey> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "DatanodeTableKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        let captures = DATANODE_TABLE_KEY_PATTERN
            .captures(key)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid DatanodeTableKey '{key}'"),
            })?;
        // Safety: pass the regex check above
        let datanode_id = captures[1].parse::<DatanodeId>().unwrap();
        let table_id = captures[2].parse::<TableId>().unwrap();
        Ok(DatanodeTableKey {
            datanode_id,
            table_id,
        })
    }
}

impl Display for DatanodeTableKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", Self::prefix(self.datanode_id), self.table_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DatanodeTableValue {
    pub table_id: TableId,
    pub regions: Vec<RegionNumber>,
    #[serde(flatten)]
    pub region_info: RegionInfo,
    version: u64,
}

impl DatanodeTableValue {
    pub fn new(table_id: TableId, regions: Vec<RegionNumber>, region_info: RegionInfo) -> Self {
        Self {
            table_id,
            regions,
            region_info,
            version: 0,
        }
    }
}

/// Decodes `KeyValue` to ((),`DatanodeTableValue`)
pub fn datanode_table_value_decoder(kv: KeyValue) -> Result<DatanodeTableValue> {
    DatanodeTableValue::try_from_raw_value(&kv.value)
}

pub struct DatanodeTableManager {
    kv_backend: KvBackendRef,
}

impl DatanodeTableManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    pub async fn get(&self, key: &DatanodeTableKey) -> Result<Option<DatanodeTableValue>> {
        self.kv_backend
            .get(&key.to_bytes())
            .await?
            .map(|kv| DatanodeTableValue::try_from_raw_value(&kv.value))
            .transpose()
    }

    pub fn tables(
        &self,
        datanode_id: DatanodeId,
    ) -> BoxStream<'static, Result<DatanodeTableValue>> {
        let start_key = DatanodeTableKey::prefix(datanode_id);
        let req = RangeRequest::new().with_prefix(start_key.as_bytes());

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            DEFAULT_PAGE_SIZE,
            Arc::new(datanode_table_value_decoder),
        )
        .into_stream();

        Box::pin(stream)
    }

    /// Builds the create datanode table transactions. It only executes while the primary keys comparing successes.
    pub fn build_create_txn(
        &self,
        table_id: TableId,
        engine: &str,
        region_storage_path: &str,
        region_options: HashMap<String, String>,
        region_wal_options: HashMap<RegionNumber, String>,
        distribution: RegionDistribution,
    ) -> Result<Txn> {
        let txns = distribution
            .into_iter()
            .map(|(datanode_id, regions)| {
                let key = DatanodeTableKey::new(datanode_id, table_id);
                let val = DatanodeTableValue::new(
                    table_id,
                    regions,
                    RegionInfo {
                        engine: engine.to_string(),
                        region_storage_path: region_storage_path.to_string(),
                        region_options: region_options.clone(),
                        // FIXME(weny): Before we store all region wal options into table metadata or somewhere,
                        // We must store all region wal options.
                        region_wal_options: region_wal_options.clone(),
                    },
                );

                Ok(TxnOp::Put(key.to_bytes(), val.try_as_raw_value()?))
            })
            .collect::<Result<Vec<_>>>()?;

        let txn = Txn::new().and_then(txns);

        Ok(txn)
    }

    /// Builds the update datanode table transactions. It only executes while the primary keys comparing successes.
    pub(crate) fn build_update_txn(
        &self,
        table_id: TableId,
        region_info: RegionInfo,
        current_region_distribution: RegionDistribution,
        new_region_distribution: RegionDistribution,
        new_region_options: &HashMap<String, String>,
        new_region_wal_options: &HashMap<RegionNumber, String>,
    ) -> Result<Txn> {
        let mut opts = Vec::new();

        // Removes the old datanode table key value pairs
        for current_datanode in current_region_distribution.keys() {
            if !new_region_distribution.contains_key(current_datanode) {
                let key = DatanodeTableKey::new(*current_datanode, table_id);
                let raw_key = key.to_bytes();
                opts.push(TxnOp::Delete(raw_key))
            }
        }

        let need_update_options = region_info.region_options != *new_region_options;
        let need_update_wal_options = region_info.region_wal_options != *new_region_wal_options;

        for (datanode, regions) in new_region_distribution.into_iter() {
            let need_update =
                if let Some(current_region) = current_region_distribution.get(&datanode) {
                    // Updates if need.
                    *current_region != regions || need_update_options || need_update_wal_options
                } else {
                    true
                };
            if need_update {
                let key = DatanodeTableKey::new(datanode, table_id);
                let raw_key = key.to_bytes();
                // FIXME(weny): add unit tests.
                let mut new_region_info = region_info.clone();
                if need_update_options {
                    new_region_info
                        .region_options
                        .clone_from(new_region_options);
                }
                if need_update_wal_options {
                    new_region_info
                        .region_wal_options
                        .clone_from(new_region_wal_options);
                }
                let val = DatanodeTableValue::new(table_id, regions, new_region_info)
                    .try_as_raw_value()?;
                opts.push(TxnOp::Put(raw_key, val));
            }
        }

        let txn = Txn::new().and_then(opts);
        Ok(txn)
    }

    /// Builds the delete datanode table transactions. It only executes while the primary keys comparing successes.
    pub fn build_delete_txn(
        &self,
        table_id: TableId,
        distribution: RegionDistribution,
    ) -> Result<Txn> {
        let txns = distribution
            .into_keys()
            .map(|datanode_id| {
                let key = DatanodeTableKey::new(datanode_id, table_id);
                let raw_key = key.to_bytes();

                Ok(TxnOp::Delete(raw_key))
            })
            .collect::<Result<Vec<_>>>()?;

        let txn = Txn::new().and_then(txns);

        Ok(txn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization() {
        let key = DatanodeTableKey {
            datanode_id: 1,
            table_id: 2,
        };
        let raw_key = key.to_bytes();
        assert_eq!(raw_key, b"__dn_table/1/2");

        let value = DatanodeTableValue {
            table_id: 42,
            regions: vec![1, 2, 3],
            region_info: RegionInfo::default(),
            version: 1,
        };
        let literal = br#"{"table_id":42,"regions":[1,2,3],"engine":"","region_storage_path":"","region_options":{},"region_wal_options":{},"version":1}"#;

        let raw_value = value.try_as_raw_value().unwrap();
        assert_eq!(raw_value, literal);

        let actual = DatanodeTableValue::try_from_raw_value(literal).unwrap();
        assert_eq!(actual, value);

        // test serde default
        let raw_str = br#"{"table_id":42,"regions":[1,2,3],"version":1}"#;
        let parsed = DatanodeTableValue::try_from_raw_value(raw_str);
        assert!(parsed.is_ok());
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct StringHashMap {
        inner: HashMap<String, String>,
    }

    #[serde_with::serde_as]
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct IntegerHashMap {
        #[serde_as(as = "HashMap<serde_with::DisplayFromStr, _>")]
        inner: HashMap<u32, String>,
    }

    #[test]
    fn test_serde_with_integer_hash_map() {
        let map = StringHashMap {
            inner: HashMap::from([
                ("1".to_string(), "aaa".to_string()),
                ("2".to_string(), "bbb".to_string()),
                ("3".to_string(), "ccc".to_string()),
            ]),
        };
        let encoded = serde_json::to_string(&map).unwrap();
        let decoded: IntegerHashMap = serde_json::from_str(&encoded).unwrap();
        assert_eq!(
            IntegerHashMap {
                inner: HashMap::from([
                    (1, "aaa".to_string()),
                    (2, "bbb".to_string()),
                    (3, "ccc".to_string()),
                ]),
            },
            decoded
        );

        let map = IntegerHashMap {
            inner: HashMap::from([
                (1, "aaa".to_string()),
                (2, "bbb".to_string()),
                (3, "ccc".to_string()),
            ]),
        };
        let encoded = serde_json::to_string(&map).unwrap();
        let decoded: StringHashMap = serde_json::from_str(&encoded).unwrap();
        assert_eq!(
            StringHashMap {
                inner: HashMap::from([
                    ("1".to_string(), "aaa".to_string()),
                    ("2".to_string(), "bbb".to_string()),
                    ("3".to_string(), "ccc".to_string()),
                ]),
            },
            decoded
        );
    }

    // This test intends to ensure both the `serde_json::to_string` + `serde_json::from_str`
    // and `serde_json::to_vec` + `serde_json::from_slice` work for `DatanodeTableValue`.
    // Warning: if the key of `region_wal_options` is of type non-String, this test would fail.
    #[test]
    fn test_serde_with_region_info() {
        let region_info = RegionInfo {
            engine: "test_engine".to_string(),
            region_storage_path: "test_storage_path".to_string(),
            region_options: HashMap::from([
                ("a".to_string(), "aa".to_string()),
                ("b".to_string(), "bb".to_string()),
                ("c".to_string(), "cc".to_string()),
            ]),
            region_wal_options: HashMap::from([
                (1, "aaa".to_string()),
                (2, "bbb".to_string()),
                (3, "ccc".to_string()),
            ]),
        };
        let table_value = DatanodeTableValue {
            table_id: 1,
            regions: vec![],
            region_info,
            version: 1,
        };

        let encoded = serde_json::to_string(&table_value).unwrap();
        let decoded = serde_json::from_str(&encoded).unwrap();
        assert_eq!(table_value, decoded);

        let encoded = serde_json::to_vec(&table_value).unwrap();
        let decoded = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(table_value, decoded);
    }

    #[test]
    fn test_deserialization() {
        fn test_err(raw_key: &[u8]) {
            let result = DatanodeTableKey::from_bytes(raw_key);
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

        let key = DatanodeTableKey::from_bytes(b"__dn_table/11/21").unwrap();
        assert_eq!(DatanodeTableKey::new(11, 21), key);
    }
}
