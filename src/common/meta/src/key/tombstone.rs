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

use common_telemetry::debug;
use futures_util::stream::BoxStream;
use snafu::ensure;

use crate::error::{self, Result};
use crate::key::TABLE_NAME_KEY_PREFIX;
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::kv_backend::KvBackendRef;
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::range_stream::{DEFAULT_PAGE_SIZE, PaginationStream};
use crate::rpc::KeyValue;
use crate::rpc::store::{BatchDeleteRequest, BatchGetRequest, RangeRequest};

/// [TombstoneManager] provides the ability to:
/// - logically delete values
/// - restore the deleted values
///
/// The tombstone mechanism is primarily used for table metadata deletion.
/// When a table is logically deleted, its associated metadata keys are moved
/// to a tombstone area by prepending a prefix (default is `__tombstone/`).
///
/// All involved keys in the tombstone mechanism:
/// - `__table_name/{catalog}/{schema}/{table_name}`: Maps table name to table ID.
/// - `__table_info/{table_id}`: Stores table information/metadata.
/// - `__table_route/{table_id}`: Stores table routing information.
/// - `__table_repart/{table_id}`: Stores table repartition information.
/// - `__dn_table/{datanode_id}/{table_id}`: Maps datanodes to table regions.
/// - `__topic_region/{topic_name}/{region_id}`: Maps regions to WAL topics.
///
/// These keys are moved to:
/// - `__tombstone/__table_name/...`
/// - `__tombstone/__table_info/...`
/// - `__tombstone/__table_route/...`
/// - `__tombstone/__table_repart/...`
/// - `__tombstone/__dn_table/...`
/// - `__tombstone/__topic_region/...`
pub struct TombstoneManager {
    kv_backend: KvBackendRef,
    tombstone_prefix: String,
    // Only used for testing.
    #[cfg(test)]
    max_txn_ops: Option<usize>,
}

const TOMBSTONE_PREFIX: &str = "__tombstone/";

impl TombstoneManager {
    /// Returns [TombstoneManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self::new_with_prefix(kv_backend, TOMBSTONE_PREFIX)
    }

    /// Returns [TombstoneManager] with a custom tombstone prefix.
    pub fn new_with_prefix(kv_backend: KvBackendRef, prefix: &str) -> Self {
        Self {
            kv_backend,
            tombstone_prefix: prefix.to_string(),
            #[cfg(test)]
            max_txn_ops: None,
        }
    }

    pub fn to_tombstone(&self, key: &[u8]) -> Vec<u8> {
        [self.tombstone_prefix.as_bytes(), key].concat()
    }

    /// Removes the tombstone prefix from a tombstoned key.
    pub fn strip_tombstone_prefix<'a>(&self, tombstone_key: &'a [u8]) -> Result<&'a [u8]> {
        ensure!(
            tombstone_key.starts_with(self.tombstone_prefix.as_bytes()),
            error::UnexpectedSnafu {
                err_msg: format!(
                    "The key '{}' does not start with tombstone prefix '{}'.",
                    String::from_utf8_lossy(tombstone_key),
                    self.tombstone_prefix
                ),
            }
        );

        Ok(&tombstone_key[self.tombstone_prefix.len()..])
    }

    /// Gets a single tombstoned value by its original key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<KeyValue>> {
        let tombstone_key = self.to_tombstone(key);
        self.kv_backend.get(&tombstone_key).await
    }

    /// Gets tombstoned values by their original keys.
    pub async fn batch_get(&self, keys: &[Vec<u8>]) -> Result<HashMap<Vec<u8>, KeyValue>> {
        let tombstone_keys = keys
            .iter()
            .map(|key| self.to_tombstone(key))
            .collect::<Vec<_>>();
        let resp = self
            .kv_backend
            .batch_get(BatchGetRequest::new().with_keys(tombstone_keys))
            .await?;

        resp.kvs
            .into_iter()
            .map(|kv| Ok((self.strip_tombstone_prefix(&kv.key)?.to_vec(), kv)))
            .collect::<Result<HashMap<_, _>>>()
    }

    /// Streams all tombstoned key-value pairs.
    pub fn tombstones(&self) -> BoxStream<'static, Result<KeyValue>> {
        self.scan_prefix(self.tombstone_prefix.as_bytes().to_vec())
    }

    /// Streams tombstoned table-name entries only.
    pub fn tombstoned_table_names(&self) -> BoxStream<'static, Result<KeyValue>> {
        self.scan_prefix(
            format!("{}{}/", self.tombstone_prefix, TABLE_NAME_KEY_PREFIX).into_bytes(),
        )
    }

    /// Streams tombstoned entries under the provided prefix.
    fn scan_prefix(&self, prefix: Vec<u8>) -> BoxStream<'static, Result<KeyValue>> {
        let req = RangeRequest::new().with_prefix(prefix);
        let stream = PaginationStream::new(self.kv_backend.clone(), req, DEFAULT_PAGE_SIZE, Ok)
            .into_stream();

        Box::pin(stream)
    }

    #[cfg(test)]
    pub fn set_max_txn_ops(&mut self, max_txn_ops: usize) {
        self.max_txn_ops = Some(max_txn_ops);
    }

    /// Moves value to `dest_key`.
    ///
    /// Puts `value` to `dest_key` if the value of `src_key` equals `value`.
    ///
    /// Otherwise retrieves the value of `src_key`.
    fn build_move_value_txn(
        &self,
        src_key: Vec<u8>,
        value: Vec<u8>,
        dest_key: Vec<u8>,
    ) -> (Txn, impl FnMut(&mut TxnOpGetResponseSet) -> Option<Vec<u8>>) {
        let txn = Txn::new()
            .when(vec![Compare::with_value(
                src_key.clone(),
                CompareOp::Equal,
                value.clone(),
            )])
            .and_then(vec![
                TxnOp::Put(dest_key.clone(), value.clone()),
                TxnOp::Delete(src_key.clone()),
            ])
            .or_else(vec![TxnOp::Get(src_key.clone())]);

        (txn, TxnOpGetResponseSet::filter(src_key))
    }

    async fn move_values_inner(&self, keys: &[Vec<u8>], dest_keys: &[Vec<u8>]) -> Result<usize> {
        ensure!(
            keys.len() == dest_keys.len(),
            error::UnexpectedSnafu {
                err_msg: format!(
                    "The length of keys({}) does not match the length of dest_keys({}).",
                    keys.len(),
                    dest_keys.len()
                ),
            }
        );
        // The key -> dest key mapping.
        let lookup_table = keys.iter().zip(dest_keys.iter()).collect::<HashMap<_, _>>();

        let resp = self
            .kv_backend
            .batch_get(BatchGetRequest::new().with_keys(keys.to_vec()))
            .await?;
        let mut results = resp
            .kvs
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<HashMap<_, _>>();

        const MAX_RETRIES: usize = 8;
        for _ in 0..MAX_RETRIES {
            let (txns, (keys, filters)): (Vec<_>, (Vec<_>, Vec<_>)) = results
                .iter()
                .map(|(key, value)| {
                    let (txn, filter) = self.build_move_value_txn(
                        key.clone(),
                        value.clone(),
                        lookup_table[&key].clone(),
                    );
                    (txn, (key.clone(), filter))
                })
                .unzip();
            let mut resp = self.kv_backend.txn(Txn::merge_all(txns)).await?;
            if resp.succeeded {
                return Ok(keys.len());
            }
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            // Updates results.
            for (idx, mut filter) in filters.into_iter().enumerate() {
                if let Some(value) = filter(&mut set) {
                    results.insert(keys[idx].clone(), value);
                } else {
                    results.remove(&keys[idx]);
                }
            }
        }

        error::MoveValuesSnafu {
            err_msg: format!(
                "keys: {:?}",
                keys.iter().map(|key| String::from_utf8_lossy(key)),
            ),
        }
        .fail()
    }

    fn max_txn_ops(&self) -> usize {
        #[cfg(test)]
        if let Some(max_txn_ops) = self.max_txn_ops {
            return max_txn_ops;
        }
        self.kv_backend.max_txn_ops()
    }

    /// Moves values to `dest_key`.
    ///
    /// Returns the number of keys that were moved.
    async fn move_values(&self, keys: Vec<Vec<u8>>, dest_keys: Vec<Vec<u8>>) -> Result<usize> {
        ensure!(
            keys.len() == dest_keys.len(),
            error::UnexpectedSnafu {
                err_msg: format!(
                    "The length of keys({}) does not match the length of dest_keys({}).",
                    keys.len(),
                    dest_keys.len()
                ),
            }
        );
        if keys.is_empty() {
            return Ok(0);
        }
        let chunk_size = self.max_txn_ops() / 2;
        if keys.len() > chunk_size {
            debug!(
                "Moving values with multiple chunks, keys len: {}, chunk_size: {}",
                keys.len(),
                chunk_size
            );
            let mut moved_keys = 0;
            let keys_chunks = keys.chunks(chunk_size).collect::<Vec<_>>();
            let dest_keys_chunks = dest_keys.chunks(chunk_size).collect::<Vec<_>>();
            for (keys, dest_keys) in keys_chunks.into_iter().zip(dest_keys_chunks) {
                moved_keys += self.move_values_inner(keys, dest_keys).await?;
            }
            Ok(moved_keys)
        } else {
            self.move_values_inner(&keys, &dest_keys).await
        }
    }

    /// Creates tombstones for keys.
    ///
    /// Preforms to:
    /// - deletes origin values.
    /// - stores tombstone values.
    ///
    /// Returns the number of keys that were moved.
    pub async fn create(&self, keys: Vec<Vec<u8>>) -> Result<usize> {
        let (keys, dest_keys): (Vec<_>, Vec<_>) = keys
            .into_iter()
            .map(|key| {
                let tombstone_key = self.to_tombstone(&key);
                (key, tombstone_key)
            })
            .unzip();

        self.move_values(keys, dest_keys).await
    }

    /// Restores tombstones for keys.
    ///
    /// Preforms to:
    /// - restore origin value.
    /// - deletes tombstone values.
    ///
    /// Returns the number of keys that were restored.
    pub async fn restore(&self, keys: Vec<Vec<u8>>) -> Result<usize> {
        let (keys, dest_keys): (Vec<_>, Vec<_>) = keys
            .into_iter()
            .map(|key| {
                let tombstone_key = self.to_tombstone(&key);
                (tombstone_key, key)
            })
            .unzip();

        self.move_values(keys, dest_keys).await
    }

    /// Deletes tombstones values for the specified `keys`.
    ///
    /// Returns the number of keys that were deleted.
    pub async fn delete(&self, keys: Vec<Vec<u8>>) -> Result<usize> {
        let keys = keys
            .iter()
            .map(|key| self.to_tombstone(key))
            .collect::<Vec<_>>();

        let num_keys = keys.len();
        let _ = self
            .kv_backend
            .batch_delete(BatchDeleteRequest::new().with_keys(keys))
            .await?;

        Ok(num_keys)
    }
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::error::Error;
    use crate::key::tombstone::TombstoneManager;
    use crate::kv_backend::KvBackend;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::rpc::store::PutRequest;

    #[derive(Debug, Clone)]
    struct MoveValue {
        key: Vec<u8>,
        dest_key: Vec<u8>,
        value: Vec<u8>,
    }

    async fn check_moved_values(
        kv_backend: Arc<MemoryKvBackend<Error>>,
        move_values: &[MoveValue],
    ) {
        for MoveValue {
            key,
            dest_key,
            value,
        } in move_values
        {
            assert!(kv_backend.get(key).await.unwrap().is_none());
            assert_eq!(
                &kv_backend.get(dest_key).await.unwrap().unwrap().value,
                value,
            );
        }
    }

    #[tokio::test]
    async fn test_create_tombstone() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        kv_backend
            .put(PutRequest::new().with_key("bar").with_value("baz"))
            .await
            .unwrap();
        kv_backend
            .put(PutRequest::new().with_key("foo").with_value("hi"))
            .await
            .unwrap();
        tombstone_manager
            .create(vec![b"bar".to_vec(), b"foo".to_vec()])
            .await
            .unwrap();
        assert!(!kv_backend.exists(b"bar").await.unwrap());
        assert!(!kv_backend.exists(b"foo").await.unwrap());
        assert_eq!(
            kv_backend
                .get(&tombstone_manager.to_tombstone(b"bar"))
                .await
                .unwrap()
                .unwrap()
                .value,
            b"baz"
        );
        assert_eq!(
            kv_backend
                .get(&tombstone_manager.to_tombstone(b"foo"))
                .await
                .unwrap()
                .unwrap()
                .value,
            b"hi"
        );
        assert_eq!(kv_backend.len(), 2);
    }

    #[tokio::test]
    async fn test_create_tombstone_with_non_exist_values() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());

        kv_backend
            .put(PutRequest::new().with_key("bar").with_value("baz"))
            .await
            .unwrap();
        kv_backend
            .put(PutRequest::new().with_key("foo").with_value("hi"))
            .await
            .unwrap();

        tombstone_manager
            .create(vec![b"bar".to_vec(), b"baz".to_vec()])
            .await
            .unwrap();
        check_moved_values(
            kv_backend.clone(),
            &[MoveValue {
                key: b"bar".to_vec(),
                dest_key: tombstone_manager.to_tombstone(b"bar"),
                value: b"baz".to_vec(),
            }],
        )
        .await;
    }

    #[tokio::test]
    async fn test_restore_tombstone() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        kv_backend
            .put(PutRequest::new().with_key("bar").with_value("baz"))
            .await
            .unwrap();
        kv_backend
            .put(PutRequest::new().with_key("foo").with_value("hi"))
            .await
            .unwrap();
        let expected_kvs = kv_backend.dump();
        tombstone_manager
            .create(vec![b"bar".to_vec(), b"foo".to_vec()])
            .await
            .unwrap();
        tombstone_manager
            .restore(vec![b"bar".to_vec(), b"foo".to_vec()])
            .await
            .unwrap();
        assert_eq!(expected_kvs, kv_backend.dump());
    }

    #[tokio::test]
    async fn test_delete_tombstone() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        kv_backend
            .put(PutRequest::new().with_key("bar").with_value("baz"))
            .await
            .unwrap();
        kv_backend
            .put(PutRequest::new().with_key("foo").with_value("hi"))
            .await
            .unwrap();
        tombstone_manager
            .create(vec![b"bar".to_vec(), b"foo".to_vec()])
            .await
            .unwrap();
        tombstone_manager
            .delete(vec![b"bar".to_vec(), b"foo".to_vec()])
            .await
            .unwrap();
        assert!(kv_backend.is_empty());
    }

    #[tokio::test]
    async fn test_batch_get_tombstones() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        kv_backend
            .put(PutRequest::new().with_key("bar").with_value("baz"))
            .await
            .unwrap();
        kv_backend
            .put(PutRequest::new().with_key("foo").with_value("hi"))
            .await
            .unwrap();

        tombstone_manager
            .create(vec![b"bar".to_vec(), b"foo".to_vec()])
            .await
            .unwrap();

        let kvs = tombstone_manager
            .batch_get(&[b"bar".to_vec(), b"foo".to_vec(), b"missing".to_vec()])
            .await
            .unwrap();

        assert_eq!(kvs.len(), 2);
        assert_eq!(kvs.get(b"bar".as_slice()).unwrap().value, b"baz");
        assert_eq!(kvs.get(b"foo".as_slice()).unwrap().value, b"hi");
        assert!(!kvs.contains_key(b"missing".as_slice()));
    }

    #[tokio::test]
    async fn test_move_values() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        let kvs = HashMap::from([
            (b"bar".to_vec(), b"baz".to_vec()),
            (b"foo".to_vec(), b"hi".to_vec()),
            (b"baz".to_vec(), b"hello".to_vec()),
        ]);
        for (key, value) in &kvs {
            kv_backend
                .put(
                    PutRequest::new()
                        .with_key(key.clone())
                        .with_value(value.clone()),
                )
                .await
                .unwrap();
        }
        let move_values = kvs
            .iter()
            .map(|(key, value)| MoveValue {
                key: key.clone(),
                dest_key: tombstone_manager.to_tombstone(key),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        let (keys, dest_keys): (Vec<_>, Vec<_>) = move_values
            .clone()
            .into_iter()
            .map(|kv| (kv.key, kv.dest_key))
            .unzip();
        let moved_keys = tombstone_manager
            .move_values(keys.clone(), dest_keys.clone())
            .await
            .unwrap();
        assert_eq!(kvs.len(), moved_keys);
        check_moved_values(kv_backend.clone(), &move_values).await;
        // Moves again
        let moved_keys = tombstone_manager
            .move_values(keys.clone(), dest_keys.clone())
            .await
            .unwrap();
        assert_eq!(0, moved_keys);
        check_moved_values(kv_backend.clone(), &move_values).await;
    }

    #[tokio::test]
    async fn test_move_values_with_max_txn_ops() {
        common_telemetry::init_default_ut_logging();
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let mut tombstone_manager = TombstoneManager::new(kv_backend.clone());
        tombstone_manager.set_max_txn_ops(4);
        let kvs = HashMap::from([
            (b"bar".to_vec(), b"baz".to_vec()),
            (b"foo".to_vec(), b"hi".to_vec()),
            (b"baz".to_vec(), b"hello".to_vec()),
            (b"qux".to_vec(), b"world".to_vec()),
            (b"quux".to_vec(), b"world".to_vec()),
            (b"quuux".to_vec(), b"world".to_vec()),
            (b"quuuux".to_vec(), b"world".to_vec()),
            (b"quuuuux".to_vec(), b"world".to_vec()),
            (b"quuuuuux".to_vec(), b"world".to_vec()),
        ]);
        for (key, value) in &kvs {
            kv_backend
                .put(
                    PutRequest::new()
                        .with_key(key.clone())
                        .with_value(value.clone()),
                )
                .await
                .unwrap();
        }
        let move_values = kvs
            .iter()
            .map(|(key, value)| MoveValue {
                key: key.clone(),
                dest_key: tombstone_manager.to_tombstone(key),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        let (keys, dest_keys): (Vec<_>, Vec<_>) = move_values
            .clone()
            .into_iter()
            .map(|kv| (kv.key, kv.dest_key))
            .unzip();
        let moved_keys = tombstone_manager
            .move_values(keys.clone(), dest_keys.clone())
            .await
            .unwrap();
        assert_eq!(kvs.len(), moved_keys);
        check_moved_values(kv_backend.clone(), &move_values).await;
        // Moves again
        let moved_keys = tombstone_manager
            .move_values(keys.clone(), dest_keys.clone())
            .await
            .unwrap();
        assert_eq!(0, moved_keys);
        check_moved_values(kv_backend.clone(), &move_values).await;
    }

    #[tokio::test]
    async fn test_move_values_with_non_exists_values() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        let kvs = HashMap::from([
            (b"bar".to_vec(), b"baz".to_vec()),
            (b"foo".to_vec(), b"hi".to_vec()),
            (b"baz".to_vec(), b"hello".to_vec()),
        ]);
        for (key, value) in &kvs {
            kv_backend
                .put(
                    PutRequest::new()
                        .with_key(key.clone())
                        .with_value(value.clone()),
                )
                .await
                .unwrap();
        }
        let move_values = kvs
            .iter()
            .map(|(key, value)| MoveValue {
                key: key.clone(),
                dest_key: tombstone_manager.to_tombstone(key),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        let (mut keys, mut dest_keys): (Vec<_>, Vec<_>) = move_values
            .clone()
            .into_iter()
            .map(|kv| (kv.key, kv.dest_key))
            .unzip();
        keys.push(b"non-exists".to_vec());
        dest_keys.push(b"hi/non-exists".to_vec());
        let moved_keys = tombstone_manager
            .move_values(keys.clone(), dest_keys.clone())
            .await
            .unwrap();
        check_moved_values(kv_backend.clone(), &move_values).await;
        assert_eq!(3, moved_keys);
        // Moves again
        let moved_keys = tombstone_manager
            .move_values(keys.clone(), dest_keys.clone())
            .await
            .unwrap();
        check_moved_values(kv_backend.clone(), &move_values).await;
        assert_eq!(0, moved_keys);
    }

    #[tokio::test]
    async fn test_move_values_changed() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        let kvs = HashMap::from([
            (b"bar".to_vec(), b"baz".to_vec()),
            (b"foo".to_vec(), b"hi".to_vec()),
            (b"baz".to_vec(), b"hello".to_vec()),
        ]);
        for (key, value) in &kvs {
            kv_backend
                .put(
                    PutRequest::new()
                        .with_key(key.clone())
                        .with_value(value.clone()),
                )
                .await
                .unwrap();
        }

        kv_backend
            .put(PutRequest::new().with_key("baz").with_value("changed"))
            .await
            .unwrap();

        let move_values = kvs
            .iter()
            .map(|(key, value)| MoveValue {
                key: key.clone(),
                dest_key: tombstone_manager.to_tombstone(key),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        let (keys, dest_keys): (Vec<_>, Vec<_>) = move_values
            .clone()
            .into_iter()
            .map(|kv| (kv.key, kv.dest_key))
            .unzip();
        let moved_keys = tombstone_manager
            .move_values(keys, dest_keys)
            .await
            .unwrap();
        assert_eq!(kvs.len(), moved_keys);
    }

    #[tokio::test]
    async fn test_move_values_overwrite_dest_values() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        let kvs = HashMap::from([
            (b"bar".to_vec(), b"baz".to_vec()),
            (b"foo".to_vec(), b"hi".to_vec()),
            (b"baz".to_vec(), b"hello".to_vec()),
        ]);
        for (key, value) in &kvs {
            kv_backend
                .put(
                    PutRequest::new()
                        .with_key(key.clone())
                        .with_value(value.clone()),
                )
                .await
                .unwrap();
        }

        // Prepares
        let move_values = kvs
            .iter()
            .map(|(key, value)| MoveValue {
                key: key.clone(),
                dest_key: tombstone_manager.to_tombstone(key),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        let (keys, dest_keys): (Vec<_>, Vec<_>) = move_values
            .clone()
            .into_iter()
            .map(|kv| (kv.key, kv.dest_key))
            .unzip();
        tombstone_manager
            .move_values(keys, dest_keys)
            .await
            .unwrap();
        check_moved_values(kv_backend.clone(), &move_values).await;

        // Overwrites existing dest keys.
        let kvs = HashMap::from([
            (b"bar".to_vec(), b"new baz".to_vec()),
            (b"foo".to_vec(), b"new hi".to_vec()),
            (b"baz".to_vec(), b"new baz".to_vec()),
        ]);
        for (key, value) in &kvs {
            kv_backend
                .put(
                    PutRequest::new()
                        .with_key(key.clone())
                        .with_value(value.clone()),
                )
                .await
                .unwrap();
        }
        let move_values = kvs
            .iter()
            .map(|(key, value)| MoveValue {
                key: key.clone(),
                dest_key: tombstone_manager.to_tombstone(key),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        let (keys, dest_keys): (Vec<_>, Vec<_>) = move_values
            .clone()
            .into_iter()
            .map(|kv| (kv.key, kv.dest_key))
            .unzip();
        tombstone_manager
            .move_values(keys, dest_keys)
            .await
            .unwrap();
        check_moved_values(kv_backend.clone(), &move_values).await;
    }

    #[tokio::test]
    async fn test_move_values_with_different_lengths() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());

        let keys = vec![b"bar".to_vec(), b"foo".to_vec()];
        let dest_keys = vec![b"bar".to_vec(), b"foo".to_vec(), b"baz".to_vec()];

        let err = tombstone_manager
            .move_values(keys, dest_keys)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("The length of keys(2) does not match the length of dest_keys(3)."),
        );

        let moved_keys = tombstone_manager.move_values(vec![], vec![]).await.unwrap();
        assert_eq!(0, moved_keys);
    }
}
