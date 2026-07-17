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
const MOVE_VALUE_TXN_OPS_PER_KEY: usize = 4;
const RESTORE_VALUE_TXN_OPS_PER_KEY: usize = 6;

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
        require_dest_not_exists: bool,
    ) -> Txn {
        let mut compares = vec![Compare::with_value(
            src_key.clone(),
            CompareOp::Equal,
            value.clone(),
        )];
        if require_dest_not_exists {
            compares.push(Compare::with_value_not_exists(
                dest_key.clone(),
                CompareOp::Equal,
            ));
        }

        let mut failure = vec![TxnOp::Get(src_key.clone())];
        if require_dest_not_exists {
            failure.push(TxnOp::Get(dest_key.clone()));
        }

        Txn::new()
            .when(compares)
            .and_then(vec![
                TxnOp::Put(dest_key.clone(), value.clone()),
                TxnOp::Delete(src_key.clone()),
            ])
            .or_else(failure)
    }

    async fn move_values_inner(
        &self,
        keys: &[Vec<u8>],
        dest_keys: &[Vec<u8>],
        require_dest_not_exists: bool,
        extra_ops: Vec<TxnOp>,
    ) -> Result<usize> {
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
        if results.is_empty() && extra_ops.is_empty() {
            return Ok(0);
        }

        const MAX_RETRIES: usize = 8;
        for _ in 0..MAX_RETRIES {
            let (txns, keys): (Vec<_>, Vec<_>) = results
                .iter()
                .map(|(key, value)| {
                    let txn = self.build_move_value_txn(
                        key.clone(),
                        value.clone(),
                        lookup_table[&key].clone(),
                        require_dest_not_exists,
                    );
                    (txn, key.clone())
                })
                .unzip();
            let mut txns = txns;
            if !extra_ops.is_empty() {
                txns.push(Txn::new().and_then(extra_ops.clone()));
            }
            let mut resp = self.kv_backend.txn(Txn::merge_all(txns)).await?;
            if resp.succeeded {
                return Ok(keys.len());
            }
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            // Updates results.
            for key in &keys {
                let dest_key = lookup_table[&key].clone();
                if require_dest_not_exists {
                    let mut filter = TxnOpGetResponseSet::filter(dest_key.clone());
                    if filter(&mut set).is_some() {
                        return error::TombstoneTargetAlreadyExistsSnafu {
                            key: String::from_utf8_lossy(&dest_key).to_string(),
                        }
                        .fail();
                    }
                }

                let mut filter = TxnOpGetResponseSet::filter(key.clone());
                if let Some(value) = filter(&mut set) {
                    results.insert(key.clone(), value);
                } else {
                    results.remove(key);
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

    async fn execute_extra_ops(&self, extra_ops: Vec<TxnOp>) -> Result<()> {
        let max_txn_ops = self.max_txn_ops();
        ensure!(
            max_txn_ops > 0,
            error::UnexpectedSnafu {
                err_msg: "max_txn_ops must be greater than 0".to_string(),
            }
        );
        for chunk in extra_ops.chunks(max_txn_ops) {
            self.move_values_inner(&[], &[], false, chunk.to_vec())
                .await?;
        }
        Ok(())
    }

    /// Moves values to `dest_key`.
    ///
    /// Returns the number of keys that were moved.
    async fn move_values_with_extra(
        &self,
        keys: Vec<Vec<u8>>,
        dest_keys: Vec<Vec<u8>>,
        require_dest_not_exists: bool,
        mut extra_ops: Vec<TxnOp>,
    ) -> Result<usize> {
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
            if !extra_ops.is_empty() {
                self.execute_extra_ops(extra_ops).await?;
            }
            return Ok(0);
        }
        let txn_ops_per_key = if require_dest_not_exists {
            RESTORE_VALUE_TXN_OPS_PER_KEY
        } else {
            MOVE_VALUE_TXN_OPS_PER_KEY
        };
        let max_txn_ops = self.max_txn_ops();
        ensure!(
            max_txn_ops >= txn_ops_per_key,
            error::UnexpectedSnafu {
                err_msg: format!(
                    "max_txn_ops {max_txn_ops} is smaller than required txn ops per key {txn_ops_per_key}"
                )
            }
        );
        let merge_extra_ops =
            !extra_ops.is_empty() && max_txn_ops.saturating_sub(txn_ops_per_key) >= extra_ops.len();
        let reserved_ops = if merge_extra_ops { extra_ops.len() } else { 0 };
        let chunk_size = (max_txn_ops - reserved_ops) / txn_ops_per_key;
        let total_ops = keys
            .len()
            .saturating_mul(txn_ops_per_key)
            .saturating_add(extra_ops.len());
        if keys.len() > chunk_size || total_ops > max_txn_ops {
            debug!(
                "Moving values with multiple chunks, keys len: {}, chunk_size: {}",
                keys.len(),
                chunk_size
            );
            let mut moved_keys = 0;
            let keys_chunks = keys.chunks(chunk_size).collect::<Vec<_>>();
            let dest_keys_chunks = dest_keys.chunks(chunk_size).collect::<Vec<_>>();
            let num_chunks = keys_chunks.len();
            for (index, (keys, dest_keys)) in
                keys_chunks.into_iter().zip(dest_keys_chunks).enumerate()
            {
                let chunk_extra_ops = if merge_extra_ops && index + 1 == num_chunks {
                    std::mem::take(&mut extra_ops)
                } else {
                    vec![]
                };
                moved_keys += self
                    .move_values_inner(keys, dest_keys, require_dest_not_exists, chunk_extra_ops)
                    .await?;
            }
            if !extra_ops.is_empty() {
                self.execute_extra_ops(extra_ops).await?;
            }
            Ok(moved_keys)
        } else {
            self.move_values_inner(&keys, &dest_keys, require_dest_not_exists, extra_ops)
                .await
        }
    }

    async fn move_values(
        &self,
        keys: Vec<Vec<u8>>,
        dest_keys: Vec<Vec<u8>>,
        require_dest_not_exists: bool,
    ) -> Result<usize> {
        self.move_values_with_extra(keys, dest_keys, require_dest_not_exists, vec![])
            .await
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

        self.move_values(keys, dest_keys, false).await
    }

    /// Creates tombstones and stores an additional marker in the tombstone namespace.
    pub async fn create_with_marker(
        &self,
        keys: Vec<Vec<u8>>,
        marker_key: Vec<u8>,
        marker_value: Vec<u8>,
    ) -> Result<usize> {
        self.create_with_markers(keys, vec![(marker_key, marker_value)])
            .await
    }

    /// Creates tombstones and stores additional markers in the tombstone namespace.
    pub async fn create_with_markers(
        &self,
        keys: Vec<Vec<u8>>,
        markers: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<usize> {
        let (keys, dest_keys): (Vec<_>, Vec<_>) = keys
            .into_iter()
            .map(|key| {
                let tombstone_key = self.to_tombstone(&key);
                (key, tombstone_key)
            })
            .unzip();
        let marker_ops = markers
            .into_iter()
            .map(|(key, value)| TxnOp::Put(self.to_tombstone(&key), value))
            .collect();

        self.move_values_with_extra(keys, dest_keys, false, marker_ops)
            .await
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

        self.move_values(keys, dest_keys, true).await
    }

    /// Restores tombstones and deletes an associated marker.
    pub async fn restore_with_marker(
        &self,
        keys: Vec<Vec<u8>>,
        marker_key: Vec<u8>,
    ) -> Result<usize> {
        let (keys, dest_keys): (Vec<_>, Vec<_>) = keys
            .into_iter()
            .map(|key| {
                let tombstone_key = self.to_tombstone(&key);
                (tombstone_key, key)
            })
            .unzip();
        let marker_key = self.to_tombstone(&marker_key);

        self.move_values_with_extra(keys, dest_keys, true, vec![TxnOp::Delete(marker_key)])
            .await
    }

    /// Restores tombstones and deletes associated markers.
    pub async fn restore_with_markers(
        &self,
        keys: Vec<Vec<u8>>,
        marker_keys: Vec<Vec<u8>>,
    ) -> Result<usize> {
        let (keys, dest_keys): (Vec<_>, Vec<_>) = keys
            .into_iter()
            .map(|key| {
                let tombstone_key = self.to_tombstone(&key);
                (tombstone_key, key)
            })
            .unzip();
        let marker_ops = marker_keys
            .into_iter()
            .map(|key| TxnOp::Delete(self.to_tombstone(&key)))
            .collect();
        self.move_values_with_extra(keys, dest_keys, true, marker_ops)
            .await
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

    /// Deletes tombstones and an associated marker.
    pub async fn delete_with_marker(
        &self,
        mut keys: Vec<Vec<u8>>,
        marker_key: Vec<u8>,
    ) -> Result<usize> {
        keys.push(marker_key);
        self.delete(keys).await
    }

    /// Deletes tombstones and associated markers.
    pub async fn delete_with_markers(
        &self,
        mut keys: Vec<Vec<u8>>,
        marker_keys: Vec<Vec<u8>>,
    ) -> Result<usize> {
        keys.extend(marker_keys);
        self.delete(keys).await
    }
}

#[cfg(test)]
mod tests {

    use std::any::Any;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::error::{Error, Result};
    use crate::key::tombstone::{
        MOVE_VALUE_TXN_OPS_PER_KEY, RESTORE_VALUE_TXN_OPS_PER_KEY, TombstoneManager,
    };
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::txn::{Txn, TxnRequest, TxnResponse};
    use crate::kv_backend::{KvBackend, TxnService};
    use crate::rpc::store::{
        BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse,
        BatchPutRequest, BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest,
        PutResponse, RangeRequest, RangeResponse,
    };

    struct TxnOpLimitKvBackend {
        inner: Arc<MemoryKvBackend<Error>>,
        max_txn_ops: usize,
        txn_op_counts: Mutex<Vec<usize>>,
    }

    #[async_trait::async_trait]
    impl TxnService for TxnOpLimitKvBackend {
        type Error = Error;

        async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
            let TxnRequest {
                compare,
                success,
                failure,
            } = txn.req();
            let txn_ops = compare.len() + success.len() + failure.len();
            assert!(
                txn_ops <= self.max_txn_ops,
                "txn ops {txn_ops} exceeds limit {}",
                self.max_txn_ops
            );
            self.txn_op_counts.lock().unwrap().push(txn_ops);
            self.inner.txn(txn).await
        }

        fn max_txn_ops(&self) -> usize {
            self.max_txn_ops
        }
    }

    #[async_trait::async_trait]
    impl KvBackend for TxnOpLimitKvBackend {
        fn name(&self) -> &str {
            "txn_op_limit"
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
            self.inner.range(req).await
        }

        async fn put(&self, req: PutRequest) -> Result<PutResponse> {
            self.inner.put(req).await
        }

        async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
            self.inner.batch_put(req).await
        }

        async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
            self.inner.batch_get(req).await
        }

        async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
            self.inner.delete_range(req).await
        }

        async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
            self.inner.batch_delete(req).await
        }
    }

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
            .move_values(keys.clone(), dest_keys.clone(), false)
            .await
            .unwrap();
        assert_eq!(kvs.len(), moved_keys);
        check_moved_values(kv_backend.clone(), &move_values).await;
        // Moves again
        let moved_keys = tombstone_manager
            .move_values(keys.clone(), dest_keys.clone(), false)
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
            .move_values(keys.clone(), dest_keys.clone(), false)
            .await
            .unwrap();
        assert_eq!(kvs.len(), moved_keys);
        check_moved_values(kv_backend.clone(), &move_values).await;
        // Moves again
        let moved_keys = tombstone_manager
            .move_values(keys.clone(), dest_keys.clone(), false)
            .await
            .unwrap();
        assert_eq!(0, moved_keys);
        check_moved_values(kv_backend.clone(), &move_values).await;
    }

    #[tokio::test]
    async fn test_restore_chunks_by_total_txn_ops_limit() {
        let inner = Arc::new(MemoryKvBackend::default());
        let kv_backend = Arc::new(TxnOpLimitKvBackend {
            inner: inner.clone(),
            max_txn_ops: 6,
            txn_op_counts: Mutex::new(Vec::new()),
        });
        let tombstone_manager = TombstoneManager::new(kv_backend);
        let kvs = HashMap::from([
            (b"bar".to_vec(), b"baz".to_vec()),
            (b"foo".to_vec(), b"hi".to_vec()),
            (b"baz".to_vec(), b"hello".to_vec()),
        ]);
        for (key, value) in &kvs {
            inner
                .put(
                    PutRequest::new()
                        .with_key(tombstone_manager.to_tombstone(key))
                        .with_value(value.clone()),
                )
                .await
                .unwrap();
        }

        let restored = tombstone_manager
            .restore(kvs.keys().cloned().collect())
            .await
            .unwrap();

        assert_eq!(kvs.len(), restored);
    }

    #[tokio::test]
    async fn test_restore_fails_fast_when_txn_op_limit_too_small() {
        let inner = Arc::new(MemoryKvBackend::default());
        let kv_backend = Arc::new(TxnOpLimitKvBackend {
            inner: inner.clone(),
            max_txn_ops: 5,
            txn_op_counts: Mutex::new(Vec::new()),
        });
        let tombstone_manager = TombstoneManager::new(kv_backend);
        let key = b"foo".to_vec();
        inner
            .put(
                PutRequest::new()
                    .with_key(tombstone_manager.to_tombstone(&key))
                    .with_value(b"hi".to_vec()),
            )
            .await
            .unwrap();

        let err = tombstone_manager.restore(vec![key]).await.unwrap_err();

        assert!(matches!(err, Error::Unexpected { .. }));
        assert!(
            err.to_string()
                .contains("max_txn_ops 5 is smaller than required txn ops per key 6")
        );
    }

    #[tokio::test]
    async fn test_create_chunks_by_total_txn_ops_limit() {
        let inner = Arc::new(MemoryKvBackend::default());
        let kv_backend = Arc::new(TxnOpLimitKvBackend {
            inner: inner.clone(),
            max_txn_ops: 4,
            txn_op_counts: Mutex::new(Vec::new()),
        });
        let tombstone_manager = TombstoneManager::new(kv_backend);
        let kvs = HashMap::from([
            (b"bar".to_vec(), b"baz".to_vec()),
            (b"foo".to_vec(), b"hi".to_vec()),
            (b"baz".to_vec(), b"hello".to_vec()),
        ]);
        for (key, value) in &kvs {
            inner
                .put(
                    PutRequest::new()
                        .with_key(key.clone())
                        .with_value(value.clone()),
                )
                .await
                .unwrap();
        }

        let moved = tombstone_manager
            .create(kvs.keys().cloned().collect())
            .await
            .unwrap();

        assert_eq!(kvs.len(), moved);
    }

    #[tokio::test]
    async fn test_create_with_marker_uses_final_transaction_at_exact_limit() {
        let inner = Arc::new(MemoryKvBackend::default());
        let kv_backend = Arc::new(TxnOpLimitKvBackend {
            inner: inner.clone(),
            max_txn_ops: MOVE_VALUE_TXN_OPS_PER_KEY,
            txn_op_counts: Mutex::new(Vec::new()),
        });
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        for (key, value) in [(b"bar".as_slice(), b"baz".as_slice()), (b"foo", b"hi")] {
            inner
                .put(PutRequest::new().with_key(key).with_value(value))
                .await
                .unwrap();
        }

        tombstone_manager
            .create_with_marker(
                vec![b"bar".to_vec(), b"foo".to_vec()],
                b"__dropped_at/42".to_vec(),
                b"1234".to_vec(),
            )
            .await
            .unwrap();
        tombstone_manager
            .create_with_marker(
                vec![b"bar".to_vec(), b"foo".to_vec()],
                b"__dropped_at/42".to_vec(),
                b"1234".to_vec(),
            )
            .await
            .unwrap();

        assert_eq!(
            kv_backend.txn_op_counts.lock().unwrap().as_slice(),
            &[4, 4, 1, 1]
        );
        assert_eq!(
            inner
                .get(&tombstone_manager.to_tombstone(b"__dropped_at/42"))
                .await
                .unwrap()
                .unwrap()
                .value,
            b"1234"
        );
    }

    #[tokio::test]
    async fn test_create_with_markers_chunks_extra_ops_after_key_move() {
        let inner = Arc::new(MemoryKvBackend::default());
        let kv_backend = Arc::new(TxnOpLimitKvBackend {
            inner: inner.clone(),
            max_txn_ops: MOVE_VALUE_TXN_OPS_PER_KEY + 1,
            txn_op_counts: Mutex::new(Vec::new()),
        });
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        inner
            .put(PutRequest::new().with_key(b"foo").with_value(b"bar"))
            .await
            .unwrap();

        tombstone_manager
            .create_with_markers(
                vec![b"foo".to_vec()],
                (0..6)
                    .map(|index| (format!("marker-{index}").into_bytes(), vec![index]))
                    .collect(),
            )
            .await
            .unwrap();

        assert_eq!(
            kv_backend.txn_op_counts.lock().unwrap().as_slice(),
            &[MOVE_VALUE_TXN_OPS_PER_KEY, 5, 1]
        );
    }

    #[tokio::test]
    async fn test_create_with_markers_chunks_marker_only_transactions() {
        let inner = Arc::new(MemoryKvBackend::default());
        let kv_backend = Arc::new(TxnOpLimitKvBackend {
            inner,
            max_txn_ops: 2,
            txn_op_counts: Mutex::new(Vec::new()),
        });
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        let markers = (0..5)
            .map(|index| (format!("marker-{index}").into_bytes(), vec![index]))
            .collect();

        tombstone_manager
            .create_with_markers(vec![], markers)
            .await
            .unwrap();

        assert_eq!(
            kv_backend.txn_op_counts.lock().unwrap().as_slice(),
            &[2, 2, 1]
        );
    }

    #[tokio::test]
    async fn test_restore_with_marker_uses_final_transaction_at_exact_limit() {
        let inner = Arc::new(MemoryKvBackend::default());
        let kv_backend = Arc::new(TxnOpLimitKvBackend {
            inner: inner.clone(),
            max_txn_ops: RESTORE_VALUE_TXN_OPS_PER_KEY,
            txn_op_counts: Mutex::new(Vec::new()),
        });
        let tombstone_manager = TombstoneManager::new(kv_backend.clone());
        for (key, value) in [(b"bar".as_slice(), b"baz".as_slice()), (b"foo", b"hi")] {
            inner
                .put(
                    PutRequest::new()
                        .with_key(tombstone_manager.to_tombstone(key))
                        .with_value(value),
                )
                .await
                .unwrap();
        }
        inner
            .put(
                PutRequest::new()
                    .with_key(tombstone_manager.to_tombstone(b"__dropped_at/42"))
                    .with_value(b"1234"),
            )
            .await
            .unwrap();

        tombstone_manager
            .restore_with_marker(
                vec![b"bar".to_vec(), b"foo".to_vec()],
                b"__dropped_at/42".to_vec(),
            )
            .await
            .unwrap();
        tombstone_manager
            .restore_with_marker(
                vec![b"bar".to_vec(), b"foo".to_vec()],
                b"__dropped_at/42".to_vec(),
            )
            .await
            .unwrap();

        assert_eq!(
            kv_backend.txn_op_counts.lock().unwrap().as_slice(),
            &[6, 6, 1, 1]
        );
        assert!(
            inner
                .get(&tombstone_manager.to_tombstone(b"__dropped_at/42"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_delete_with_marker_removes_marker_with_limited_backend() {
        let inner = Arc::new(MemoryKvBackend::default());
        let kv_backend = Arc::new(TxnOpLimitKvBackend {
            inner: inner.clone(),
            max_txn_ops: MOVE_VALUE_TXN_OPS_PER_KEY,
            txn_op_counts: Mutex::new(Vec::new()),
        });
        let tombstone_manager = TombstoneManager::new(kv_backend);
        for key in [b"bar".as_slice(), b"foo", b"__dropped_at/42"] {
            inner
                .put(
                    PutRequest::new()
                        .with_key(tombstone_manager.to_tombstone(key))
                        .with_value(b"value"),
                )
                .await
                .unwrap();
        }

        tombstone_manager
            .delete_with_marker(
                vec![b"bar".to_vec(), b"foo".to_vec()],
                b"__dropped_at/42".to_vec(),
            )
            .await
            .unwrap();

        assert!(inner.is_empty());
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
            .move_values(keys.clone(), dest_keys.clone(), false)
            .await
            .unwrap();
        check_moved_values(kv_backend.clone(), &move_values).await;
        assert_eq!(3, moved_keys);
        // Moves again
        let moved_keys = tombstone_manager
            .move_values(keys.clone(), dest_keys.clone(), false)
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
            .move_values(keys, dest_keys, false)
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
            .move_values(keys, dest_keys, false)
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
            .move_values(keys, dest_keys, false)
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
            .move_values(keys, dest_keys, false)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("The length of keys(2) does not match the length of dest_keys(3)."),
        );

        let moved_keys = tombstone_manager
            .move_values(vec![], vec![], false)
            .await
            .unwrap();
        assert_eq!(0, moved_keys);
    }
}
