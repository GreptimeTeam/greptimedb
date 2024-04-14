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

use snafu::{ensure, OptionExt};

use super::TableMetaKeyGetTxnOp;
use crate::error::{self, Result};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::kv_backend::KvBackendRef;

/// [TombstoneManager] provides the ability to:
/// - logically delete values
/// - restore the deleted values
pub(crate) struct TombstoneManager {
    kv_backend: KvBackendRef,
}

const TOMBSTONE_PREFIX: &str = "__tombstone/";

pub(crate) struct TombstoneKey<T>(T);

fn to_tombstone(key: &[u8]) -> Vec<u8> {
    [TOMBSTONE_PREFIX.as_bytes(), key].concat()
}

impl TombstoneKey<&Vec<u8>> {
    /// Returns the origin key and tombstone key.
    fn to_keys(&self) -> (Vec<u8>, Vec<u8>) {
        let key = self.0;
        let tombstone_key = to_tombstone(key);
        (key.clone(), tombstone_key)
    }

    /// Returns the origin key and tombstone key.
    fn into_keys(self) -> (Vec<u8>, Vec<u8>) {
        self.to_keys()
    }

    /// Returns the tombstone key.
    fn to_tombstone_key(&self) -> Vec<u8> {
        let key = self.0;
        to_tombstone(key)
    }
}

impl TableMetaKeyGetTxnOp for TombstoneKey<&Vec<u8>> {
    fn build_get_op(
        &self,
    ) -> (
        TxnOp,
        impl FnMut(&'_ mut TxnOpGetResponseSet) -> Option<Vec<u8>>,
    ) {
        let key = to_tombstone(self.0);
        (TxnOp::Get(key.clone()), TxnOpGetResponseSet::filter(key))
    }
}

/// Atomic Key:
/// The value corresponding to the key remains consistent between two transactions.
pub(crate) enum Key {
    Atomic(Vec<u8>),
    Other(Vec<u8>),
}

impl Key {
    /// Returns a new [Key::Atomic].
    pub(crate) fn atomic<T: Into<Vec<u8>>>(key: T) -> Self {
        Self::Atomic(key.into())
    }

    /// Returns a new [Key::Other].
    pub(crate) fn other<T: Into<Vec<u8>>>(key: T) -> Self {
        Self::Other(key.into())
    }

    fn get_inner(&self) -> &Vec<u8> {
        match self {
            Key::Atomic(key) => key,
            Key::Other(key) => key,
        }
    }

    fn is_atomic(&self) -> bool {
        matches!(self, Key::Atomic(_))
    }
}

impl TableMetaKeyGetTxnOp for Key {
    fn build_get_op(
        &self,
    ) -> (
        TxnOp,
        impl FnMut(&'_ mut TxnOpGetResponseSet) -> Option<Vec<u8>>,
    ) {
        let key = self.get_inner().clone();
        (TxnOp::Get(key.clone()), TxnOpGetResponseSet::filter(key))
    }
}

impl TombstoneManager {
    /// Returns [TombstoneManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }
    /// Creates tombstones for keys.
    ///
    /// Preforms to:
    /// - retrieve all values corresponding `keys`.
    /// - stores tombstone values.
    pub(crate) async fn create(&self, keys: Vec<Key>) -> Result<bool> {
        // Builds transaction to retrieve all values
        let (operations, mut filters): (Vec<_>, Vec<_>) =
            keys.iter().map(|key| key.build_get_op()).unzip();

        let txn = Txn::new().and_then(operations);
        let mut resp = self.kv_backend.txn(txn).await?;
        ensure!(
            resp.succeeded,
            error::UnexpectedSnafu {
                err_msg: "Failed to retrieves the metadata"
            }
        );

        let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
        // Builds the create tombstone transaction.
        let mut tombstone_operations = Vec::with_capacity(keys.len() * 2);
        let mut tombstone_comparison = vec![];
        for (idx, key) in keys.iter().enumerate() {
            let filter = &mut filters[idx];
            let value = filter(&mut set).with_context(|| error::UnexpectedSnafu {
                err_msg: format!(
                    "Missing value, key: {}",
                    String::from_utf8_lossy(key.get_inner())
                ),
            })?;
            let (origin_key, tombstone_key) = TombstoneKey(key.get_inner()).into_keys();
            // Compares the atomic key.
            if key.is_atomic() {
                tombstone_comparison.push(Compare::with_value(
                    origin_key.clone(),
                    CompareOp::Equal,
                    value.clone(),
                ));
            }
            tombstone_operations.push(TxnOp::Delete(origin_key));
            tombstone_operations.push(TxnOp::Put(tombstone_key, value));
        }

        let txn = if !tombstone_comparison.is_empty() {
            Txn::new().when(tombstone_comparison)
        } else {
            Txn::new()
        }
        .and_then(tombstone_operations);

        let resp = self.kv_backend.txn(txn).await?;
        Ok(resp.succeeded)
    }

    /// Restores tombstones for keys.
    ///
    /// Preforms to:
    /// - retrieve all tombstone values corresponding `keys`.
    /// - stores tombstone values.
    pub(crate) async fn restore(&self, keys: Vec<Key>) -> Result<bool> {
        // Builds transaction to retrieve all tombstone values
        let tombstone_keys = keys
            .iter()
            .map(|key| TombstoneKey(key.get_inner()))
            .collect::<Vec<_>>();
        let (operations, mut filters): (Vec<_>, Vec<_>) =
            tombstone_keys.iter().map(|key| key.build_get_op()).unzip();

        let txn = Txn::new().and_then(operations);
        let mut resp = self.kv_backend.txn(txn).await?;
        ensure!(
            resp.succeeded,
            error::UnexpectedSnafu {
                err_msg: "Failed to retrieves the metadata"
            }
        );

        let mut set = TxnOpGetResponseSet::from(&mut resp.responses);

        // Builds the restore tombstone transaction.
        let mut tombstone_operations = Vec::with_capacity(keys.len() * 2);
        let mut tombstone_comparison = vec![];
        for (idx, key) in keys.iter().enumerate() {
            let filter = &mut filters[idx];
            let value = filter(&mut set).with_context(|| error::UnexpectedSnafu {
                err_msg: format!(
                    "Missing value, key: {}",
                    String::from_utf8_lossy(key.get_inner())
                ),
            })?;
            let (origin_key, tombstone_key) = tombstone_keys[idx].to_keys();
            // Compares the atomic key.
            if key.is_atomic() {
                tombstone_comparison.push(Compare::with_value(
                    tombstone_key.clone(),
                    CompareOp::Equal,
                    value.clone(),
                ));
            }
            tombstone_operations.push(TxnOp::Delete(tombstone_key));
            tombstone_operations.push(TxnOp::Put(origin_key, value));
        }

        let txn = if !tombstone_comparison.is_empty() {
            Txn::new().when(tombstone_comparison)
        } else {
            Txn::new()
        }
        .and_then(tombstone_operations);

        let resp = self.kv_backend.txn(txn).await?;
        Ok(resp.succeeded)
    }

    /// Deletes tombstones for keys.
    pub(crate) async fn delete(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        let operations = keys
            .iter()
            .map(|key| TxnOp::Delete(TombstoneKey(key).to_tombstone_key()))
            .collect::<Vec<_>>();

        let txn = Txn::new().and_then(operations);
        // Always success.
        let _ = self.kv_backend.txn(txn).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use crate::key::tombstone::{Key, TombstoneKey, TombstoneManager};
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::KvBackend;
    use crate::rpc::store::PutRequest;

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
        assert!(tombstone_manager
            .create(vec![Key::atomic("bar"), Key::other("foo")])
            .await
            .unwrap());
        assert!(!kv_backend.exists(b"bar").await.unwrap());
        assert!(!kv_backend.exists(b"foo").await.unwrap());
        assert_eq!(
            kv_backend
                .get(&TombstoneKey(&"bar".into()).to_tombstone_key())
                .await
                .unwrap()
                .unwrap()
                .value,
            b"baz"
        );
        assert_eq!(
            kv_backend
                .get(&TombstoneKey(&"foo".into()).to_tombstone_key())
                .await
                .unwrap()
                .unwrap()
                .value,
            b"hi"
        );
        assert_eq!(kv_backend.len(), 2);
    }

    #[tokio::test]
    async fn test_create_tombstone_without_atomic_key() {
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
        assert!(tombstone_manager
            .create(vec![Key::other("bar"), Key::other("foo")])
            .await
            .unwrap());
        assert!(!kv_backend.exists(b"bar").await.unwrap());
        assert!(!kv_backend.exists(b"foo").await.unwrap());
        assert_eq!(
            kv_backend
                .get(&TombstoneKey(&"bar".into()).to_tombstone_key())
                .await
                .unwrap()
                .unwrap()
                .value,
            b"baz"
        );
        assert_eq!(
            kv_backend
                .get(&TombstoneKey(&"foo".into()).to_tombstone_key())
                .await
                .unwrap()
                .unwrap()
                .value,
            b"hi"
        );
        assert_eq!(kv_backend.len(), 2);
    }

    #[tokio::test]
    async fn test_create_tombstone_origin_value_not_found_err() {
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

        let err = tombstone_manager
            .create(vec![Key::atomic("bar"), Key::other("baz")])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Missing value"));
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
        assert!(tombstone_manager
            .create(vec![Key::atomic("bar"), Key::other("foo")])
            .await
            .unwrap());
        assert!(tombstone_manager
            .restore(vec![Key::atomic("bar"), Key::other("foo")])
            .await
            .unwrap());
        assert_eq!(expected_kvs, kv_backend.dump());
    }

    #[tokio::test]
    async fn test_restore_tombstone_without_atomic_key() {
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
        assert!(tombstone_manager
            .create(vec![Key::atomic("bar"), Key::other("foo")])
            .await
            .unwrap());
        assert!(tombstone_manager
            .restore(vec![Key::other("bar"), Key::other("foo")])
            .await
            .unwrap());
        assert_eq!(expected_kvs, kv_backend.dump());
    }

    #[tokio::test]
    async fn test_restore_tombstone_origin_value_not_found_err() {
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
        assert!(tombstone_manager
            .create(vec![Key::atomic("bar"), Key::other("foo")])
            .await
            .unwrap());
        let err = tombstone_manager
            .restore(vec![Key::other("bar"), Key::other("baz")])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Missing value"));
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
        assert!(tombstone_manager
            .create(vec![Key::atomic("bar"), Key::other("foo")])
            .await
            .unwrap());
        tombstone_manager
            .delete(vec![b"bar".to_vec(), b"foo".to_vec()])
            .await
            .unwrap();
        assert!(kv_backend.is_empty());
    }
}
