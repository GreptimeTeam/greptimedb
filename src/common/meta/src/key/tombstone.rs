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
        TxnOpGetResponseSet::build_get_op(to_tombstone(self.0))
    }
}

/// The key used in the [TombstoneManager].
pub(crate) struct Key {
    bytes: Vec<u8>,
    // Atomic Key:
    // The value corresponding to the key remains consistent between two transactions.
    atomic: bool,
}

impl Key {
    /// Returns a new atomic key.
    pub(crate) fn compare_and_swap<T: Into<Vec<u8>>>(key: T) -> Self {
        Self {
            bytes: key.into(),
            atomic: true,
        }
    }

    /// Returns a new normal key.
    pub(crate) fn new<T: Into<Vec<u8>>>(key: T) -> Self {
        Self {
            bytes: key.into(),
            atomic: false,
        }
    }

    /// Into bytes
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    fn get_inner(&self) -> &Vec<u8> {
        &self.bytes
    }

    fn is_atomic(&self) -> bool {
        self.atomic
    }
}

fn format_move_value_error(
    unexpected_items: Vec<(MoveValue, Option<Vec<u8>>, Option<Vec<u8>>)>,
) -> String {
    unexpected_items
        .into_iter()
        .map(
            |(
                MoveValue {
                    key,
                    dest_key,
                    value,
                },
                src_value,
                dest_value,
            )| {
                format!(
                    "Moving key: '{}' to dest: '{}' with value: '{}'\nSrc value: {:?}\nDest value: {:?}",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&dest_key),
                    String::from_utf8_lossy(&value),
                    src_value.map(|value| String::from_utf8_lossy(&value).to_string()),
                    dest_value.map(|value| String::from_utf8_lossy(&value).to_string()),
                )
            },
        )
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Debug, Clone)]
struct MoveValue {
    key: Vec<u8>,
    dest_key: Vec<u8>,
    value: Vec<u8>,
}

impl From<(Vec<u8>, Vec<u8>, Vec<u8>)> for MoveValue {
    fn from((key, dest_key, value): (Vec<u8>, Vec<u8>, Vec<u8>)) -> Self {
        MoveValue {
            key,
            dest_key,
            value,
        }
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

fn format_on_failure_error_message<F: FnMut(&mut TxnOpGetResponseSet) -> Option<Vec<u8>>>(
    mut set: TxnOpGetResponseSet,
    on_failure_kv_and_filters: Vec<(Vec<u8>, Vec<u8>, F)>,
) -> String {
    on_failure_kv_and_filters
        .into_iter()
        .flat_map(|(key, value, mut filter)| {
            let got = filter(&mut set);
            let Some(got) = got else {
                return Some(format!(
                    "For key: {} was expected: {}, but value does not exists",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&value),
                ));
            };

            if got != value {
                Some(format!(
                    "For key: {} was expected: {}, but got: {}",
                    String::from_utf8_lossy(&key),
                    String::from_utf8_lossy(&value),
                    String::from_utf8_lossy(&got),
                ))
            } else {
                None
            }
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn format_keys(keys: &[Key]) -> String {
    keys.iter()
        .map(|key| String::from_utf8_lossy(&key.bytes))
        .collect::<Vec<_>>()
        .join(", ")
}

impl TombstoneManager {
    /// Returns [TombstoneManager].
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Moves value to `dest_key`.
    ///
    /// Puts `value` to `dest_key` if:
    /// - the value of `src_key` equals `value`.
    /// - the `dest_key` is vacant.
    ///
    /// Otherwise retrieves the values of `src_key`, `dest_key`.
    fn build_move_value_txn(
        &self,
        src_key: Vec<u8>,
        value: Vec<u8>,
        dest_key: Vec<u8>,
    ) -> (
        Txn,
        impl FnMut(&mut TxnOpGetResponseSet) -> Option<Vec<u8>>,
        impl FnMut(&mut TxnOpGetResponseSet) -> Option<Vec<u8>>,
    ) {
        let txn = Txn::new()
            .when(vec![
                Compare::with_not_exist_value(dest_key.clone(), CompareOp::Equal),
                Compare::with_value(src_key.clone(), CompareOp::Equal, value.clone()),
            ])
            .and_then(vec![
                TxnOp::Put(dest_key.clone(), value.clone()),
                TxnOp::Delete(src_key.clone()),
            ])
            .or_else(vec![
                TxnOp::Get(src_key.clone()),
                TxnOp::Get(dest_key.clone()),
            ]);

        (
            txn,
            TxnOpGetResponseSet::filter(src_key),
            TxnOpGetResponseSet::filter(dest_key),
        )
    }

    async fn move_values(&self, kvs: Vec<MoveValue>) -> Result<()> {
        let mut txns = Vec::with_capacity(kvs.len());
        let mut src_key_filters = Vec::with_capacity(kvs.len());
        let mut dest_key_filters = Vec::with_capacity(kvs.len());
        for MoveValue {
            key,
            dest_key,
            value,
        } in &kvs
        {
            let (txn, src_key_filter, dest_key_filter) =
                self.build_move_value_txn(key.clone(), value.clone(), dest_key.clone());
            txns.push(txn);
            src_key_filters.push(src_key_filter);
            dest_key_filters.push(dest_key_filter);
        }
        let txn = Txn::merge_all(txns);
        let mut resp = self.kv_backend.txn(txn).await?;
        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let mut comparison_result = Vec::with_capacity(kvs.len());
            let mut unexpected_items = Vec::with_capacity(kvs.len());
            for (
                idx,
                MoveValue {
                    key,
                    dest_key,
                    value,
                },
            ) in kvs.into_iter().enumerate()
            {
                // Checks value is moved
                let remote_srv_value = (src_key_filters[idx])(&mut set);
                let remote_dest_value = (dest_key_filters[idx])(&mut set);
                let expected =
                    remote_srv_value.is_none() && remote_dest_value.as_ref() == Some(&value);
                comparison_result.push(expected);
                if !expected {
                    unexpected_items.push((
                        MoveValue {
                            key,
                            dest_key,
                            value,
                        },
                        remote_srv_value,
                        remote_dest_value,
                    ));
                }
            }
            ensure!(
                comparison_result.into_iter().all(Into::into),
                error::MoveValuesSnafu {
                    err_msg: format_move_value_error(unexpected_items)
                }
            )
        }
        Ok(())
    }

    /// Creates tombstones for keys.
    ///
    /// Preforms to:
    /// - retrieve all values corresponding `keys`.
    /// - stores tombstone values.
    pub(crate) async fn create(&self, keys: Vec<Key>) -> Result<()> {
        // Builds transaction to retrieve all values
        let (operations, mut filters): (Vec<_>, Vec<_>) =
            keys.iter().map(|key| key.build_get_op()).unzip();

        let txn = Txn::new().and_then(operations);
        let mut resp = self.kv_backend.txn(txn).await?;
        ensure!(
            resp.succeeded,
            error::UnexpectedSnafu {
                err_msg: format!(
                    "Failed to retrieves the metadata, keys: {}",
                    format_keys(&keys)
                ),
            }
        );

        let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
        // Builds the create tombstone transaction.
        let mut tombstone_operations = Vec::with_capacity(keys.len() * 2);
        let mut tombstone_comparison = vec![];
        let mut on_failure_operations = vec![];
        let mut on_failure_kv_and_filters = vec![];
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
                tombstone_comparison.push(Compare::with_not_exist_value(
                    tombstone_key.clone(),
                    CompareOp::Equal,
                ));
                tombstone_comparison.push(Compare::with_value(
                    origin_key.clone(),
                    CompareOp::Equal,
                    value.clone(),
                ));
                let (op, filter) = TxnOpGetResponseSet::build_get_op(origin_key.clone());
                on_failure_operations.push(op);
                on_failure_kv_and_filters.push((origin_key.clone(), value.clone(), filter));
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

        let txn = if !on_failure_operations.is_empty() {
            txn.or_else(on_failure_operations)
        } else {
            txn
        };

        let mut resp = self.kv_backend.txn(txn).await?;
        // TODO(weny): add tests for atomic key changed.
        if !resp.succeeded {
            let set = TxnOpGetResponseSet::from(&mut resp.responses);
            let err_msg = format_on_failure_error_message(set, on_failure_kv_and_filters);
            return error::CasKeyChangedSnafu { err_msg }.fail();
        }
        Ok(())
    }

    /// Restores tombstones for keys.
    ///
    /// Preforms to:
    /// - retrieve all tombstone values corresponding `keys`.
    /// - stores tombstone values.
    pub(crate) async fn restore(&self, keys: Vec<Key>) -> Result<()> {
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
                err_msg: format!(
                    "Failed to retrieves the metadata, keys: {}",
                    format_keys(&keys)
                ),
            }
        );

        let mut set = TxnOpGetResponseSet::from(&mut resp.responses);

        // Builds the restore tombstone transaction.
        let mut tombstone_operations = Vec::with_capacity(keys.len() * 2);
        let mut tombstone_comparison = vec![];
        let mut on_failure_operations = vec![];
        let mut on_failure_kv_and_filters = vec![];
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
                tombstone_comparison.push(Compare::with_not_exist_value(
                    origin_key.clone(),
                    CompareOp::Equal,
                ));
                tombstone_comparison.push(Compare::with_value(
                    tombstone_key.clone(),
                    CompareOp::Equal,
                    value.clone(),
                ));
                let (op, filter) = tombstone_keys[idx].build_get_op();
                on_failure_operations.push(op);
                on_failure_kv_and_filters.push((tombstone_key.clone(), value.clone(), filter));
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

        let txn = if !on_failure_operations.is_empty() {
            txn.or_else(on_failure_operations)
        } else {
            txn
        };

        let mut resp = self.kv_backend.txn(txn).await?;
        // TODO(weny): add tests for atomic key changed.
        if !resp.succeeded {
            let set = TxnOpGetResponseSet::from(&mut resp.responses);
            let err_msg = format_on_failure_error_message(set, on_failure_kv_and_filters);
            return error::CasKeyChangedSnafu { err_msg }.fail();
        }

        Ok(())
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

    use std::assert_matches::assert_matches;
    use std::collections::HashMap;
    use std::sync::Arc;

    use super::to_tombstone;
    use crate::error::{self, Error};
    use crate::key::tombstone::{Key, MoveValue, TombstoneKey, TombstoneManager};
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
        tombstone_manager
            .create(vec![Key::compare_and_swap("bar"), Key::new("foo")])
            .await
            .unwrap();
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
        tombstone_manager
            .create(vec![Key::new("bar"), Key::new("foo")])
            .await
            .unwrap();
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
            .create(vec![Key::compare_and_swap("bar"), Key::new("baz")])
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
        tombstone_manager
            .create(vec![Key::compare_and_swap("bar"), Key::new("foo")])
            .await
            .unwrap();
        tombstone_manager
            .restore(vec![Key::compare_and_swap("bar"), Key::new("foo")])
            .await
            .unwrap();
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
        tombstone_manager
            .create(vec![Key::compare_and_swap("bar"), Key::new("foo")])
            .await
            .unwrap();
        tombstone_manager
            .restore(vec![Key::new("bar"), Key::new("foo")])
            .await
            .unwrap();
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
        tombstone_manager
            .create(vec![Key::compare_and_swap("bar"), Key::new("foo")])
            .await
            .unwrap();
        let err = tombstone_manager
            .restore(vec![Key::new("bar"), Key::new("baz")])
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
        tombstone_manager
            .create(vec![Key::compare_and_swap("bar"), Key::new("foo")])
            .await
            .unwrap();
        tombstone_manager
            .delete(vec![b"bar".to_vec(), b"foo".to_vec()])
            .await
            .unwrap();
        assert!(kv_backend.is_empty());
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
    async fn test_move_value() {
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
                dest_key: to_tombstone(key),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        tombstone_manager
            .move_values(move_values.clone())
            .await
            .unwrap();
        check_moved_values(kv_backend.clone(), &move_values).await;
        // Moves again
        tombstone_manager
            .move_values(move_values.clone())
            .await
            .unwrap();
        check_moved_values(kv_backend.clone(), &move_values).await;
    }

    #[tokio::test]
    async fn test_move_value_changed() {
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
                dest_key: to_tombstone(key),
                value: value.clone(),
            })
            .collect::<Vec<_>>();
        let err = tombstone_manager
            .move_values(move_values.clone())
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::MoveValues { .. });
    }
}
