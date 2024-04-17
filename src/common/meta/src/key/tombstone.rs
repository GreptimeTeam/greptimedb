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

use snafu::ensure;

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

pub(crate) struct TombstoneKeyValue {
    pub(crate) origin_key: Vec<u8>,
    pub(crate) tombstone_key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

fn to_tombstone(key: &[u8]) -> Vec<u8> {
    [TOMBSTONE_PREFIX.as_bytes(), key].concat()
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

    /// Moves values to `dest_key` if:
    ///
    /// - All `dest_key` are vacant.
    /// - All origin values remain unchanged.
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
    /// - deletes origin values.
    /// - stores tombstone values.
    pub(crate) async fn create(&self, kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        let move_values = kvs
            .into_iter()
            .map(|(key, value)| {
                let dest_key = to_tombstone(&key);
                MoveValue {
                    key,
                    dest_key,
                    value,
                }
            })
            .collect();

        self.move_values(move_values).await
    }

    /// Restores tombstones for keys.
    ///
    /// Preforms to:
    /// - restore origin value.
    /// - deletes tombstone values.
    pub(crate) async fn restore(&self, kvs: Vec<(Vec<u8>, Vec<u8>)>) -> Result<()> {
        let move_values = kvs
            .into_iter()
            .map(|(key, value)| {
                let tombstone_key = to_tombstone(&key);
                MoveValue {
                    key: tombstone_key,
                    dest_key: key,
                    value,
                }
            })
            .collect();

        self.move_values(move_values).await
    }

    /// Retrieves a batch of tombstone values for the specified `keys`.
    pub(crate) async fn batch_get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<TombstoneKeyValue>> {
        let (ops, mut filters): (Vec<_>, Vec<_>) = keys
            .iter()
            .map(|key| TxnOpGetResponseSet::build_get_op(to_tombstone(key)))
            .unzip();

        let txn = Txn::new().and_then(ops);
        let mut resp = self.kv_backend.txn(txn).await?;
        let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
        let mut result = Vec::with_capacity(keys.len());
        for (idx, key) in keys.into_iter().enumerate() {
            let value = filters[idx](&mut set);
            if let Some(value) = value {
                let tombstone_key = to_tombstone(&key);
                result.push(TombstoneKeyValue {
                    origin_key: key,
                    tombstone_key,
                    value,
                })
            }
        }

        Ok(result)
    }

    /// Deletes tombstones values for the specified `keys`.
    pub(crate) async fn delete(&self, keys: Vec<Vec<u8>>) -> Result<()> {
        let operations = keys
            .iter()
            .map(|key| TxnOp::Delete(to_tombstone(key)))
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
    use crate::key::tombstone::{MoveValue, TombstoneKeyValue, TombstoneManager};
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
            .create(vec![
                (b"bar".to_vec(), b"baz".to_vec()),
                (b"foo".to_vec(), b"hi".to_vec()),
            ])
            .await
            .unwrap();
        assert!(!kv_backend.exists(b"bar").await.unwrap());
        assert!(!kv_backend.exists(b"foo").await.unwrap());
        assert_eq!(
            kv_backend
                .get(&to_tombstone(b"bar"))
                .await
                .unwrap()
                .unwrap()
                .value,
            b"baz"
        );
        assert_eq!(
            kv_backend
                .get(&to_tombstone(b"foo"))
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
            .create(vec![
                (b"bar".to_vec(), b"baz".to_vec()),
                (b"baz".to_vec(), b"hi".to_vec()),
            ])
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::MoveValues { .. });
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
            .create(vec![
                (b"bar".to_vec(), b"baz".to_vec()),
                (b"foo".to_vec(), b"hi".to_vec()),
            ])
            .await
            .unwrap();
        tombstone_manager
            .restore(vec![
                (b"bar".to_vec(), b"baz".to_vec()),
                (b"foo".to_vec(), b"hi".to_vec()),
            ])
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
            .create(vec![
                (b"bar".to_vec(), b"baz".to_vec()),
                (b"foo".to_vec(), b"hi".to_vec()),
            ])
            .await
            .unwrap();
        let err = tombstone_manager
            .restore(vec![
                (b"bar".to_vec(), b"baz".to_vec()),
                (b"baz".to_vec(), b"hi".to_vec()),
            ])
            .await
            .unwrap_err();
        assert_matches!(err, error::Error::MoveValues { .. });
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
            .create(vec![
                (b"bar".to_vec(), b"baz".to_vec()),
                (b"foo".to_vec(), b"hi".to_vec()),
            ])
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

    #[tokio::test]
    async fn test_batch_get() {
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
        tombstone_manager
            .create(vec![
                (b"bar".to_vec(), b"baz".to_vec()),
                (b"foo".to_vec(), b"hi".to_vec()),
            ])
            .await
            .unwrap();

        let values = tombstone_manager
            .batch_get(vec![b"bar".to_vec(), b"foo".to_vec(), b"hi".to_vec()])
            .await
            .unwrap();
        assert_eq!(values.len(), 2);
        for TombstoneKeyValue {
            origin_key,
            tombstone_key,
            ..
        } in values
        {
            assert_eq!(to_tombstone(&origin_key), tombstone_key)
        }
    }
}
