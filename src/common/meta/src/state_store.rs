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

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_procedure::error::{
    DeletePoisonSnafu, DeleteStatesSnafu, GetPoisonSnafu, ListStateSnafu, PutPoisonSnafu,
    PutStateSnafu, Result as ProcedureResult,
};
use common_procedure::store::poison_store::PoisonStore;
use common_procedure::store::state_store::{KeySet, KeyValueStream, StateStore};
use common_procedure::store::util::multiple_value_stream;
use futures::future::try_join_all;
use futures::StreamExt;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{ProcedurePoisonConflictSnafu, Result};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{DeserializedValueWithBytes, MetadataValue};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::PaginationStream;
use crate::rpc::store::{BatchDeleteRequest, PutRequest, RangeRequest};
use crate::rpc::KeyValue;

const DELIMITER: &str = "/";

const PROCEDURE_PREFIX: &str = "/__procedure__/";
const PROCEDURE_POISON_KEY_PREFIX: &str = "/__procedure_poison/";

fn with_prefix(key: &str) -> String {
    format!("{PROCEDURE_PREFIX}{key}")
}

fn with_poison_prefix(key: &str) -> String {
    format!("{}{}", PROCEDURE_POISON_KEY_PREFIX, key)
}

fn strip_prefix(key: &str) -> String {
    key.trim_start_matches(PROCEDURE_PREFIX).to_string()
}

pub struct KvStateStore {
    kv_backend: KvBackendRef,
    // The max num of keys to be returned in a range scan request
    // `None` stands for no limit.
    max_num_per_range_request: Option<usize>,
    // The max bytes of value.
    // `None` stands for no limit.
    max_value_size: Option<usize>,
}

impl KvStateStore {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            kv_backend,
            max_num_per_range_request: None,
            max_value_size: None,
        }
    }

    /// Sets the `max_value_size`. `None` stands for no limit.
    ///
    /// If a value is larger than the `max_value_size`,
    /// the [`KvStateStore`] will automatically split the large value into multiple values.
    pub fn with_max_value_size(mut self, max_value_size: Option<usize>) -> Self {
        self.max_value_size = max_value_size;
        self
    }
}

fn decode_kv(kv: KeyValue) -> Result<(String, Vec<u8>)> {
    let key = String::from_utf8_lossy(&kv.key);
    let key = strip_prefix(&key);
    let value = kv.value;

    Ok((key, value))
}

enum SplitValue {
    Single(Vec<u8>),
    Multiple(Vec<Vec<u8>>),
}

fn split_value(value: Vec<u8>, max_value_size: Option<usize>) -> SplitValue {
    if let Some(max_value_size) = max_value_size {
        if value.len() <= max_value_size {
            SplitValue::Single(value)
        } else {
            let mut values = vec![];
            for chunk in value.into_iter().chunks(max_value_size).into_iter() {
                values.push(chunk.collect());
            }
            SplitValue::Multiple(values)
        }
    } else {
        SplitValue::Single(value)
    }
}

#[async_trait]
impl StateStore for KvStateStore {
    async fn put(&self, key: &str, value: Vec<u8>) -> ProcedureResult<()> {
        let split = split_value(value, self.max_value_size);
        let key = with_prefix(key);
        match split {
            SplitValue::Single(value) => {
                self.kv_backend
                    .put(
                        PutRequest::new()
                            .with_key(key.to_string().into_bytes())
                            .with_value(value),
                    )
                    .await
                    .map_err(BoxedError::new)
                    .context(PutStateSnafu { key })?;
                Ok(())
            }
            SplitValue::Multiple(values) => {
                // Note:
                // The length of values can be up to usize::MAX.
                // The KeySet::with_segment_suffix method uses a 10-digit number to store the segment number,
                // which is large enough for the usize type.

                // The first segment key: "0b00001111"
                // The 2nd segment key: "0b00001111/0000000001"
                // The 3rd segment key: "0b00001111/0000000002"
                let operations = values
                    .into_iter()
                    .enumerate()
                    .map(|(idx, value)| {
                        let key = if idx > 0 {
                            KeySet::with_segment_suffix(&key, idx)
                        } else {
                            key.to_string()
                        };
                        let kv_backend = self.kv_backend.clone();
                        async move {
                            kv_backend
                                .put(
                                    PutRequest::new()
                                        .with_key(key.into_bytes())
                                        .with_value(value),
                                )
                                .await
                        }
                    })
                    .collect::<Vec<_>>();

                try_join_all(operations)
                    .await
                    .map_err(BoxedError::new)
                    .context(PutStateSnafu { key })?;

                Ok(())
            }
        }
    }

    async fn walk_top_down(&self, path: &str) -> ProcedureResult<KeyValueStream> {
        // extend their lifetimes to be used in the stream
        let path = path.to_string();

        let key = with_prefix(path.trim_start_matches(DELIMITER)).into_bytes();
        let req = RangeRequest::new().with_prefix(key);

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            self.max_num_per_range_request.unwrap_or_default(),
            decode_kv,
        )
        .into_stream();

        let stream = stream.map(move |r| {
            let path = path.clone();
            r.map_err(BoxedError::new)
                .with_context(|_| ListStateSnafu { path })
        });

        let stream = multiple_value_stream(Box::pin(stream));

        Ok(Box::pin(stream))
    }

    async fn batch_delete(&self, keys: &[String]) -> ProcedureResult<()> {
        let _ = self
            .kv_backend
            .batch_delete(BatchDeleteRequest {
                keys: keys
                    .iter()
                    .map(|x| with_prefix(x).into_bytes())
                    .collect::<Vec<_>>(),
                ..Default::default()
            })
            .await
            .map_err(BoxedError::new)
            .with_context(|_| DeleteStatesSnafu {
                keys: format!("{:?}", keys.to_vec()),
            })?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> ProcedureResult<()> {
        self.batch_delete(&[key.to_string()]).await
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoisonValue {
    token: String,
}

type PoisonDecodeResult = Result<Option<DeserializedValueWithBytes<PoisonValue>>>;

impl KvStateStore {
    /// Builds a create poison transaction,
    /// it expected the `__procedure_poison/{resource_type}/{resource_id}` wasn't occupied.
    fn build_create_poison_txn(
        &self,
        key: &str,
        value: &PoisonValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> PoisonDecodeResult,
    )> {
        let key = key.as_bytes().to_vec();
        let value = value.try_as_raw_value()?;
        let txn = Txn::put_if_not_exists(key.clone(), value);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(key)),
        ))
    }

    /// Builds a delete poison transaction,
    /// it expected the `__procedure_poison/{resource_type}/{resource_id}` was occupied.
    fn build_delete_poison_txn(
        &self,
        key: &str,
        value: PoisonValue,
    ) -> Result<(
        Txn,
        impl FnOnce(&mut TxnOpGetResponseSet) -> PoisonDecodeResult,
    )> {
        let key = key.as_bytes().to_vec();
        let value = value.try_as_raw_value()?;

        let txn = Txn::new()
            .when(vec![Compare::with_value(
                key.clone(),
                CompareOp::Equal,
                value,
            )])
            .and_then(vec![TxnOp::Delete(key.clone())])
            .or_else(vec![TxnOp::Get(key.clone())]);

        Ok((
            txn,
            TxnOpGetResponseSet::decode_with(TxnOpGetResponseSet::filter(key)),
        ))
    }

    async fn get_poison_inner(&self, key: &str) -> Result<Option<PoisonValue>> {
        let key = with_poison_prefix(key);
        let value = self.kv_backend.get(key.as_bytes()).await?;
        value
            .map(|v| PoisonValue::try_from_raw_value(&v.value))
            .transpose()
    }

    /// Put the poison.
    ///
    /// If the poison is already put by other procedure, it will return an error.
    async fn set_poison_inner(&self, key: &str, token: &str) -> Result<()> {
        let key = with_poison_prefix(key);
        let (txn, on_failure) = self.build_create_poison_txn(
            &key,
            &PoisonValue {
                token: token.to_string(),
            },
        )?;

        let mut resp = self.kv_backend.txn(txn).await?;

        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let remote_value = on_failure(&mut set)?
                .context(ProcedurePoisonConflictSnafu {
                    msg: "Reads the empty poison value in comparing operation of the put consistency poison",
                })?
                .into_inner();

            ensure!(
                remote_value.token == token,
                ProcedurePoisonConflictSnafu {
                    msg: format!(
                        "The poison is already put by other token {}",
                        remote_value.token
                    ),
                }
            );
        }

        Ok(())
    }

    /// Deletes the poison.
    ///
    /// If the poison is not put by the procedure, it will return an error.
    async fn delete_poison_inner(&self, key: &str, token: &str) -> Result<()> {
        let key = with_poison_prefix(key);
        let (txn, on_failure) = self.build_delete_poison_txn(
            &key,
            PoisonValue {
                token: token.to_string(),
            },
        )?;

        let mut resp = self.kv_backend.txn(txn).await?;

        if !resp.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut resp.responses);
            let remote_value = on_failure(&mut set)?;

            ensure!(
                remote_value.is_none(),
                ProcedurePoisonConflictSnafu {
                    msg: format!(
                        "The poison is already put by other token {}",
                        remote_value.unwrap().into_inner().token
                    ),
                }
            );
        }

        Ok(())
    }
}

#[async_trait]
impl PoisonStore for KvStateStore {
    async fn set_poison(&self, key: String, token: String) -> ProcedureResult<()> {
        self.set_poison_inner(&key, &token)
            .await
            .map_err(BoxedError::new)
            .context(PutPoisonSnafu { key, token })
    }

    async fn delete_poison(&self, key: String, token: String) -> ProcedureResult<()> {
        self.delete_poison_inner(&key, &token)
            .await
            .map_err(BoxedError::new)
            .context(DeletePoisonSnafu { key, token })
    }

    async fn get_poison(&self, key: &str) -> ProcedureResult<Option<String>> {
        self.get_poison_inner(key)
            .await
            .map(|v| v.map(|v| v.token))
            .map_err(BoxedError::new)
            .context(GetPoisonSnafu { key })
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::env;
    use std::sync::Arc;

    use common_procedure::store::state_store::KeyValue;
    use common_telemetry::info;
    use futures::TryStreamExt;
    use rand::{Rng, RngCore};
    use uuid::Uuid;

    use super::*;
    use crate::error::Error;
    use crate::kv_backend::chroot::ChrootKvBackend;
    use crate::kv_backend::etcd::EtcdStore;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_meta_state_store() {
        let store = &KvStateStore {
            kv_backend: Arc::new(MemoryKvBackend::new()),
            max_num_per_range_request: Some(1), // for testing "more" in range
            max_value_size: None,
        };

        let walk_top_down = async move |path: &str| -> Vec<KeyValue> {
            let mut data = store
                .walk_top_down(path)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            data.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            data
        };

        let data = walk_top_down("/").await;
        assert!(data.is_empty());

        store.put("a/1", b"v1".to_vec()).await.unwrap();
        store.put("a/2", b"v2".to_vec()).await.unwrap();
        store.put("b/1", b"v3".to_vec()).await.unwrap();

        let data = walk_top_down("/").await;
        assert_eq!(
            vec![
                ("a/1".into(), b"v1".to_vec()),
                ("a/2".into(), b"v2".to_vec()),
                ("b/1".into(), b"v3".to_vec())
            ],
            data
        );

        let data = walk_top_down("a/").await;
        assert_eq!(
            vec![
                ("a/1".into(), b"v1".to_vec()),
                ("a/2".into(), b"v2".to_vec()),
            ],
            data
        );

        store
            .batch_delete(&["a/2".to_string(), "b/1".to_string()])
            .await
            .unwrap();

        let data = walk_top_down("a/").await;
        assert_eq!(vec![("a/1".into(), b"v1".to_vec()),], data);
    }

    struct TestCase {
        prefix: String,
        key: String,
        value: Vec<u8>,
    }

    async fn test_meta_state_store_split_value_with_size_limit(
        kv_backend: KvBackendRef,
        size_limit: u32,
        num_per_range: u32,
        max_bytes: u32,
    ) {
        let num_cases = rand::rng().random_range(1..=8);
        common_telemetry::info!("num_cases: {}", num_cases);
        let mut cases = Vec::with_capacity(num_cases);
        for i in 0..num_cases {
            let size = rand::rng().random_range(size_limit..=max_bytes);
            let mut large_value = vec![0u8; size as usize];
            rand::rng().fill_bytes(&mut large_value);

            // Starts from `a`.
            let prefix = format!("{}/", std::char::from_u32(97 + i as u32).unwrap());
            cases.push(TestCase {
                key: format!("{}{}.commit", prefix, Uuid::new_v4()),
                prefix,
                value: large_value,
            })
        }
        let store = &KvStateStore {
            kv_backend: kv_backend.clone(),
            max_num_per_range_request: Some(num_per_range as usize), // for testing "more" in range
            max_value_size: Some(size_limit as usize),
        };
        let walk_top_down = async move |path: &str| -> Vec<KeyValue> {
            let mut data = store
                .walk_top_down(path)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            data.sort_unstable_by(|a, b| a.0.cmp(&b.0));
            data
        };

        // Puts the values
        for TestCase { key, value, .. } in &cases {
            common_telemetry::info!("put key: {}, size: {}", key, value.len());
            store.put(key, value.clone()).await.unwrap();
        }

        // Validates the values
        for TestCase { prefix, key, value } in &cases {
            let data = walk_top_down(prefix).await;
            assert_eq!(data.len(), 1);
            let (keyset, got) = data.into_iter().next().unwrap();
            common_telemetry::info!("get key: {}", keyset.key());
            let num_expected_keys = value.len().div_ceil(size_limit as usize);
            assert_eq!(&got, value);
            assert_eq!(keyset.key(), key);
            assert_eq!(keyset.keys().len(), num_expected_keys);
        }

        // Deletes the values
        for TestCase { prefix, .. } in &cases {
            let data = walk_top_down(prefix).await;
            let (keyset, _) = data.into_iter().next().unwrap();
            // Deletes values
            store.batch_delete(keyset.keys().as_slice()).await.unwrap();
            let data = walk_top_down(prefix).await;
            assert_eq!(data.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_meta_state_store_split_value() {
        let size_limit = rand::rng().random_range(128..=512);
        let page_size = rand::rng().random_range(1..10);
        let kv_backend = Arc::new(MemoryKvBackend::new());
        test_meta_state_store_split_value_with_size_limit(kv_backend, size_limit, page_size, 8192)
            .await;
    }

    #[tokio::test]
    async fn test_etcd_store_split_value() {
        common_telemetry::init_default_ut_logging();
        let prefix = "test_etcd_store_split_value/";
        let endpoints = env::var("GT_ETCD_ENDPOINTS").unwrap_or_default();
        let kv_backend: KvBackendRef = if endpoints.is_empty() {
            common_telemetry::info!("Using MemoryKvBackend");
            Arc::new(MemoryKvBackend::new())
        } else {
            let endpoints = endpoints
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>();
            let backend = EtcdStore::with_endpoints(endpoints, 128)
                .await
                .expect("malformed endpoints");
            // Each retry requires a new isolation namespace.
            let chroot = format!("{}{}", prefix, Uuid::new_v4());
            info!("chroot length: {}", chroot.len());
            Arc::new(ChrootKvBackend::new(chroot.into(), backend))
        };

        let key_size = 1024;
        // The etcd default size limit of any requests is 1.5MiB.
        // However, some KvBackends, the `ChrootKvBackend`, will add the prefix to `key`;
        // we don't know the exact size of the key.
        let size_limit = 1536 * 1024 - key_size;
        let page_size = rand::rng().random_range(1..10);
        test_meta_state_store_split_value_with_size_limit(
            kv_backend,
            size_limit,
            page_size,
            size_limit * 10,
        )
        .await;
    }

    #[tokio::test]
    async fn test_poison() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let poison_manager = KvStateStore::new(mem_kv.clone());

        let key = "table/1";

        let token = "expected_token";

        poison_manager.set_poison_inner(key, token).await.unwrap();

        // Put again, should be ok.
        poison_manager.set_poison_inner(key, token).await.unwrap();

        // Delete, should be ok.
        poison_manager
            .delete_poison_inner(key, token)
            .await
            .unwrap();

        // Delete again, should be ok.
        poison_manager
            .delete_poison_inner(key, token)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_consistency_poison_failed() {
        let mem_kv = Arc::new(MemoryKvBackend::default());
        let poison_manager = KvStateStore::new(mem_kv.clone());

        let key = "table/1";

        let token = "expected_token";
        let token2 = "expected_token2";

        poison_manager.set_poison_inner(key, token).await.unwrap();

        let err = poison_manager
            .set_poison_inner(key, token2)
            .await
            .unwrap_err();
        assert_matches!(err, Error::ProcedurePoisonConflict { .. });

        let err = poison_manager
            .delete_poison_inner(key, token2)
            .await
            .unwrap_err();
        assert_matches!(err, Error::ProcedurePoisonConflict { .. });
    }

    #[test]
    fn test_serialize_deserialize() {
        let key = "table/1";
        let value = PoisonValue {
            token: "expected_token".to_string(),
        };

        let serialized_key = with_poison_prefix(key).as_bytes().to_vec();
        let serialized_value = value.try_as_raw_value().unwrap();

        let expected_key = "/__procedure_poison/table/1";
        let expected_value = r#"{"token":"expected_token"}"#;

        assert_eq!(expected_key.as_bytes(), serialized_key);
        assert_eq!(expected_value.as_bytes(), serialized_value);
    }
}
