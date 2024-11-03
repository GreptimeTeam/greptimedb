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

use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_procedure::error::{DeleteStatesSnafu, ListStateSnafu, PutStateSnafu};
use common_procedure::store::state_store::{KeySet, KeyValueStream, StateStore};
use common_procedure::store::util::multiple_value_stream;
use common_procedure::Result as ProcedureResult;
use futures::future::try_join_all;
use futures::StreamExt;
use itertools::Itertools;
use snafu::ResultExt;

use crate::error::Result;
use crate::kv_backend::KvBackendRef;
use crate::range_stream::PaginationStream;
use crate::rpc::store::{BatchDeleteRequest, PutRequest, RangeRequest};
use crate::rpc::KeyValue;

const DELIMITER: &str = "/";

const PROCEDURE_PREFIX: &str = "/__procedure__/";

fn with_prefix(key: &str) -> String {
    format!("{PROCEDURE_PREFIX}{key}")
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
            Arc::new(decode_kv),
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

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Arc;

    use common_procedure::store::state_store::KeyValue;
    use common_telemetry::info;
    use futures::TryStreamExt;
    use rand::{Rng, RngCore};
    use uuid::Uuid;

    use super::*;
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
        let num_cases = rand::thread_rng().gen_range(1..=26);
        let mut cases = Vec::with_capacity(num_cases);
        for i in 0..num_cases {
            let size = rand::thread_rng().gen_range(size_limit..=max_bytes);
            let mut large_value = vec![0u8; size as usize];
            rand::thread_rng().fill_bytes(&mut large_value);

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
            store.put(key, value.clone()).await.unwrap();
        }

        // Validates the values
        for TestCase { prefix, key, value } in &cases {
            let data = walk_top_down(prefix).await;
            assert_eq!(data.len(), 1);
            let (keyset, got) = data.into_iter().next().unwrap();
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
        let size_limit = rand::thread_rng().gen_range(128..=512);
        let page_size = rand::thread_rng().gen_range(1..10);
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
        let page_size = rand::thread_rng().gen_range(1..10);
        test_meta_state_store_split_value_with_size_limit(
            kv_backend,
            size_limit,
            page_size,
            size_limit * 10,
        )
        .await;
    }
}
