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
use common_procedure::store::state_store::{KeyValueStream, StateStore};
use common_procedure::Result as ProcedureResult;
use futures::StreamExt;
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
    // limit is set to 0, it is treated as no limit.
    max_size_per_range: usize,
}

impl KvStateStore {
    // `max_size_per_range` is set to 0, it is treated as no limit.
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            kv_backend,
            max_size_per_range: 0,
        }
    }
}

fn decode_kv(kv: KeyValue) -> Result<(String, Vec<u8>)> {
    let key = String::from_utf8_lossy(&kv.key);
    let key = strip_prefix(&key);
    let value = kv.value;

    Ok((key, value))
}

#[async_trait]
impl StateStore for KvStateStore {
    async fn put(&self, key: &str, value: Vec<u8>) -> ProcedureResult<()> {
        let _ = self
            .kv_backend
            .put(PutRequest {
                key: with_prefix(key).into_bytes(),
                value,
                ..Default::default()
            })
            .await
            .map_err(BoxedError::new)
            .context(PutStateSnafu { key })?;
        Ok(())
    }

    async fn walk_top_down(&self, path: &str) -> ProcedureResult<KeyValueStream> {
        // extend their lifetimes to be used in the stream
        let path = path.to_string();

        let key = with_prefix(path.trim_start_matches(DELIMITER)).into_bytes();
        let req = RangeRequest::new().with_prefix(key);

        let stream = PaginationStream::new(
            self.kv_backend.clone(),
            req,
            self.max_size_per_range,
            Arc::new(decode_kv),
        );

        let stream = stream.map(move |r| {
            let path = path.clone();
            r.map_err(BoxedError::new)
                .with_context(|_| ListStateSnafu { path })
        });

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
    use std::sync::Arc;

    use common_procedure::store::state_store::KeyValue;
    use futures::TryStreamExt;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_meta_state_store() {
        let store = &KvStateStore {
            kv_backend: Arc::new(MemoryKvBackend::new()),
            max_size_per_range: 1, // for testing "more" in range
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
                ("a/1".to_string(), b"v1".to_vec()),
                ("a/2".to_string(), b"v2".to_vec()),
                ("b/1".to_string(), b"v3".to_vec())
            ],
            data
        );

        let data = walk_top_down("a/").await;
        assert_eq!(
            vec![
                ("a/1".to_string(), b"v1".to_vec()),
                ("a/2".to_string(), b"v2".to_vec()),
            ],
            data
        );

        store
            .batch_delete(&["a/2".to_string(), "b/1".to_string()])
            .await
            .unwrap();

        let data = walk_top_down("a/").await;
        assert_eq!(vec![("a/1".to_string(), b"v1".to_vec()),], data);
    }
}
