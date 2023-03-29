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

use api::v1::meta::{BatchDeleteRequest, PutRequest, RangeRequest};
use async_stream::try_stream;
use async_trait::async_trait;
use common_procedure::error::{
    CorruptedDataSnafu, DeleteStatesSnafu, ListStateSnafu, PutStateSnafu,
};
use common_procedure::store::state_store::{KeyValueStream, StateStore};
use common_procedure::Result;
use snafu::ResultExt;

use crate::service::store::kv::KvStoreRef;
use crate::util;

const PROCEDURE_PREFIX: &str = "/__procedure__/";

fn with_prefix(key: &str) -> String {
    format!("{PROCEDURE_PREFIX}{key}")
}

fn strip_prefix(key: &str) -> String {
    key.trim_start_matches(PROCEDURE_PREFIX).to_string()
}

pub(crate) struct MetaStateStore {
    kv_store: KvStoreRef,
    max_size_per_range: i64,
}

impl MetaStateStore {
    pub(crate) fn new(kv_store: KvStoreRef) -> Self {
        Self {
            kv_store,
            max_size_per_range: -1,
        }
    }
}

#[async_trait]
impl StateStore for MetaStateStore {
    async fn put(&self, key: &str, value: Vec<u8>) -> Result<()> {
        let _ = self
            .kv_store
            .put(PutRequest {
                key: with_prefix(key).into_bytes(),
                value,
                ..Default::default()
            })
            .await
            .map_err(|e| {
                PutStateSnafu {
                    key,
                    err_msg: e.to_string(),
                }
                .build()
            })?;
        Ok(())
    }

    async fn walk_top_down(&self, path: &str) -> Result<KeyValueStream> {
        // extend their lifetimes to be used in the stream
        let path = path.to_string();
        let kv_store = self.kv_store.clone();
        let limit = self.max_size_per_range;

        let stream = try_stream! {
            let mut key = with_prefix(path.trim_start_matches('/')).into_bytes();
            let range_end = util::get_prefix_end_key(&key);
            loop {
                let req = RangeRequest {
                    key: key.clone(),
                    range_end: range_end.clone(),
                    limit,
                    ..Default::default()
                };
                let resp = kv_store.range(req).await.map_err(|e| ListStateSnafu {
                        path: path.clone(),
                        err_msg: e.to_string(),
                    }.build()
                )?;

                let mut no_more_data = true;
                if resp.more {
                    if let Some(last) = resp.kvs.last() {
                        key = util::get_prefix_end_key(&last.key);
                        no_more_data = false;
                    }
                }

                for kv in resp.kvs {
                    let key = String::from_utf8(kv.key).context(CorruptedDataSnafu)?;
                    let key = strip_prefix(&key);
                    let value = kv.value;
                    yield (key, value)
                }

                if no_more_data {
                    break;
                }
            }
        };
        Ok(Box::pin(stream))
    }

    async fn delete(&self, keys: &[String]) -> Result<()> {
        let _ = self
            .kv_store
            .batch_delete(BatchDeleteRequest {
                keys: keys
                    .iter()
                    .map(|x| with_prefix(x).into_bytes())
                    .collect::<Vec<_>>(),
                ..Default::default()
            })
            .await
            .map_err(|e| {
                DeleteStatesSnafu {
                    keys: format!("{:?}", keys.to_vec()),
                    err_msg: e.to_string(),
                }
                .build()
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_procedure::store::state_store::KeyValue;
    use futures::TryStreamExt;

    use super::*;
    use crate::service::store::memory::MemStore;

    #[tokio::test]
    async fn test_meta_state_store() {
        let store = &MetaStateStore {
            kv_store: Arc::new(MemStore::new()),
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
            .delete(&["a/2".to_string(), "b/1".to_string()])
            .await
            .unwrap();

        let data = walk_top_down("a/").await;
        assert_eq!(vec![("a/1".to_string(), b"v1".to_vec()),], data);
    }
}
