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

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::ops::Range;

use api::v1::meta::{
    BatchGetRequest, BatchGetResponse, BatchPutRequest, BatchPutResponse, CompareAndPutRequest,
    CompareAndPutResponse, DeleteRangeRequest, DeleteRangeResponse, KeyValue, MoveValueRequest,
    MoveValueResponse, PutRequest, PutResponse, RangeRequest, RangeResponse, ResponseHeader,
};
use parking_lot::RwLock;

use super::ext::KvStoreExt;
use crate::error::Result;
use crate::service::store::kv::{KvStore, ResettableKvStore};

pub struct MemStore {
    inner: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

impl Default for MemStore {
    fn default() -> Self {
        Self::new()
    }
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Default::default()),
        }
    }
}

impl ResettableKvStore for MemStore {
    fn reset(&self) {
        self.inner.write().clear();
    }
}

#[async_trait::async_trait]
impl KvStore for MemStore {
    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        let RangeRequest {
            header,
            key,
            range_end,
            limit,
            keys_only,
        } = req;

        let memory = self.inner.read();

        let mut kvs = if range_end.is_empty() {
            memory.get_key_value(&key).map_or(vec![], |(k, v)| {
                vec![KeyValue {
                    key: k.clone(),
                    value: if keys_only { vec![] } else { v.clone() },
                }]
            })
        } else {
            let range = Range {
                start: key,
                end: range_end,
            };
            memory
                .range(range)
                .map(|kv| KeyValue {
                    key: kv.0.clone(),
                    value: if keys_only { vec![] } else { kv.1.clone() },
                })
                .collect::<Vec<_>>()
        };

        let more = if limit > 0 {
            kvs.truncate(limit as usize);
            true
        } else {
            false
        };

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(RangeResponse { header, kvs, more })
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let PutRequest {
            header,
            key,
            value,
            prev_kv,
        } = req;

        let mut memory = self.inner.write();
        let prev_value = memory.insert(key.clone(), value);
        let prev_kv = if prev_kv {
            prev_value.map(|value| KeyValue { key, value })
        } else {
            None
        };

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(PutResponse { header, prev_kv })
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        let keys = req.keys;

        let mut kvs = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(kv) = self.get(key).await? {
                kvs.push(kv);
            }
        }

        Ok(BatchGetResponse {
            kvs,
            ..Default::default()
        })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let BatchPutRequest {
            header,
            kvs,
            prev_kv,
        } = req;

        let mut memory = self.inner.write();
        let prev_kvs = if prev_kv {
            kvs.into_iter()
                .map(|kv| (kv.key.clone(), memory.insert(kv.key, kv.value)))
                .filter(|(_, v)| v.is_some())
                .map(|(key, value)| KeyValue {
                    key,
                    value: value.unwrap(),
                })
                .collect()
        } else {
            for kv in kvs.into_iter() {
                memory.insert(kv.key, kv.value);
            }
            vec![]
        };

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(BatchPutResponse { header, prev_kvs })
    }

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        let CompareAndPutRequest {
            header,
            key,
            expect,
            value,
        } = req;

        let mut memory = self.inner.write();

        let (success, prev_kv) = match memory.entry(key) {
            Entry::Vacant(e) => {
                let success = expect.is_empty();
                if success {
                    e.insert(value);
                }
                (success, None)
            }
            Entry::Occupied(mut e) => {
                let key = e.key().clone();
                let prev_val = e.get().clone();
                let success = prev_val == expect;
                if success {
                    e.insert(value);
                }
                (success, Some((key, prev_val)))
            }
        };

        let prev_kv = prev_kv.map(|(key, value)| KeyValue { key, value });

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(CompareAndPutResponse {
            header,
            success,
            prev_kv,
        })
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let DeleteRangeRequest {
            header,
            key,
            range_end,
            prev_kv,
        } = req;

        let mut memory = self.inner.write();

        let prev_kvs = if range_end.is_empty() {
            let prev_val = memory.remove(&key);
            prev_val.map_or(vec![], |value| vec![KeyValue { key, value }])
        } else {
            let range = Range {
                start: key,
                end: range_end,
            };
            memory
                .drain_filter(|key, _| range.contains(key))
                .map(|(key, value)| KeyValue { key, value })
                .collect::<Vec<_>>()
        };

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(DeleteRangeResponse {
            header,
            deleted: prev_kvs.len() as i64,
            prev_kvs: if prev_kv {
                prev_kvs
            } else {
                Default::default()
            },
        })
    }

    async fn move_value(&self, req: MoveValueRequest) -> Result<MoveValueResponse> {
        let MoveValueRequest {
            header,
            from_key,
            to_key,
        } = req;

        let mut memory = self.inner.write();

        let kv = match memory.remove(&from_key) {
            Some(v) => {
                memory.insert(to_key, v.clone());
                Some((from_key, v))
            }
            None => memory.get(&to_key).map(|v| (to_key, v.clone())),
        };

        let kv = kv.map(|(key, value)| KeyValue { key, value });

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(MoveValueResponse { header, kv })
    }
}

#[cfg(test)]
mod tests {
    use api::v1::meta::{BatchPutRequest, KeyValue, PutRequest, RangeRequest};

    use super::MemStore;
    use crate::service::store::kv::KvStore;
    use crate::util;

    async fn mock_mem_store_with_data() -> MemStore {
        let kv_store = MemStore::new();
        let kvs = mock_kvs();

        kv_store
            .batch_put(BatchPutRequest {
                kvs,
                ..Default::default()
            })
            .await
            .unwrap();

        kv_store
            .put(PutRequest {
                key: b"key11".to_vec(),
                value: b"val11".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();

        kv_store
    }

    fn mock_kvs() -> Vec<KeyValue> {
        vec![
            KeyValue {
                key: b"key1".to_vec(),
                value: b"val1".to_vec(),
            },
            KeyValue {
                key: b"key2".to_vec(),
                value: b"val2".to_vec(),
            },
            KeyValue {
                key: b"key3".to_vec(),
                value: b"val3".to_vec(),
            },
        ]
    }

    #[tokio::test]
    async fn test_put() {
        let kv_store = mock_mem_store_with_data().await;

        let resp = kv_store
            .put(PutRequest {
                key: b"key11".to_vec(),
                value: b"val12".to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(resp.prev_kv.is_none());

        let resp = kv_store
            .put(PutRequest {
                key: b"key11".to_vec(),
                value: b"val13".to_vec(),
                prev_kv: true,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(b"key11".to_vec(), resp.prev_kv.as_ref().unwrap().key);
        assert_eq!(b"val12".to_vec(), resp.prev_kv.as_ref().unwrap().value);
    }

    #[tokio::test]
    async fn test_range() {
        let kv_store = mock_mem_store_with_data().await;

        let key = b"key1".to_vec();
        let range_end = util::get_prefix_end_key(b"key1");

        let resp = kv_store
            .range(RangeRequest {
                key: key.clone(),
                range_end: range_end.clone(),
                limit: 0,
                keys_only: false,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(2, resp.kvs.len());
        assert_eq!(b"key1".to_vec(), resp.kvs[0].key);
        assert_eq!(b"val1".to_vec(), resp.kvs[0].value);
        assert_eq!(b"key11".to_vec(), resp.kvs[1].key);
        assert_eq!(b"val11".to_vec(), resp.kvs[1].value);

        let resp = kv_store
            .range(RangeRequest {
                key: key.clone(),
                range_end: range_end.clone(),
                limit: 0,
                keys_only: true,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(2, resp.kvs.len());
        assert_eq!(b"key1".to_vec(), resp.kvs[0].key);
        assert_eq!(b"".to_vec(), resp.kvs[0].value);
        assert_eq!(b"key11".to_vec(), resp.kvs[1].key);
        assert_eq!(b"".to_vec(), resp.kvs[1].value);

        let resp = kv_store
            .range(RangeRequest {
                key: key.clone(),
                limit: 0,
                keys_only: false,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(1, resp.kvs.len());
        assert_eq!(b"key1".to_vec(), resp.kvs[0].key);
        assert_eq!(b"val1".to_vec(), resp.kvs[0].value);

        let resp = kv_store
            .range(RangeRequest {
                key,
                range_end,
                limit: 1,
                keys_only: false,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(1, resp.kvs.len());
        assert_eq!(b"key1".to_vec(), resp.kvs[0].key);
        assert_eq!(b"val1".to_vec(), resp.kvs[0].value);
    }

    #[tokio::test]
    async fn test_batch_get() {
        let kv_store = mock_mem_store_with_data().await;

        let keys = vec![];
        let batch_resp = kv_store
            .batch_get(api::v1::meta::BatchGetRequest {
                keys,
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(batch_resp.kvs.is_empty());

        let keys = vec![b"key10".to_vec()];
        let batch_resp = kv_store
            .batch_get(api::v1::meta::BatchGetRequest {
                keys,
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(batch_resp.kvs.is_empty());

        let keys = vec![b"key1".to_vec(), b"key3".to_vec(), b"key4".to_vec()];
        let batch_resp = kv_store
            .batch_get(api::v1::meta::BatchGetRequest {
                keys,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(2, batch_resp.kvs.len());
        assert_eq!(b"key1".to_vec(), batch_resp.kvs[0].key);
        assert_eq!(b"val1".to_vec(), batch_resp.kvs[0].value);
        assert_eq!(b"key3".to_vec(), batch_resp.kvs[1].key);
        assert_eq!(b"val3".to_vec(), batch_resp.kvs[1].value);
    }

    #[tokio::test]
    async fn test_compare_and_put() {}

    #[tokio::test]
    async fn test_delete_range() {}

    #[tokio::test]
    async fn test_move_value() {}
}
