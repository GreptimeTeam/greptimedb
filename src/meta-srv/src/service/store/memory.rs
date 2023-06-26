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
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, KeyValue, MoveValueRequest, MoveValueResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse, ResponseHeader,
};
use common_telemetry::timer;
use parking_lot::RwLock;

use super::ext::KvStoreExt;
use crate::error::Result;
use crate::metrics::{METRIC_META_KV_REQUEST, METRIC_META_TXN_REQUEST};
use crate::service::store::kv::{KvStore, ResettableKvStore};
use crate::service::store::txn::{Txn, TxnOp, TxnOpResponse, TxnRequest, TxnResponse, TxnService};

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
        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[("target", "memory"), ("op", "range"),]
        );

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
        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[("target", "memory"), ("op", "put"),]
        );

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
        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[("target", "memory"), ("op", "batch_get"),]
        );

        let BatchGetRequest { header, keys } = req;

        let mut kvs = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(kv) = self.get(key).await? {
                kvs.push(kv);
            }
        }

        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(BatchGetResponse { header, kvs })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[("target", "memory"), ("op", "batch_put"),]
        );

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

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[("target", "memory"), ("op", "batch_delete"),]
        );

        let BatchDeleteRequest {
            header,
            keys,
            prev_kv,
        } = req;

        let mut memory = self.inner.write();

        let prev_kvs = if prev_kv {
            keys.into_iter()
                .filter_map(|key| memory.remove(&key).map(|value| KeyValue { key, value }))
                .collect()
        } else {
            for key in keys.into_iter() {
                memory.remove(&key);
            }
            vec![]
        };
        let cluster_id = header.map_or(0, |h| h.cluster_id);
        let header = Some(ResponseHeader::success(cluster_id));
        Ok(BatchDeleteResponse { header, prev_kvs })
    }

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[("target", "memory"), ("op", "compare_and_put"),]
        );

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
        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[("target", "memory"), ("op", "deleteL_range"),]
        );

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
        let _timer = timer!(
            METRIC_META_KV_REQUEST,
            &[("target", "memory"), ("op", "move_value"),]
        );

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

#[async_trait::async_trait]
impl TxnService for MemStore {
    async fn txn(&self, txn: Txn) -> Result<TxnResponse> {
        let _timer = timer!(
            METRIC_META_TXN_REQUEST,
            &[("target", "memory".to_string()), ("op", "txn".to_string()),]
        );

        let TxnRequest {
            compare,
            success,
            failure,
        } = txn.into();

        let mut memory = self.inner.write();

        let mut succeeded = true;
        for cmp in compare {
            let value = memory.get(&cmp.key);
            if cmp.compare_with_value(value) {
                continue;
            }
            succeeded = false;
        }

        let do_txn = |txn_op| match txn_op {
            TxnOp::Put(key, value) => {
                let prev_value = memory.insert(key.clone(), value);
                let prev_kv = prev_value.map(|value| KeyValue { key, value });
                let put_res = PutResponse {
                    prev_kv,
                    ..Default::default()
                };
                TxnOpResponse::ResponsePut(put_res)
            }
            TxnOp::Get(key) => {
                let value = memory.get(&key);
                let kv = value.map(|value| KeyValue {
                    key,
                    value: value.clone(),
                });
                let get_res = RangeResponse {
                    kvs: kv.map(|kv| vec![kv]).unwrap_or(vec![]),
                    ..Default::default()
                };
                TxnOpResponse::ResponseGet(get_res)
            }
            TxnOp::Delete(key) => {
                let prev_value = memory.remove(&key);
                let prev_kv = prev_value.map(|value| KeyValue { key, value });
                let delete_res = DeleteRangeResponse {
                    prev_kvs: prev_kv.map(|kv| vec![kv]).unwrap_or(vec![]),
                    ..Default::default()
                };
                TxnOpResponse::ResponseDelete(delete_res)
            }
        };

        let responses: Vec<_> = if succeeded {
            success.into_iter().map(do_txn).collect()
        } else {
            failure.into_iter().map(do_txn).collect()
        };

        Ok(TxnResponse {
            succeeded,
            responses,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU8, Ordering};
    use std::sync::Arc;

    use api::v1::meta::{
        BatchGetRequest, BatchPutRequest, CompareAndPutRequest, DeleteRangeRequest, KeyValue,
        MoveValueRequest, PutRequest, RangeRequest,
    };

    use super::MemStore;
    use crate::service::store::ext::KvStoreExt;
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
        assert_eq!(b"key11".as_slice(), resp.prev_kv.as_ref().unwrap().key);
        assert_eq!(b"val12".as_slice(), resp.prev_kv.as_ref().unwrap().value);
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
        assert_eq!(b"key1".as_slice(), resp.kvs[0].key);
        assert_eq!(b"val1".as_slice(), resp.kvs[0].value);
        assert_eq!(b"key11".as_slice(), resp.kvs[1].key);
        assert_eq!(b"val11".as_slice(), resp.kvs[1].value);

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
        assert_eq!(b"key1".as_slice(), resp.kvs[0].key);
        assert_eq!(b"".as_slice(), resp.kvs[0].value);
        assert_eq!(b"key11".as_slice(), resp.kvs[1].key);
        assert_eq!(b"".as_slice(), resp.kvs[1].value);

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
        assert_eq!(b"key1".as_slice(), resp.kvs[0].key);
        assert_eq!(b"val1".as_slice(), resp.kvs[0].value);

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
        assert_eq!(b"key1".as_slice(), resp.kvs[0].key);
        assert_eq!(b"val1".as_slice(), resp.kvs[0].value);
    }

    #[tokio::test]
    async fn test_batch_get() {
        let kv_store = mock_mem_store_with_data().await;

        let keys = vec![];
        let batch_resp = kv_store
            .batch_get(BatchGetRequest {
                keys,
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(batch_resp.kvs.is_empty());

        let keys = vec![b"key10".to_vec()];
        let batch_resp = kv_store
            .batch_get(BatchGetRequest {
                keys,
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(batch_resp.kvs.is_empty());

        let keys = vec![b"key1".to_vec(), b"key3".to_vec(), b"key4".to_vec()];
        let batch_resp = kv_store
            .batch_get(BatchGetRequest {
                keys,
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(2, batch_resp.kvs.len());
        assert_eq!(b"key1".as_slice(), batch_resp.kvs[0].key);
        assert_eq!(b"val1".as_slice(), batch_resp.kvs[0].value);
        assert_eq!(b"key3".as_slice(), batch_resp.kvs[1].key);
        assert_eq!(b"val3".as_slice(), batch_resp.kvs[1].value);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compare_and_put() {
        let kv_store = Arc::new(MemStore::new());
        let success = Arc::new(AtomicU8::new(0));

        let mut joins = vec![];
        for _ in 0..20 {
            let kv_store_clone = kv_store.clone();
            let success_clone = success.clone();
            let join = tokio::spawn(async move {
                let req = CompareAndPutRequest {
                    key: b"key".to_vec(),
                    expect: vec![],
                    value: b"val_new".to_vec(),
                    ..Default::default()
                };
                let resp = kv_store_clone.compare_and_put(req).await.unwrap();
                if resp.success {
                    success_clone.fetch_add(1, Ordering::SeqCst);
                }
            });
            joins.push(join);
        }

        for join in joins {
            join.await.unwrap();
        }

        assert_eq!(1, success.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_delete_range() {
        let kv_store = mock_mem_store_with_data().await;

        let req = DeleteRangeRequest {
            key: b"key3".to_vec(),
            range_end: vec![],
            prev_kv: true,
            ..Default::default()
        };

        let resp = kv_store.delete_range(req).await.unwrap();
        assert_eq!(1, resp.prev_kvs.len());
        assert_eq!(b"key3".as_slice(), resp.prev_kvs[0].key);
        assert_eq!(b"val3".as_slice(), resp.prev_kvs[0].value);

        let get_resp = kv_store.get(b"key3".to_vec()).await.unwrap();
        assert!(get_resp.is_none());

        let req = DeleteRangeRequest {
            key: b"key2".to_vec(),
            range_end: vec![],
            prev_kv: false,
            ..Default::default()
        };

        let resp = kv_store.delete_range(req).await.unwrap();
        assert!(resp.prev_kvs.is_empty());

        let get_resp = kv_store.get(b"key2".to_vec()).await.unwrap();
        assert!(get_resp.is_none());

        let key = b"key1".to_vec();
        let range_end = util::get_prefix_end_key(b"key1");

        let req = DeleteRangeRequest {
            key: key.clone(),
            range_end: range_end.clone(),
            prev_kv: true,
            ..Default::default()
        };
        let resp = kv_store.delete_range(req).await.unwrap();
        assert_eq!(2, resp.prev_kvs.len());

        let req = RangeRequest {
            key,
            range_end,
            ..Default::default()
        };
        let resp = kv_store.range(req).await.unwrap();
        assert!(resp.kvs.is_empty());
    }

    #[tokio::test]
    async fn test_move_value() {
        let kv_store = mock_mem_store_with_data().await;

        let req = MoveValueRequest {
            from_key: b"key1".to_vec(),
            to_key: b"key111".to_vec(),
            ..Default::default()
        };

        let resp = kv_store.move_value(req).await.unwrap();
        assert_eq!(b"key1".as_slice(), resp.kv.as_ref().unwrap().key);
        assert_eq!(b"val1".as_slice(), resp.kv.as_ref().unwrap().value);

        let kv_store = mock_mem_store_with_data().await;

        let req = MoveValueRequest {
            from_key: b"notexistkey".to_vec(),
            to_key: b"key222".to_vec(),
            ..Default::default()
        };

        let resp = kv_store.move_value(req).await.unwrap();
        assert!(resp.kv.is_none());
    }
}
