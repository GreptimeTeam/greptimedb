// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::sync::RwLock;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use common_telemetry::timer;
use serde::Serializer;

use crate::kv_backend::txn::{Txn, TxnOp, TxnOpResponse, TxnRequest, TxnResponse};
use crate::kv_backend::{KvBackend, TxnService};
use crate::metrics::METRIC_META_TXN_REQUEST;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, MoveValueRequest, MoveValueResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

pub struct MemoryKvBackend<T> {
    kvs: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
    _phantom: PhantomData<T>,
}

impl<T> Display for MemoryKvBackend<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let kvs = self.kvs.read().unwrap();
        for (k, v) in kvs.iter() {
            f.serialize_str(&String::from_utf8_lossy(k))?;
            f.serialize_str(" -> ")?;
            f.serialize_str(&String::from_utf8_lossy(v))?;
            f.serialize_str("\n")?;
        }
        Ok(())
    }
}

impl<T> Default for MemoryKvBackend<T> {
    fn default() -> Self {
        Self {
            kvs: RwLock::new(BTreeMap::new()),
            _phantom: PhantomData,
        }
    }
}

impl<T> MemoryKvBackend<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn clear(&self) {
        let mut kvs = self.kvs.write().unwrap();
        kvs.clear();
    }
}

#[async_trait]
impl<T: ErrorExt + Send + Sync + 'static> KvBackend for MemoryKvBackend<T> {
    fn name(&self) -> &str {
        "Memory"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse, Self::Error> {
        let range = req.range();
        let RangeRequest {
            limit, keys_only, ..
        } = req;

        let kvs = self.kvs.read().unwrap();
        let iter = kvs.range(range);

        let mut kvs = iter
            .map(|(k, v)| {
                let key = k.clone();
                let value = if keys_only { vec![] } else { v.clone() };
                KeyValue { key, value }
            })
            .collect::<Vec<_>>();

        let more = if limit > 0 && kvs.len() > limit as usize {
            kvs.truncate(limit as usize);
            true
        } else {
            false
        };

        Ok(RangeResponse { kvs, more })
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse, Self::Error> {
        let PutRequest {
            key,
            value,
            prev_kv,
        } = req;

        let mut kvs = self.kvs.write().unwrap();

        let prev_kv = if prev_kv {
            kvs.insert(key.clone(), value)
                .map(|value| KeyValue { key, value })
        } else {
            kvs.insert(key, value);
            None
        };

        Ok(PutResponse { prev_kv })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse, Self::Error> {
        let mut kvs = self.kvs.write().unwrap();

        let mut prev_kvs = if req.prev_kv {
            Vec::with_capacity(req.kvs.len())
        } else {
            vec![]
        };

        for kv in req.kvs {
            if req.prev_kv {
                if let Some(value) = kvs.insert(kv.key.clone(), kv.value) {
                    prev_kvs.push(KeyValue { key: kv.key, value });
                }
            } else {
                kvs.insert(kv.key, kv.value);
            }
        }

        Ok(BatchPutResponse { prev_kvs })
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse, Self::Error> {
        let kvs = self.kvs.read().unwrap();

        let kvs = req
            .keys
            .into_iter()
            .filter_map(|key| {
                kvs.get_key_value(&key).map(|(k, v)| KeyValue {
                    key: k.clone(),
                    value: v.clone(),
                })
            })
            .collect::<Vec<_>>();

        Ok(BatchGetResponse { kvs })
    }

    async fn compare_and_put(
        &self,
        req: CompareAndPutRequest,
    ) -> Result<CompareAndPutResponse, Self::Error> {
        let CompareAndPutRequest { key, expect, value } = req;

        let mut kvs = self.kvs.write().unwrap();

        let existed = kvs.entry(key);
        let (success, prev_kv) = match existed {
            Entry::Vacant(e) => {
                let expected = expect.is_empty();
                if expected {
                    let _ = e.insert(value);
                }
                (expected, None)
            }
            Entry::Occupied(mut existed) => {
                let expected = existed.get() == &expect;
                let prev_kv = if expected {
                    let _ = existed.insert(value);
                    None
                } else {
                    Some(KeyValue {
                        key: existed.key().clone(),
                        value: existed.get().clone(),
                    })
                };
                (expected, prev_kv)
            }
        };

        Ok(CompareAndPutResponse { success, prev_kv })
    }

    async fn delete_range(
        &self,
        req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, Self::Error> {
        let range = req.range();
        let DeleteRangeRequest { prev_kv, .. } = req;

        let mut kvs = self.kvs.write().unwrap();

        let keys = kvs
            .range(range)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();

        let mut prev_kvs = Vec::with_capacity(keys.len());

        for key in keys {
            if let Some(value) = kvs.remove(&key) {
                prev_kvs.push((key.clone(), value).into())
            }
        }

        Ok(DeleteRangeResponse {
            deleted: prev_kvs.len() as i64,
            prev_kvs: if prev_kv { prev_kvs } else { vec![] },
        })
    }

    async fn batch_delete(
        &self,
        req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse, Self::Error> {
        let mut kvs = self.kvs.write().unwrap();

        let mut prev_kvs = if req.prev_kv {
            Vec::with_capacity(req.keys.len())
        } else {
            vec![]
        };

        for key in req.keys {
            if req.prev_kv {
                if let Some(value) = kvs.remove(&key) {
                    prev_kvs.push(KeyValue { key, value });
                }
            } else {
                kvs.remove(&key);
            }
        }

        Ok(BatchDeleteResponse { prev_kvs })
    }

    async fn move_value(&self, req: MoveValueRequest) -> Result<MoveValueResponse, Self::Error> {
        let MoveValueRequest { from_key, to_key } = req;

        let mut kvs = self.kvs.write().unwrap();

        let kv = if let Some(v) = kvs.remove(&from_key) {
            kvs.insert(to_key, v.clone());
            Some(KeyValue {
                key: from_key,
                value: v,
            })
        } else {
            kvs.get(&to_key).map(|v| KeyValue {
                key: to_key,
                value: v.clone(),
            })
        };

        Ok(MoveValueResponse(kv))
    }
}

#[async_trait]
impl<T: ErrorExt + Send + Sync> TxnService for MemoryKvBackend<T> {
    type Error = T;

    async fn txn(&self, txn: Txn) -> Result<TxnResponse, Self::Error> {
        let _timer = timer!(
            METRIC_META_TXN_REQUEST,
            &[("target", "memory"), ("op", "txn")]
        );

        let TxnRequest {
            compare,
            success,
            failure,
        } = txn.into();

        let mut kvs = self.kvs.write().unwrap();

        let succeeded = compare
            .iter()
            .all(|x| x.compare_with_value(kvs.get(&x.key)));

        let do_txn = |txn_op| match txn_op {
            TxnOp::Put(key, value) => {
                kvs.insert(key.clone(), value);
                TxnOpResponse::ResponsePut(PutResponse { prev_kv: None })
            }

            TxnOp::Get(key) => {
                let value = kvs.get(&key);
                let kvs = value
                    .into_iter()
                    .map(|value| KeyValue {
                        key: key.clone(),
                        value: value.clone(),
                    })
                    .collect();
                TxnOpResponse::ResponseGet(RangeResponse { kvs, more: false })
            }

            TxnOp::Delete(key) => {
                let prev_value = kvs.remove(&key);
                let deleted = if prev_value.is_some() { 1 } else { 0 };
                TxnOpResponse::ResponseDelete(DeleteRangeResponse {
                    deleted,
                    prev_kvs: vec![],
                })
            }
        };

        let responses: Vec<_> = if succeeded { success } else { failure }
            .into_iter()
            .map(do_txn)
            .collect();

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

    use super::*;
    use crate::error::Error;
    use crate::kv_backend::KvBackend;
    use crate::rpc::store::{BatchGetRequest, BatchPutRequest};
    use crate::rpc::KeyValue;
    use crate::util;

    async fn mock_mem_store_with_data() -> MemoryKvBackend<Error> {
        let kv_store = MemoryKvBackend::<Error>::new();
        let kvs = mock_kvs();

        assert!(kv_store
            .batch_put(BatchPutRequest {
                kvs,
                ..Default::default()
            })
            .await
            .is_ok());

        assert!(kv_store
            .put(PutRequest {
                key: b"key11".to_vec(),
                value: b"val11".to_vec(),
                ..Default::default()
            })
            .await
            .is_ok());

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
                prev_kv: false,
            })
            .await
            .unwrap();
        assert!(resp.prev_kv.is_none());

        let resp = kv_store
            .put(PutRequest {
                key: b"key11".to_vec(),
                value: b"val13".to_vec(),
                prev_kv: true,
            })
            .await
            .unwrap();
        let prev_kv = resp.prev_kv.unwrap();
        assert_eq!(b"key11", prev_kv.key());
        assert_eq!(b"val12", prev_kv.value());
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
            })
            .await
            .unwrap();

        assert_eq!(2, resp.kvs.len());
        assert_eq!(b"key1", resp.kvs[0].key());
        assert_eq!(b"val1", resp.kvs[0].value());
        assert_eq!(b"key11", resp.kvs[1].key());
        assert_eq!(b"val11", resp.kvs[1].value());

        let resp = kv_store
            .range(RangeRequest {
                key: key.clone(),
                range_end: range_end.clone(),
                limit: 0,
                keys_only: true,
            })
            .await
            .unwrap();

        assert_eq!(2, resp.kvs.len());
        assert_eq!(b"key1", resp.kvs[0].key());
        assert_eq!(b"", resp.kvs[0].value());
        assert_eq!(b"key11", resp.kvs[1].key());
        assert_eq!(b"", resp.kvs[1].value());

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
        assert_eq!(b"key1", resp.kvs[0].key());
        assert_eq!(b"val1", resp.kvs[0].value());

        let resp = kv_store
            .range(RangeRequest {
                key,
                range_end,
                limit: 1,
                keys_only: false,
            })
            .await
            .unwrap();

        assert_eq!(1, resp.kvs.len());
        assert_eq!(b"key1", resp.kvs[0].key());
        assert_eq!(b"val1", resp.kvs[0].value());
    }

    #[tokio::test]
    async fn test_range_2() {
        let kv = MemoryKvBackend::<Error>::new();

        kv.put(PutRequest::new().with_key("atest").with_value("value"))
            .await
            .unwrap();

        kv.put(PutRequest::new().with_key("test").with_value("value"))
            .await
            .unwrap();

        // If both key and range_end are ‘\0’, then range represents all keys.
        let result = kv
            .range(RangeRequest::new().with_range(b"\0".to_vec(), b"\0".to_vec()))
            .await
            .unwrap();

        assert_eq!(result.kvs.len(), 2);

        // If range_end is ‘\0’, the range is all keys greater than or equal to the key argument.
        let result = kv
            .range(RangeRequest::new().with_range(b"a".to_vec(), b"\0".to_vec()))
            .await
            .unwrap();

        assert_eq!(result.kvs.len(), 2);

        let result = kv
            .range(RangeRequest::new().with_range(b"b".to_vec(), b"\0".to_vec()))
            .await
            .unwrap();

        assert_eq!(result.kvs.len(), 1);
        assert_eq!(result.kvs[0].key, b"test");
    }

    #[tokio::test]
    async fn test_batch_get() {
        let kv_store = mock_mem_store_with_data().await;

        let keys = vec![];
        let resp = kv_store.batch_get(BatchGetRequest { keys }).await.unwrap();

        assert!(resp.kvs.is_empty());

        let keys = vec![b"key10".to_vec()];
        let resp = kv_store.batch_get(BatchGetRequest { keys }).await.unwrap();

        assert!(resp.kvs.is_empty());

        let keys = vec![b"key1".to_vec(), b"key3".to_vec(), b"key4".to_vec()];
        let resp = kv_store.batch_get(BatchGetRequest { keys }).await.unwrap();

        assert_eq!(2, resp.kvs.len());
        assert_eq!(b"key1", resp.kvs[0].key());
        assert_eq!(b"val1", resp.kvs[0].value());
        assert_eq!(b"key3", resp.kvs[1].key());
        assert_eq!(b"val3", resp.kvs[1].value());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compare_and_put() {
        let kv_store = Arc::new(MemoryKvBackend::<Error>::new());
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
        };

        let resp = kv_store.delete_range(req).await.unwrap();
        assert_eq!(1, resp.prev_kvs.len());
        assert_eq!(b"key3", resp.prev_kvs[0].key());
        assert_eq!(b"val3", resp.prev_kvs[0].value());

        let resp = kv_store.get(b"key3").await.unwrap();
        assert!(resp.is_none());

        let req = DeleteRangeRequest {
            key: b"key2".to_vec(),
            range_end: vec![],
            prev_kv: false,
        };

        let resp = kv_store.delete_range(req).await.unwrap();
        assert!(resp.prev_kvs.is_empty());

        let resp = kv_store.get(b"key2").await.unwrap();
        assert!(resp.is_none());

        let key = b"key1".to_vec();
        let range_end = util::get_prefix_end_key(b"key1");

        let req = DeleteRangeRequest {
            key: key.clone(),
            range_end: range_end.clone(),
            prev_kv: true,
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
        };

        let resp = kv_store.move_value(req).await.unwrap();
        assert_eq!(b"key1", resp.0.as_ref().unwrap().key());
        assert_eq!(b"val1", resp.0.as_ref().unwrap().value());

        let kv_store = mock_mem_store_with_data().await;

        let req = MoveValueRequest {
            from_key: b"notexistkey".to_vec(),
            to_key: b"key222".to_vec(),
        };

        let resp = kv_store.move_value(req).await.unwrap();
        assert!(resp.0.is_none());
    }

    #[tokio::test]
    async fn test_batch_delete() {
        let kv_store = mock_mem_store_with_data().await;

        assert!(kv_store.get(b"key1").await.unwrap().is_some());
        assert!(kv_store.get(b"key100").await.unwrap().is_none());

        let req = BatchDeleteRequest {
            keys: vec![b"key1".to_vec(), b"key100".to_vec()],
            prev_kv: true,
        };
        let resp = kv_store.batch_delete(req).await.unwrap();
        assert_eq!(1, resp.prev_kvs.len());
        assert_eq!(
            vec![KeyValue {
                key: b"key1".to_vec(),
                value: b"val1".to_vec()
            }],
            resp.prev_kvs
        );
        assert!(kv_store.get(b"key1").await.unwrap().is_none());

        assert!(kv_store.get(b"key2").await.unwrap().is_some());
        assert!(kv_store.get(b"key3").await.unwrap().is_some());

        let req = BatchDeleteRequest {
            keys: vec![b"key2".to_vec(), b"key3".to_vec()],
            prev_kv: false,
        };
        let resp = kv_store.batch_delete(req).await.unwrap();
        assert!(resp.prev_kvs.is_empty());

        assert!(kv_store.get(b"key2").await.unwrap().is_none());
        assert!(kv_store.get(b"key3").await.unwrap().is_none());
    }
}
