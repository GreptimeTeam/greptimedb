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

use std::any::Any;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;
use std::sync::RwLock;

use async_trait::async_trait;
use common_error::ext::ErrorExt;
use serde::Serializer;

use super::ResettableKvBackend;
use crate::kv_backend::txn::{Txn, TxnOp, TxnOpResponse, TxnRequest, TxnResponse};
use crate::kv_backend::{KvBackend, TxnService};
use crate::metrics::METRIC_META_TXN_REQUEST;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
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
        let values = kvs.range(range);

        let mut more = false;
        let mut iter: i64 = 0;

        let kvs = values
            .take_while(|_| {
                let take = limit == 0 || iter != limit;
                iter += 1;
                more = limit > 0 && iter > limit;

                take
            })
            .map(|(k, v)| {
                let key = k.clone();
                let value = if keys_only { vec![] } else { v.clone() };
                KeyValue { key, value }
            })
            .collect::<Vec<_>>();

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

        let mut prev_kvs = if prev_kv {
            Vec::with_capacity(keys.len())
        } else {
            vec![]
        };
        let deleted = keys.len() as i64;

        for key in keys {
            if let Some(value) = kvs.remove(&key) {
                if prev_kv {
                    prev_kvs.push((key.clone(), value).into())
                }
            }
        }

        Ok(DeleteRangeResponse { deleted, prev_kvs })
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
}

#[async_trait]
impl<T: ErrorExt + Send + Sync> TxnService for MemoryKvBackend<T> {
    type Error = T;

    async fn txn(&self, txn: Txn) -> Result<TxnResponse, Self::Error> {
        let _timer = METRIC_META_TXN_REQUEST
            .with_label_values(&["memory", "txn"])
            .start_timer();

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

impl<T: ErrorExt + Send + Sync + 'static> ResettableKvBackend for MemoryKvBackend<T> {
    fn reset(&self) {
        self.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::error::Error;
    use crate::kv_backend::test::{
        prepare_kv, test_kv_batch_delete, test_kv_batch_get, test_kv_compare_and_put,
        test_kv_delete_range, test_kv_put, test_kv_range, test_kv_range_2,
    };

    async fn mock_mem_store_with_data() -> MemoryKvBackend<Error> {
        let kv_backend = MemoryKvBackend::<Error>::new();
        prepare_kv(&kv_backend).await;

        kv_backend
    }

    #[tokio::test]
    async fn test_put() {
        let kv_backend = mock_mem_store_with_data().await;

        test_kv_put(kv_backend).await;
    }

    #[tokio::test]
    async fn test_range() {
        let kv_backend = mock_mem_store_with_data().await;

        test_kv_range(kv_backend).await;
    }

    #[tokio::test]
    async fn test_range_2() {
        let kv = MemoryKvBackend::<Error>::new();

        test_kv_range_2(kv).await;
    }

    #[tokio::test]
    async fn test_batch_get() {
        let kv_backend = mock_mem_store_with_data().await;

        test_kv_batch_get(kv_backend).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compare_and_put() {
        let kv_backend = Arc::new(MemoryKvBackend::<Error>::new());

        test_kv_compare_and_put(kv_backend).await;
    }

    #[tokio::test]
    async fn test_delete_range() {
        let kv_backend = mock_mem_store_with_data().await;

        test_kv_delete_range(kv_backend).await;
    }

    #[tokio::test]
    async fn test_batch_delete() {
        let kv_backend = mock_mem_store_with_data().await;

        test_kv_batch_delete(kv_backend).await;
    }
}
