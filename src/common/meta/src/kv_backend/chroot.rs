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

use crate::kv_backend::txn::{Txn, TxnOp, TxnOpResponse, TxnResponse};
use crate::kv_backend::{KvBackend, TxnService};
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

pub struct ChrootKvBackend<B> {
    root: Vec<u8>,
    inner: B,
}

impl<B> ChrootKvBackend<B> {
    pub fn new<R>(root: Vec<u8>, inner: B) -> Self {
        let root = root.into();
        debug_assert!(!root.is_empty());
        ChrootKvBackend { root, inner }
    }
}

#[async_trait::async_trait]
impl<B: KvBackend> TxnService for ChrootKvBackend<B> {
    type Error = B::Error;

    async fn txn(&self, txn: Txn) -> Result<TxnResponse, Self::Error> {
        let txn = txn_prepend_root(&self.root, txn);
        let txn_res = self.inner.txn(txn).await?;
        Ok(chroot_txn_response(&self.root, txn_res))
    }
}

#[async_trait::async_trait]
impl<B: KvBackend> KvBackend for ChrootKvBackend<B> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }

    async fn range(&self, mut req: RangeRequest) -> Result<RangeResponse, Self::Error> {
        req.key = key_prepend_root(&self.root, req.key);
        req.range_end = range_end_prepend_root(&self.root, req.range_end);
        let mut res = self.inner.range(req).await?;
        res.kvs = res
            .kvs
            .drain(..)
            .map(chroot_key_value_with(&self.root))
            .collect();
        Ok(res)
    }

    async fn put(&self, mut req: PutRequest) -> Result<PutResponse, Self::Error> {
        req.key = key_prepend_root(&self.root, req.key);
        let mut res = self.inner.put(req).await?;
        res.prev_kv = res.prev_kv.take().map(chroot_key_value_with(&self.root));
        Ok(res)
    }

    async fn batch_put(&self, mut req: BatchPutRequest) -> Result<BatchPutResponse, Self::Error> {
        for kv in req.kvs.iter_mut() {
            kv.key = key_prepend_root(&self.root, kv.key.drain(..).collect());
        }
        let mut res = self.inner.batch_put(req).await?;
        res.prev_kvs = res
            .prev_kvs
            .drain(..)
            .map(chroot_key_value_with(&self.root))
            .collect();
        Ok(res)
    }

    async fn batch_get(&self, mut req: BatchGetRequest) -> Result<BatchGetResponse, Self::Error> {
        req.keys = req
            .keys
            .drain(..)
            .map(|key| key_prepend_root(&self.root, key))
            .collect();
        let mut res = self.inner.batch_get(req).await?;
        res.kvs = res
            .kvs
            .drain(..)
            .map(chroot_key_value_with(&self.root))
            .collect();
        Ok(res)
    }

    async fn compare_and_put(
        &self,
        mut req: CompareAndPutRequest,
    ) -> Result<CompareAndPutResponse, Self::Error> {
        req.key = key_prepend_root(&self.root, req.key);
        let mut res = self.inner.compare_and_put(req).await?;
        res.prev_kv = res.prev_kv.take().map(chroot_key_value_with(&self.root));
        Ok(res)
    }

    async fn delete_range(
        &self,
        mut req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, Self::Error> {
        req.key = key_prepend_root(&self.root, req.key);
        req.range_end = range_end_prepend_root(&self.root, req.range_end);
        let mut res = self.inner.delete_range(req).await?;
        res.prev_kvs = res
            .prev_kvs
            .drain(..)
            .map(chroot_key_value_with(&self.root))
            .collect();
        Ok(res)
    }

    async fn batch_delete(
        &self,
        mut req: BatchDeleteRequest,
    ) -> Result<BatchDeleteResponse, Self::Error> {
        req.keys = req
            .keys
            .drain(..)
            .map(|key| key_prepend_root(&self.root, key))
            .collect();
        let mut res = self.inner.batch_delete(req).await?;
        res.prev_kvs = res
            .prev_kvs
            .drain(..)
            .map(chroot_key_value_with(&self.root))
            .collect();
        Ok(res)
    }
}

fn key_strip_root(root: &[u8], mut key: Vec<u8>) -> Vec<u8> {
    debug_assert!(
        key.starts_with(root),
        "key={}, root={}",
        String::from_utf8_lossy(&key),
        String::from_utf8_lossy(root),
    );
    key.split_off(root.len())
}

fn chroot_key_value_with(root: &[u8]) -> impl FnMut(KeyValue) -> KeyValue + '_ {
    |kv| KeyValue {
        key: key_strip_root(root, kv.key),
        value: kv.value,
    }
}
fn chroot_txn_response(root: &[u8], mut txn_res: TxnResponse) -> TxnResponse {
    for resp in txn_res.responses.iter_mut() {
        match resp {
            TxnOpResponse::ResponsePut(r) => {
                r.prev_kv = r.prev_kv.take().map(chroot_key_value_with(root));
            }
            TxnOpResponse::ResponseGet(r) => {
                r.kvs = r.kvs.drain(..).map(chroot_key_value_with(root)).collect();
            }
            TxnOpResponse::ResponseDelete(r) => {
                r.prev_kvs = r
                    .prev_kvs
                    .drain(..)
                    .map(chroot_key_value_with(root))
                    .collect();
            }
        }
    }
    txn_res
}

fn key_prepend_root(root: &[u8], mut key: Vec<u8>) -> Vec<u8> {
    let mut new_key = root.to_vec();
    new_key.append(&mut key);
    new_key
}

// see namespace.prefixInterval - https://github.com/etcd-io/etcd/blob/v3.5.10/client/v3/namespace/util.go
fn range_end_prepend_root(root: &[u8], mut range_end: Vec<u8>) -> Vec<u8> {
    if range_end == [0] {
        // the edge of the keyspace
        let mut new_end = root.to_vec();
        let mut ok = false;
        for i in (0..new_end.len()).rev() {
            new_end[i] = new_end[i].wrapping_add(1);
            if new_end[i] != 0 {
                ok = true;
                break;
            }
        }
        if !ok {
            // 0xff..ff => 0x00
            new_end = vec![0];
        }
        new_end
    } else if !range_end.is_empty() {
        let mut new_end = root.to_vec();
        new_end.append(&mut range_end);
        new_end
    } else {
        vec![]
    }
}

fn txn_prepend_root(root: &[u8], mut txn: Txn) -> Txn {
    fn op_prepend_root(root: &[u8], op: TxnOp) -> TxnOp {
        match op {
            TxnOp::Put(k, v) => TxnOp::Put(key_prepend_root(root, k), v),
            TxnOp::Get(k) => TxnOp::Get(key_prepend_root(root, k)),
            TxnOp::Delete(k) => TxnOp::Delete(key_prepend_root(root, k)),
        }
    }

    txn.req.success = txn
        .req
        .success
        .drain(..)
        .map(|op| op_prepend_root(root, op))
        .collect();

    txn.req.failure = txn
        .req
        .failure
        .drain(..)
        .map(|op| op_prepend_root(root, op))
        .collect();

    txn.req.compare = txn
        .req
        .compare
        .drain(..)
        .map(|cmp| super::txn::Compare {
            key: key_prepend_root(root, cmp.key),
            cmp: cmp.cmp,
            target: cmp.target,
        })
        .collect();

    txn
}
