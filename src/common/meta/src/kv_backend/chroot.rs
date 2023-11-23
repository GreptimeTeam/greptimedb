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

use crate::error::Error;
use crate::kv_backend::txn::{Txn, TxnOp, TxnOpResponse, TxnResponse};
use crate::kv_backend::{KvBackend, KvBackendRef, TxnService};
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

pub struct ChrootKvBackend {
    root: Vec<u8>,
    inner: KvBackendRef,
}

impl ChrootKvBackend {
    pub fn new(root: Vec<u8>, inner: KvBackendRef) -> ChrootKvBackend {
        debug_assert!(!root.is_empty());
        ChrootKvBackend { root, inner }
    }
}

#[async_trait::async_trait]
impl TxnService for ChrootKvBackend {
    type Error = Error;

    async fn txn(&self, txn: Txn) -> Result<TxnResponse, Self::Error> {
        let txn = self.txn_prepend_root(txn);
        let txn_res = self.inner.txn(txn).await?;
        Ok(self.chroot_txn_response(txn_res))
    }
}

#[async_trait::async_trait]
impl KvBackend for ChrootKvBackend {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, mut req: RangeRequest) -> Result<RangeResponse, Self::Error> {
        req.key = self.key_prepend_root(req.key);
        req.range_end = self.range_end_prepend_root(req.range_end);
        let mut res = self.inner.range(req).await?;
        res.kvs = res
            .kvs
            .drain(..)
            .map(self.chroot_key_value_with())
            .collect();
        Ok(res)
    }

    async fn put(&self, mut req: PutRequest) -> Result<PutResponse, Self::Error> {
        req.key = self.key_prepend_root(req.key);
        let mut res = self.inner.put(req).await?;
        res.prev_kv = res.prev_kv.take().map(self.chroot_key_value_with());
        Ok(res)
    }

    async fn batch_put(&self, mut req: BatchPutRequest) -> Result<BatchPutResponse, Self::Error> {
        for kv in req.kvs.iter_mut() {
            kv.key = self.key_prepend_root(kv.key.drain(..).collect());
        }
        let mut res = self.inner.batch_put(req).await?;
        res.prev_kvs = res
            .prev_kvs
            .drain(..)
            .map(self.chroot_key_value_with())
            .collect();
        Ok(res)
    }

    async fn batch_get(&self, mut req: BatchGetRequest) -> Result<BatchGetResponse, Self::Error> {
        req.keys = req
            .keys
            .drain(..)
            .map(|key| self.key_prepend_root(key))
            .collect();
        let mut res = self.inner.batch_get(req).await?;
        res.kvs = res
            .kvs
            .drain(..)
            .map(self.chroot_key_value_with())
            .collect();
        Ok(res)
    }

    async fn compare_and_put(
        &self,
        mut req: CompareAndPutRequest,
    ) -> Result<CompareAndPutResponse, Self::Error> {
        req.key = self.key_prepend_root(req.key);
        let mut res = self.inner.compare_and_put(req).await?;
        res.prev_kv = res.prev_kv.take().map(self.chroot_key_value_with());
        Ok(res)
    }

    async fn delete_range(
        &self,
        mut req: DeleteRangeRequest,
    ) -> Result<DeleteRangeResponse, Self::Error> {
        req.key = self.key_prepend_root(req.key);
        req.range_end = self.range_end_prepend_root(req.range_end);
        let mut res = self.inner.delete_range(req).await?;
        res.prev_kvs = res
            .prev_kvs
            .drain(..)
            .map(self.chroot_key_value_with())
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
            .map(|key| self.key_prepend_root(key))
            .collect();
        let mut res = self.inner.batch_delete(req).await?;
        res.prev_kvs = res
            .prev_kvs
            .drain(..)
            .map(self.chroot_key_value_with())
            .collect();
        Ok(res)
    }
}

impl ChrootKvBackend {
    fn key_strip_root(&self, mut key: Vec<u8>) -> Vec<u8> {
        let root = &self.root;
        debug_assert!(
            key.starts_with(root),
            "key={}, root={}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(root),
        );
        key.split_off(root.len())
    }

    fn chroot_key_value_with(&self) -> impl FnMut(KeyValue) -> KeyValue + '_ {
        |kv| KeyValue {
            key: self.key_strip_root(kv.key),
            value: kv.value,
        }
    }
    fn chroot_txn_response(&self, mut txn_res: TxnResponse) -> TxnResponse {
        for resp in txn_res.responses.iter_mut() {
            match resp {
                TxnOpResponse::ResponsePut(r) => {
                    r.prev_kv = r.prev_kv.take().map(self.chroot_key_value_with());
                }
                TxnOpResponse::ResponseGet(r) => {
                    r.kvs = r.kvs.drain(..).map(self.chroot_key_value_with()).collect();
                }
                TxnOpResponse::ResponseDelete(r) => {
                    r.prev_kvs = r
                        .prev_kvs
                        .drain(..)
                        .map(self.chroot_key_value_with())
                        .collect();
                }
            }
        }
        txn_res
    }

    fn key_prepend_root(&self, mut key: Vec<u8>) -> Vec<u8> {
        let mut new_key = self.root.clone();
        new_key.append(&mut key);
        new_key
    }

    // see namespace.prefixInterval - https://github.com/etcd-io/etcd/blob/v3.5.10/client/v3/namespace/util.go
    fn range_end_prepend_root(&self, mut range_end: Vec<u8>) -> Vec<u8> {
        let root = &self.root;
        if range_end == [0] {
            // the edge of the keyspace
            let mut new_end = root.clone();
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
            let mut new_end = root.clone();
            new_end.append(&mut range_end);
            new_end
        } else {
            vec![]
        }
    }

    fn txn_prepend_root(&self, mut txn: Txn) -> Txn {
        let op_prepend_root = |op: TxnOp| match op {
            TxnOp::Put(k, v) => TxnOp::Put(self.key_prepend_root(k), v),
            TxnOp::Get(k) => TxnOp::Get(self.key_prepend_root(k)),
            TxnOp::Delete(k) => TxnOp::Delete(self.key_prepend_root(k)),
        };
        txn.req.success = txn.req.success.drain(..).map(op_prepend_root).collect();
        txn.req.failure = txn.req.failure.drain(..).map(op_prepend_root).collect();
        txn.req.compare = txn
            .req
            .compare
            .drain(..)
            .map(|cmp| super::txn::Compare {
                key: self.key_prepend_root(cmp.key),
                cmp: cmp.cmp,
                target: cmp.target,
            })
            .collect();
        txn
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::kv_backend::chroot::ChrootKvBackend;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[test]
    fn test_prefix_key_and_range_end() {
        fn run_test_case(pfx: &[u8], key: &[u8], end: &[u8], w_key: &[u8], w_end: &[u8]) {
            let chroot = ChrootKvBackend::new(pfx.into(), Arc::new(MemoryKvBackend::new()));
            assert_eq!(chroot.key_prepend_root(key.into()), w_key);
            assert_eq!(chroot.range_end_prepend_root(end.into()), w_end);
        }

        // single key
        run_test_case(b"pfx/", b"a", b"", b"pfx/a", b"");

        // range
        run_test_case(b"pfx/", b"abc", b"def", b"pfx/abc", b"pfx/def");

        // one-sided range (HACK - b'/' + 1 = b'0')
        run_test_case(b"pfx/", b"abc", b"\0", b"pfx/abc", b"pfx0");

        // one-sided range, end of keyspace
        run_test_case(b"\xFF\xFF", b"abc", b"\0", b"\xff\xffabc", b"\0");
    }
}
