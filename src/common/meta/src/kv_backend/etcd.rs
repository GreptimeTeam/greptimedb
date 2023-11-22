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
use std::sync::Arc;

use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, GetOptions, PutOptions, Txn, TxnOp, TxnOpResponse,
    TxnResponse,
};
use snafu::{ensure, OptionExt, ResultExt};

use super::KvBackendRef;
use crate::error::{self, Error, Result};
use crate::kv_backend::txn::{Txn as KvTxn, TxnResponse as KvTxnResponse};
use crate::kv_backend::{KvBackend, TxnService};
use crate::metrics::METRIC_META_TXN_REQUEST;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, PutRequest, PutResponse, RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

// Maximum number of operations permitted in a transaction.
// The etcd default configuration's `--max-txn-ops` is 128.
//
// For more detail, see: https://etcd.io/docs/v3.5/op-guide/configuration/
const MAX_TXN_SIZE: usize = 128;

pub const NO_CHROOT: &str = "";

fn key_strip_root(root: &[u8], mut key: Vec<u8>) -> Vec<u8> {
    debug_assert!(
        key.starts_with(root),
        "key={}, root={}",
        String::from_utf8_lossy(&key),
        String::from_utf8_lossy(root),
    );
    key.split_off(root.len())
}

fn chroot_key_value_with(root: &[u8]) -> impl FnMut(etcd_client::KeyValue) -> KeyValue + '_ {
    |kv: etcd_client::KeyValue| {
        let (key, value) = kv.into_key_value();
        KeyValue {
            key: key_strip_root(root, key),
            value,
        }
    }
}

fn chroot_txn_response(root: &[u8], mut txn_res: KvTxnResponse) -> KvTxnResponse {
    use super::txn::TxnOpResponse as KvTxnOpResponse;

    fn chroot_key_value_with(root: &[u8]) -> impl FnMut(KeyValue) -> KeyValue + '_ {
        |kv| KeyValue {
            key: key_strip_root(root, kv.key),
            value: kv.value,
        }
    }

    for resp in txn_res.responses.iter_mut() {
        match resp {
            KvTxnOpResponse::ResponsePut(r) => {
                r.prev_kv = r.prev_kv.take().map(chroot_key_value_with(root));
            }
            KvTxnOpResponse::ResponseGet(r) => {
                r.kvs = r.kvs.drain(..).map(chroot_key_value_with(root)).collect();
            }
            KvTxnOpResponse::ResponseDelete(r) => {
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
    } else if range_end.len() >= 1 {
        let mut new_end = root.to_vec();
        new_end.append(&mut range_end);
        new_end
    } else {
        vec![]
    }
}

fn txn_prepend_root(root: &[u8], mut txn: KvTxn) -> KvTxn {
    use super::txn::TxnOp as KvTxnOp;

    fn op_prepend_root(root: &[u8], op: KvTxnOp) -> KvTxnOp {
        match op {
            KvTxnOp::Put(k, v) => KvTxnOp::Put(key_prepend_root(root, k), v),
            KvTxnOp::Get(k) => KvTxnOp::Get(key_prepend_root(root, k)),
            KvTxnOp::Delete(k) => KvTxnOp::Delete(key_prepend_root(root, k)),
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

pub struct EtcdStore {
    root: Vec<u8>,
    client: Client,
}

impl EtcdStore {
    pub async fn with_endpoints<R, E, S>(root: R, endpoints: S) -> Result<KvBackendRef>
    where
        R: Into<Vec<u8>>,
        E: AsRef<str>,
        S: AsRef<[E]>,
    {
        let client = Client::connect(endpoints, None)
            .await
            .context(error::ConnectEtcdSnafu)?;

        Ok(Self::with_etcd_client(root, client))
    }

    pub fn with_etcd_client<R>(root: R, client: Client) -> KvBackendRef
    where
        R: Into<Vec<u8>>,
    {
        Arc::new(Self {
            root: root.into(),
            client,
        })
    }

    async fn do_multi_txn(&self, txn_ops: Vec<TxnOp>) -> Result<Vec<TxnResponse>> {
        if txn_ops.len() < MAX_TXN_SIZE {
            // fast path
            let txn = Txn::new().and_then(txn_ops);
            let txn_res = self
                .client
                .kv_client()
                .txn(txn)
                .await
                .context(error::EtcdFailedSnafu)?;
            return Ok(vec![txn_res]);
        }

        let txns = txn_ops
            .chunks(MAX_TXN_SIZE)
            .map(|part| async move {
                let txn = Txn::new().and_then(part);
                self.client.kv_client().txn(txn).await
            })
            .collect::<Vec<_>>();

        futures::future::try_join_all(txns)
            .await
            .context(error::EtcdFailedSnafu)
    }
}

#[async_trait::async_trait]
impl KvBackend for EtcdStore {
    fn name(&self) -> &str {
        "Etcd"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn range(&self, mut req: RangeRequest) -> Result<RangeResponse> {
        req.key = key_prepend_root(&self.root, req.key);
        req.range_end = range_end_prepend_root(&self.root, req.range_end);
        let Get { key, options } = req.try_into()?;

        let mut res = self
            .client
            .kv_client()
            .get(key, options)
            .await
            .context(error::EtcdFailedSnafu)?;

        let kvs = res
            .take_kvs()
            .into_iter()
            .map(chroot_key_value_with(&self.root))
            .collect::<Vec<_>>();

        Ok(RangeResponse {
            kvs,
            more: res.more(),
        })
    }

    async fn put(&self, mut req: PutRequest) -> Result<PutResponse> {
        req.key = key_prepend_root(&self.root, req.key);
        let Put {
            key,
            value,
            options,
        } = req.try_into()?;

        let mut res = self
            .client
            .kv_client()
            .put(key, value, options)
            .await
            .context(error::EtcdFailedSnafu)?;

        let prev_kv = res.take_prev_key().map(chroot_key_value_with(&self.root));
        Ok(PutResponse { prev_kv })
    }

    async fn batch_put(&self, mut req: BatchPutRequest) -> Result<BatchPutResponse> {
        for kv in req.kvs.iter_mut() {
            kv.key = key_prepend_root(&self.root, kv.key.drain(..).collect());
        }
        let BatchPut { kvs, options } = req.try_into()?;

        let put_ops = kvs
            .into_iter()
            .map(|kv| TxnOp::put(kv.key, kv.value, options.clone()))
            .collect::<Vec<_>>();

        let txn_responses = self.do_multi_txn(put_ops).await?;

        let mut prev_kvs = vec![];
        for txn_res in txn_responses {
            for op_res in txn_res.op_responses() {
                match op_res {
                    TxnOpResponse::Put(mut put_res) => {
                        if let Some(prev_kv) = put_res.take_prev_key() {
                            prev_kvs.push(chroot_key_value_with(&self.root)(prev_kv));
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(BatchPutResponse { prev_kvs })
    }

    async fn batch_get(&self, mut req: BatchGetRequest) -> Result<BatchGetResponse> {
        req.keys = req
            .keys
            .drain(..)
            .map(|key| key_prepend_root(&self.root, key))
            .collect();
        let BatchGet { keys, options } = req.try_into()?;

        let get_ops: Vec<_> = keys
            .into_iter()
            .map(|key| TxnOp::get(key, options.clone()))
            .collect();

        let txn_responses = self.do_multi_txn(get_ops).await?;

        let mut kvs = vec![];
        for txn_res in txn_responses {
            for op_res in txn_res.op_responses() {
                let mut get_res = match op_res {
                    TxnOpResponse::Get(get_res) => get_res,
                    _ => unreachable!(),
                };

                kvs.extend(
                    get_res
                        .take_kvs()
                        .into_iter()
                        .map(chroot_key_value_with(&self.root)),
                );
            }
        }

        Ok(BatchGetResponse { kvs })
    }

    async fn compare_and_put(
        &self,
        mut req: CompareAndPutRequest,
    ) -> Result<CompareAndPutResponse> {
        req.key = key_prepend_root(&self.root, req.key);
        let CompareAndPut {
            key,
            expect,
            value,
            put_options,
        } = req.try_into()?;

        let compare = if expect.is_empty() {
            // create if absent
            // revision 0 means key was not exist
            Compare::create_revision(key.clone(), CompareOp::Equal, 0)
        } else {
            // compare and put
            Compare::value(key.clone(), CompareOp::Equal, expect)
        };
        let put = TxnOp::put(key.clone(), value, put_options);
        let get = TxnOp::get(key, None);
        let txn = Txn::new()
            .when(vec![compare])
            .and_then(vec![put])
            .or_else(vec![get]);

        let txn_res = self
            .client
            .kv_client()
            .txn(txn)
            .await
            .context(error::EtcdFailedSnafu)?;

        let success = txn_res.succeeded();
        let op_res = txn_res
            .op_responses()
            .pop()
            .context(error::InvalidTxnResultSnafu {
                err_msg: "empty response",
            })?;

        let prev_kv = match op_res {
            TxnOpResponse::Put(mut res) => {
                res.take_prev_key().map(chroot_key_value_with(&self.root))
            }
            TxnOpResponse::Get(mut res) => res
                .take_kvs()
                .into_iter()
                .next()
                .map(chroot_key_value_with(&self.root)),
            _ => unreachable!(),
        };

        Ok(CompareAndPutResponse { success, prev_kv })
    }

    async fn delete_range(&self, mut req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        req.key = key_prepend_root(&self.root, req.key);
        req.range_end = range_end_prepend_root(&self.root, req.range_end);
        let Delete { key, options } = req.try_into()?;

        let mut res = self
            .client
            .kv_client()
            .delete(key, options)
            .await
            .context(error::EtcdFailedSnafu)?;

        let prev_kvs = res
            .take_prev_kvs()
            .into_iter()
            .map(chroot_key_value_with(&self.root))
            .collect::<Vec<_>>();

        Ok(DeleteRangeResponse {
            deleted: res.deleted(),
            prev_kvs,
        })
    }

    async fn batch_delete(&self, mut req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        req.keys = req
            .keys
            .drain(..)
            .map(|key| key_prepend_root(&self.root, key))
            .collect();
        let BatchDelete { keys, options } = req.try_into()?;

        let mut prev_kvs = Vec::with_capacity(keys.len());

        let delete_ops = keys
            .into_iter()
            .map(|key| TxnOp::delete(key, options.clone()))
            .collect::<Vec<_>>();

        let txn_responses = self.do_multi_txn(delete_ops).await?;

        for txn_res in txn_responses {
            for op_res in txn_res.op_responses() {
                match op_res {
                    TxnOpResponse::Delete(mut delete_res) => {
                        delete_res.take_prev_kvs().into_iter().for_each(|kv| {
                            prev_kvs.push(chroot_key_value_with(&self.root)(kv));
                        });
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(BatchDeleteResponse { prev_kvs })
    }
}

#[async_trait::async_trait]
impl TxnService for EtcdStore {
    type Error = Error;

    async fn txn(&self, txn: KvTxn) -> Result<KvTxnResponse> {
        let _timer = METRIC_META_TXN_REQUEST
            .with_label_values(&["etcd", "txn"])
            .start_timer();

        let etcd_txn: Txn = txn_prepend_root(&self.root, txn).into();
        let txn_res = self
            .client
            .kv_client()
            .txn(etcd_txn)
            .await
            .context(error::EtcdFailedSnafu)?;
        Ok(chroot_txn_response(&self.root, txn_res.try_into()?))
    }
}

struct Get {
    key: Vec<u8>,
    options: Option<GetOptions>,
}

impl TryFrom<RangeRequest> for Get {
    type Error = Error;

    fn try_from(req: RangeRequest) -> Result<Self> {
        let RangeRequest {
            key,
            range_end,
            limit,
            keys_only,
        } = req;

        ensure!(!key.is_empty(), error::EmptyKeySnafu);

        let mut options = GetOptions::default();
        if !range_end.is_empty() {
            options = options.with_range(range_end);
            if limit > 0 {
                options = options.with_limit(limit);
            }
        }
        if keys_only {
            options = options.with_keys_only();
        }

        Ok(Get {
            key,
            options: Some(options),
        })
    }
}

struct Put {
    key: Vec<u8>,
    value: Vec<u8>,
    options: Option<PutOptions>,
}

impl TryFrom<PutRequest> for Put {
    type Error = Error;

    fn try_from(req: PutRequest) -> Result<Self> {
        let PutRequest {
            key,
            value,
            prev_kv,
        } = req;

        let mut options = PutOptions::default();
        if prev_kv {
            options = options.with_prev_key();
        }

        Ok(Put {
            key,
            value,
            options: Some(options),
        })
    }
}

struct BatchGet {
    keys: Vec<Vec<u8>>,
    options: Option<GetOptions>,
}

impl TryFrom<BatchGetRequest> for BatchGet {
    type Error = Error;

    fn try_from(req: BatchGetRequest) -> Result<Self> {
        let BatchGetRequest { keys } = req;

        let options = GetOptions::default();

        Ok(BatchGet {
            keys,
            options: Some(options),
        })
    }
}

struct BatchPut {
    kvs: Vec<KeyValue>,
    options: Option<PutOptions>,
}

impl TryFrom<BatchPutRequest> for BatchPut {
    type Error = Error;

    fn try_from(req: BatchPutRequest) -> Result<Self> {
        let BatchPutRequest { kvs, prev_kv } = req;

        let mut options = PutOptions::default();
        if prev_kv {
            options = options.with_prev_key();
        }

        Ok(BatchPut {
            kvs,
            options: Some(options),
        })
    }
}

struct BatchDelete {
    keys: Vec<Vec<u8>>,
    options: Option<DeleteOptions>,
}

impl TryFrom<BatchDeleteRequest> for BatchDelete {
    type Error = Error;

    fn try_from(req: BatchDeleteRequest) -> Result<Self> {
        let BatchDeleteRequest { keys, prev_kv } = req;

        let mut options = DeleteOptions::default();
        if prev_kv {
            options = options.with_prev_key();
        }

        Ok(BatchDelete {
            keys,
            options: Some(options),
        })
    }
}

struct CompareAndPut {
    key: Vec<u8>,
    expect: Vec<u8>,
    value: Vec<u8>,
    put_options: Option<PutOptions>,
}

impl TryFrom<CompareAndPutRequest> for CompareAndPut {
    type Error = Error;

    fn try_from(req: CompareAndPutRequest) -> Result<Self> {
        let CompareAndPutRequest { key, expect, value } = req;

        Ok(CompareAndPut {
            key,
            expect,
            value,
            put_options: Some(PutOptions::default().with_prev_key()),
        })
    }
}

struct Delete {
    key: Vec<u8>,
    options: Option<DeleteOptions>,
}

impl TryFrom<DeleteRangeRequest> for Delete {
    type Error = Error;

    fn try_from(req: DeleteRangeRequest) -> Result<Self> {
        let DeleteRangeRequest {
            key,
            range_end,
            prev_kv,
        } = req;

        ensure!(!key.is_empty(), error::EmptyKeySnafu);

        let mut options = DeleteOptions::default();
        if !range_end.is_empty() {
            options = options.with_range(range_end);
        }
        if prev_kv {
            options = options.with_prev_key();
        }

        Ok(Delete {
            key,
            options: Some(options),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_get() {
        let req = RangeRequest {
            key: b"test_key".to_vec(),
            range_end: b"test_range_end".to_vec(),
            limit: 64,
            keys_only: true,
        };

        let get: Get = req.try_into().unwrap();

        assert_eq!(b"test_key".to_vec(), get.key);
        let _ = get.options.unwrap();
    }

    #[test]
    fn test_parse_put() {
        let req = PutRequest {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            prev_kv: true,
        };

        let put: Put = req.try_into().unwrap();

        assert_eq!(b"test_key".to_vec(), put.key);
        assert_eq!(b"test_value".to_vec(), put.value);
        let _ = put.options.unwrap();
    }

    #[test]
    fn test_parse_batch_get() {
        let req = BatchGetRequest {
            keys: vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
        };

        let batch_get: BatchGet = req.try_into().unwrap();
        let keys = batch_get.keys;

        assert_eq!(b"k1".to_vec(), keys.get(0).unwrap().clone());
        assert_eq!(b"k2".to_vec(), keys.get(1).unwrap().clone());
        assert_eq!(b"k3".to_vec(), keys.get(2).unwrap().clone());
    }

    #[test]
    fn test_parse_batch_put() {
        let req = BatchPutRequest {
            kvs: vec![KeyValue {
                key: b"test_key".to_vec(),
                value: b"test_value".to_vec(),
            }],
            prev_kv: true,
        };

        let batch_put: BatchPut = req.try_into().unwrap();

        let kv = batch_put.kvs.get(0).unwrap();
        assert_eq!(b"test_key", kv.key());
        assert_eq!(b"test_value", kv.value());
        let _ = batch_put.options.unwrap();
    }

    #[test]
    fn test_parse_batch_delete() {
        let req = BatchDeleteRequest {
            keys: vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
            prev_kv: true,
        };

        let batch_delete: BatchDelete = req.try_into().unwrap();

        assert_eq!(batch_delete.keys.len(), 3);
        assert_eq!(b"k1".to_vec(), batch_delete.keys.get(0).unwrap().clone());
        assert_eq!(b"k2".to_vec(), batch_delete.keys.get(1).unwrap().clone());
        assert_eq!(b"k3".to_vec(), batch_delete.keys.get(2).unwrap().clone());
        let _ = batch_delete.options.unwrap();
    }

    #[test]
    fn test_parse_compare_and_put() {
        let req = CompareAndPutRequest {
            key: b"test_key".to_vec(),
            expect: b"test_expect".to_vec(),
            value: b"test_value".to_vec(),
        };

        let compare_and_put: CompareAndPut = req.try_into().unwrap();

        assert_eq!(b"test_key".to_vec(), compare_and_put.key);
        assert_eq!(b"test_expect".to_vec(), compare_and_put.expect);
        assert_eq!(b"test_value".to_vec(), compare_and_put.value);
        let _ = compare_and_put.put_options.unwrap();
    }

    #[test]
    fn test_parse_delete() {
        let req = DeleteRangeRequest {
            key: b"test_key".to_vec(),
            range_end: b"test_range_end".to_vec(),
            prev_kv: true,
        };

        let delete: Delete = req.try_into().unwrap();

        assert_eq!(b"test_key".to_vec(), delete.key);
        let _ = delete.options.unwrap();
    }
}
