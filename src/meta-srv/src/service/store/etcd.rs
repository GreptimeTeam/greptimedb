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

use common_meta::kv_backend::txn::{Txn as KvTxn, TxnResponse as KvTxnResponse};
use common_meta::kv_backend::{KvBackend, TxnService};
use common_meta::metrics::METRIC_META_TXN_REQUEST;
use common_meta::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, MoveValueRequest, MoveValueResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse,
};
use common_meta::rpc::KeyValue;
use common_telemetry::{timer, warn};
use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, GetOptions, PutOptions, Txn, TxnOp, TxnOpResponse,
    TxnResponse,
};
use snafu::{ensure, OptionExt, ResultExt};

use crate::error;
use crate::error::{ConvertEtcdTxnObjectSnafu, Error, Result};
use crate::service::store::etcd_util::KvPair;
use crate::service::store::kv::KvStoreRef;

// Maximum number of operations permitted in a transaction.
// The etcd default configuration's `--max-txn-ops` is 128.
//
// For more detail, see: https://etcd.io/docs/v3.5/op-guide/configuration/
const MAX_TXN_SIZE: usize = 128;

pub struct EtcdStore {
    client: Client,
}

impl EtcdStore {
    pub async fn with_endpoints<E, S>(endpoints: S) -> Result<KvStoreRef>
    where
        E: AsRef<str>,
        S: AsRef<[E]>,
    {
        let client = Client::connect(endpoints, None)
            .await
            .context(error::ConnectEtcdSnafu)?;

        Ok(Self::with_etcd_client(client))
    }

    pub fn with_etcd_client(client: Client) -> KvStoreRef {
        Arc::new(Self { client })
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

    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        let Get { key, options } = req.try_into()?;

        let res = self
            .client
            .kv_client()
            .get(key, options)
            .await
            .context(error::EtcdFailedSnafu)?;

        let kvs = res
            .kvs()
            .iter()
            .map(KvPair::from_etcd_kv)
            .collect::<Vec<_>>();

        Ok(RangeResponse {
            kvs,
            more: res.more(),
        })
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let Put {
            key,
            value,
            options,
        } = req.try_into()?;

        let res = self
            .client
            .kv_client()
            .put(key, value, options)
            .await
            .context(error::EtcdFailedSnafu)?;

        let prev_kv = res.prev_key().map(KvPair::from_etcd_kv);
        Ok(PutResponse { prev_kv })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let BatchPut { kvs, options } = req.try_into()?;

        let put_ops = kvs
            .into_iter()
            .map(|kv| (TxnOp::put(kv.key, kv.value, options.clone())))
            .collect::<Vec<_>>();

        let txn_responses = self.do_multi_txn(put_ops).await?;

        let mut prev_kvs = vec![];
        for txn_res in txn_responses {
            for op_res in txn_res.op_responses() {
                match op_res {
                    TxnOpResponse::Put(put_res) => {
                        if let Some(prev_kv) = put_res.prev_key() {
                            prev_kvs.push(KvPair::from_etcd_kv(prev_kv));
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(BatchPutResponse { prev_kvs })
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        let BatchGet { keys, options } = req.try_into()?;

        let get_ops: Vec<_> = keys
            .into_iter()
            .map(|k| TxnOp::get(k, options.clone()))
            .collect();

        let txn_responses = self.do_multi_txn(get_ops).await?;

        let mut kvs = vec![];
        for txn_res in txn_responses {
            for op_res in txn_res.op_responses() {
                let get_res = match op_res {
                    TxnOpResponse::Get(get_res) => get_res,
                    _ => unreachable!(),
                };

                kvs.extend(get_res.kvs().iter().map(KvPair::from_etcd_kv));
            }
        }

        Ok(BatchGetResponse { kvs })
    }

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
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
            TxnOpResponse::Put(res) => res.prev_key().map(KvPair::from_etcd_kv),
            TxnOpResponse::Get(res) => res.kvs().first().map(KvPair::from_etcd_kv),
            _ => unreachable!(),
        };

        Ok(CompareAndPutResponse { success, prev_kv })
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let Delete { key, options } = req.try_into()?;

        let res = self
            .client
            .kv_client()
            .delete(key, options)
            .await
            .context(error::EtcdFailedSnafu)?;

        let prev_kvs = res
            .prev_kvs()
            .iter()
            .map(KvPair::from_etcd_kv)
            .collect::<Vec<_>>();

        Ok(DeleteRangeResponse {
            deleted: res.deleted(),
            prev_kvs,
        })
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        let BatchDelete { keys, options } = req.try_into()?;

        let mut prev_kvs = Vec::with_capacity(keys.len());

        let delete_ops = keys
            .into_iter()
            .map(|k| TxnOp::delete(k, options.clone()))
            .collect::<Vec<_>>();

        let txn_responses = self.do_multi_txn(delete_ops).await?;

        for txn_res in txn_responses {
            for op_res in txn_res.op_responses() {
                match op_res {
                    TxnOpResponse::Delete(delete_res) => {
                        delete_res.prev_kvs().iter().for_each(|kv| {
                            prev_kvs.push(KvPair::from_etcd_kv(kv));
                        });
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(BatchDeleteResponse { prev_kvs })
    }

    async fn move_value(&self, req: MoveValueRequest) -> Result<MoveValueResponse> {
        let MoveValue {
            from_key,
            to_key,
            delete_options,
        } = req.try_into()?;

        let mut client = self.client.kv_client();

        // TODO(jiachun): Maybe it's better to let the users control it in the request
        const MAX_RETRIES: usize = 8;
        for _ in 0..MAX_RETRIES {
            let from_key = from_key.as_slice();
            let to_key = to_key.as_slice();

            let res = client
                .get(from_key, None)
                .await
                .context(error::EtcdFailedSnafu)?;

            let txn = match res.kvs().first() {
                None => {
                    // get `to_key` if `from_key` absent
                    // revision 0 means key was not exist
                    let compare = Compare::create_revision(from_key, CompareOp::Equal, 0);
                    let get = TxnOp::get(to_key, None);
                    Txn::new().when(vec![compare]).and_then(vec![get])
                }
                Some(kv) => {
                    // compare `from_key` and move to `to_key`
                    let value = kv.value();
                    let compare = Compare::value(from_key, CompareOp::Equal, value);
                    let delete = TxnOp::delete(from_key, delete_options.clone());
                    let put = TxnOp::put(to_key, value, None);
                    Txn::new().when(vec![compare]).and_then(vec![delete, put])
                }
            };

            let txn_res = client.txn(txn).await.context(error::EtcdFailedSnafu)?;

            if !txn_res.succeeded() {
                warn!(
                    "Failed to atomically move {:?} to {:?}, try again...",
                    String::from_utf8_lossy(from_key),
                    String::from_utf8_lossy(to_key)
                );
                continue;
            }

            // [`get_res'] or [`delete_res`, `put_res`], `put_res` will be ignored.
            for op_res in txn_res.op_responses() {
                match op_res {
                    TxnOpResponse::Get(res) => {
                        return Ok(MoveValueResponse(
                            res.kvs().first().map(KvPair::from_etcd_kv),
                        ));
                    }
                    TxnOpResponse::Delete(res) => {
                        return Ok(MoveValueResponse(
                            res.prev_kvs().first().map(KvPair::from_etcd_kv),
                        ));
                    }
                    _ => {}
                }
            }
        }

        error::MoveValueSnafu {
            key: String::from_utf8_lossy(&from_key),
        }
        .fail()
    }
}

#[async_trait::async_trait]
impl TxnService for EtcdStore {
    type Error = Error;

    async fn txn(&self, txn: KvTxn) -> Result<KvTxnResponse> {
        let _timer = timer!(
            METRIC_META_TXN_REQUEST,
            &[("target", "etcd".to_string()), ("op", "txn".to_string()),]
        );

        let etcd_txn: Txn = txn.into();
        let txn_res = self
            .client
            .kv_client()
            .txn(etcd_txn)
            .await
            .context(error::EtcdFailedSnafu)?;
        txn_res.try_into().context(ConvertEtcdTxnObjectSnafu)
    }
}

struct Get {
    key: Vec<u8>,
    options: Option<GetOptions>,
}

impl TryFrom<RangeRequest> for Get {
    type Error = error::Error;

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
    type Error = error::Error;

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
    type Error = error::Error;

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
    type Error = error::Error;

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
    type Error = error::Error;

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
    type Error = error::Error;

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
    type Error = error::Error;

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

struct MoveValue {
    from_key: Vec<u8>,
    to_key: Vec<u8>,
    delete_options: Option<DeleteOptions>,
}

impl TryFrom<MoveValueRequest> for MoveValue {
    type Error = error::Error;

    fn try_from(req: MoveValueRequest) -> Result<Self> {
        let MoveValueRequest { from_key, to_key } = req;

        Ok(MoveValue {
            from_key,
            to_key,
            delete_options: Some(DeleteOptions::default().with_prev_key()),
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

    #[test]
    fn test_parse_move_value() {
        let req = MoveValueRequest {
            from_key: b"test_from_key".to_vec(),
            to_key: b"test_to_key".to_vec(),
        };

        let move_value: MoveValue = req.try_into().unwrap();

        assert_eq!(b"test_from_key".to_vec(), move_value.from_key);
        assert_eq!(b"test_to_key".to_vec(), move_value.to_key);
        let _ = move_value.delete_options.unwrap();
    }
}
