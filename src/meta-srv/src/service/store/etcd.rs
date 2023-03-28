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

use api::v1::meta::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, CompareAndPutRequest, CompareAndPutResponse, DeleteRangeRequest,
    DeleteRangeResponse, KeyValue, MoveValueRequest, MoveValueResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse, ResponseHeader,
};
use common_error::prelude::*;
use common_telemetry::warn;
use etcd_client::{
    Client, Compare, CompareOp, DeleteOptions, GetOptions, PutOptions, Txn, TxnOp, TxnOpResponse,
};

use crate::error;
use crate::error::Result;
use crate::service::store::kv::{KvStore, KvStoreRef};

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

        Self::with_etcd_client(client)
    }

    pub fn with_etcd_client(client: Client) -> Result<KvStoreRef> {
        Ok(Arc::new(Self { client }))
    }
}

#[async_trait::async_trait]
impl KvStore for EtcdStore {
    async fn range(&self, req: RangeRequest) -> Result<RangeResponse> {
        let Get {
            cluster_id,
            key,
            options,
        } = req.try_into()?;

        let res = self
            .client
            .kv_client()
            .get(key, options)
            .await
            .context(error::EtcdFailedSnafu)?;

        let kvs = res.kvs().iter().map(KvPair::to_kv).collect::<Vec<_>>();

        let header = Some(ResponseHeader::success(cluster_id));
        Ok(RangeResponse {
            header,
            kvs,
            more: res.more(),
        })
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let Put {
            cluster_id,
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

        let prev_kv = res.prev_key().map(KvPair::to_kv);

        let header = Some(ResponseHeader::success(cluster_id));
        Ok(PutResponse { header, prev_kv })
    }

    async fn batch_get(&self, req: BatchGetRequest) -> Result<BatchGetResponse> {
        let BatchGet {
            cluster_id,
            keys,
            options,
        } = req.try_into()?;

        let get_ops: Vec<_> = keys
            .into_iter()
            .map(|k| TxnOp::get(k, options.clone()))
            .collect();
        let txn = Txn::new().and_then(get_ops);

        let txn_res = self
            .client
            .kv_client()
            .txn(txn)
            .await
            .context(error::EtcdFailedSnafu)?;

        let mut kvs = vec![];
        for op_res in txn_res.op_responses() {
            let get_res = match op_res {
                TxnOpResponse::Get(get_res) => get_res,
                _ => unreachable!(),
            };

            kvs.extend(get_res.kvs().iter().map(KvPair::to_kv));
        }

        let header = Some(ResponseHeader::success(cluster_id));
        Ok(BatchGetResponse { header, kvs })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
        let BatchPut {
            cluster_id,
            kvs,
            options,
        } = req.try_into()?;

        let put_ops = kvs
            .into_iter()
            .map(|kv| (TxnOp::put(kv.key, kv.value, options.clone())))
            .collect::<Vec<_>>();
        let txn = Txn::new().and_then(put_ops);

        let txn_res = self
            .client
            .kv_client()
            .txn(txn)
            .await
            .context(error::EtcdFailedSnafu)?;

        let mut prev_kvs = vec![];
        for op_res in txn_res.op_responses() {
            match op_res {
                TxnOpResponse::Put(put_res) => {
                    if let Some(prev_kv) = put_res.prev_key() {
                        prev_kvs.push(KvPair::to_kv(prev_kv));
                    }
                }
                _ => unreachable!(), // never get here
            }
        }

        let header = Some(ResponseHeader::success(cluster_id));
        Ok(BatchPutResponse { header, prev_kvs })
    }

    async fn batch_delete(&self, req: BatchDeleteRequest) -> Result<BatchDeleteResponse> {
        let BatchDelete {
            cluster_id,
            keys,
            options,
        } = req.try_into()?;

        let mut prev_kvs = Vec::with_capacity(keys.len());

        let delete_ops = keys
            .into_iter()
            .map(|k| TxnOp::delete(k, options.clone()))
            .collect::<Vec<_>>();
        let txn = Txn::new().and_then(delete_ops);

        let txn_res = self
            .client
            .kv_client()
            .txn(txn)
            .await
            .context(error::EtcdFailedSnafu)?;

        for op_res in txn_res.op_responses() {
            match op_res {
                TxnOpResponse::Delete(delete_res) => {
                    delete_res.prev_kvs().iter().for_each(|kv| {
                        prev_kvs.push(KvPair::to_kv(kv));
                    });
                }
                _ => unreachable!(), // never get here
            }
        }

        let header = Some(ResponseHeader::success(cluster_id));

        Ok(BatchDeleteResponse { header, prev_kvs })
    }

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        let CompareAndPut {
            cluster_id,
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
            TxnOpResponse::Put(res) => res.prev_key().map(KvPair::to_kv),
            TxnOpResponse::Get(res) => res.kvs().first().map(KvPair::to_kv),
            _ => unreachable!(), // never get here
        };

        let header = Some(ResponseHeader::success(cluster_id));
        Ok(CompareAndPutResponse {
            header,
            success,
            prev_kv,
        })
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
        let Delete {
            cluster_id,
            key,
            options,
        } = req.try_into()?;

        let res = self
            .client
            .kv_client()
            .delete(key, options)
            .await
            .context(error::EtcdFailedSnafu)?;

        let prev_kvs = res.prev_kvs().iter().map(KvPair::to_kv).collect::<Vec<_>>();

        let header = Some(ResponseHeader::success(cluster_id));
        Ok(DeleteRangeResponse {
            header,
            deleted: res.deleted(),
            prev_kvs,
        })
    }

    async fn move_value(&self, req: MoveValueRequest) -> Result<MoveValueResponse> {
        let MoveValue {
            cluster_id,
            from_key,
            to_key,
            delete_options,
        } = req.try_into()?;

        let mut client = self.client.kv_client();

        let header = Some(ResponseHeader::success(cluster_id));
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
                        return Ok(MoveValueResponse {
                            header,
                            kv: res.kvs().first().map(KvPair::to_kv),
                        });
                    }
                    TxnOpResponse::Delete(res) => {
                        return Ok(MoveValueResponse {
                            header,
                            kv: res.prev_kvs().first().map(KvPair::to_kv),
                        });
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

struct Get {
    cluster_id: u64,
    key: Vec<u8>,
    options: Option<GetOptions>,
}

impl TryFrom<RangeRequest> for Get {
    type Error = error::Error;

    fn try_from(req: RangeRequest) -> Result<Self> {
        let RangeRequest {
            header,
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
            cluster_id: header.map_or(0, |h| h.cluster_id),
            key,
            options: Some(options),
        })
    }
}

struct Put {
    cluster_id: u64,
    key: Vec<u8>,
    value: Vec<u8>,
    options: Option<PutOptions>,
}

impl TryFrom<PutRequest> for Put {
    type Error = error::Error;

    fn try_from(req: PutRequest) -> Result<Self> {
        let PutRequest {
            header,
            key,
            value,
            prev_kv,
        } = req;

        let mut options = PutOptions::default();
        if prev_kv {
            options = options.with_prev_key();
        }

        Ok(Put {
            cluster_id: header.map_or(0, |h| h.cluster_id),
            key,
            value,
            options: Some(options),
        })
    }
}

struct BatchGet {
    cluster_id: u64,
    keys: Vec<Vec<u8>>,
    options: Option<GetOptions>,
}

impl TryFrom<BatchGetRequest> for BatchGet {
    type Error = error::Error;

    fn try_from(req: BatchGetRequest) -> Result<Self> {
        let BatchGetRequest { header, keys } = req;

        let options = GetOptions::default();

        Ok(BatchGet {
            cluster_id: header.map_or(0, |h| h.cluster_id),
            keys,
            options: Some(options),
        })
    }
}

struct BatchPut {
    cluster_id: u64,
    kvs: Vec<KeyValue>,
    options: Option<PutOptions>,
}

impl TryFrom<BatchPutRequest> for BatchPut {
    type Error = error::Error;

    fn try_from(req: BatchPutRequest) -> Result<Self> {
        let BatchPutRequest {
            header,
            kvs,
            prev_kv,
        } = req;

        let mut options = PutOptions::default();
        if prev_kv {
            options = options.with_prev_key();
        }

        Ok(BatchPut {
            cluster_id: header.map_or(0, |h| h.cluster_id),
            kvs,
            options: Some(options),
        })
    }
}

struct BatchDelete {
    cluster_id: u64,
    keys: Vec<Vec<u8>>,
    options: Option<DeleteOptions>,
}

impl TryFrom<BatchDeleteRequest> for BatchDelete {
    type Error = error::Error;

    fn try_from(req: BatchDeleteRequest) -> Result<Self> {
        let BatchDeleteRequest {
            header,
            keys,
            prev_kv,
        } = req;

        let mut options = DeleteOptions::default();
        if prev_kv {
            options = options.with_prev_key();
        }

        Ok(BatchDelete {
            cluster_id: header.map_or(0, |h| h.cluster_id),
            keys,
            options: Some(options),
        })
    }
}

struct CompareAndPut {
    cluster_id: u64,
    key: Vec<u8>,
    expect: Vec<u8>,
    value: Vec<u8>,
    put_options: Option<PutOptions>,
}

impl TryFrom<CompareAndPutRequest> for CompareAndPut {
    type Error = error::Error;

    fn try_from(req: CompareAndPutRequest) -> Result<Self> {
        let CompareAndPutRequest {
            header,
            key,
            expect,
            value,
        } = req;

        Ok(CompareAndPut {
            cluster_id: header.map_or(0, |h| h.cluster_id),
            key,
            expect,
            value,
            put_options: Some(PutOptions::default().with_prev_key()),
        })
    }
}

struct Delete {
    cluster_id: u64,
    key: Vec<u8>,
    options: Option<DeleteOptions>,
}

impl TryFrom<DeleteRangeRequest> for Delete {
    type Error = error::Error;

    fn try_from(req: DeleteRangeRequest) -> Result<Self> {
        let DeleteRangeRequest {
            header,
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
            cluster_id: header.map_or(0, |h| h.cluster_id),
            key,
            options: Some(options),
        })
    }
}

struct MoveValue {
    cluster_id: u64,
    from_key: Vec<u8>,
    to_key: Vec<u8>,
    delete_options: Option<DeleteOptions>,
}

impl TryFrom<MoveValueRequest> for MoveValue {
    type Error = error::Error;

    fn try_from(req: MoveValueRequest) -> Result<Self> {
        let MoveValueRequest {
            header,
            from_key,
            to_key,
        } = req;

        Ok(MoveValue {
            cluster_id: header.map_or(0, |h| h.cluster_id),
            from_key,
            to_key,
            delete_options: Some(DeleteOptions::default().with_prev_key()),
        })
    }
}

struct KvPair<'a>(&'a etcd_client::KeyValue);

impl<'a> KvPair<'a> {
    /// Creates a `KvPair` from etcd KeyValue
    #[inline]
    fn new(kv: &'a etcd_client::KeyValue) -> Self {
        Self(kv)
    }

    #[inline]
    fn to_kv(kv: &etcd_client::KeyValue) -> KeyValue {
        KeyValue::from(KvPair::new(kv))
    }
}

impl<'a> From<KvPair<'a>> for KeyValue {
    fn from(kv: KvPair<'a>) -> Self {
        Self {
            key: kv.0.key().to_vec(),
            value: kv.0.value().to_vec(),
        }
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
            ..Default::default()
        };

        let get: Get = req.try_into().unwrap();

        assert_eq!(b"test_key".to_vec(), get.key);
        assert!(get.options.is_some());
    }

    #[test]
    fn test_parse_put() {
        let req = PutRequest {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            prev_kv: true,
            ..Default::default()
        };

        let put: Put = req.try_into().unwrap();

        assert_eq!(b"test_key".to_vec(), put.key);
        assert_eq!(b"test_value".to_vec(), put.value);
        assert!(put.options.is_some());
    }

    #[test]
    fn test_parse_batch_get() {
        let req = BatchGetRequest {
            keys: vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
            ..Default::default()
        };

        let batch_get: BatchGet = req.try_into().unwrap();
        let keys = batch_get.keys;

        assert_eq!(b"k1".to_vec(), keys.get(0).unwrap().to_vec());
        assert_eq!(b"k2".to_vec(), keys.get(1).unwrap().to_vec());
        assert_eq!(b"k3".to_vec(), keys.get(2).unwrap().to_vec());
    }

    #[test]
    fn test_parse_batch_put() {
        let req = BatchPutRequest {
            kvs: vec![KeyValue {
                key: b"test_key".to_vec(),
                value: b"test_value".to_vec(),
            }],
            prev_kv: true,
            ..Default::default()
        };

        let batch_put: BatchPut = req.try_into().unwrap();

        assert_eq!(b"test_key".to_vec(), batch_put.kvs.get(0).unwrap().key);
        assert_eq!(b"test_value".to_vec(), batch_put.kvs.get(0).unwrap().value);
        assert!(batch_put.options.is_some());
    }

    #[test]
    fn test_parse_batch_delete() {
        let req = BatchDeleteRequest {
            keys: vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()],
            prev_kv: true,
            ..Default::default()
        };

        let batch_delete: BatchDelete = req.try_into().unwrap();

        assert_eq!(batch_delete.keys.len(), 3);
        assert_eq!(b"k1".to_vec(), batch_delete.keys.get(0).unwrap().to_vec());
        assert_eq!(b"k2".to_vec(), batch_delete.keys.get(1).unwrap().to_vec());
        assert_eq!(b"k3".to_vec(), batch_delete.keys.get(2).unwrap().to_vec());
        assert!(batch_delete.options.is_some());
    }

    #[test]
    fn test_parse_compare_and_put() {
        let req = CompareAndPutRequest {
            key: b"test_key".to_vec(),
            expect: b"test_expect".to_vec(),
            value: b"test_value".to_vec(),
            ..Default::default()
        };

        let compare_and_put: CompareAndPut = req.try_into().unwrap();

        assert_eq!(b"test_key".to_vec(), compare_and_put.key);
        assert_eq!(b"test_expect".to_vec(), compare_and_put.expect);
        assert_eq!(b"test_value".to_vec(), compare_and_put.value);
        assert!(compare_and_put.put_options.is_some());
    }

    #[test]
    fn test_parse_delete() {
        let req = DeleteRangeRequest {
            key: b"test_key".to_vec(),
            range_end: b"test_range_end".to_vec(),
            prev_kv: true,
            ..Default::default()
        };

        let delete: Delete = req.try_into().unwrap();

        assert_eq!(b"test_key".to_vec(), delete.key);
        assert!(delete.options.is_some());
    }

    #[test]
    fn test_parse_move_value() {
        let req = MoveValueRequest {
            from_key: b"test_from_key".to_vec(),
            to_key: b"test_to_key".to_vec(),
            ..Default::default()
        };

        let move_value: MoveValue = req.try_into().unwrap();

        assert_eq!(b"test_from_key".to_vec(), move_value.from_key);
        assert_eq!(b"test_to_key".to_vec(), move_value.to_key);
        assert!(move_value.delete_options.is_some());
    }
}
