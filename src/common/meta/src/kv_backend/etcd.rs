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

use common_telemetry::info;
use etcd_client::{
    Client, DeleteOptions, GetOptions, PutOptions, Txn, TxnOp, TxnOpResponse, TxnResponse,
};
use snafu::{ensure, ResultExt};

use super::KvBackendRef;
use crate::error::{self, Error, Result};
use crate::kv_backend::txn::{Txn as KvTxn, TxnResponse as KvTxnResponse};
use crate::kv_backend::{KvBackend, TxnService};
use crate::metrics::METRIC_META_TXN_REQUEST;
use crate::rpc::store::{
    BatchDeleteRequest, BatchDeleteResponse, BatchGetRequest, BatchGetResponse, BatchPutRequest,
    BatchPutResponse, DeleteRangeRequest, DeleteRangeResponse, PutRequest, PutResponse,
    RangeRequest, RangeResponse,
};
use crate::rpc::KeyValue;

pub struct EtcdStore {
    client: Client,
    // Maximum number of operations permitted in a transaction.
    // The etcd default configuration's `--max-txn-ops` is 128.
    //
    // For more detail, see: https://etcd.io/docs/v3.5/op-guide/configuration/
    max_txn_ops: usize,
}

impl EtcdStore {
    pub async fn with_endpoints<E, S>(endpoints: S, max_txn_ops: usize) -> Result<KvBackendRef>
    where
        E: AsRef<str>,
        S: AsRef<[E]>,
    {
        let client = Client::connect(endpoints, None)
            .await
            .context(error::ConnectEtcdSnafu)?;

        Ok(Self::with_etcd_client(client, max_txn_ops))
    }

    pub fn with_etcd_client(client: Client, max_txn_ops: usize) -> KvBackendRef {
        info!("Connected to etcd");
        Arc::new(Self {
            client,
            max_txn_ops,
        })
    }

    async fn do_multi_txn(&self, txn_ops: Vec<TxnOp>) -> Result<Vec<TxnResponse>> {
        let max_txn_ops = self.max_txn_ops();
        if txn_ops.len() < max_txn_ops {
            // fast path
            let _timer = METRIC_META_TXN_REQUEST
                .with_label_values(&["etcd", "txn"])
                .start_timer();
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
            .chunks(max_txn_ops)
            .map(|part| async move {
                let _timer = METRIC_META_TXN_REQUEST
                    .with_label_values(&["etcd", "txn"])
                    .start_timer();
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

        let mut res = self
            .client
            .kv_client()
            .get(key, options)
            .await
            .context(error::EtcdFailedSnafu)?;

        let kvs = res
            .take_kvs()
            .into_iter()
            .map(KeyValue::from)
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

        let mut res = self
            .client
            .kv_client()
            .put(key, value, options)
            .await
            .context(error::EtcdFailedSnafu)?;

        let prev_kv = res.take_prev_key().map(KeyValue::from);
        Ok(PutResponse { prev_kv })
    }

    async fn batch_put(&self, req: BatchPutRequest) -> Result<BatchPutResponse> {
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
                        if let Some(prev_kv) = put_res.take_prev_key().map(KeyValue::from) {
                            prev_kvs.push(prev_kv);
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
                kvs.extend(get_res.take_kvs().into_iter().map(KeyValue::from));
            }
        }

        Ok(BatchGetResponse { kvs })
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
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
            .map(KeyValue::from)
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
            .map(|key| TxnOp::delete(key, options.clone()))
            .collect::<Vec<_>>();

        let txn_responses = self.do_multi_txn(delete_ops).await?;

        for txn_res in txn_responses {
            for op_res in txn_res.op_responses() {
                match op_res {
                    TxnOpResponse::Delete(mut delete_res) => {
                        delete_res
                            .take_prev_kvs()
                            .into_iter()
                            .map(KeyValue::from)
                            .for_each(|kv| {
                                prev_kvs.push(kv);
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

        let max_operations = txn.max_operations();

        let etcd_txn: Txn = txn.into();
        let txn_res = self
            .client
            .kv_client()
            .txn(etcd_txn)
            .await
            .context(error::EtcdTxnFailedSnafu { max_operations })?;
        txn_res.try_into()
    }

    fn max_txn_ops(&self) -> usize {
        self.max_txn_ops
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

        assert_eq!(b"k1".to_vec(), keys.first().unwrap().clone());
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

        let kv = batch_put.kvs.first().unwrap();
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
        assert_eq!(b"k1".to_vec(), batch_delete.keys.first().unwrap().clone());
        assert_eq!(b"k2".to_vec(), batch_delete.keys.get(1).unwrap().clone());
        assert_eq!(b"k3".to_vec(), batch_delete.keys.get(2).unwrap().clone());
        let _ = batch_delete.options.unwrap();
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

    use crate::kv_backend::test::{
        prepare_kv_with_prefix, test_kv_batch_delete_with_prefix, test_kv_batch_get_with_prefix,
        test_kv_compare_and_put_with_prefix, test_kv_delete_range_with_prefix,
        test_kv_put_with_prefix, test_kv_range_2_with_prefix, test_kv_range_with_prefix,
        unprepare_kv,
    };

    async fn build_kv_backend() -> Option<EtcdStore> {
        let endpoints = std::env::var("GT_ETCD_ENDPOINTS").unwrap_or_default();
        if endpoints.is_empty() {
            return None;
        }

        let endpoints = endpoints
            .split(',')
            .map(|s| s.to_string())
            .collect::<Vec<String>>();

        let client = Client::connect(endpoints, None)
            .await
            .expect("malformed endpoints");

        Some(EtcdStore {
            client,
            max_txn_ops: 128,
        })
    }

    #[tokio::test]
    async fn test_put() {
        if let Some(kv_backend) = build_kv_backend().await {
            let prefix = b"put/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_put_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_range() {
        if let Some(kv_backend) = build_kv_backend().await {
            let prefix = b"range/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_range_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test]
    async fn test_range_2() {
        if let Some(kv_backend) = build_kv_backend().await {
            test_kv_range_2_with_prefix(kv_backend, b"range2/".to_vec()).await;
        }
    }

    #[tokio::test]
    async fn test_batch_get() {
        if let Some(kv_backend) = build_kv_backend().await {
            let prefix = b"batchGet/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_batch_get_with_prefix(&kv_backend, prefix.to_vec()).await;
            unprepare_kv(&kv_backend, prefix).await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_compare_and_put() {
        if let Some(kv_backend) = build_kv_backend().await {
            let kv_backend = Arc::new(kv_backend);
            test_kv_compare_and_put_with_prefix(kv_backend, b"compareAndPut/".to_vec()).await;
        }
    }

    #[tokio::test]
    async fn test_delete_range() {
        if let Some(kv_backend) = build_kv_backend().await {
            let prefix = b"deleteRange/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_delete_range_with_prefix(kv_backend, prefix.to_vec()).await;
        }
    }

    #[tokio::test]
    async fn test_batch_delete() {
        if let Some(kv_backend) = build_kv_backend().await {
            let prefix = b"batchDelete/";
            prepare_kv_with_prefix(&kv_backend, prefix.to_vec()).await;
            test_kv_batch_delete_with_prefix(kv_backend, prefix.to_vec()).await;
        }
    }
}
