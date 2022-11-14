use std::sync::Arc;

use api::v1::meta::BatchPutRequest;
use api::v1::meta::BatchPutResponse;
use api::v1::meta::CompareAndPutRequest;
use api::v1::meta::CompareAndPutResponse;
use api::v1::meta::DeleteRangeRequest;
use api::v1::meta::DeleteRangeResponse;
use api::v1::meta::KeyValue;
use api::v1::meta::PutRequest;
use api::v1::meta::PutResponse;
use api::v1::meta::RangeRequest;
use api::v1::meta::RangeResponse;
use api::v1::meta::ResponseHeader;
use common_error::prelude::*;
use etcd_client::Client;
use etcd_client::Compare;
use etcd_client::CompareOp;
use etcd_client::DeleteOptions;
use etcd_client::GetOptions;
use etcd_client::PutOptions;
use etcd_client::Txn;
use etcd_client::TxnOp;
use etcd_client::TxnOpResponse;

use crate::error;
use crate::error::Result;
use crate::service::store::kv::KvStore;
use crate::service::store::kv::KvStoreRef;

#[derive(Clone)]
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

        let kvs = res
            .kvs()
            .iter()
            .map(|kv| KvPair::new(kv).into())
            .collect::<Vec<_>>();

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

        let prev_kv = res.prev_key().map(|kv| KvPair::new(kv).into());

        let header = Some(ResponseHeader::success(cluster_id));
        Ok(PutResponse { header, prev_kv })
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
                        prev_kvs.push(KvPair::new(prev_kv).into());
                    }
                }
                _ => unreachable!(), // never get here
            }
        }

        let header = Some(ResponseHeader::success(cluster_id));
        Ok(BatchPutResponse { header, prev_kvs })
    }

    async fn compare_and_put(&self, req: CompareAndPutRequest) -> Result<CompareAndPutResponse> {
        let CompareAndPut {
            cluster_id,
            key,
            expect,
            value,
            options,
        } = req.try_into()?;

        let put_op = vec![TxnOp::put(key.clone(), value, options)];
        let get_op = vec![TxnOp::get(key.clone(), None)];
        let mut txn = if expect.is_empty() {
            // create if absent
            // revision 0 means key was not exist
            Txn::new().when(vec![Compare::create_revision(key, CompareOp::Equal, 0)])
        } else {
            // compare and put
            Txn::new().when(vec![Compare::value(key, CompareOp::Equal, expect)])
        };
        txn = txn.and_then(put_op).or_else(get_op);

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
            TxnOpResponse::Put(put_res) => {
                put_res.prev_key().map(|kv| KeyValue::from(KvPair::new(kv)))
            }
            TxnOpResponse::Get(get_res) => {
                if get_res.count() == 0 {
                    // do not exists
                    None
                } else {
                    ensure!(
                        get_res.count() == 1,
                        error::InvalidTxnResultSnafu {
                            err_msg: format!("expect 1 response, actual {}", get_res.count())
                        }
                    );
                    Some(KeyValue::from(KvPair::new(&get_res.kvs()[0])))
                }
            }
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

        let prev_kvs = res
            .prev_kvs()
            .iter()
            .map(|kv| KvPair::new(kv).into())
            .collect::<Vec<_>>();

        let header = Some(ResponseHeader::success(cluster_id));
        Ok(DeleteRangeResponse {
            header,
            deleted: res.deleted(),
            prev_kvs,
        })
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

struct CompareAndPut {
    cluster_id: u64,
    key: Vec<u8>,
    expect: Vec<u8>,
    value: Vec<u8>,
    options: Option<PutOptions>,
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
            options: Some(PutOptions::default().with_prev_key()),
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

struct KvPair<'a>(&'a etcd_client::KeyValue);

impl<'a> KvPair<'a> {
    /// Creates a `KvPair` from etcd KeyValue
    #[inline]
    fn new(kv: &'a etcd_client::KeyValue) -> Self {
        Self(kv)
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
        assert!(compare_and_put.options.is_some());
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
}
