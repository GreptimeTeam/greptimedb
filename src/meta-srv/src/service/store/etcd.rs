use std::sync::Arc;

use api::v1::meta::DeleteRangeRequest;
use api::v1::meta::DeleteRangeResponse;
use api::v1::meta::KeyValue;
use api::v1::meta::PutRequest;
use api::v1::meta::PutResponse;
use api::v1::meta::RangeRequest;
use api::v1::meta::RangeResponse;
use common_error::prelude::*;
use etcd_client::Client;
use etcd_client::DeleteOptions;
use etcd_client::GetOptions;
use etcd_client::PutOptions;

use super::kv::KvStore;
use super::kv::KvStoreRef;
use crate::error;
use crate::error::Result;

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
            .map(|kv| KvPair::new(kv).into())
            .collect::<Vec<_>>();

        Ok(RangeResponse {
            kvs,
            more: res.more(),
            ..Default::default()
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

        let prev_kv = res.prev_key().map(|kv| KvPair::new(kv).into());

        Ok(PutResponse {
            prev_kv,
            ..Default::default()
        })
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
            .map(|kv| KvPair::new(kv).into())
            .collect::<Vec<_>>();

        Ok(DeleteRangeResponse {
            deleted: res.deleted(),
            prev_kvs,
            ..Default::default()
        })
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
            ..
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
            ..
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
            ..
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

struct KvPair<'a>(&'a etcd_client::KeyValue);

impl<'a> KvPair<'a> {
    /// Creates a `KvPair` from etcd KeyValue
    #[inline]
    const fn new(kv: &'a etcd_client::KeyValue) -> Self {
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
