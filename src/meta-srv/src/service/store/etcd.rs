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

        let res = self
            .client
            .clone()
            .get(key, Some(options))
            .await
            .context(error::EtcdFailedSnafu)?;

        let kvs = res
            .kvs()
            .iter()
            .map(|kv| KeyValue {
                key: kv.key().to_vec(),
                value: kv.value().to_vec(),
            })
            .collect::<Vec<_>>();

        Ok(RangeResponse {
            kvs,
            more: res.more(),
            ..Default::default()
        })
    }

    async fn put(&self, req: PutRequest) -> Result<PutResponse> {
        let PutRequest {
            key,
            value,
            prev_kv,
            ..
        } = req;

        ensure!(!key.is_empty(), error::EmptyKeySnafu);

        let mut options = PutOptions::default();
        if prev_kv {
            options = options.with_prev_key();
        }

        let res = self
            .client
            .clone()
            .put(key, value, Some(options))
            .await
            .context(error::EtcdFailedSnafu)?;

        let prev_kv = res.prev_key().map(|kv| KeyValue {
            key: kv.key().to_vec(),
            value: kv.value().to_vec(),
        });

        Ok(PutResponse {
            prev_kv,
            ..Default::default()
        })
    }

    async fn delete_range(&self, req: DeleteRangeRequest) -> Result<DeleteRangeResponse> {
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

        let res = self
            .client
            .clone()
            .delete(key, Some(options))
            .await
            .context(error::EtcdFailedSnafu)?;

        let prev_kvs = res
            .prev_kvs()
            .iter()
            .map(|kv| KeyValue {
                key: kv.key().to_vec(),
                value: kv.value().to_vec(),
            })
            .collect::<Vec<_>>();

        Ok(DeleteRangeResponse {
            deleted: res.deleted(),
            prev_kvs,
            ..Default::default()
        })
    }
}
