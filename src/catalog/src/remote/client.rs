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

use std::fmt::Debug;
use std::sync::Arc;

use async_stream::stream;
use common_meta::rpc::store::{CompareAndPutRequest, DeleteRangeRequest, PutRequest, RangeRequest};
use common_telemetry::info;
use meta_client::client::MetaClient;
use snafu::ResultExt;

use crate::error::{Error, MetaSrvSnafu};
use crate::remote::{Kv, KvBackend, ValueIter};
#[derive(Debug)]
pub struct MetaKvBackend {
    pub client: Arc<MetaClient>,
}

/// Implement `KvBackend` trait for `MetaKvBackend` instead of opendal's `Accessor` since
/// `MetaClient`'s range method can return both keys and values, which can reduce IO overhead
/// comparing to `Accessor`'s list and get method.
#[async_trait::async_trait]
impl KvBackend for MetaKvBackend {
    fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Error>
    where
        'a: 'b,
    {
        let key = key.to_vec();
        Box::pin(stream!({
            let mut resp = self
                .client
                .range(RangeRequest::new().with_prefix(key))
                .await
                .context(MetaSrvSnafu)?;
            let kvs = resp.take_kvs();
            for mut kv in kvs.into_iter() {
                yield Ok(Kv(kv.take_key(), kv.take_value()))
            }
        }))
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Kv>, Error> {
        let mut response = self
            .client
            .range(RangeRequest::new().with_key(key))
            .await
            .context(MetaSrvSnafu)?;
        Ok(response
            .take_kvs()
            .get_mut(0)
            .map(|kv| Kv(kv.take_key(), kv.take_value())))
    }

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Error> {
        let req = PutRequest::new()
            .with_key(key.to_vec())
            .with_value(val.to_vec());
        let _ = self.client.put(req).await.context(MetaSrvSnafu)?;
        Ok(())
    }

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Error> {
        let req = DeleteRangeRequest::new().with_range(key.to_vec(), end.to_vec());
        let resp = self.client.delete_range(req).await.context(MetaSrvSnafu)?;
        info!(
            "Delete range, key: {}, end: {}, deleted: {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(end),
            resp.deleted()
        );

        Ok(())
    }

    async fn compare_and_set(
        &self,
        key: &[u8],
        expect: &[u8],
        val: &[u8],
    ) -> Result<Result<(), Option<Vec<u8>>>, Error> {
        let request = CompareAndPutRequest::new()
            .with_key(key.to_vec())
            .with_expect(expect.to_vec())
            .with_value(val.to_vec());
        let mut response = self
            .client
            .compare_and_put(request)
            .await
            .context(MetaSrvSnafu)?;
        if response.is_success() {
            Ok(Ok(()))
        } else {
            Ok(Err(response.take_prev_kv().map(|v| v.value().to_vec())))
        }
    }
}
