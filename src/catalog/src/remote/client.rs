use std::fmt::Debug;

use async_stream::stream;
use common_telemetry::info;
use meta_client::client::MetaClient;
use meta_client::rpc::{DeleteRangeRequest, PutRequest, RangeRequest};
use snafu::ResultExt;

use crate::error::{Error, MetaSrvSnafu};
use crate::remote::{Kv, KvBackend, ValueIter};

#[derive(Debug)]
pub struct MetaKvBackend {
    pub client: MetaClient,
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
        let mut start_key = key.to_vec();

        Box::pin(stream!({
            let mut more = true;
            while more {
                let mut resp = self
                    .client
                    .range(RangeRequest::new().with_range(start_key.clone(), vec![0]))
                    .await
                    .context(MetaSrvSnafu)?;

                more = resp.more();
                let kvs = resp.take_kvs();
                start_key = kvs
                    .last()
                    .map(|kv| kv.key().to_vec())
                    .unwrap_or(start_key)
                    .clone();
                for mut kv in kvs.into_iter() {
                    yield Ok(Kv(kv.take_key(), kv.take_value()))
                }
            }
        }))
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
}
