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
                    .range(RangeRequest::new().with_prefix(start_key.clone()))
                    .await
                    .context(MetaSrvSnafu)?;

                more = resp.more();
                let kvs = resp.take_kvs();
                // advance range start key
                start_key = match kvs
                    .last()
                    .map(|kv| meta_client::util::get_prefix_end_key(kv.key()))
                {
                    Some(key) => key,
                    None => {
                        // kvs is empty means end of range, regardless of resp.more()
                        return;
                    }
                };

                for mut kv in kvs.into_iter() {
                    yield Ok(Kv(kv.take_key(), kv.take_value()))
                }
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
}
