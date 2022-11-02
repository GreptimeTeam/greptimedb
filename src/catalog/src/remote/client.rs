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
                    .range(RangeRequest::new().with_prefix(start_key))
                    .await
                    .context(MetaSrvSnafu)?;

                more = resp.more();
                let kvs = resp.take_kvs();
                // advance range start key
                start_key = match kvs.last().map(|kv| next_range_start(kv.key())) {
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

/// Create next range start key from last key in current range response.
/// For example, for keys: `["1", "11", "123", "1234", "13", "2"]`, if current range response contains
/// `["1", "11"]`, then next range start will be `11\0`, and expected to return `["123", "1234", "13"]`
/// but not `2`.
fn next_range_start(last: &[u8]) -> Vec<u8> {
    let mut res = Vec::with_capacity(last.len() + 1);
    res.extend(last);
    res.push(0);
    res
}

#[cfg(test)]
mod tests {
    use crate::remote::client::next_range_start;

    fn filter<'a>(keys: &'a [&str], prefix: &'a [u8], range_start: &'a [u8]) -> Vec<&'a str> {
        let mut res = vec![];
        for key in keys {
            if key.as_bytes() >= range_start && key.as_bytes().starts_with(prefix) {
                res.push(*key)
            }
        }
        res
    }

    #[test]
    fn test_next_prefix() {
        let mut keys = vec!["1", "11", "123", "1234", "13", "2"];
        keys.sort();

        assert_eq!(
            vec!["11", "123", "1234", "13"],
            filter(&keys, b"1", next_range_start(b"1").as_slice())
        );

        assert_eq!(
            vec!["123", "1234", "13"],
            filter(&keys, b"1", next_range_start(b"11").as_slice())
        );

        assert_eq!(
            vec!["1234", "13"],
            filter(&keys, b"1", next_range_start(b"123").as_slice())
        );

        assert_eq!(
            vec!["123", "1234", "13"],
            filter(&keys, b"1", next_range_start(b"12").as_slice())
        );
    }
}
