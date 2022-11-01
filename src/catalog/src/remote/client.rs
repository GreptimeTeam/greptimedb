use std::fmt::Debug;

use async_stream::stream;
use common_telemetry::{debug, info};
use futures_util::io::Cursor;
use futures_util::AsyncReadExt;
use futures_util::StreamExt;
use meta_client::client::MetaClient;
use meta_client::rpc::{DeleteRangeRequest, PutRequest, RangeRequest};
use opendal::ops::{OpDelete, OpList, OpRead, OpWrite};
use opendal::{Accessor, BytesReader};
use snafu::ResultExt;

use crate::error::IoSnafu;
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

#[async_trait::async_trait]
impl<T: ?Sized + Accessor> KvBackend for T {
    fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Error>
    where
        'a: 'b,
    {
        let key = format!("./{}", String::from_utf8_lossy(key));
        Box::pin(stream!({
            let op = OpList::new();
            let mut files = self.list("./", op).await.context(IoSnafu)?;
            while let Some(r) = files.next().await {
                let e = r.context(IoSnafu)?;
                let path = e.path();
                if !path.starts_with(&key) {
                    debug!("Range key: {}, filter file: {}", key, path);
                    continue;
                }
                let op_read: OpRead = OpRead::new(..);
                let mut value_reader: BytesReader =
                    self.read(path, op_read).await.context(IoSnafu)?;
                let mut res = vec![];
                value_reader.read(&mut res).await.context(IoSnafu)?;
                let key = path.strip_prefix("./").unwrap();
                yield Ok(Kv(key.as_bytes().to_vec(), res))
            }
        }))
    }

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Error> {
        let path = String::from_utf8_lossy(key).to_string();
        let op = OpWrite::new(val.len() as u64);
        let val_len = val.len();
        let cursor = Cursor::new(val.to_owned());
        let bytes_written = self
            .write(&path, op, Box::new(cursor))
            .await
            .context(IoSnafu)?;
        debug!(
            "Setting file: {}, content length: {}, bytes written: {}",
            path, val_len, bytes_written
        );
        Ok(())
    }

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Error> {
        let start = format!("./{}", String::from_utf8_lossy(key));
        let end = format!("./{}", String::from_utf8_lossy(end));

        let op_list = OpList::new();
        let mut files = self.list("./", op_list).await.context(IoSnafu)?;
        while let Some(r) = files.next().await {
            let dir = r.context(IoSnafu)?;
            let dir_bytes = dir.path().as_bytes();
            if dir_bytes >= start.as_bytes() && dir_bytes < end.as_bytes() {
                debug!("Deleting file with path: {}", dir.path());
                let op_delete = OpDelete::new();
                self.delete(dir.path(), op_delete).await.context(IoSnafu)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use super::*;
    use crate::remote::KvBackendRef;

    async fn collect_file_names(backend: KvBackendRef, path: impl AsRef<str>) -> HashSet<String> {
        let mut iter = backend.range(path.as_ref().as_bytes());
        let mut res = HashSet::new();
        while let Some(v) = iter.next().await {
            let kv = v.unwrap();
            res.insert(String::from_utf8_lossy(&kv.0).to_string());
        }
        res
    }

    #[tokio::test]
    async fn test_opendal_backend() {
        common_telemetry::init_default_ut_logging();
        let dir = tempdir::TempDir::new("opendal_kv").unwrap();
        let accessor = Arc::new(
            opendal::services::fs::Builder::default()
                .root(dir.path().to_str().unwrap())
                .build()
                .unwrap(),
        );

        let backend: KvBackendRef = Arc::new(crate::remote::OpendalBackend::new(accessor));
        assert_eq!(
            HashSet::new(),
            collect_file_names(backend.clone(), "").await
        );

        backend.set("h".as_bytes(), "h".as_bytes()).await.unwrap();
        assert_eq!(
            vec!["h".to_string()].into_iter().collect::<HashSet<_>>(),
            collect_file_names(backend.clone(), "h").await
        );

        backend.set("he".as_bytes(), "he".as_bytes()).await.unwrap();

        assert_eq!(
            vec!["h".to_string(), "he".to_string()]
                .into_iter()
                .collect::<HashSet<_>>(),
            collect_file_names(backend.clone(), "h").await
        );

        backend
            .set("world".as_bytes(), "world".as_bytes())
            .await
            .unwrap();
        assert_eq!(
            vec!["h".to_string(), "he".to_string()]
                .into_iter()
                .collect::<HashSet<_>>(),
            collect_file_names(backend.clone(), "h").await
        );

        backend
            .delete_range("h".as_bytes(), "he".as_bytes())
            .await
            .unwrap();

        // "he" is not deleted
        assert_eq!(
            vec!["he".to_string()].into_iter().collect::<HashSet<_>>(),
            collect_file_names(backend.clone(), "h").await
        );

        backend
            .set("hello".as_bytes(), "hello".as_bytes())
            .await
            .unwrap();

        assert_eq!(
            vec!["he".to_string(), "hello".to_string()]
                .into_iter()
                .collect::<HashSet<_>>(),
            collect_file_names(backend.clone(), "h").await
        );

        assert_eq!(
            vec!["world".to_string()]
                .into_iter()
                .collect::<HashSet<_>>(),
            collect_file_names(backend.clone(), "w").await
        );
    }
}
