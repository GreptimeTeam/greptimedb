use std::fmt::Debug;

use async_stream::stream;
use common_telemetry::debug;
use futures_util::AsyncReadExt;
use futures_util::AsyncWriteExt;
use futures_util::StreamExt;
use opendal::ops::{OpDelete, OpList, OpRead, OpWrite};
use opendal::{Accessor, BytesReader};
use snafu::ResultExt;

use crate::error::IoSnafu;
use crate::remote::{Kv, KvBackend, ValueIter};
use crate::Error;

#[derive(Debug)]
pub struct MetaKvBackend {}

#[async_trait::async_trait]
impl KvBackend for MetaKvBackend {
    fn range<'a, 'b>(&'a self, key: &[u8]) -> ValueIter<'b, Error>
    where
        'a: 'b,
    {
        let _ = key;
        todo!()
    }

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Error> {
        let _ = key;
        let _ = val;
        todo!()
    }

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Error> {
        let _ = key;
        let _ = end;
        todo!()
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
            let op = OpList::new("./").context(IoSnafu)?;
            let mut files = self.list(&op).await.context(IoSnafu)?;
            while let Some(r) = files.next().await {
                let e = r.context(IoSnafu)?;
                let path = e.path();
                if !path.starts_with(&key) {
                    debug!("Filter file:{}", path);
                    continue;
                }
                let op_read: OpRead = OpRead::new(path, ..).context(IoSnafu)?;
                let mut value_reader: BytesReader = self.read(&op_read).await.context(IoSnafu)?;
                let mut res = vec![];
                value_reader.read(&mut res).await.context(IoSnafu)?;
                let key = path.strip_prefix("./").unwrap();
                yield Ok(Kv(key.as_bytes().to_vec(), res))
            }
        }))
    }

    async fn set(&self, key: &[u8], val: &[u8]) -> Result<(), Error> {
        let path = String::from_utf8_lossy(key).to_string();
        let op = OpWrite::new(&path, val.len() as u64).context(IoSnafu)?;
        let mut writer = self.write(&op).await.context(IoSnafu)?;
        debug!("Setting file: {}, content length: {}", path, val.len());
        writer.write_all(val).await.context(IoSnafu)
    }

    async fn delete_range(&self, key: &[u8], end: &[u8]) -> Result<(), Error> {
        let start = format!("./{}", String::from_utf8_lossy(key));
        let end = format!("./{}", String::from_utf8_lossy(end));

        let op_list = OpList::new("./").context(IoSnafu)?;
        let mut files = self.list(&op_list).await.context(IoSnafu)?;
        while let Some(r) = files.next().await {
            let dir = r.context(IoSnafu)?;
            let dir_bytes = dir.path().as_bytes();
            if dir_bytes >= start.as_bytes() && dir_bytes < end.as_bytes() {
                debug!("Deleting file with path: {}", dir.path());
                let op_delete = OpDelete::new(dir.path()).context(IoSnafu)?;
                self.delete(&op_delete).await.context(IoSnafu)?;
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
        let accessor = opendal::services::fs::Builder::default()
            .root(dir.path().to_str().unwrap())
            .finish()
            .await
            .unwrap();

        let backend: KvBackendRef = Arc::new(crate::remote::OpendalBackend { accessor });
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
