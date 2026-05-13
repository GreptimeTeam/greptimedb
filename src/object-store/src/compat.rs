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

use std::collections::HashMap;
use std::fmt::{self, Debug, Display, Formatter};
use std::future::IntoFuture;
use std::io;
use std::ops::Range;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use datafusion_object_store::path::Path;
use datafusion_object_store::{
    Attribute, Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore as ArrowObjectStore, PutMode, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, UploadPart,
};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use opendal::options::CopyOptions;
use opendal::raw::percent_decode_path;
use opendal::{Buffer, Operator, OperatorInfo, Writer};
use tokio::sync::{Mutex, oneshot};

/// OpendalStore implements ObjectStore trait by using opendal.
///
/// This allows users to use opendal as an object store without extra cost.
///
/// Visit [`opendal::services`] for more information about supported services.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use bytes::Bytes;
/// use object_store::path::Path;
/// use object_store::ObjectStore;
/// use object_store_opendal::OpendalStore;
/// use opendal::services::S3;
/// use opendal::{Builder, Operator};
///
/// #[tokio::main]
/// async fn main() {
///    let builder = S3::default()
///     .access_key_id("my_access_key")
///     .secret_access_key("my_secret_key")
///     .endpoint("my_endpoint")
///     .region("my_region");
///
///     // Create a new operator
///     let operator = Operator::new(builder).unwrap().finish();
///
///     // Create a new object store
///     let object_store = Arc::new(OpendalStore::new(operator));
///
///     let path = Path::from("data/nested/test.txt");
///     let bytes = Bytes::from_static(b"hello, world! I am nested.");
///
///     object_store.put(&path, bytes.clone().into()).await.unwrap();
///
///     let content = object_store
///         .get(&path)
///         .await
///         .unwrap()
///         .bytes()
///         .await
///         .unwrap();
///
///     assert_eq!(content, bytes);
/// }
/// ```
#[derive(Clone)]
pub struct OpendalStore {
    info: Arc<OperatorInfo>,
    inner: Operator,
}

impl OpendalStore {
    /// Create OpendalStore by given Operator.
    pub fn new(op: Operator) -> Self {
        Self {
            info: op.info().into(),
            inner: op,
        }
    }

    /// Get the Operator info.
    pub fn info(&self) -> &OperatorInfo {
        self.info.as_ref()
    }

    /// Copy a file from one location to another.
    async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> datafusion_object_store::Result<()> {
        let mut copy_options = CopyOptions::default();
        if if_not_exists {
            copy_options.if_not_exists = true;
        }

        // Perform the copy operation
        self.inner
            .copy_options(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
                copy_options,
            )
            .await
            .map_err(|err| {
                if if_not_exists && err.kind() == opendal::ErrorKind::AlreadyExists {
                    datafusion_object_store::Error::AlreadyExists {
                        path: to.to_string(),
                        source: Box::new(err),
                    }
                } else {
                    format_object_store_error(err, from.as_ref())
                }
            })?;

        Ok(())
    }
}

impl Debug for OpendalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalStore")
            .field("scheme", &self.info.scheme())
            .field("name", &self.info.name())
            .field("root", &self.info.root())
            .field("capability", &self.info.full_capability())
            .finish()
    }
}

impl Display for OpendalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let info = self.inner.info();
        write!(
            f,
            "Opendal({}, bucket={}, root={})",
            info.scheme(),
            info.name(),
            info.root()
        )
    }
}

impl From<Operator> for OpendalStore {
    fn from(value: Operator) -> Self {
        Self::new(value)
    }
}

#[async_trait]
impl ArrowObjectStore for OpendalStore {
    async fn put_opts(
        &self,
        location: &Path,
        bytes: PutPayload,
        opts: PutOptions,
    ) -> datafusion_object_store::Result<PutResult> {
        let decoded_location = percent_decode_path(location.as_ref());
        let mut future_write = self
            .inner
            .write_with(&decoded_location, Buffer::from_iter(bytes));
        let opts_mode = opts.mode.clone();
        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                future_write = future_write.if_not_exists(true);
            }
            PutMode::Update(update_version) => {
                let Some(etag) = update_version.e_tag else {
                    return Err(datafusion_object_store::Error::NotSupported {
                        source: Box::new(opendal::Error::new(
                            opendal::ErrorKind::Unsupported,
                            "etag is required for conditional put",
                        )),
                    });
                };
                future_write = future_write.if_match(etag.as_str());
            }
        }
        let rp = future_write.await.map_err(|err| {
            match format_object_store_error(err, location.as_ref()) {
                datafusion_object_store::Error::Precondition { path, source }
                    if opts_mode == PutMode::Create =>
                {
                    datafusion_object_store::Error::AlreadyExists { path, source }
                }
                e => e,
            }
        })?;

        let e_tag = rp.etag().map(|s| s.to_string());
        let version = rp.version().map(|s| s.to_string());

        Ok(PutResult { e_tag, version })
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> datafusion_object_store::Result<Box<dyn MultipartUpload>> {
        let decoded_location = percent_decode_path(location.as_ref());
        let writer = self
            .inner
            .writer_with(&decoded_location)
            .concurrent(8)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let upload = OpendalMultipartUpload::new(writer, location.clone());

        Ok(Box::new(upload))
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> datafusion_object_store::Result<Box<dyn MultipartUpload>> {
        const DEFAULT_CONCURRENT: usize = 8;

        let mut options = opendal::options::WriteOptions {
            concurrent: DEFAULT_CONCURRENT,
            ..Default::default()
        };

        let mut user_metadata = HashMap::new();

        for (key, value) in opts.attributes.iter() {
            match key {
                Attribute::CacheControl => {
                    options.cache_control = Some(value.to_string());
                }
                Attribute::ContentDisposition => {
                    options.content_disposition = Some(value.to_string());
                }
                Attribute::ContentEncoding => {
                    options.content_encoding = Some(value.to_string());
                }
                Attribute::ContentLanguage => continue,
                Attribute::ContentType => {
                    options.content_type = Some(value.to_string());
                }
                Attribute::Metadata(k) => {
                    user_metadata.insert(k.to_string(), value.to_string());
                }
                _ => {}
            }
        }

        if !user_metadata.is_empty() {
            options.user_metadata = Some(user_metadata);
        }

        let decoded_location = percent_decode_path(location.as_ref());
        let writer = self
            .inner
            .writer_options(&decoded_location, options)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let upload = OpendalMultipartUpload::new(writer, location.clone());

        Ok(Box::new(upload))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> datafusion_object_store::Result<GetResult> {
        let raw_location = percent_decode_path(location.as_ref());
        let meta = {
            let mut s = self.inner.stat_with(&raw_location);
            if let Some(version) = &options.version {
                s = s.version(version.as_str())
            }
            if let Some(if_match) = &options.if_match {
                s = s.if_match(if_match.as_str());
            }
            if let Some(if_none_match) = &options.if_none_match {
                s = s.if_none_match(if_none_match.as_str());
            }
            if let Some(if_modified_since) =
                options.if_modified_since.and_then(datetime_to_timestamp)
            {
                s = s.if_modified_since(if_modified_since);
            }
            if let Some(if_unmodified_since) =
                options.if_unmodified_since.and_then(datetime_to_timestamp)
            {
                s = s.if_unmodified_since(if_unmodified_since);
            }
            s.await
                .map_err(|err| format_object_store_error(err, location.as_ref()))?
        };

        let mut attributes = Attributes::new();
        if let Some(user_meta) = meta.user_metadata() {
            for (key, value) in user_meta {
                attributes.insert(
                    Attribute::Metadata(key.clone().into()),
                    value.clone().into(),
                );
            }
        }

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: meta
                .last_modified()
                .and_then(timestamp_to_datetime)
                .unwrap_or_default(),
            size: meta.content_length(),
            e_tag: meta.etag().map(|x| x.to_string()),
            version: meta.version().map(|x| x.to_string()),
        };

        if options.head {
            return Ok(GetResult {
                payload: GetResultPayload::Stream(Box::pin(futures::stream::empty())),
                range: 0..0,
                meta,
                attributes,
            });
        }

        let reader = {
            let mut r = self.inner.reader_with(raw_location.as_ref());
            if let Some(version) = options.version {
                r = r.version(version.as_str());
            }
            if let Some(if_match) = options.if_match {
                r = r.if_match(if_match.as_str());
            }
            if let Some(if_none_match) = options.if_none_match {
                r = r.if_none_match(if_none_match.as_str());
            }
            if let Some(if_modified_since) =
                options.if_modified_since.and_then(datetime_to_timestamp)
            {
                r = r.if_modified_since(if_modified_since);
            }
            if let Some(if_unmodified_since) =
                options.if_unmodified_since.and_then(datetime_to_timestamp)
            {
                r = r.if_unmodified_since(if_unmodified_since);
            }
            r.await
                .map_err(|err| format_object_store_error(err, location.as_ref()))?
        };

        let read_range = match options.range {
            Some(GetRange::Bounded(r)) => {
                if r.start >= r.end || r.start >= meta.size {
                    0..0
                } else {
                    let end = r.end.min(meta.size);
                    r.start..end
                }
            }
            Some(GetRange::Offset(r)) => {
                if r < meta.size {
                    r..meta.size
                } else {
                    0..0
                }
            }
            Some(GetRange::Suffix(r)) if r < meta.size => (meta.size - r)..meta.size,
            _ => 0..meta.size,
        };

        let stream = reader
            .into_bytes_stream(read_range.start..read_range.end)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?
            .map_ok(|buf| buf)
            .map_err(|err: io::Error| datafusion_object_store::Error::Generic {
                store: "IoError",
                source: Box::new(err),
            });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: read_range.start..read_range.end,
            meta,
            attributes,
        })
    }

    async fn get_range(
        &self,
        location: &Path,
        range: Range<u64>,
    ) -> datafusion_object_store::Result<Bytes> {
        let raw_location = percent_decode_path(location.as_ref());
        let reader = self
            .inner
            .reader_with(&raw_location)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        reader
            .read(range.start..range.end)
            .await
            .map(|buf| buf.to_bytes())
            .map_err(|err| format_object_store_error(err, location.as_ref()))
    }

    async fn delete(&self, location: &Path) -> datafusion_object_store::Result<()> {
        let decoded_location = percent_decode_path(location.as_ref());
        self.inner
            .delete(&decoded_location)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(())
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, datafusion_object_store::Result<ObjectMeta>> {
        // object_store `Path` always removes trailing slash
        // need to add it back
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });

        let this = self.clone();
        let fut = async move {
            let stream = this
                .inner
                .lister_with(&path)
                .recursive(true)
                .await
                .map_err(|err| format_object_store_error(err, &path))?;

            let stream = stream.then(|res| async {
                let entry = res.map_err(|err| format_object_store_error(err, ""))?;
                let meta = entry.metadata();

                Ok(format_object_meta(entry.path(), meta))
            });
            Ok::<_, datafusion_object_store::Error>(stream)
        };

        fut.into_stream().try_flatten().boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, datafusion_object_store::Result<ObjectMeta>> {
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });
        let offset = offset.clone();

        // clone self for 'static lifetime
        // clone self is cheap
        let this = self.clone();

        let fut = async move {
            let list_with_start_after = this.inner.info().full_capability().list_with_start_after;
            let mut fut = this.inner.lister_with(&path).recursive(true);

            // Use native start_after support if possible.
            if list_with_start_after {
                fut = fut.start_after(offset.as_ref());
            }

            let lister = fut
                .await
                .map_err(|err| format_object_store_error(err, &path))?
                .then(move |entry| {
                    let path = path.clone();
                    let this = this.clone();
                    async move {
                        let entry = entry.map_err(|err| format_object_store_error(err, &path))?;
                        let (path, metadata) = entry.into_parts();

                        // If it's a dir or last_modified is present, we can use it directly.
                        if metadata.is_dir() || metadata.last_modified().is_some() {
                            let object_meta = format_object_meta(&path, &metadata);
                            return Ok(object_meta);
                        }

                        let metadata = this
                            .inner
                            .stat(&path)
                            .await
                            .map_err(|err| format_object_store_error(err, &path))?;
                        let object_meta = format_object_meta(&path, &metadata);
                        Ok::<_, datafusion_object_store::Error>(object_meta)
                    }
                })
                .boxed();

            let stream = if list_with_start_after {
                lister
            } else {
                lister
                    .try_filter(move |entry| futures::future::ready(entry.location > offset))
                    .boxed()
            };

            Ok::<_, datafusion_object_store::Error>(stream)
        };

        fut.into_stream().try_flatten().boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> datafusion_object_store::Result<ListResult> {
        let path = prefix.map_or("".into(), |x| {
            format!("{}/", percent_decode_path(x.as_ref()))
        });
        let mut stream = self
            .inner
            .lister_with(&path)
            .into_future()
            .await
            .map_err(|err| format_object_store_error(err, &path))?;

        let mut common_prefixes = Vec::new();
        let mut objects = Vec::new();

        while let Some(res) = stream.next().await {
            let entry = res.map_err(|err| format_object_store_error(err, ""))?;
            let meta = entry.metadata();

            if meta.is_dir() {
                common_prefixes.push(entry.path().into());
            } else if meta.last_modified().is_some() {
                objects.push(format_object_meta(entry.path(), meta));
            } else {
                let meta = self
                    .inner
                    .stat(entry.path())
                    .await
                    .map_err(|err| format_object_store_error(err, entry.path()))?;
                objects.push(format_object_meta(entry.path(), &meta));
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> datafusion_object_store::Result<()> {
        self.copy_request(from, to, false).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> datafusion_object_store::Result<()> {
        self.inner
            .rename(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
            )
            .await
            .map_err(|err| format_object_store_error(err, from.as_ref()))?;

        Ok(())
    }

    async fn copy_if_not_exists(
        &self,
        from: &Path,
        to: &Path,
    ) -> datafusion_object_store::Result<()> {
        self.copy_request(from, to, true).await
    }
}

/// `MultipartUpload` implementation based on `Writer` in opendal.
///
/// # Notes
///
/// OpenDAL writer can handle concurrent internally we don't generate real `UploadPart` like existing
/// implementation do. Instead, we just write the part and notify the next task to be written.
///
/// The lock here doesn't really involve the write process, it's just for the notify mechanism.
struct OpendalMultipartUpload {
    writer: Arc<Mutex<Writer>>,
    location: Path,
    next_notify: oneshot::Receiver<()>,
}

impl OpendalMultipartUpload {
    fn new(writer: Writer, location: Path) -> Self {
        // an immediately dropped sender for the first part to write without waiting
        let (_, rx) = oneshot::channel();

        Self {
            writer: Arc::new(Mutex::new(writer)),
            location,
            next_notify: rx,
        }
    }
}

#[async_trait]
impl MultipartUpload for OpendalMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let writer = self.writer.clone();
        let location = self.location.clone();

        // Generate next notify which will be notified after the current part is written.
        let (tx, rx) = oneshot::channel();
        // Fetch the notify for current part to wait for it to be written.
        let last_rx = std::mem::replace(&mut self.next_notify, rx);

        async move {
            // Wait for the previous part to be written
            let _ = last_rx.await;

            let mut writer = writer.lock().await;
            let result = writer
                .write(Buffer::from_iter(data))
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()));

            // Notify the next part to be written
            drop(tx);

            result
        }
        .boxed()
    }

    async fn complete(&mut self) -> datafusion_object_store::Result<PutResult> {
        let mut writer = self.writer.lock().await;
        let metadata = writer
            .close()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))?;

        let e_tag = metadata.etag().map(|s| s.to_string());
        let version = metadata.version().map(|s| s.to_string());

        Ok(PutResult { e_tag, version })
    }

    async fn abort(&mut self) -> datafusion_object_store::Result<()> {
        let mut writer = self.writer.lock().await;
        writer
            .abort()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))
    }
}

impl Debug for OpendalMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalMultipartUpload")
            .field("location", &self.location)
            .finish()
    }
}

fn format_object_store_error(err: opendal::Error, path: &str) -> datafusion_object_store::Error {
    match err.kind() {
        opendal::ErrorKind::NotFound => datafusion_object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        },
        opendal::ErrorKind::Unsupported => datafusion_object_store::Error::NotSupported {
            source: Box::new(err),
        },
        opendal::ErrorKind::AlreadyExists => datafusion_object_store::Error::AlreadyExists {
            path: path.to_string(),
            source: Box::new(err),
        },
        opendal::ErrorKind::ConditionNotMatch => datafusion_object_store::Error::Precondition {
            path: path.to_string(),
            source: Box::new(err),
        },
        kind => datafusion_object_store::Error::Generic {
            store: kind.into_static(),
            source: Box::new(err),
        },
    }
}

fn format_object_meta(path: &str, meta: &opendal::Metadata) -> ObjectMeta {
    ObjectMeta {
        location: path.into(),
        last_modified: meta
            .last_modified()
            .and_then(timestamp_to_datetime)
            .unwrap_or_default(),
        size: meta.content_length(),
        e_tag: meta.etag().map(|x| x.to_string()),
        version: meta.version().map(|x| x.to_string()),
    }
}

fn timestamp_to_datetime(ts: opendal::raw::Timestamp) -> Option<chrono::DateTime<chrono::Utc>> {
    let ts = ts.into_inner();
    chrono::DateTime::<chrono::Utc>::from_timestamp(ts.as_second(), ts.subsec_nanosecond() as u32)
}

fn datetime_to_timestamp(dt: chrono::DateTime<chrono::Utc>) -> Option<opendal::raw::Timestamp> {
    opendal::raw::Timestamp::new(dt.timestamp(), dt.timestamp_subsec_nanos() as i32).ok()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use datafusion_object_store::path::Path;
    use datafusion_object_store::{ObjectStore as ArrowObjectStore, WriteMultipart};
    use opendal::{Operator, services};
    use rand::{Rng, RngCore};

    use super::*;

    async fn create_test_object_store() -> Arc<dyn ArrowObjectStore> {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let object_store = Arc::new(OpendalStore::new(op));

        let path: Path = "data/test.txt".into();
        let bytes = Bytes::from_static(b"hello, world!");
        object_store.put(&path, bytes.into()).await.unwrap();

        let path: Path = "data/nested/test.txt".into();
        let bytes = Bytes::from_static(b"hello, world! I am nested.");
        object_store.put(&path, bytes.into()).await.unwrap();

        object_store
    }

    #[tokio::test]
    async fn test_basic() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let object_store: Arc<dyn ArrowObjectStore> = Arc::new(OpendalStore::new(op));

        // Retrieve a specific file
        let path: Path = "data/test.txt".into();

        let bytes = Bytes::from_static(b"hello, world!");
        object_store.put(&path, bytes.clone().into()).await.unwrap();

        let meta = object_store.head(&path).await.unwrap();

        assert_eq!(meta.size, 13);

        assert_eq!(
            object_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            bytes
        );
    }

    #[tokio::test]
    async fn test_put_multipart() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let object_store: Arc<dyn ArrowObjectStore> = Arc::new(OpendalStore::new(op));

        let mut rng = rand::rng();

        // Case complete
        let path: Path = "data/test_complete.txt".into();
        let upload = object_store.put_multipart(&path).await.unwrap();

        let mut write = WriteMultipart::new(upload);

        let mut all_bytes = vec![];
        let round = rng.random_range(1..=1024);
        for _ in 0..round {
            let size = rng.random_range(1..=1024);
            let mut bytes = vec![0; size];
            rng.fill_bytes(&mut bytes);

            all_bytes.extend_from_slice(&bytes);
            write.put(bytes.into());
        }

        let _ = write.finish().await.unwrap();

        let meta = object_store.head(&path).await.unwrap();

        assert_eq!(meta.size, all_bytes.len() as u64);

        assert_eq!(
            object_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from(all_bytes)
        );

        // Case abort
        let path: Path = "data/test_abort.txt".into();
        let mut upload = object_store.put_multipart(&path).await.unwrap();
        upload.put_part(vec![1; 1024].into()).await.unwrap();
        upload.abort().await.unwrap();

        let res = object_store.head(&path).await;
        let err = res.unwrap_err();

        assert!(matches!(
            err,
            datafusion_object_store::Error::NotFound { .. }
        ))
    }

    #[tokio::test]
    async fn test_list() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let results = object_store.list(Some(&path)).collect::<Vec<_>>().await;
        assert_eq!(results.len(), 2);
        let mut locations = results
            .iter()
            .map(|x| x.as_ref().unwrap().location.as_ref())
            .collect::<Vec<_>>();

        let expected_files = vec![
            (
                "data/nested/test.txt",
                Bytes::from_static(b"hello, world! I am nested."),
            ),
            ("data/test.txt", Bytes::from_static(b"hello, world!")),
        ];

        let expected_locations = expected_files.iter().map(|x| x.0).collect::<Vec<&str>>();

        locations.sort();
        assert_eq!(locations, expected_locations);

        for (location, bytes) in expected_files {
            let path: Path = location.into();
            assert_eq!(
                object_store
                    .get(&path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                bytes
            );
        }
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let result = object_store.list_with_delimiter(Some(&path)).await.unwrap();
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.common_prefixes.len(), 1);
        assert_eq!(result.objects[0].location.as_ref(), "data/test.txt");
        assert_eq!(result.common_prefixes[0].as_ref(), "data/nested");
    }

    #[tokio::test]
    async fn test_list_with_offset() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let offset: Path = "data/nested/test.txt".into();
        let result = object_store
            .list_with_offset(Some(&path), &offset)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].as_ref().unwrap().location.as_ref(),
            "data/test.txt"
        );
    }

    mod stat_counter {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use super::*;

        #[derive(Debug, Clone)]
        pub struct StatCounterLayer {
            count: Arc<AtomicUsize>,
        }

        impl StatCounterLayer {
            pub fn new(count: Arc<AtomicUsize>) -> Self {
                Self { count }
            }
        }

        impl<A: opendal::raw::Access> opendal::raw::Layer<A> for StatCounterLayer {
            type LayeredAccess = StatCounterAccessor<A>;

            fn layer(&self, inner: A) -> Self::LayeredAccess {
                StatCounterAccessor {
                    inner,
                    count: self.count.clone(),
                }
            }
        }

        #[derive(Debug, Clone)]
        pub struct StatCounterAccessor<A> {
            inner: A,
            count: Arc<AtomicUsize>,
        }

        impl<A: opendal::raw::Access> opendal::raw::LayeredAccess for StatCounterAccessor<A> {
            type Inner = A;
            type Reader = A::Reader;
            type Writer = A::Writer;
            type Lister = A::Lister;
            type Deleter = A::Deleter;

            fn inner(&self) -> &Self::Inner {
                &self.inner
            }

            async fn stat(
                &self,
                path: &str,
                args: opendal::raw::OpStat,
            ) -> opendal::Result<opendal::raw::RpStat> {
                self.count.fetch_add(1, Ordering::SeqCst);
                self.inner.stat(path, args).await
            }

            async fn read(
                &self,
                path: &str,
                args: opendal::raw::OpRead,
            ) -> opendal::Result<(opendal::raw::RpRead, Self::Reader)> {
                self.inner.read(path, args).await
            }

            async fn write(
                &self,
                path: &str,
                args: opendal::raw::OpWrite,
            ) -> opendal::Result<(opendal::raw::RpWrite, Self::Writer)> {
                self.inner.write(path, args).await
            }

            async fn delete(&self) -> opendal::Result<(opendal::raw::RpDelete, Self::Deleter)> {
                self.inner.delete().await
            }

            async fn list(
                &self,
                path: &str,
                args: opendal::raw::OpList,
            ) -> opendal::Result<(opendal::raw::RpList, Self::Lister)> {
                self.inner.list(path, args).await
            }

            async fn copy(
                &self,
                from: &str,
                to: &str,
                args: opendal::raw::OpCopy,
            ) -> opendal::Result<opendal::raw::RpCopy> {
                self.inner.copy(from, to, args).await
            }

            async fn rename(
                &self,
                from: &str,
                to: &str,
                args: opendal::raw::OpRename,
            ) -> opendal::Result<opendal::raw::RpRename> {
                self.inner.rename(from, to, args).await
            }
        }
    }

    #[tokio::test]
    async fn test_get_range_no_stat() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // Create a stat counter and operator with tracking layer
        let stat_count = Arc::new(AtomicUsize::new(0));
        let op = Operator::new(opendal::services::Memory::default())
            .unwrap()
            .layer(stat_counter::StatCounterLayer::new(stat_count.clone()))
            .finish();
        let store = OpendalStore::new(op);

        // Create a test file
        let location = "test_get_range.txt".into();
        let value = Bytes::from_static(b"Hello, world!");
        store.put(&location, value.clone().into()).await.unwrap();

        // Reset counter after put
        stat_count.store(0, Ordering::SeqCst);

        // Test 1: get_range should NOT call stat()
        let ret = store.get_range(&location, 0..5).await.unwrap();
        assert_eq!(Bytes::from_static(b"Hello"), ret);
        assert_eq!(
            stat_count.load(Ordering::SeqCst),
            0,
            "get_range should not call stat()"
        );

        // Reset counter
        stat_count.store(0, Ordering::SeqCst);

        // Test 2: get_opts SHOULD call stat() to get metadata
        let opts = datafusion_object_store::GetOptions {
            range: Some(datafusion_object_store::GetRange::Bounded(0..5)),
            ..Default::default()
        };
        let ret = store.get_opts(&location, opts).await.unwrap();
        let data = ret.bytes().await.unwrap();
        assert_eq!(Bytes::from_static(b"Hello"), data);
        assert!(
            stat_count.load(Ordering::SeqCst) > 0,
            "get_opts should call stat() to get metadata"
        );

        // Cleanup
        store.delete(&location).await.unwrap();
    }
}
