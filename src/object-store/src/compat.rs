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
use std::io;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use object_store_012::path::Path;
use object_store_012::{
    Attribute, Attributes, GetOptions, GetRange, GetResult, GetResultPayload, ListResult,
    MultipartUpload, ObjectMeta, ObjectStore as ArrowObjectStore, PutMode, PutMultipartOptions,
    PutPayload, PutResult, UploadPart,
};
use opendal::options::{CopyOptions, WriteOptions};
use opendal::raw::percent_decode_path;
use opendal::{Buffer, Operator, Writer};
use tokio::sync::{Mutex, oneshot};

const DEFAULT_CONCURRENT: usize = 8;

/// Adapter from Greptime's OpenDAL operator to `object_store` 0.12 used by DataFusion.
#[derive(Clone)]
pub struct OpendalStore {
    inner: Operator,
}

impl OpendalStore {
    pub fn new(op: Operator) -> Self {
        Self { inner: op }
    }

    async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> object_store_012::Result<()> {
        let options = CopyOptions { if_not_exists };

        self.inner
            .copy_options(
                &percent_decode_path(from.as_ref()),
                &percent_decode_path(to.as_ref()),
                options,
            )
            .await
            .map_err(|err| {
                if if_not_exists && err.kind() == opendal::ErrorKind::AlreadyExists {
                    object_store_012::Error::AlreadyExists {
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
        let info = self.inner.info();
        f.debug_struct("OpendalStore")
            .field("scheme", &info.scheme())
            .field("name", &info.name())
            .field("root", &info.root())
            .field("capability", &info.full_capability())
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

#[async_trait::async_trait]
impl ArrowObjectStore for OpendalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: object_store_012::PutOptions,
    ) -> object_store_012::Result<PutResult> {
        let decoded_location = percent_decode_path(location.as_ref());
        let mut future_write = self
            .inner
            .write_with(&decoded_location, Buffer::from_iter(payload));
        let opts_mode = opts.mode.clone();

        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                future_write = future_write.if_not_exists(true);
            }
            PutMode::Update(update_version) => {
                let Some(etag) = update_version.e_tag else {
                    return Err(object_store_012::Error::NotSupported {
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
                object_store_012::Error::Precondition { path, source }
                    if opts_mode == PutMode::Create =>
                {
                    object_store_012::Error::AlreadyExists { path, source }
                }
                err => err,
            }
        })?;

        Ok(PutResult {
            e_tag: rp.etag().map(|s| s.to_string()),
            version: rp.version().map(|s| s.to_string()),
        })
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store_012::Result<Box<dyn MultipartUpload>> {
        let mut options = WriteOptions {
            concurrent: DEFAULT_CONCURRENT,
            ..Default::default()
        };

        let mut user_metadata = HashMap::new();
        for (key, value) in opts.attributes.iter() {
            match key {
                Attribute::CacheControl => options.cache_control = Some(value.to_string()),
                Attribute::ContentDisposition => {
                    options.content_disposition = Some(value.to_string())
                }
                Attribute::ContentEncoding => options.content_encoding = Some(value.to_string()),
                Attribute::ContentLanguage => {}
                Attribute::ContentType => options.content_type = Some(value.to_string()),
                Attribute::Metadata(key) => {
                    user_metadata.insert(key.to_string(), value.to_string());
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

        Ok(Box::new(OpendalMultipartUpload::new(
            writer,
            location.clone(),
        )))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store_012::Result<GetResult> {
        let raw_location = percent_decode_path(location.as_ref());
        let meta = {
            let mut stat = self.inner.stat_with(&raw_location);
            if let Some(version) = &options.version {
                stat = stat.version(version.as_str());
            }
            if let Some(if_match) = &options.if_match {
                stat = stat.if_match(if_match.as_str());
            }
            if let Some(if_none_match) = &options.if_none_match {
                stat = stat.if_none_match(if_none_match.as_str());
            }
            if let Some(if_modified_since) =
                options.if_modified_since.and_then(datetime_to_timestamp)
            {
                stat = stat.if_modified_since(if_modified_since);
            }
            if let Some(if_unmodified_since) =
                options.if_unmodified_since.and_then(datetime_to_timestamp)
            {
                stat = stat.if_unmodified_since(if_unmodified_since);
            }
            stat.await
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
                meta,
                range: 0..0,
                attributes,
            });
        }

        let reader = {
            let mut read = self.inner.reader_with(&raw_location);
            if let Some(version) = options.version {
                read = read.version(version.as_str());
            }
            if let Some(if_match) = options.if_match {
                read = read.if_match(if_match.as_str());
            }
            if let Some(if_none_match) = options.if_none_match {
                read = read.if_none_match(if_none_match.as_str());
            }
            if let Some(if_modified_since) =
                options.if_modified_since.and_then(datetime_to_timestamp)
            {
                read = read.if_modified_since(if_modified_since);
            }
            if let Some(if_unmodified_since) =
                options.if_unmodified_since.and_then(datetime_to_timestamp)
            {
                read = read.if_unmodified_since(if_unmodified_since);
            }
            read.await
                .map_err(|err| format_object_store_error(err, location.as_ref()))?
        };

        let read_range = match options.range {
            Some(GetRange::Bounded(range)) => {
                if range.start >= range.end || range.start >= meta.size {
                    0..0
                } else {
                    let end = range.end.min(meta.size);
                    range.start..end
                }
            }
            Some(GetRange::Offset(offset)) => {
                if offset < meta.size {
                    offset..meta.size
                } else {
                    0..0
                }
            }
            Some(GetRange::Suffix(length)) if length < meta.size => (meta.size - length)..meta.size,
            _ => 0..meta.size,
        };

        let stream = reader
            .into_bytes_stream(read_range.clone())
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?
            .map_ok(|buf| buf)
            .map_err(|err: io::Error| object_store_012::Error::Generic {
                store: "IoError",
                source: Box::new(err),
            });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            meta,
            range: read_range,
            attributes,
        })
    }

    async fn delete(&self, location: &Path) -> object_store_012::Result<()> {
        self.inner
            .delete(&percent_decode_path(location.as_ref()))
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store_012::Result<ObjectMeta>> {
        let path = prefix.map_or_else(String::new, |prefix| {
            format!("{}/", percent_decode_path(prefix.as_ref()))
        });

        let this = self.clone();
        let fut = async move {
            let stream = this
                .inner
                .lister_with(&path)
                .recursive(true)
                .await
                .map_err(|err| format_object_store_error(err, &path))?;

            Ok::<_, object_store_012::Error>(stream.then(|res| async {
                let entry = res.map_err(|err| format_object_store_error(err, ""))?;
                Ok(format_object_meta(entry.path(), entry.metadata()))
            }))
        };

        fut.into_stream().try_flatten().boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store_012::Result<ListResult> {
        let path = prefix.map_or_else(String::new, |prefix| {
            format!("{}/", percent_decode_path(prefix.as_ref()))
        });
        let mut stream = self
            .inner
            .lister_with(&path)
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

    async fn copy(&self, from: &Path, to: &Path) -> object_store_012::Result<()> {
        self.copy_request(from, to, false).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store_012::Result<()> {
        self.copy_request(from, to, true).await
    }
}

struct OpendalMultipartUpload {
    writer: Arc<Mutex<Writer>>,
    location: Path,
    next_notify: Option<oneshot::Receiver<()>>,
}

impl OpendalMultipartUpload {
    fn new(writer: Writer, location: Path) -> Self {
        Self {
            writer: Arc::new(Mutex::new(writer)),
            location,
            next_notify: None,
        }
    }
}

#[async_trait::async_trait]
impl MultipartUpload for OpendalMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let writer = self.writer.clone();
        let location = self.location.clone();
        let (tx, rx) = oneshot::channel();
        let last_rx = self.next_notify.replace(rx);

        async move {
            if let Some(last_rx) = last_rx {
                let _ = last_rx.await;
            }

            let mut writer = writer.lock().await;
            let result = writer
                .write(Buffer::from_iter(data))
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()));

            let _ = tx.send(());
            result
        }
        .boxed()
    }

    async fn complete(&mut self) -> object_store_012::Result<PutResult> {
        let mut writer = self.writer.lock().await;
        let metadata = writer
            .close()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))?;

        Ok(PutResult {
            e_tag: metadata.etag().map(|s| s.to_string()),
            version: metadata.version().map(|s| s.to_string()),
        })
    }

    async fn abort(&mut self) -> object_store_012::Result<()> {
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

fn format_object_store_error(err: opendal::Error, path: &str) -> object_store_012::Error {
    match err.kind() {
        opendal::ErrorKind::NotFound => object_store_012::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        },
        opendal::ErrorKind::Unsupported => object_store_012::Error::NotSupported {
            source: Box::new(err),
        },
        opendal::ErrorKind::AlreadyExists => object_store_012::Error::AlreadyExists {
            path: path.to_string(),
            source: Box::new(err),
        },
        opendal::ErrorKind::ConditionNotMatch => object_store_012::Error::Precondition {
            path: path.to_string(),
            source: Box::new(err),
        },
        kind => object_store_012::Error::Generic {
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
