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

use std::fmt::{self, Debug};
use std::sync::Arc;

use hdfs_native::{Client, ClientBuilder};
#[cfg(any(test, feature = "testing"))]
use opendal::raw::OpRename;
use opendal::raw::oio::{Delete as _, Read as _, Write as _};
use opendal::raw::{
    Access, Layer, LayeredAccess, OpCopier, OpCopy, OpDelete, OpList, OpRead, OpWrite, RpCopy,
    RpDelete, RpList, RpRead, RpWrite, oio,
};
use opendal::{Buffer, ErrorKind, Metadata, Result};
use uuid::Uuid;

/// Adds atomic writes and streaming copies to the native HDFS backend.
#[derive(Debug, Clone)]
pub struct HdfsCompatibilityLayer {
    renamer: AtomicRenamer,
}

impl HdfsCompatibilityLayer {
    /// Creates a compatibility layer for the HDFS connection.
    pub fn new(
        name_node: &str,
        root: &str,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<Self> {
        let mut config = std::collections::HashMap::new();
        let namenodes = name_node
            .split(',')
            .filter_map(|value| {
                let value = value
                    .trim()
                    .trim_start_matches("hdfs://")
                    .trim_end_matches('/');
                (!value.is_empty()).then_some(value)
            })
            .collect::<Vec<_>>();
        for (index, namenode) in namenodes.iter().enumerate() {
            config.insert(
                format!("dfs.namenode.rpc-address.nameservice.nn{index}"),
                (*namenode).to_string(),
            );
        }
        config.insert(
            "dfs.ha.namenodes.nameservice".to_string(),
            (0..namenodes.len())
                .map(|index| format!("nn{index}"))
                .collect::<Vec<_>>()
                .join(","),
        );
        config.extend(options.clone());
        let client = ClientBuilder::new()
            .with_url("hdfs://nameservice")
            .with_config(config)
            .build()
            .map_err(hdfs_error)?;
        Ok(Self {
            renamer: AtomicRenamer::Native {
                client,
                root: opendal::raw::normalize_root(root),
            },
        })
    }

    /// Creates a compatibility layer backed by the inner accessor's rename.
    #[cfg(any(test, feature = "testing"))]
    pub fn new_for_test() -> Self {
        Self {
            renamer: AtomicRenamer::Raw,
        }
    }
}

#[derive(Debug, Clone)]
enum AtomicRenamer {
    Native {
        client: Client,
        root: String,
    },
    #[cfg(any(test, feature = "testing"))]
    Raw,
}

impl AtomicRenamer {
    async fn rename<A: Access>(&self, _inner: &A, from: &str, to: &str) -> Result<()> {
        match self {
            Self::Native { client, root } => {
                // OpenDAL's HDFS rename removes an existing destination before
                // renaming. Use HDFS Rename2 with overwrite to keep replacement atomic.
                client
                    .rename(
                        &opendal::raw::build_rooted_abs_path(root, from),
                        &opendal::raw::build_rooted_abs_path(root, to),
                        true,
                    )
                    .await
                    .map_err(hdfs_error)
            }
            #[cfg(any(test, feature = "testing"))]
            Self::Raw => _inner.rename(from, to, OpRename::new()).await.map(|_| ()),
        }
    }
}

fn hdfs_error(error: hdfs_native::HdfsError) -> opendal::Error {
    opendal::Error::new(ErrorKind::Unexpected, "native HDFS operation failed").set_source(error)
}

impl<A: Access> Layer<A> for HdfsCompatibilityLayer {
    type LayeredAccess = HdfsCompatibilityAccessor<A>;

    fn layer(&self, inner: A) -> Self::LayeredAccess {
        inner.info().update_full_capability(|mut capability| {
            capability.copy = true;
            capability
        });
        HdfsCompatibilityAccessor {
            inner: Arc::new(inner),
            renamer: self.renamer.clone(),
        }
    }
}

pub struct HdfsCompatibilityAccessor<A: Access> {
    inner: Arc<A>,
    renamer: AtomicRenamer,
}

impl<A: Access> Debug for HdfsCompatibilityAccessor<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HdfsCompatibilityAccessor")
            .field("inner", &self.inner)
            .finish()
    }
}

/// A writer that publishes non-append writes with an atomic rename.
pub struct HdfsWriter<A: Access>(HdfsWriterInner<A>);

enum HdfsWriterInner<A: Access> {
    Direct(A::Writer),
    Atomic {
        inner: Arc<A>,
        renamer: AtomicRenamer,
        writer: Option<A::Writer>,
        temporary_path: String,
        target_path: String,
    },
}

impl<A: Access> oio::Write for HdfsWriter<A> {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        match &mut self.0 {
            HdfsWriterInner::Direct(writer) => writer.write(buffer).await,
            HdfsWriterInner::Atomic { writer, .. } => {
                writer
                    .as_mut()
                    .ok_or_else(writer_unavailable)?
                    .write(buffer)
                    .await
            }
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        match &mut self.0 {
            HdfsWriterInner::Direct(writer) => writer.close().await,
            HdfsWriterInner::Atomic {
                inner,
                renamer,
                writer,
                temporary_path,
                target_path,
            } => {
                let mut writer = writer.take().ok_or_else(writer_unavailable)?;
                let metadata = match writer.close().await {
                    Ok(metadata) => metadata,
                    Err(error) => {
                        drop(writer);
                        let _ = delete_path(inner, temporary_path).await;
                        return Err(error);
                    }
                };
                drop(writer);

                if let Err(error) = renamer.rename(inner, temporary_path, target_path).await {
                    let _ = delete_path(inner, temporary_path).await;
                    return Err(error);
                }

                Ok(metadata)
            }
        }
    }

    async fn abort(&mut self) -> Result<()> {
        match &mut self.0 {
            HdfsWriterInner::Direct(writer) => writer.abort().await,
            HdfsWriterInner::Atomic {
                inner,
                writer,
                temporary_path,
                ..
            } => {
                let abort_result = if let Some(mut writer) = writer.take() {
                    let result = writer.abort().await;
                    drop(writer);
                    result
                } else {
                    Ok(())
                };
                let cleanup_result = delete_path(inner, temporary_path).await;

                match abort_result {
                    Err(error) if error.kind() != ErrorKind::Unsupported => Err(error),
                    _ => cleanup_result,
                }
            }
        }
    }
}

fn writer_unavailable() -> opendal::Error {
    opendal::Error::new(
        ErrorKind::Unexpected,
        "HDFS writer is unavailable after close or abort",
    )
}

impl<A: Access> LayeredAccess for HdfsCompatibilityAccessor<A> {
    type Inner = Arc<A>;
    type Reader = A::Reader;
    type Writer = HdfsWriter<A>;
    type Lister = A::Lister;
    type Deleter = A::Deleter;
    type Copier = oio::OneShotCopier;

    fn inner(&self) -> &Self::Inner {
        &self.inner
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        self.inner.read(path, args).await
    }

    async fn write(&self, path: &str, args: OpWrite) -> Result<(RpWrite, Self::Writer)> {
        if args.append() {
            return self
                .inner
                .write(path, args)
                .await
                .map(|(response, writer)| (response, HdfsWriter(HdfsWriterInner::Direct(writer))));
        }

        let temporary_path = temporary_path(path);
        let (response, writer) = self.inner.write(&temporary_path, args).await?;
        Ok((
            response,
            HdfsWriter(HdfsWriterInner::Atomic {
                inner: Arc::clone(&self.inner),
                renamer: self.renamer.clone(),
                writer: Some(writer),
                temporary_path,
                target_path: path.to_string(),
            }),
        ))
    }

    async fn copy(
        &self,
        from: &str,
        to: &str,
        args: OpCopy,
        _opts: OpCopier,
    ) -> Result<(RpCopy, Self::Copier)> {
        if args.if_not_exists() || args.if_match().is_some() {
            return Err(opendal::Error::new(
                ErrorKind::Unsupported,
                "conditional copy is not supported by the HDFS fallback",
            ));
        }

        let inner = Arc::clone(&self.inner);
        let renamer = self.renamer.clone();
        let from = from.to_string();
        let to = to.to_string();
        Ok((
            RpCopy::default(),
            oio::OneShotCopier::new(async move {
                copy_via_read_write(inner, renamer, &from, &to).await
            }),
        ))
    }

    async fn delete(&self) -> Result<(RpDelete, Self::Deleter)> {
        self.inner.delete().await
    }

    async fn list(&self, path: &str, args: OpList) -> Result<(RpList, Self::Lister)> {
        self.inner.list(path, args).await
    }
}

async fn copy_via_read_write<A: Access>(
    inner: Arc<A>,
    renamer: AtomicRenamer,
    source_path: &str,
    target_path: &str,
) -> Result<Metadata> {
    let (_, mut reader) = inner.read(source_path, OpRead::new()).await?;
    let temporary_path = temporary_path(target_path);
    let (_, mut writer) = inner.write(&temporary_path, OpWrite::new()).await?;

    loop {
        let buffer = match reader.read().await {
            Ok(buffer) => buffer,
            Err(error) => {
                abort_and_delete(&inner, writer, &temporary_path).await;
                return Err(error);
            }
        };
        if buffer.is_empty() {
            break;
        }
        if let Err(error) = writer.write(buffer).await {
            abort_and_delete(&inner, writer, &temporary_path).await;
            return Err(error);
        }
    }

    let metadata = match writer.close().await {
        Ok(metadata) => metadata,
        Err(error) => {
            drop(writer);
            let _ = delete_path(&inner, &temporary_path).await;
            return Err(error);
        }
    };
    drop(writer);

    if let Err(error) = renamer.rename(&inner, &temporary_path, target_path).await {
        let _ = delete_path(&inner, &temporary_path).await;
        return Err(error);
    }

    Ok(metadata)
}

async fn abort_and_delete<A: Access>(inner: &A, mut writer: A::Writer, path: &str) {
    let _ = writer.abort().await;
    drop(writer);
    let _ = delete_path(inner, path).await;
}

async fn delete_path<A: Access>(inner: &A, path: &str) -> Result<()> {
    let (_, mut deleter) = inner.delete().await?;
    deleter.delete(path, OpDelete::new()).await?;
    deleter.close().await
}

fn temporary_path(path: &str) -> String {
    let suffix = format!(".greptime-{}.tmp", Uuid::new_v4());
    match path.rsplit_once('/') {
        Some((parent, name)) => format!("{parent}/.{name}{suffix}"),
        None => format!(".{path}{suffix}"),
    }
}

#[cfg(test)]
mod tests {
    use opendal::services::Fs;
    use opendal::{Operator, Writer};
    use tempfile::TempDir;

    use super::*;

    fn test_store() -> (TempDir, Operator) {
        let directory = tempfile::tempdir().unwrap();
        let store = Operator::new(Fs::default().root(directory.path().to_str().unwrap()))
            .unwrap()
            .layer(HdfsCompatibilityLayer::new_for_test())
            .finish();
        (directory, store)
    }

    #[tokio::test]
    async fn test_atomic_write_keeps_old_data_after_abort() {
        let (_directory, store) = test_store();
        store.write("manifest.json", "old").await.unwrap();

        let mut writer: Writer = store.writer("manifest.json").await.unwrap();
        writer.write("new").await.unwrap();
        writer.abort().await.unwrap();

        assert_eq!(
            b"old",
            store
                .read("manifest.json")
                .await
                .unwrap()
                .to_bytes()
                .as_ref()
        );
        assert!(
            store
                .list("")
                .await
                .unwrap()
                .iter()
                .all(|entry| !entry.path().contains(".greptime-"))
        );
    }

    #[tokio::test]
    async fn test_atomic_write_replaces_on_close() {
        let (_directory, store) = test_store();
        store.write("manifest.json", "old").await.unwrap();
        store.write("manifest.json", "new").await.unwrap();

        assert_eq!(
            b"new",
            store
                .read("manifest.json")
                .await
                .unwrap()
                .to_bytes()
                .as_ref()
        );
        assert!(
            store
                .list("")
                .await
                .unwrap()
                .iter()
                .all(|entry| !entry.path().contains(".greptime-"))
        );
    }

    #[tokio::test]
    async fn test_copy_fallback_streams_to_target() {
        let (_directory, store) = test_store();
        store.write("source.parquet", "contents").await.unwrap();
        store
            .copy("source.parquet", "target.parquet")
            .await
            .unwrap();

        assert_eq!(
            b"contents",
            store
                .read("target.parquet")
                .await
                .unwrap()
                .to_bytes()
                .as_ref()
        );
    }
}
