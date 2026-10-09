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
use opendal::raw::oio::{Delete as _, Read as _, ReadStream as _, Write as _};
use opendal::raw::{
    Layer, OpCompose, OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename, OpStat,
    OpWrite, RpCreateDir, RpPresign, RpRename, RpStat, Service, ServiceInfo, Servicer, oio,
};
use opendal::{Buffer, Capability, ErrorKind, Metadata, OperationContext, Result};
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

    /// Creates a compatibility layer backed by the inner service's rename.
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
    async fn rename(
        &self,
        _inner: &Servicer,
        _ctx: &OperationContext,
        from: &str,
        to: &str,
    ) -> Result<()> {
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
            Self::Raw => _inner
                .rename(_ctx, from, to, OpRename::new())
                .await
                .map(|_| ()),
        }
    }
}

fn hdfs_error(error: hdfs_native::HdfsError) -> opendal::Error {
    opendal::Error::new(ErrorKind::Unexpected, "native HDFS operation failed").set_source(error)
}

impl Layer for HdfsCompatibilityLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(HdfsCompatibilityService {
            inner,
            renamer: self.renamer.clone(),
        })
    }
}

struct HdfsCompatibilityService {
    inner: Servicer,
    renamer: AtomicRenamer,
}

impl Debug for HdfsCompatibilityService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HdfsCompatibilityService")
            .field("inner", &self.inner)
            .finish()
    }
}

/// A writer that publishes non-append writes with an atomic rename.
struct HdfsWriter(HdfsWriterInner);

enum HdfsWriterInner {
    Direct(oio::Writer),
    Atomic {
        inner: Servicer,
        context: OperationContext,
        renamer: AtomicRenamer,
        writer: Option<oio::Writer>,
        temporary_path: String,
        target_path: String,
    },
}

impl oio::Write for HdfsWriter {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        match &mut self.0 {
            HdfsWriterInner::Direct(writer) => writer.write(buffer).await,
            HdfsWriterInner::Atomic {
                inner,
                context,
                writer,
                temporary_path,
                ..
            } => {
                let result = writer
                    .as_mut()
                    .ok_or_else(writer_unavailable)?
                    .write(buffer)
                    .await;
                if let Err(error) = result {
                    // A failed write leaves the temporary file behind, so abort
                    // it before returning the original error to the caller.
                    if let Some(writer) = writer.take() {
                        abort_and_delete(inner, context, writer, temporary_path).await;
                    }
                    return Err(error);
                }
                Ok(())
            }
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        match &mut self.0 {
            HdfsWriterInner::Direct(writer) => writer.close().await,
            HdfsWriterInner::Atomic {
                inner,
                context,
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
                        let _ = delete_path(inner, context, temporary_path).await;
                        return Err(error);
                    }
                };
                drop(writer);

                if let Err(error) = renamer
                    .rename(inner, context, temporary_path, target_path)
                    .await
                {
                    let _ = delete_path(inner, context, temporary_path).await;
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
                context,
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
                let cleanup_result = delete_path(inner, context, temporary_path).await;

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

impl Service for HdfsCompatibilityService {
    type Reader = oio::Reader;
    type Writer = HdfsWriter;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = oio::OneShotCopier;
    type Composer = oio::Composer;

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }

    fn capability(&self) -> Capability {
        let mut capability = self.inner.capability();
        capability.copy = true;
        capability
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }

    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        self.inner.read(ctx, path, args)
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        if args.append() {
            return self
                .inner
                .write(ctx, path, args)
                .map(|writer| HdfsWriter(HdfsWriterInner::Direct(writer)));
        }

        let temporary_path = temporary_path(path);
        let writer = self.inner.write(ctx, &temporary_path, args)?;
        Ok(HdfsWriter(HdfsWriterInner::Atomic {
            inner: Arc::clone(&self.inner),
            context: ctx.clone(),
            renamer: self.renamer.clone(),
            writer: Some(writer),
            temporary_path,
            target_path: path.to_string(),
        }))
    }

    fn copy(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpCopy,
    ) -> Result<Self::Copier> {
        if args.if_not_exists() || args.if_match().is_some() {
            return Err(opendal::Error::new(
                ErrorKind::Unsupported,
                "conditional copy is not supported by the HDFS fallback",
            ));
        }

        let inner = Arc::clone(&self.inner);
        let context = ctx.clone();
        let renamer = self.renamer.clone();
        let from = from.to_string();
        let to = to.to_string();
        Ok(oio::OneShotCopier::new(async move {
            copy_via_read_write(inner, &context, renamer, &from, &to).await
        }))
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.inner.delete(ctx)
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }

    fn compose(&self, ctx: &OperationContext, to: &str, args: OpCompose) -> Result<Self::Composer> {
        self.inner.compose(ctx, to, args)
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.inner.rename(ctx, from, to, args).await
    }

    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
}

async fn copy_via_read_write(
    inner: Servicer,
    context: &OperationContext,
    renamer: AtomicRenamer,
    source_path: &str,
    target_path: &str,
) -> Result<Metadata> {
    let reader = inner.read(context, source_path, OpRead::new())?;
    let (_, mut reader) = reader.open(opendal::BytesRange::from(..)).await?;
    let temporary_path = temporary_path(target_path);
    let mut writer = inner.write(context, &temporary_path, OpWrite::new())?;

    loop {
        let buffer = match reader.read().await {
            Ok(buffer) => buffer,
            Err(error) => {
                abort_and_delete(&inner, context, writer, &temporary_path).await;
                return Err(error);
            }
        };
        if buffer.is_empty() {
            break;
        }
        if let Err(error) = writer.write(buffer).await {
            abort_and_delete(&inner, context, writer, &temporary_path).await;
            return Err(error);
        }
    }

    let metadata = match writer.close().await {
        Ok(metadata) => metadata,
        Err(error) => {
            drop(writer);
            let _ = delete_path(&inner, context, &temporary_path).await;
            return Err(error);
        }
    };
    drop(writer);

    if let Err(error) = renamer
        .rename(&inner, context, &temporary_path, target_path)
        .await
    {
        let _ = delete_path(&inner, context, &temporary_path).await;
        return Err(error);
    }

    Ok(metadata)
}

async fn abort_and_delete(
    inner: &Servicer,
    context: &OperationContext,
    mut writer: oio::Writer,
    path: &str,
) {
    let _ = writer.abort().await;
    drop(writer);
    let _ = delete_path(inner, context, path).await;
}

async fn delete_path(inner: &Servicer, context: &OperationContext, path: &str) -> Result<()> {
    let mut deleter = inner.delete(context)?;
    deleter.delete(path, OpDelete::new()).await?;
    deleter.close().await
}

// TODO(fengjiachun): Clean up temporary files left behind after a process crash.
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
    use crate::layers::mock::{MockLayerBuilder, MockWriterFactory};

    fn test_store() -> (TempDir, Operator) {
        let directory = tempfile::tempdir().unwrap();
        let store = Operator::new(Fs::default().root(directory.path().to_str().unwrap()))
            .unwrap()
            .layer(HdfsCompatibilityLayer::new_for_test());
        (directory, store)
    }

    #[tokio::test]
    async fn test_service_operations() {
        let (_directory, store) = test_store();
        store.create_dir("data/").await.unwrap();
        store.write("data/source", "contents").await.unwrap();
        assert_eq!(8, store.stat("data/source").await.unwrap().content_length());
        assert_eq!(
            b"onte",
            store
                .read_with("data/source")
                .range(1..5)
                .await
                .unwrap()
                .to_bytes()
                .as_ref()
        );
        store.rename("data/source", "data/target").await.unwrap();
        let entries = store.list("data/").await.unwrap();
        assert!(entries.iter().any(|entry| entry.path() == "data/target"));
        assert!(!store.exists("data/source").await.unwrap());
        store.delete("data/target").await.unwrap();
        assert!(!store.exists("data/target").await.unwrap());
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

    /// A writer that starts failing once more than `fail_after` bytes have been
    /// accepted, to simulate a failure in the middle of an atomic write.
    struct FailingWriter {
        inner: oio::Writer,
        written: usize,
        fail_after: usize,
    }

    impl oio::Write for FailingWriter {
        async fn write(&mut self, buffer: Buffer) -> Result<()> {
            if self.written + buffer.len() > self.fail_after {
                return Err(opendal::Error::new(
                    ErrorKind::Unexpected,
                    "injected write failure",
                ));
            }
            self.written += buffer.len();
            self.inner.write(buffer).await
        }

        async fn close(&mut self) -> Result<Metadata> {
            self.inner.close().await
        }

        async fn abort(&mut self) -> Result<()> {
            self.inner.abort().await
        }
    }

    #[tokio::test]
    async fn test_atomic_write_cleans_up_temporary_file_after_write_error() {
        let directory = tempfile::tempdir().unwrap();
        let factory: MockWriterFactory = Arc::new(|path, _args, inner| {
            if path.contains(".greptime-") {
                Box::new(FailingWriter {
                    inner,
                    written: 0,
                    fail_after: 4,
                })
            } else {
                inner
            }
        });
        let mock_layer = MockLayerBuilder::default()
            .writer_factory(factory)
            .build()
            .unwrap();
        let store = Operator::new(Fs::default().root(directory.path().to_str().unwrap()))
            .unwrap()
            .layer(mock_layer)
            .layer(HdfsCompatibilityLayer::new_for_test());

        store.write("data/target", "old").await.unwrap();

        let mut writer: Writer = store.writer("data/target").await.unwrap();
        writer.write("part").await.unwrap();

        let error = writer.write("ial").await.unwrap_err();
        assert_eq!(ErrorKind::Unexpected, error.kind());
        assert!(error.to_string().contains("injected write failure"));

        // The destination keeps its previous contents...
        assert_eq!(
            b"old",
            store
                .read("data/target")
                .await
                .unwrap()
                .to_bytes()
                .as_ref()
        );
        // ...and the temporary file is removed even though the writer was not
        // closed or aborted by the caller.
        assert!(
            store
                .list("")
                .await
                .unwrap()
                .iter()
                .all(|entry| !entry.path().contains(".greptime-"))
        );
    }
}
