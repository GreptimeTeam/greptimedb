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

//! Best-effort local disk copies of immutable objects. The remote service remains authoritative.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use opendal::raw::oio::{Read as _, Write as _};
use opendal::raw::{
    Layer, OpCopy, OpCreateDir, OpDelete, OpList, OpPresign, OpRead, OpRename, OpStat, OpWrite,
    RpCreateDir, RpPresign, RpRead, RpRename, RpStat, Service, ServiceInfo, Servicer, oio,
};
use opendal::{Buffer, BytesRange, Capability, Metadata, OperationContext, Operator, Result};
use tokio::sync::Semaphore;

struct PendingFill {
    cache: Arc<DiskCache>,
    key: String,
    token: u64,
}

impl Drop for PendingFill {
    fn drop(&mut self) {
        self.cache.finish(&self.key, self.token, None);
    }
}

/// A shared, byte-bounded disk cache. Instances of its layers share one budget.
#[derive(Debug)]
pub struct DiskCache {
    local: Operator,
    capacity: u64,
    state: Mutex<State>,
    downloads: Arc<Semaphore>,
}

#[derive(Debug, Default)]
struct State {
    clock: u64,
    bytes: u64,
    entries: HashMap<String, (Arc<LocalFile>, u64)>,
    pending: HashMap<String, u64>,
}

/// Pins a local file until both the cache and outstanding readers release it.
#[derive(Debug)]
struct LocalFile {
    store: Operator,
    path: String,
    size: u64,
}

impl Drop for LocalFile {
    fn drop(&mut self) {
        let store = self.store.clone();
        let path = self.path.clone();
        common_runtime::spawn_compact(async move {
            let _ = store.delete(&path).await;
        });
    }
}

impl DiskCache {
    /// Creates an empty cache. The caller owns cleanup of files from previous runs.
    pub fn new(local: Operator, capacity: u64) -> Arc<Self> {
        Arc::new(Self {
            local,
            capacity,
            state: Mutex::new(State::default()),
            downloads: Arc::new(Semaphore::new(2)),
        })
    }

    /// Creates a layer for one remote namespace.
    /// Mutable objects must be excluded by the predicate.
    pub fn layer(
        self: &Arc<Self>,
        namespace: String,
        cacheable: fn(&str) -> bool,
    ) -> DiskCacheLayer {
        DiskCacheLayer {
            cache: self.clone(),
            namespace,
            cacheable,
        }
    }

    fn lookup(&self, key: &str) -> Option<Arc<LocalFile>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.clock += 1;
        let clock = state.clock;
        state.entries.get_mut(key).map(|(file, used)| {
            *used = clock;
            file.clone()
        })
    }

    fn invalidate(&self, key: &str) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.pending.remove(key);
        if let Some((file, _)) = state.entries.remove(key) {
            state.bytes -= file.size;
        }
    }

    fn begin(&self, key: &str) -> Option<u64> {
        if self.capacity == 0 {
            return None;
        }
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.pending.contains_key(key) || state.entries.contains_key(key) {
            return None;
        }
        state.clock += 1;
        let token = state.clock;
        state.pending.insert(key.to_string(), token);
        Some(token)
    }

    fn finish(&self, key: &str, token: u64, file: Option<Arc<LocalFile>>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.pending.get(key) != Some(&token) {
            return;
        }
        state.pending.remove(key);
        let Some(file) = file else { return };
        while state.bytes.saturating_add(file.size) > self.capacity {
            let Some(oldest) = state
                .entries
                .iter()
                .min_by_key(|(_, (_, used))| *used)
                .map(|(key, _)| key.clone())
            else {
                return;
            };
            if let Some((old, _)) = state.entries.remove(&oldest) {
                state.bytes -= old.size;
            }
        }
        state.bytes += file.size;
        state.clock += 1;
        let clock = state.clock;
        state.entries.insert(key.to_string(), (file, clock));
    }

    fn fill(self: &Arc<Self>, remote: Servicer, ctx: OperationContext, path: String, key: String) {
        let Ok(permit) = self.downloads.clone().try_acquire_owned() else {
            return;
        };
        let Some(token) = self.begin(&key) else {
            return;
        };
        let cache = self.clone();
        let pending = PendingFill {
            cache: cache.clone(),
            key: key.clone(),
            token,
        };
        common_runtime::spawn_compact(async move {
            let _pending = pending;
            let _permit = permit;
            let result = cache.download(&remote, &ctx, &path, token).await;
            cache.finish(&key, token, result.ok().flatten());
        });
    }

    async fn download(
        &self,
        remote: &Servicer,
        ctx: &OperationContext,
        path: &str,
        _token: u64,
    ) -> Result<Option<Arc<LocalFile>>> {
        let size = remote
            .stat(ctx, path, OpStat::default())
            .await?
            .into_metadata()
            .content_length();
        if size > self.capacity {
            return Ok(None);
        }
        let file = Arc::new(LocalFile {
            store: self.local.clone(),
            path: format!("objects/{}", uuid::Uuid::new_v4()),
            size,
        });
        let mut writer =
            self.local
                .service()
                .write(self.local.context(), &file.path, OpWrite::default())?;
        let reader = remote.read(ctx, path, OpRead::default())?;
        let result: Result<()> = async {
            let mut offset = 0u64;
            while offset < size {
                let end = size.min(offset.saturating_add(8 * 1024 * 1024));
                let (_, buffer) = reader.read((offset..end).into()).await?;
                if buffer.len() as u64 != end - offset {
                    return Err(opendal::Error::new(
                        opendal::ErrorKind::Unexpected,
                        "incomplete cache download",
                    ));
                }
                writer.write(buffer).await?;
                offset = end;
            }
            writer.close().await?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let _ = writer.abort().await;
            return Err(error);
        }
        Ok(Some(file))
    }
}

/// Remote writes and metadata operations are never satisfied by the local cache.
#[derive(Clone, Debug)]
pub struct DiskCacheLayer {
    cache: Arc<DiskCache>,
    namespace: String,
    cacheable: fn(&str) -> bool,
}

impl Layer for DiskCacheLayer {
    fn apply_service(&self, inner: Servicer) -> Servicer {
        Arc::new(CacheService {
            inner,
            layer: self.clone(),
        })
    }
}

#[derive(Debug)]
struct CacheService {
    inner: Servicer,
    layer: DiskCacheLayer,
}

impl CacheService {
    fn key(&self, path: &str) -> String {
        format!("{}:{}", self.layer.namespace, path)
    }
}

struct CacheReader {
    remote: oio::Reader,
    service: Servicer,
    context: OperationContext,
    cache: Arc<DiskCache>,
    key: String,
    path: String,
}

impl CacheReader {
    fn fill(&self) {
        self.cache.fill(
            self.service.clone(),
            self.context.clone(),
            self.path.clone(),
            self.key.clone(),
        );
    }
}

impl oio::Read for CacheReader {
    async fn read(&self, range: BytesRange) -> Result<(RpRead, Buffer)> {
        if !range.is_suffix()
            && let Some(file) = self.cache.lookup(&self.key)
        {
            if let Ok(reader) =
                file.store
                    .service()
                    .read(file.store.context(), &file.path, OpRead::default())
                && let Ok(result) = reader.read(range).await
            {
                let expected = range
                    .size()
                    .unwrap_or_else(|| file.size.saturating_sub(range.offset()));
                if result.1.len() as u64 == expected {
                    return Ok(result);
                }
            }
            self.cache.invalidate(&self.key);
        }
        let result = self.remote.read(range).await?;
        self.fill();
        Ok(result)
    }

    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        if !range.is_suffix()
            && let Some(file) = self.cache.lookup(&self.key)
        {
            if let Ok(reader) =
                file.store
                    .service()
                    .read(file.store.context(), &file.path, OpRead::default())
                && let Ok((rp, stream)) = reader.open(range).await
            {
                return Ok((
                    rp,
                    Box::new(PinnedStream {
                        stream,
                        remaining: BytesRange::new(
                            range.offset(),
                            Some(
                                range
                                    .size()
                                    .unwrap_or_else(|| file.size.saturating_sub(range.offset())),
                            ),
                        ),
                        remote: Some(self.service.read(
                            &self.context,
                            &self.path,
                            OpRead::default(),
                        )?),
                        cache: self.cache.clone(),
                        key: self.key.clone(),
                        _file: file,
                    }),
                ));
            }
            self.cache.invalidate(&self.key);
        }
        let result = self.remote.open(range).await?;
        self.fill();
        Ok(result)
    }
}

struct PinnedStream {
    stream: Box<dyn oio::ReadStreamDyn>,
    _file: Arc<LocalFile>,
    remaining: BytesRange,
    remote: Option<oio::Reader>,
    cache: Arc<DiskCache>,
    key: String,
}

impl oio::ReadStream for PinnedStream {
    async fn read(&mut self) -> Result<Buffer> {
        let result = oio::ReadStream::read(&mut self.stream).await;
        let incomplete = result.as_ref().is_ok_and(|buffer| {
            buffer.is_empty() && self.remaining.size().is_some_and(|size| size > 0)
        });
        if (result.is_err() || incomplete)
            && let Some(remote) = self.remote.take()
        {
            self.cache.invalidate(&self.key);
            self.stream = remote.open(self.remaining).await?.1;
            let buffer = oio::ReadStream::read(&mut self.stream).await?;
            self.remaining.advance(buffer.len() as u64);
            return Ok(buffer);
        }
        let buffer = result?;
        self.remaining.advance(buffer.len() as u64);
        Ok(buffer)
    }
}

struct LocalOutput {
    writer: oio::Writer,
    file: LocalFile,
    pending: PendingFill,
}

struct CacheWriter {
    inner: oio::Writer,
    cache: Arc<DiskCache>,
    key: String,
    local: Option<LocalOutput>,
}

impl oio::Write for CacheWriter {
    async fn write(&mut self, buffer: Buffer) -> Result<()> {
        self.inner.write(buffer.clone()).await?;
        if let Some(mut local) = self.local.take() {
            local.file.size = local.file.size.saturating_add(buffer.len() as u64);
            if local.file.size <= self.cache.capacity && local.writer.write(buffer).await.is_ok() {
                self.local = Some(local);
            } else {
                let _ = local.writer.abort().await;
            }
        }
        Ok(())
    }
    async fn close(&mut self) -> Result<Metadata> {
        let result = self.inner.close().await;
        if self.local.is_none() {
            self.cache.invalidate(&self.key);
        }
        if let Some(mut local) = self.local.take() {
            if result.is_ok() && local.writer.close().await.is_ok() {
                self.cache
                    .finish(&self.key, local.pending.token, Some(Arc::new(local.file)));
            } else {
                let _ = local.writer.abort().await;
            }
        }
        result
    }
    async fn abort(&mut self) -> Result<()> {
        if let Some(mut local) = self.local.take() {
            let _ = local.writer.abort().await;
        }
        self.cache.invalidate(&self.key);
        self.inner.abort().await
    }
}

struct CacheDeleter {
    inner: oio::Deleter,
    cache: Arc<DiskCache>,
    namespace: String,
    keys: Vec<String>,
}

impl oio::Delete for CacheDeleter {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        let key = format!("{}:{}", self.namespace, path);
        self.cache.invalidate(&key);
        self.keys.push(key);
        self.inner.delete(path, args).await
    }
    async fn close(&mut self) -> Result<()> {
        let result = self.inner.close().await;
        for key in self.keys.drain(..) {
            self.cache.invalidate(&key);
        }
        result
    }
}

impl Service for CacheService {
    type Reader = oio::Reader;
    type Writer = CacheWriter;
    type Lister = oio::Lister;
    type Deleter = CacheDeleter;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.inner.info()
    }
    fn capability(&self) -> Capability {
        let mut capabilities = self.inner.capability();
        // These operations bypass write/delete invalidation.
        capabilities.copy = false;
        capabilities.rename = false;
        capabilities.compose = false;
        capabilities
    }
    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.inner.stat(ctx, path, args).await
    }
    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.inner.create_dir(ctx, path, args).await
    }
    fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let cacheable =
            (self.layer.cacheable)(path) && !args.is_conditional() && args.version().is_none();
        let reader = self.inner.read(ctx, path, args)?;
        if !cacheable {
            return Ok(reader);
        }
        Ok(Box::new(CacheReader {
            remote: reader,
            service: self.inner.clone(),
            context: ctx.clone(),
            cache: self.layer.cache.clone(),
            key: self.key(path),
            path: path.to_string(),
        }))
    }
    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let key = self.key(path);
        self.layer.cache.invalidate(&key);
        let cacheable = (self.layer.cacheable)(path) && !args.append();
        let inner = self.inner.write(ctx, path, args)?;
        let local = if cacheable {
            self.layer.cache.begin(&key).and_then(|token| {
                let pending = PendingFill {
                    cache: self.layer.cache.clone(),
                    key: key.clone(),
                    token,
                };
                let file = LocalFile {
                    store: self.layer.cache.local.clone(),
                    path: format!("objects/{}", uuid::Uuid::new_v4()),
                    size: 0,
                };
                file.store
                    .service()
                    .write(file.store.context(), &file.path, OpWrite::default())
                    .ok()
                    .map(|writer| LocalOutput {
                        writer,
                        file,
                        pending,
                    })
            })
        } else {
            None
        };
        Ok(CacheWriter {
            inner,
            cache: self.layer.cache.clone(),
            key,
            local,
        })
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        Ok(CacheDeleter {
            inner: self.inner.delete(ctx)?,
            cache: self.layer.cache.clone(),
            namespace: self.layer.namespace.clone(),
            keys: Vec::new(),
        })
    }
    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        Err(opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "copy bypasses cache invalidation",
        ))
    }
    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        Err(opendal::Error::new(
            opendal::ErrorKind::Unsupported,
            "rename bypasses cache invalidation",
        ))
    }
    async fn presign(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpPresign,
    ) -> Result<RpPresign> {
        self.inner.presign(ctx, path, args).await
    }
    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.inner.list(ctx, path, args)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    struct RejectClose(oio::Writer);

    impl oio::Write for RejectClose {
        async fn write(&mut self, buffer: Buffer) -> Result<()> {
            self.0.write(buffer).await
        }
        async fn close(&mut self) -> Result<Metadata> {
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "injected remote close failure",
            ))
        }
        async fn abort(&mut self) -> Result<()> {
            self.0.abort().await
        }
    }

    async fn wait_cached(cache: &DiskCache, key: &str) -> Arc<LocalFile> {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(file) = cache.lookup(key) {
                    return file;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn eviction_local_loss_and_remote_repair() {
        let remote_dir = tempfile::tempdir().unwrap();
        let local_dir = tempfile::tempdir().unwrap();
        let remote = Operator::new(
            opendal::services::Fs::default().root(remote_dir.path().to_str().unwrap()),
        )
        .unwrap();
        let local = Operator::new(
            opendal::services::Fs::default().root(local_dir.path().to_str().unwrap()),
        )
        .unwrap();
        let cache = DiskCache::new(local.clone(), 12);
        let store = remote
            .clone()
            .layer(cache.layer("remote".to_string(), |path| path != "catalog"));
        store.write("first", "12345678").await.unwrap();
        assert_eq!(
            remote.read("first").await.unwrap().to_bytes().as_ref(),
            b"12345678"
        );
        let first = wait_cached(&cache, "remote:first").await;
        store.write("second", "abcdefgh").await.unwrap();
        let second = wait_cached(&cache, "remote:second").await;
        assert!(cache.lookup("remote:first").is_none());
        // Removing a local object cannot remove remote coverage or fail the query.
        local.delete(&second.path).await.unwrap();
        assert_eq!(
            store
                .read_with("second")
                .range(0..8)
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"abcdefgh"
        );
        assert_eq!(
            store.read("first").await.unwrap().to_bytes().as_ref(),
            b"12345678"
        );
        let _refilled = wait_cached(&cache, "remote:first").await;
        // An object exceeding the entire budget is still persisted and readable.
        store.write("large", "01234567890123456789").await.unwrap();
        assert_eq!(store.read("large").await.unwrap().len(), 20);
        assert!(cache.lookup("remote:large").is_none());

        store.read("second").await.unwrap();
        let cached = wait_cached(&cache, "remote:second").await;
        remote.delete("second").await.unwrap();
        assert!(!store.exists("second").await.unwrap());
        assert_eq!(
            store
                .read_with("second")
                .range(0..8)
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"abcdefgh"
        );
        // Maintenance repair replaces the remote object and invalidates the old copy.
        store.write("second", "repaired").await.unwrap();
        assert_eq!(
            store.read("second").await.unwrap().to_bytes().as_ref(),
            b"repaired"
        );
        store.write("catalog", "old").await.unwrap();
        remote.write("catalog", "new").await.unwrap();
        assert_eq!(
            store.read("catalog").await.unwrap().to_bytes().as_ref(),
            b"new"
        );
        let failing = remote
            .clone()
            .layer(
                crate::layers::mock::MockLayerBuilder::default()
                    .writer_factory(Arc::new(|_, _, inner| Box::new(RejectClose(inner))))
                    .build()
                    .unwrap(),
            )
            .layer(cache.layer("failed-remote".to_string(), |_| true));
        assert!(failing.write("failed", "partial").await.is_err());
        assert!(cache.lookup("failed-remote:failed").is_none());
        // A broken local cache must not prevent successful remote persistence.
        let broken_local = local.clone().layer(
            crate::layers::mock::MockLayerBuilder::default()
                .writer_factory(Arc::new(|_, _, inner| Box::new(RejectClose(inner))))
                .build()
                .unwrap(),
        );
        let broken_cache = DiskCache::new(broken_local, 12);
        let bypass = remote
            .clone()
            .layer(broken_cache.layer("remote".to_string(), |_| true));
        bypass.write("uncached", "durable").await.unwrap();
        assert_eq!(
            bypass.read("uncached").await.unwrap().to_bytes().as_ref(),
            b"durable"
        );
        assert!(broken_cache.lookup("remote:uncached").is_none());
        assert!(cache.state.lock().unwrap().bytes <= 12);
        drop((first, second, cached));
    }
}
