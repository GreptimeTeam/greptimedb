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

use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use async_stream::stream;
use common_telemetry::{error, info};
use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize, RecoveryMode};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::logstore::entry::Id;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::Namespace as NamespaceTrait;
use store_api::logstore::{AppendResponse, LogStore};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::LogConfig;
use crate::error::{
    AddEntryLogBatchSnafu, Error, FetchEntrySnafu, IllegalNamespaceSnafu, IllegalStateSnafu,
    RaftEngineSnafu, WaitGcTaskStopSnafu,
};
use crate::raft_engine::protos::logstore::{EntryImpl as Entry, NamespaceImpl as Namespace};

const NAMESPACE_PREFIX: &str = "__sys_namespace_";
const SYSTEM_NAMESPACE: u64 = 0;

pub struct RaftEngineLogStore {
    config: LogConfig,
    engine: Arc<Engine>,
    cancel_token: Mutex<Option<CancellationToken>>,
    gc_task_handle: Mutex<Option<JoinHandle<()>>>,
    started: AtomicBool,
}

impl RaftEngineLogStore {
    pub async fn try_new(config: LogConfig) -> Result<Self, Error> {
        // TODO(hl): set according to available disk space
        let raft_engine_config = Config {
            dir: config.log_file_dir.clone(),
            purge_threshold: ReadableSize(config.purge_threshold as u64),
            recovery_mode: RecoveryMode::TolerateTailCorruption,
            batch_compression_threshold: ReadableSize::kb(8),
            target_file_size: ReadableSize(config.file_size as u64),
            ..Default::default()
        };
        let engine = Arc::new(Engine::open(raft_engine_config).context(RaftEngineSnafu)?);
        let log_store = Self {
            config,
            engine,
            cancel_token: Mutex::new(None),
            gc_task_handle: Mutex::new(None),
            started: AtomicBool::new(false),
        };
        log_store.start().await?;
        Ok(log_store)
    }

    pub fn started(&self) -> bool {
        self.started.load(Ordering::Relaxed)
    }

    async fn start(&self) -> Result<(), Error> {
        let engine_clone = self.engine.clone();
        let interval = self.config.purge_interval;
        let token = CancellationToken::new();
        let child = token.child_token();
        // TODO(hl): Maybe spawn to a blocking runtime.
        let handle = common_runtime::spawn_bg(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(interval) => {}
                    _ = child.cancelled() => {
                        info!("LogStore gc task has been cancelled");
                        return;
                    }
                }
                match engine_clone.purge_expired_files().context(RaftEngineSnafu) {
                    Ok(res) => {
                        // TODO(hl): the retval of purge_expired_files indicates the namespaces need to be compact,
                        // which is useful when monitoring regions failed to flush it's memtable to SSTs.
                        info!(
                            "Successfully purged logstore files, namespaces need compaction: {:?}",
                            res
                        );
                    }
                    Err(e) => {
                        error!(e; "Failed to purge files in logstore");
                    }
                }
            }
        });
        *self.cancel_token.lock().await = Some(token);
        *self.gc_task_handle.lock().await = Some(handle);
        self.started.store(true, Ordering::Relaxed);
        info!("RaftEngineLogStore started with config: {:?}", self.config);
        Ok(())
    }
}

impl Debug for RaftEngineLogStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftEngineLogsStore")
            .field("config", &self.config)
            .field("started", &self.started.load(Ordering::Relaxed))
            .finish()
    }
}

#[async_trait::async_trait]
impl LogStore for RaftEngineLogStore {
    type Error = Error;
    type Namespace = Namespace;
    type Entry = Entry;

    async fn stop(&self) -> Result<(), Self::Error> {
        ensure!(
            self.started
                .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok(),
            IllegalStateSnafu
        );
        let handle = self
            .gc_task_handle
            .lock()
            .await
            .take()
            .context(IllegalStateSnafu)?;
        let token = self
            .cancel_token
            .lock()
            .await
            .take()
            .context(IllegalStateSnafu)?;
        token.cancel();
        handle.await.context(WaitGcTaskStopSnafu)?;
        info!("RaftEngineLogStore stopped");
        Ok(())
    }

    /// Append an entry to logstore. Currently of existence of entry's namespace is not checked.
    async fn append(&self, e: Self::Entry) -> Result<AppendResponse, Self::Error> {
        ensure!(self.started(), IllegalStateSnafu);
        let entry_id = e.id;
        let mut batch = LogBatch::with_capacity(1);
        batch
            .add_entries::<MessageType>(e.namespace_id, &[e])
            .context(AddEntryLogBatchSnafu)?;

        self.engine
            .write(&mut batch, self.config.sync_write)
            .context(RaftEngineSnafu)?;
        Ok(AppendResponse { entry_id })
    }

    /// Append a batch of entries to logstore. `RaftEngineLogStore` assures the atomicity of
    /// batch append.
    async fn append_batch(
        &self,
        ns: &Self::Namespace,
        entries: Vec<Self::Entry>,
    ) -> Result<Vec<Id>, Self::Error> {
        ensure!(self.started(), IllegalStateSnafu);
        let entry_ids = entries.iter().map(Entry::get_id).collect::<Vec<_>>();
        let mut batch = LogBatch::with_capacity(entries.len());
        batch
            .add_entries::<MessageType>(ns.id, &entries)
            .context(AddEntryLogBatchSnafu)?;
        self.engine
            .write(&mut batch, self.config.sync_write)
            .context(RaftEngineSnafu)?;
        Ok(entry_ids)
    }

    /// Create a stream of entries from logstore in the given namespace. The end of stream is
    /// determined by the current "last index" of the namespace.
    async fn read(
        &self,
        ns: &Self::Namespace,
        id: Id,
    ) -> Result<SendableEntryStream<'_, Self::Entry, Self::Error>, Self::Error> {
        ensure!(self.started(), IllegalStateSnafu);
        let engine = self.engine.clone();

        let last_index = engine.last_index(ns.id).unwrap_or(0);
        let mut start_index = id.max(engine.first_index(ns.id).unwrap_or(last_index + 1));

        let max_batch_size = self.config.read_batch_size;
        let (tx, mut rx) = tokio::sync::mpsc::channel(max_batch_size);
        let ns = ns.clone();
        common_runtime::spawn_read(async move {
            while start_index <= last_index {
                let mut vec = Vec::with_capacity(max_batch_size);
                match engine
                    .fetch_entries_to::<MessageType>(
                        ns.id,
                        start_index,
                        last_index + 1,
                        Some(max_batch_size),
                        &mut vec,
                    )
                    .context(FetchEntrySnafu {
                        ns: ns.id,
                        start: start_index,
                        end: last_index,
                        max_size: max_batch_size,
                    }) {
                    Ok(_) => {
                        if let Some(last_entry) = vec.last() {
                            start_index = last_entry.id + 1;
                        }
                        // reader side closed, cancel following reads
                        if tx.send(Ok(vec)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        break;
                    }
                }
            }
        });

        let s = stream!({
            while let Some(res) = rx.recv().await {
                yield res;
            }
        });
        Ok(Box::pin(s))
    }

    async fn create_namespace(&mut self, ns: &Self::Namespace) -> Result<(), Self::Error> {
        ensure!(
            ns.id != SYSTEM_NAMESPACE,
            IllegalNamespaceSnafu { ns: ns.id }
        );
        ensure!(self.started(), IllegalStateSnafu);
        let key = format!("{}{}", NAMESPACE_PREFIX, ns.id).as_bytes().to_vec();
        let mut batch = LogBatch::with_capacity(1);
        batch
            .put_message::<Namespace>(SYSTEM_NAMESPACE, key, ns)
            .context(RaftEngineSnafu)?;
        self.engine
            .write(&mut batch, true)
            .context(RaftEngineSnafu)?;
        Ok(())
    }

    async fn delete_namespace(&mut self, ns: &Self::Namespace) -> Result<(), Self::Error> {
        ensure!(
            ns.id != SYSTEM_NAMESPACE,
            IllegalNamespaceSnafu { ns: ns.id }
        );
        ensure!(self.started(), IllegalStateSnafu);
        let key = format!("{}{}", NAMESPACE_PREFIX, ns.id).as_bytes().to_vec();
        let mut batch = LogBatch::with_capacity(1);
        batch.delete(SYSTEM_NAMESPACE, key);
        self.engine
            .write(&mut batch, true)
            .context(RaftEngineSnafu)?;
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>, Self::Error> {
        ensure!(self.started(), IllegalStateSnafu);
        let mut namespaces: Vec<Namespace> = vec![];
        self.engine
            .scan_messages::<Namespace, _>(
                SYSTEM_NAMESPACE,
                Some(NAMESPACE_PREFIX.as_bytes()),
                None,
                false,
                |_, v| {
                    namespaces.push(v);
                    true
                },
            )
            .context(RaftEngineSnafu)?;
        Ok(namespaces)
    }

    fn entry<D: AsRef<[u8]>>(&self, data: D, id: Id, ns: Self::Namespace) -> Self::Entry {
        Entry {
            id,
            data: data.as_ref().to_vec(),
            namespace_id: ns.id(),
            ..Default::default()
        }
    }

    fn namespace(&self, id: store_api::logstore::namespace::Id) -> Self::Namespace {
        Namespace {
            id,
            ..Default::default()
        }
    }

    async fn obsolete(&self, namespace: Self::Namespace, id: Id) -> Result<(), Self::Error> {
        ensure!(self.started(), IllegalStateSnafu);
        let obsoleted = self.engine.compact_to(namespace.id(), id + 1);
        info!(
            "Namespace {} obsoleted {} entries",
            namespace.id(),
            obsoleted
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MessageType;

impl MessageExt for MessageType {
    type Entry = Entry;

    fn index(e: &Self::Entry) -> u64 {
        e.id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use common_telemetry::debug;
    use futures_util::StreamExt;
    use raft_engine::ReadableSize;
    use store_api::logstore::entry_stream::SendableEntryStream;
    use store_api::logstore::namespace::Namespace as NamespaceTrait;
    use store_api::logstore::LogStore;
    use tempdir::TempDir;

    use crate::config::LogConfig;
    use crate::error::Error;
    use crate::raft_engine::log_store::RaftEngineLogStore;
    use crate::raft_engine::protos::logstore::{EntryImpl as Entry, NamespaceImpl as Namespace};

    #[tokio::test]
    async fn test_open_logstore() {
        let dir = TempDir::new("raft-engine-logstore-test").unwrap();
        let logstore = RaftEngineLogStore::try_new(LogConfig {
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
        logstore.start().await.unwrap();
        let namespaces = logstore.list_namespaces().await.unwrap();
        assert_eq!(0, namespaces.len());
    }

    #[tokio::test]
    async fn test_manage_namespace() {
        let dir = TempDir::new("raft-engine-logstore-test").unwrap();
        let mut logstore = RaftEngineLogStore::try_new(LogConfig {
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
        logstore.start().await.unwrap();
        assert!(logstore.list_namespaces().await.unwrap().is_empty());

        logstore
            .create_namespace(&Namespace::with_id(42))
            .await
            .unwrap();
        let namespaces = logstore.list_namespaces().await.unwrap();
        assert_eq!(1, namespaces.len());
        assert_eq!(Namespace::with_id(42), namespaces[0]);

        logstore
            .delete_namespace(&Namespace::with_id(42))
            .await
            .unwrap();
        assert!(logstore.list_namespaces().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_append_and_read() {
        let dir = TempDir::new("raft-engine-logstore-test").unwrap();
        let logstore = RaftEngineLogStore::try_new(LogConfig {
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
        logstore.start().await.unwrap();

        let namespace = Namespace::with_id(1);
        let cnt = 1024;
        for i in 0..cnt {
            let response = logstore
                .append(Entry::create(
                    i,
                    namespace.id,
                    i.to_string().as_bytes().to_vec(),
                ))
                .await
                .unwrap();
            assert_eq!(i, response.entry_id);
        }
        let mut entries = HashSet::with_capacity(1024);
        let mut s = logstore.read(&Namespace::with_id(1), 0).await.unwrap();
        while let Some(r) = s.next().await {
            let vec = r.unwrap();
            entries.extend(vec.into_iter().map(|e| e.id));
        }
        assert_eq!((0..cnt).into_iter().collect::<HashSet<_>>(), entries);
    }

    async fn collect_entries(mut s: SendableEntryStream<'_, Entry, Error>) -> Vec<Entry> {
        let mut res = vec![];
        while let Some(r) = s.next().await {
            res.extend(r.unwrap());
        }
        res
    }

    #[tokio::test]
    async fn test_reopen() {
        let dir = TempDir::new("raft-engine-logstore-reopen-test").unwrap();
        {
            let logstore = RaftEngineLogStore::try_new(LogConfig {
                log_file_dir: dir.path().to_str().unwrap().to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
            logstore
                .append(Entry::create(1, 1, "1".as_bytes().to_vec()))
                .await
                .unwrap();
            let entries = logstore
                .read(&Namespace::with_id(1), 1)
                .await
                .unwrap()
                .collect::<Vec<_>>()
                .await;
            assert_eq!(1, entries.len());
            logstore.stop().await.unwrap();
        }

        let logstore = RaftEngineLogStore::try_new(LogConfig {
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
        logstore.start().await.unwrap();

        let entries =
            collect_entries(logstore.read(&Namespace::with_id(1), 1).await.unwrap()).await;
        assert_eq!(1, entries.len());
        assert_eq!(1, entries[0].id);
        assert_eq!(1, entries[0].namespace_id);
    }

    async fn wal_dir_usage(path: impl AsRef<str>) -> usize {
        let mut size: usize = 0;
        let mut read_dir = tokio::fs::read_dir(path.as_ref()).await.unwrap();
        while let Ok(dir_entry) = read_dir.next_entry().await {
            let Some(entry) = dir_entry else {
                break;
            };
            if entry.file_type().await.unwrap().is_file() {
                let file_name = entry.file_name();
                let file_size = entry.metadata().await.unwrap().len() as usize;
                debug!("File: {file_name:?}, size: {file_size}");
                size += file_size;
            }
        }
        size
    }

    #[tokio::test]
    async fn test_compaction() {
        common_telemetry::init_default_ut_logging();
        let dir = TempDir::new("raft-engine-logstore-test").unwrap();

        let config = LogConfig {
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            file_size: ReadableSize::mb(2).0 as usize,
            purge_threshold: ReadableSize::mb(4).0 as usize,
            purge_interval: Duration::from_secs(5),
            ..Default::default()
        };

        let logstore = RaftEngineLogStore::try_new(config).await.unwrap();
        logstore.start().await.unwrap();
        let namespace = Namespace::with_id(42);
        for id in 0..4096 {
            let entry = Entry::create(id, namespace.id(), [b'x'; 4096].to_vec());
            logstore.append(entry).await.unwrap();
        }

        let before_purge = wal_dir_usage(dir.path().to_str().unwrap()).await;
        logstore.obsolete(namespace, 4000).await.unwrap();

        tokio::time::sleep(Duration::from_secs(6)).await;
        let after_purge = wal_dir_usage(dir.path().to_str().unwrap()).await;
        debug!(
            "Before purge: {}, after purge: {}",
            before_purge, after_purge
        );
        assert!(before_purge > after_purge);
    }

    #[tokio::test]
    async fn test_obsolete() {
        common_telemetry::init_default_ut_logging();
        let dir = TempDir::new("raft-engine-logstore-test").unwrap();

        let config = LogConfig {
            log_file_dir: dir.path().to_str().unwrap().to_string(),
            file_size: ReadableSize::mb(2).0 as usize,
            purge_threshold: ReadableSize::mb(4).0 as usize,
            purge_interval: Duration::from_secs(5),
            ..Default::default()
        };

        let logstore = RaftEngineLogStore::try_new(config).await.unwrap();
        logstore.start().await.unwrap();
        let namespace = Namespace::with_id(42);
        for id in 0..1024 {
            let entry = Entry::create(id, namespace.id(), [b'x'; 4096].to_vec());
            logstore.append(entry).await.unwrap();
        }

        logstore.obsolete(namespace.clone(), 100).await.unwrap();
        assert_eq!(101, logstore.engine.first_index(namespace.id).unwrap());

        let res = logstore.read(&namespace, 100).await.unwrap();
        let mut vec = collect_entries(res).await;
        vec.sort_by(|a, b| a.id.partial_cmp(&b.id).unwrap());
        assert_eq!(101, vec.first().unwrap().id);
    }
}
