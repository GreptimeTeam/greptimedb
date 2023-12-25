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
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_stream::stream;
use common_config::wal::{RaftEngineConfig, WalOptions};
use common_runtime::{RepeatedTask, TaskFunction};
use common_telemetry::{error, info};
use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize, RecoveryMode};
use snafu::{ensure, ResultExt};
use store_api::logstore::entry::{Entry, Id as EntryId};
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::{Id as NamespaceId, Namespace as NamespaceTrait};
use store_api::logstore::{AppendBatchResponse, AppendResponse, LogStore};

use crate::error;
use crate::error::{
    AddEntryLogBatchSnafu, Error, FetchEntrySnafu, IllegalNamespaceSnafu, IllegalStateSnafu,
    OverrideCompactedEntrySnafu, RaftEngineSnafu, Result, StartGcTaskSnafu, StopGcTaskSnafu,
};
use crate::raft_engine::backend::SYSTEM_NAMESPACE;
use crate::raft_engine::protos::logstore::{EntryImpl, NamespaceImpl as Namespace};

const NAMESPACE_PREFIX: &str = "$sys/";

pub struct RaftEngineLogStore {
    config: RaftEngineConfig,
    engine: Arc<Engine>,
    gc_task: RepeatedTask<Error>,
}

pub struct PurgeExpiredFilesFunction {
    engine: Arc<Engine>,
}

#[async_trait::async_trait]
impl TaskFunction<Error> for PurgeExpiredFilesFunction {
    fn name(&self) -> &str {
        "RaftEngineLogStore-gc-task"
    }

    async fn call(&mut self) -> Result<()> {
        match self.engine.purge_expired_files().context(RaftEngineSnafu) {
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

        Ok(())
    }
}

impl RaftEngineLogStore {
    pub async fn try_new(dir: String, config: RaftEngineConfig) -> Result<Self> {
        let raft_engine_config = Config {
            dir,
            purge_threshold: ReadableSize(config.purge_threshold.0),
            recovery_mode: RecoveryMode::TolerateTailCorruption,
            batch_compression_threshold: ReadableSize::kb(8),
            target_file_size: ReadableSize(config.file_size.0),
            ..Default::default()
        };
        let engine = Arc::new(Engine::open(raft_engine_config).context(RaftEngineSnafu)?);
        let gc_task = RepeatedTask::new(
            config.purge_interval,
            Box::new(PurgeExpiredFilesFunction {
                engine: engine.clone(),
            }),
        );

        let log_store = Self {
            config,
            engine,
            gc_task,
        };
        log_store.start()?;
        Ok(log_store)
    }

    pub fn started(&self) -> bool {
        self.gc_task.started()
    }

    fn start(&self) -> Result<()> {
        self.gc_task
            .start(common_runtime::bg_runtime())
            .context(StartGcTaskSnafu)
    }

    fn span(&self, namespace: &<Self as LogStore>::Namespace) -> (Option<u64>, Option<u64>) {
        (
            self.engine.first_index(namespace.id()),
            self.engine.last_index(namespace.id()),
        )
    }

    /// Checks if entry does not override the min index of namespace.
    fn check_entry(&self, e: &EntryImpl) -> Result<()> {
        if cfg!(debug_assertions) {
            let ns_id = e.namespace_id;
            if let Some(first_index) = self.engine.first_index(ns_id) {
                ensure!(
                    e.id() >= first_index,
                    OverrideCompactedEntrySnafu {
                        namespace: ns_id,
                        first_index,
                        attempt_index: e.id(),
                    }
                );
            }
        }
        Ok(())
    }
}

impl Debug for RaftEngineLogStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftEngineLogsStore")
            .field("config", &self.config)
            .field("started", &self.gc_task.started())
            .finish()
    }
}

#[async_trait::async_trait]
impl LogStore for RaftEngineLogStore {
    type Error = Error;
    type Namespace = Namespace;
    type Entry = EntryImpl;

    async fn stop(&self) -> Result<()> {
        self.gc_task.stop().await.context(StopGcTaskSnafu)
    }

    /// Appends an entry to logstore. Currently the existence of the entry's namespace is not checked.
    async fn append(&self, e: Self::Entry) -> Result<AppendResponse> {
        ensure!(self.started(), IllegalStateSnafu);
        let entry_id = e.id;
        let namespace_id = e.namespace_id;
        let mut batch = LogBatch::with_capacity(1);
        batch
            .add_entries::<MessageType>(namespace_id, &[e])
            .context(AddEntryLogBatchSnafu)?;

        if let Some(first_index) = self.engine.first_index(namespace_id) {
            ensure!(
                entry_id >= first_index,
                error::OverrideCompactedEntrySnafu {
                    namespace: namespace_id,
                    first_index,
                    attempt_index: entry_id,
                }
            );
        }

        let _ = self
            .engine
            .write(&mut batch, self.config.sync_write)
            .context(RaftEngineSnafu)?;
        Ok(AppendResponse {
            last_entry_id: entry_id,
        })
    }

    /// Appends a batch of entries to logstore. `RaftEngineLogStore` assures the atomicity of
    /// batch append.
    async fn append_batch(&self, entries: Vec<Self::Entry>) -> Result<AppendBatchResponse> {
        ensure!(self.started(), IllegalStateSnafu);
        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }

        // Records the last entry id for each region's entries.
        let mut last_entry_ids: HashMap<NamespaceId, EntryId> =
            HashMap::with_capacity(entries.len());
        let mut batch = LogBatch::with_capacity(entries.len());

        for e in entries {
            self.check_entry(&e)?;
            // For raft-engine log store, the namespace id is the region id.
            let ns_id = e.namespace_id;
            last_entry_ids
                .entry(ns_id)
                .and_modify(|x| *x = (*x).max(e.id))
                .or_insert(e.id);
            batch
                .add_entries::<MessageType>(ns_id, &[e])
                .context(AddEntryLogBatchSnafu)?;
        }

        let _ = self
            .engine
            .write(&mut batch, self.config.sync_write)
            .context(RaftEngineSnafu)?;

        Ok(AppendBatchResponse { last_entry_ids })
    }

    /// Create a stream of entries from logstore in the given namespace. The end of stream is
    /// determined by the current "last index" of the namespace.
    async fn read(
        &self,
        ns: &Self::Namespace,
        entry_id: EntryId,
    ) -> Result<SendableEntryStream<'_, Self::Entry, Self::Error>> {
        ensure!(self.started(), IllegalStateSnafu);
        let engine = self.engine.clone();

        let last_index = engine.last_index(ns.id()).unwrap_or(0);
        let mut start_index = entry_id.max(engine.first_index(ns.id()).unwrap_or(last_index + 1));

        info!(
            "Read logstore, namespace: {}, start: {}, span: {:?}",
            ns.id(),
            entry_id,
            self.span(ns)
        );
        let max_batch_size = self.config.read_batch_size;
        let (tx, mut rx) = tokio::sync::mpsc::channel(max_batch_size);
        let ns = ns.clone();
        let _handle = common_runtime::spawn_read(async move {
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

    async fn create_namespace(&self, ns: &Self::Namespace) -> Result<()> {
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
        let _ = self
            .engine
            .write(&mut batch, true)
            .context(RaftEngineSnafu)?;
        Ok(())
    }

    async fn delete_namespace(&self, ns: &Self::Namespace) -> Result<()> {
        ensure!(
            ns.id != SYSTEM_NAMESPACE,
            IllegalNamespaceSnafu { ns: ns.id }
        );
        ensure!(self.started(), IllegalStateSnafu);
        let key = format!("{}{}", NAMESPACE_PREFIX, ns.id).as_bytes().to_vec();
        let mut batch = LogBatch::with_capacity(1);
        batch.delete(SYSTEM_NAMESPACE, key);
        let _ = self
            .engine
            .write(&mut batch, true)
            .context(RaftEngineSnafu)?;
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>> {
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

    fn entry<D: AsRef<[u8]>>(
        &self,
        data: D,
        entry_id: EntryId,
        ns: Self::Namespace,
    ) -> Self::Entry {
        EntryImpl {
            id: entry_id,
            data: data.as_ref().to_vec(),
            namespace_id: ns.id(),
            ..Default::default()
        }
    }

    fn namespace(&self, ns_id: NamespaceId, wal_options: &WalOptions) -> Self::Namespace {
        let _ = wal_options;
        Namespace {
            id: ns_id,
            ..Default::default()
        }
    }

    async fn obsolete(&self, ns: Self::Namespace, entry_id: EntryId) -> Result<()> {
        ensure!(self.started(), IllegalStateSnafu);
        let obsoleted = self.engine.compact_to(ns.id(), entry_id + 1);
        info!(
            "Namespace {} obsoleted {} entries, compacted index: {}, span: {:?}",
            ns.id(),
            obsoleted,
            entry_id,
            self.span(&ns)
        );
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct MessageType;

impl MessageExt for MessageType {
    type Entry = EntryImpl;

    fn index(e: &Self::Entry) -> u64 {
        e.id
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use common_base::readable_size::ReadableSize;
    use common_telemetry::debug;
    use common_test_util::temp_dir::{create_temp_dir, TempDir};
    use futures_util::StreamExt;
    use store_api::logstore::entry_stream::SendableEntryStream;
    use store_api::logstore::namespace::Namespace as NamespaceTrait;
    use store_api::logstore::LogStore;

    use super::*;
    use crate::error::Error;
    use crate::raft_engine::log_store::RaftEngineLogStore;
    use crate::raft_engine::protos::logstore::{EntryImpl as Entry, NamespaceImpl as Namespace};

    #[tokio::test]
    async fn test_open_logstore() {
        let dir = create_temp_dir("raft-engine-logstore-test");
        let logstore = RaftEngineLogStore::try_new(
            dir.path().to_str().unwrap().to_string(),
            RaftEngineConfig::default(),
        )
        .await
        .unwrap();
        let namespaces = logstore.list_namespaces().await.unwrap();
        assert_eq!(0, namespaces.len());
    }

    #[tokio::test]
    async fn test_manage_namespace() {
        let dir = create_temp_dir("raft-engine-logstore-test");
        let logstore = RaftEngineLogStore::try_new(
            dir.path().to_str().unwrap().to_string(),
            RaftEngineConfig::default(),
        )
        .await
        .unwrap();
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
        let dir = create_temp_dir("raft-engine-logstore-test");
        let logstore = RaftEngineLogStore::try_new(
            dir.path().to_str().unwrap().to_string(),
            RaftEngineConfig::default(),
        )
        .await
        .unwrap();

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
            assert_eq!(i, response.last_entry_id);
        }
        let mut entries = HashSet::with_capacity(1024);
        let mut s = logstore.read(&Namespace::with_id(1), 0).await.unwrap();
        while let Some(r) = s.next().await {
            let vec = r.unwrap();
            entries.extend(vec.into_iter().map(|e| e.id));
        }
        assert_eq!((0..cnt).collect::<HashSet<_>>(), entries);
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
        let dir = create_temp_dir("raft-engine-logstore-reopen-test");
        {
            let logstore = RaftEngineLogStore::try_new(
                dir.path().to_str().unwrap().to_string(),
                RaftEngineConfig::default(),
            )
            .await
            .unwrap();
            assert!(logstore
                .append(Entry::create(1, 1, "1".as_bytes().to_vec()))
                .await
                .is_ok());
            let entries = logstore
                .read(&Namespace::with_id(1), 1)
                .await
                .unwrap()
                .collect::<Vec<_>>()
                .await;
            assert_eq!(1, entries.len());
            logstore.stop().await.unwrap();
        }

        let logstore = RaftEngineLogStore::try_new(
            dir.path().to_str().unwrap().to_string(),
            RaftEngineConfig::default(),
        )
        .await
        .unwrap();

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

    async fn new_test_log_store(dir: &TempDir) -> RaftEngineLogStore {
        let path = dir.path().to_str().unwrap().to_string();

        let config = RaftEngineConfig {
            file_size: ReadableSize::mb(2),
            purge_threshold: ReadableSize::mb(4),
            purge_interval: Duration::from_secs(5),
            ..Default::default()
        };

        RaftEngineLogStore::try_new(path, config).await.unwrap()
    }

    #[tokio::test]
    async fn test_compaction() {
        common_telemetry::init_default_ut_logging();
        let dir = create_temp_dir("raft-engine-logstore-test");
        let logstore = new_test_log_store(&dir).await;

        let namespace = Namespace::with_id(42);
        for id in 0..4096 {
            let entry = Entry::create(id, namespace.id(), [b'x'; 4096].to_vec());
            let _ = logstore.append(entry).await.unwrap();
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
        let dir = create_temp_dir("raft-engine-logstore-test");
        let logstore = new_test_log_store(&dir).await;

        let namespace = Namespace::with_id(42);
        for id in 0..1024 {
            let entry = Entry::create(id, namespace.id(), [b'x'; 4096].to_vec());
            let _ = logstore.append(entry).await.unwrap();
        }

        logstore.obsolete(namespace.clone(), 100).await.unwrap();
        assert_eq!(101, logstore.engine.first_index(namespace.id).unwrap());

        let res = logstore.read(&namespace, 100).await.unwrap();
        let mut vec = collect_entries(res).await;
        vec.sort_by(|a, b| a.id.partial_cmp(&b.id).unwrap());
        assert_eq!(101, vec.first().unwrap().id);
    }

    #[tokio::test]
    async fn test_append_batch() {
        common_telemetry::init_default_ut_logging();
        let dir = create_temp_dir("logstore-append-batch-test");
        let logstore = new_test_log_store(&dir).await;

        let entries = (0..8)
            .flat_map(|ns_id| {
                let data = [ns_id as u8].repeat(4096);
                (0..16).map(move |idx| Entry::create(idx, ns_id, data.clone()))
            })
            .collect();

        logstore.append_batch(entries).await.unwrap();
        for ns_id in 0..8 {
            let namespace = Namespace::with_id(ns_id);
            let (first, last) = logstore.span(&namespace);
            assert_eq!(0, first.unwrap());
            assert_eq!(15, last.unwrap());
        }
    }

    #[tokio::test]
    async fn test_append_batch_interleaved() {
        common_telemetry::init_default_ut_logging();
        let dir = create_temp_dir("logstore-append-batch-test");
        let logstore = new_test_log_store(&dir).await;

        let entries = vec![
            Entry::create(0, 0, [b'0'; 4096].to_vec()),
            Entry::create(1, 0, [b'0'; 4096].to_vec()),
            Entry::create(0, 1, [b'1'; 4096].to_vec()),
            Entry::create(2, 0, [b'0'; 4096].to_vec()),
            Entry::create(1, 1, [b'1'; 4096].to_vec()),
        ];

        logstore.append_batch(entries).await.unwrap();

        assert_eq!((Some(0), Some(2)), logstore.span(&Namespace::with_id(0)));
        assert_eq!((Some(0), Some(1)), logstore.span(&Namespace::with_id(1)));
    }

    #[tokio::test]
    async fn test_append_batch_response() {
        common_telemetry::init_default_ut_logging();
        let dir = create_temp_dir("logstore-append-batch-test");
        let logstore = new_test_log_store(&dir).await;

        let entries = vec![
            // Entry[0] from region 0.
            Entry::create(0, 0, [b'0'; 4096].to_vec()),
            // Entry[0] from region 1.
            Entry::create(0, 1, [b'1'; 4096].to_vec()),
            // Entry[1] from region 1.
            Entry::create(1, 0, [b'1'; 4096].to_vec()),
            // Entry[1] from region 0.
            Entry::create(1, 1, [b'0'; 4096].to_vec()),
            // Entry[2] from region 2.
            Entry::create(2, 2, [b'2'; 4096].to_vec()),
        ];

        // Ensure the last entry id returned for each region is the expected one.
        let last_entry_ids = logstore.append_batch(entries).await.unwrap().last_entry_ids;
        assert_eq!(last_entry_ids[&0], 1);
        assert_eq!(last_entry_ids[&1], 1);
        assert_eq!(last_entry_ids[&2], 2);
    }
}
