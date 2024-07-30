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

use std::collections::{hash_map, HashMap};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use async_stream::stream;
use common_runtime::{RepeatedTask, TaskFunction};
use common_telemetry::{error, info};
use common_wal::config::raft_engine::RaftEngineConfig;
use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize, RecoveryMode};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::logstore::entry::{Entry, Id as EntryId, NaiveEntry};
use store_api::logstore::provider::{Provider, RaftEngineProvider};
use store_api::logstore::{AppendBatchResponse, LogStore, SendableEntryStream};
use store_api::storage::RegionId;

use crate::error::{
    AddEntryLogBatchSnafu, DiscontinuousLogIndexSnafu, Error, FetchEntrySnafu,
    IllegalNamespaceSnafu, IllegalStateSnafu, InvalidProviderSnafu, OverrideCompactedEntrySnafu,
    RaftEngineSnafu, Result, StartGcTaskSnafu, StopGcTaskSnafu,
};
use crate::metrics;
use crate::raft_engine::backend::SYSTEM_NAMESPACE;
use crate::raft_engine::protos::logstore::{EntryImpl, NamespaceImpl};

const NAMESPACE_PREFIX: &str = "$sys/";

pub struct RaftEngineLogStore {
    config: RaftEngineConfig,
    engine: Arc<Engine>,
    gc_task: RepeatedTask<Error>,
    last_sync_time: AtomicI64,
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
            enable_log_recycle: config.enable_log_recycle,
            prefill_for_recycle: config.prefill_log_files,
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
            last_sync_time: AtomicI64::new(0),
        };
        log_store.start()?;
        Ok(log_store)
    }

    pub fn started(&self) -> bool {
        self.gc_task.started()
    }

    fn start(&self) -> Result<()> {
        self.gc_task
            .start(common_runtime::global_runtime())
            .context(StartGcTaskSnafu)
    }

    fn span(&self, provider: &RaftEngineProvider) -> (Option<u64>, Option<u64>) {
        (
            self.engine.first_index(provider.id),
            self.engine.last_index(provider.id),
        )
    }

    /// Converts entries to `LogBatch` and checks if entry ids are valid.
    /// Returns the `LogBatch` converted along with the last entry id
    /// to append in each namespace(region).
    fn entries_to_batch(
        &self,
        entries: Vec<Entry>,
    ) -> Result<(LogBatch, HashMap<RegionId, EntryId>)> {
        // Records the last entry id for each region's entries.
        let mut entry_ids: HashMap<RegionId, EntryId> = HashMap::with_capacity(entries.len());
        let mut batch = LogBatch::with_capacity(entries.len());

        for e in entries {
            let region_id = e.region_id();
            let entry_id = e.entry_id();
            match entry_ids.entry(region_id) {
                hash_map::Entry::Occupied(mut o) => {
                    let prev = *o.get();
                    ensure!(
                        entry_id == prev + 1,
                        DiscontinuousLogIndexSnafu {
                            region_id,
                            last_index: prev,
                            attempt_index: entry_id
                        }
                    );
                    o.insert(entry_id);
                }
                hash_map::Entry::Vacant(v) => {
                    // this entry is the first in batch of given region.
                    if let Some(first_index) = self.engine.first_index(region_id.as_u64()) {
                        // ensure the first in batch does not override compacted entry.
                        ensure!(
                            entry_id > first_index,
                            OverrideCompactedEntrySnafu {
                                namespace: region_id,
                                first_index,
                                attempt_index: entry_id,
                            }
                        );
                    }
                    // ensure the first in batch does not form a hole in raft-engine.
                    if let Some(last_index) = self.engine.last_index(region_id.as_u64()) {
                        ensure!(
                            entry_id == last_index + 1,
                            DiscontinuousLogIndexSnafu {
                                region_id,
                                last_index,
                                attempt_index: entry_id
                            }
                        );
                    }
                    v.insert(entry_id);
                }
            }
            batch
                .add_entries::<MessageType>(
                    region_id.as_u64(),
                    &[EntryImpl {
                        id: entry_id,
                        namespace_id: region_id.as_u64(),
                        data: e.into_bytes(),
                        ..Default::default()
                    }],
                )
                .context(AddEntryLogBatchSnafu)?;
        }

        Ok((batch, entry_ids))
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

    async fn stop(&self) -> Result<()> {
        self.gc_task.stop().await.context(StopGcTaskSnafu)
    }

    /// Appends a batch of entries to logstore. `RaftEngineLogStore` assures the atomicity of
    /// batch append.
    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse> {
        metrics::METRIC_RAFT_ENGINE_APPEND_BATCH_CALLS_TOTAL.inc();
        metrics::METRIC_RAFT_ENGINE_APPEND_BATCH_BYTES_TOTAL.inc_by(
            entries
                .iter()
                .map(|entry| entry.estimated_size())
                .sum::<usize>() as u64,
        );
        let _timer = metrics::METRIC_RAFT_ENGINE_APPEND_BATCH_ELAPSED.start_timer();

        ensure!(self.started(), IllegalStateSnafu);
        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }

        let (mut batch, last_entry_ids) = self.entries_to_batch(entries)?;

        let mut sync = self.config.sync_write;

        if let Some(sync_period) = &self.config.sync_period {
            let now = common_time::util::current_time_millis();
            if now - self.last_sync_time.load(Ordering::Relaxed) >= sync_period.as_millis() as i64 {
                self.last_sync_time.store(now, Ordering::Relaxed);
                sync = true;
            }
        }

        let _ = self
            .engine
            .write(&mut batch, sync)
            .context(RaftEngineSnafu)?;

        Ok(AppendBatchResponse { last_entry_ids })
    }

    /// Create a stream of entries from logstore in the given namespace. The end of stream is
    /// determined by the current "last index" of the namespace.
    async fn read(
        &self,
        provider: &Provider,
        entry_id: EntryId,
    ) -> Result<SendableEntryStream<'static, Entry, Self::Error>> {
        let ns = provider
            .as_raft_engine_provider()
            .with_context(|| InvalidProviderSnafu {
                expected: RaftEngineProvider::type_name(),
                actual: provider.type_name(),
            })?;
        let namespace_id = ns.id;
        metrics::METRIC_RAFT_ENGINE_READ_CALLS_TOTAL.inc();
        let _timer = metrics::METRIC_RAFT_ENGINE_READ_ELAPSED.start_timer();

        ensure!(self.started(), IllegalStateSnafu);
        let engine = self.engine.clone();

        let last_index = engine.last_index(namespace_id).unwrap_or(0);
        let mut start_index =
            entry_id.max(engine.first_index(namespace_id).unwrap_or(last_index + 1));

        info!(
            "Read logstore, namespace: {}, start: {}, span: {:?}",
            namespace_id,
            entry_id,
            self.span(ns)
        );
        let max_batch_size = self.config.read_batch_size;
        let (tx, mut rx) = tokio::sync::mpsc::channel(max_batch_size);
        let _handle = common_runtime::spawn_global(async move {
            while start_index <= last_index {
                let mut vec = Vec::with_capacity(max_batch_size);
                match engine
                    .fetch_entries_to::<MessageType>(
                        namespace_id,
                        start_index,
                        last_index + 1,
                        Some(max_batch_size),
                        &mut vec,
                    )
                    .context(FetchEntrySnafu {
                        ns: namespace_id,
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
                let res = res?;

                yield Ok(res.into_iter().map(Entry::from).collect::<Vec<_>>());
            }
        });
        Ok(Box::pin(s))
    }

    async fn create_namespace(&self, ns: &Provider) -> Result<()> {
        let ns = ns
            .as_raft_engine_provider()
            .with_context(|| InvalidProviderSnafu {
                expected: RaftEngineProvider::type_name(),
                actual: ns.type_name(),
            })?;
        let namespace_id = ns.id;
        ensure!(
            namespace_id != SYSTEM_NAMESPACE,
            IllegalNamespaceSnafu { ns: namespace_id }
        );
        ensure!(self.started(), IllegalStateSnafu);
        let key = format!("{}{}", NAMESPACE_PREFIX, namespace_id)
            .as_bytes()
            .to_vec();
        let mut batch = LogBatch::with_capacity(1);
        batch
            .put_message::<NamespaceImpl>(
                SYSTEM_NAMESPACE,
                key,
                &NamespaceImpl {
                    id: namespace_id,
                    ..Default::default()
                },
            )
            .context(RaftEngineSnafu)?;
        let _ = self
            .engine
            .write(&mut batch, true)
            .context(RaftEngineSnafu)?;
        Ok(())
    }

    async fn delete_namespace(&self, ns: &Provider) -> Result<()> {
        let ns = ns
            .as_raft_engine_provider()
            .with_context(|| InvalidProviderSnafu {
                expected: RaftEngineProvider::type_name(),
                actual: ns.type_name(),
            })?;
        let namespace_id = ns.id;
        ensure!(
            namespace_id != SYSTEM_NAMESPACE,
            IllegalNamespaceSnafu { ns: namespace_id }
        );
        ensure!(self.started(), IllegalStateSnafu);
        let key = format!("{}{}", NAMESPACE_PREFIX, namespace_id)
            .as_bytes()
            .to_vec();
        let mut batch = LogBatch::with_capacity(1);
        batch.delete(SYSTEM_NAMESPACE, key);
        let _ = self
            .engine
            .write(&mut batch, true)
            .context(RaftEngineSnafu)?;
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<Provider>> {
        ensure!(self.started(), IllegalStateSnafu);
        let mut namespaces: Vec<Provider> = vec![];
        self.engine
            .scan_messages::<NamespaceImpl, _>(
                SYSTEM_NAMESPACE,
                Some(NAMESPACE_PREFIX.as_bytes()),
                None,
                false,
                |_, v| {
                    namespaces.push(Provider::RaftEngine(RaftEngineProvider { id: v.id }));
                    true
                },
            )
            .context(RaftEngineSnafu)?;
        Ok(namespaces)
    }

    fn entry(
        &self,
        data: &mut Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry> {
        debug_assert_eq!(
            provider.as_raft_engine_provider().unwrap().id,
            region_id.as_u64()
        );
        Ok(Entry::Naive(NaiveEntry {
            provider: provider.clone(),
            region_id,
            entry_id,
            data: std::mem::take(data),
        }))
    }

    async fn obsolete(&self, provider: &Provider, entry_id: EntryId) -> Result<()> {
        let ns = provider
            .as_raft_engine_provider()
            .with_context(|| InvalidProviderSnafu {
                expected: RaftEngineProvider::type_name(),
                actual: provider.type_name(),
            })?;
        let namespace_id = ns.id;
        ensure!(self.started(), IllegalStateSnafu);
        let obsoleted = self.engine.compact_to(namespace_id, entry_id + 1);
        info!(
            "Namespace {} obsoleted {} entries, compacted index: {}, span: {:?}",
            namespace_id,
            obsoleted,
            entry_id,
            self.span(ns)
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
impl RaftEngineLogStore {
    /// Appends a batch of entries and returns a response containing a map where the key is a region id
    /// while the value is the id of the last successfully written entry of the region.
    async fn append(&self, entry: Entry) -> Result<store_api::logstore::AppendResponse> {
        let response = self.append_batch(vec![entry]).await?;
        if let Some((_, last_entry_id)) = response.last_entry_ids.into_iter().next() {
            return Ok(store_api::logstore::AppendResponse { last_entry_id });
        }
        unreachable!()
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
    use store_api::logstore::{LogStore, SendableEntryStream};

    use super::*;
    use crate::error::Error;
    use crate::raft_engine::log_store::RaftEngineLogStore;
    use crate::raft_engine::protos::logstore::EntryImpl;

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
            .create_namespace(&Provider::raft_engine_provider(42))
            .await
            .unwrap();
        let namespaces = logstore.list_namespaces().await.unwrap();
        assert_eq!(1, namespaces.len());
        assert_eq!(Provider::raft_engine_provider(42), namespaces[0]);

        logstore
            .delete_namespace(&Provider::raft_engine_provider(42))
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

        let namespace_id = 1;
        let cnt = 1024;
        for i in 0..cnt {
            let response = logstore
                .append(
                    EntryImpl::create(i, namespace_id, i.to_string().as_bytes().to_vec()).into(),
                )
                .await
                .unwrap();
            assert_eq!(i, response.last_entry_id);
        }
        let mut entries = HashSet::with_capacity(1024);
        let mut s = logstore
            .read(&Provider::raft_engine_provider(1), 0)
            .await
            .unwrap();
        while let Some(r) = s.next().await {
            let vec = r.unwrap();
            entries.extend(vec.into_iter().map(|e| e.entry_id()));
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
                .append(EntryImpl::create(1, 1, "1".as_bytes().to_vec()).into())
                .await
                .is_ok());
            let entries = logstore
                .read(&Provider::raft_engine_provider(1), 1)
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

        let entries = collect_entries(
            logstore
                .read(&Provider::raft_engine_provider(1), 1)
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(1, entries.len());
        assert_eq!(1, entries[0].entry_id());
        assert_eq!(1, entries[0].region_id().as_u64());
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

        let namespace_id = 42;
        let namespace = Provider::raft_engine_provider(namespace_id);
        for id in 0..4096 {
            let entry = EntryImpl::create(id, namespace_id, [b'x'; 4096].to_vec()).into();
            let _ = logstore.append(entry).await.unwrap();
        }

        let before_purge = wal_dir_usage(dir.path().to_str().unwrap()).await;
        logstore.obsolete(&namespace, 4000).await.unwrap();

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

        let namespace_id = 42;
        let namespace = Provider::raft_engine_provider(namespace_id);
        for id in 0..1024 {
            let entry = EntryImpl::create(id, namespace_id, [b'x'; 4096].to_vec()).into();
            let _ = logstore.append(entry).await.unwrap();
        }

        logstore.obsolete(&namespace, 100).await.unwrap();
        assert_eq!(101, logstore.engine.first_index(namespace_id).unwrap());

        let res = logstore.read(&namespace, 100).await.unwrap();
        let mut vec = collect_entries(res).await;
        vec.sort_by(|a, b| a.entry_id().partial_cmp(&b.entry_id()).unwrap());
        assert_eq!(101, vec.first().unwrap().entry_id());
    }

    #[tokio::test]
    async fn test_append_batch() {
        common_telemetry::init_default_ut_logging();
        let dir = create_temp_dir("logstore-append-batch-test");
        let logstore = new_test_log_store(&dir).await;

        let entries = (0..8)
            .flat_map(|ns_id| {
                let data = [ns_id as u8].repeat(4096);
                (0..16).map(move |idx| EntryImpl::create(idx, ns_id, data.clone()).into())
            })
            .collect();

        logstore.append_batch(entries).await.unwrap();
        for ns_id in 0..8 {
            let namespace = &RaftEngineProvider::new(ns_id);
            let (first, last) = logstore.span(namespace);
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
            EntryImpl::create(0, 0, [b'0'; 4096].to_vec()).into(),
            EntryImpl::create(1, 0, [b'0'; 4096].to_vec()).into(),
            EntryImpl::create(0, 1, [b'1'; 4096].to_vec()).into(),
            EntryImpl::create(2, 0, [b'0'; 4096].to_vec()).into(),
            EntryImpl::create(1, 1, [b'1'; 4096].to_vec()).into(),
        ];

        logstore.append_batch(entries).await.unwrap();

        assert_eq!(
            (Some(0), Some(2)),
            logstore.span(&RaftEngineProvider::new(0))
        );
        assert_eq!(
            (Some(0), Some(1)),
            logstore.span(&RaftEngineProvider::new(1))
        );
    }

    #[tokio::test]
    async fn test_append_batch_response() {
        common_telemetry::init_default_ut_logging();
        let dir = create_temp_dir("logstore-append-batch-test");
        let logstore = new_test_log_store(&dir).await;

        let entries = vec![
            // Entry[0] from region 0.
            EntryImpl::create(0, 0, [b'0'; 4096].to_vec()).into(),
            // Entry[0] from region 1.
            EntryImpl::create(0, 1, [b'1'; 4096].to_vec()).into(),
            // Entry[1] from region 1.
            EntryImpl::create(1, 0, [b'1'; 4096].to_vec()).into(),
            // Entry[1] from region 0.
            EntryImpl::create(1, 1, [b'0'; 4096].to_vec()).into(),
            // Entry[2] from region 2.
            EntryImpl::create(2, 2, [b'2'; 4096].to_vec()).into(),
        ];

        // Ensure the last entry id returned for each region is the expected one.
        let last_entry_ids = logstore.append_batch(entries).await.unwrap().last_entry_ids;
        assert_eq!(last_entry_ids[&(0.into())], 1);
        assert_eq!(last_entry_ids[&(1.into())], 1);
        assert_eq!(last_entry_ids[&(2.into())], 2);
    }
}
