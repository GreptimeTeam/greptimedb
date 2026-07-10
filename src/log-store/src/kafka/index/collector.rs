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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common_telemetry::{error, info};
use futures::future::try_join_all;
use object_store::ErrorKind;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::logstore::EntryId;
use store_api::logstore::provider::KafkaProvider;
use store_api::storage::RegionId;
use tokio::select;
use tokio::sync::Mutex as TokioMutex;
use tokio::sync::mpsc::Sender;

use crate::error::{self, Result};
use crate::kafka::index::encoder::{DatanodeWalIndexes, IndexEncoder};
use crate::kafka::index::{JsonIndexEncoder, default_index_file};
use crate::kafka::worker::{
    DumpIndexRequest, TruncateAllIndexRequest, TruncateIndexRequest, WorkerRequest,
};

/// The [`IndexCollector`] trait defines the operations for managing and collecting index entries.
pub trait IndexCollector: Send + Sync {
    /// Appends an [`EntryId`] for a specific region.
    fn append(&mut self, region_id: RegionId, entry_id: EntryId);

    /// Truncates the index for a specific region up to a given [`EntryId`].
    ///
    /// It removes all [`EntryId`]s smaller than `entry_id`.
    fn truncate(&mut self, region_id: RegionId, entry_id: EntryId);

    /// Removes all entry ids for a specific region.
    fn truncate_all(&mut self, region_id: RegionId);

    /// Sets the latest [`EntryId`].
    fn set_latest_entry_id(&mut self, entry_id: EntryId);

    /// Dumps the index.
    fn dump(&mut self, encoder: &dyn IndexEncoder);
}

/// The [`GlobalIndexCollector`] struct is responsible for managing index entries
/// across multiple providers.
#[derive(Debug)]
pub struct GlobalIndexCollector {
    providers: Arc<TokioMutex<HashMap<Arc<KafkaProvider>, Sender<WorkerRequest>>>>,
    operator: object_store::ObjectStore,
    task: CollectionTask,
    _handle: CollectionTaskHandle,
}

#[derive(Debug, Clone)]
pub struct CollectionTask {
    providers: Arc<TokioMutex<HashMap<Arc<KafkaProvider>, Sender<WorkerRequest>>>>,
    dump_index_interval: Duration,
    operator: object_store::ObjectStore,
    path: String,
    running: Arc<AtomicBool>,
    dump_lock: Arc<TokioMutex<()>>,
}

impl CollectionTask {
    async fn read_index(&self) -> Result<DatanodeWalIndexes> {
        match self.operator.read(&self.path).await {
            Ok(bytes) => DatanodeWalIndexes::decode(bytes.to_bytes().as_ref()),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(DatanodeWalIndexes::default()),
            Err(err) => Err(err).context(error::ReadIndexSnafu {
                path: self.path.clone(),
            }),
        }
    }

    async fn dump_index(&self) -> Result<()> {
        let _guard = self.dump_lock.lock().await;
        let encoder = Arc::new(JsonIndexEncoder::with_indexes(self.read_index().await?));
        let receivers = {
            let providers = self.providers.lock().await;
            let mut receivers = Vec::with_capacity(providers.len());
            for (provider, sender) in providers.iter() {
                let (req, rx) = DumpIndexRequest::new(encoder.clone());
                receivers.push(rx);
                if sender.send(WorkerRequest::DumpIndex(req)).await.is_err() {
                    error!(
                        "BackgroundProducerWorker is stopped, topic: {}",
                        provider.topic
                    )
                }
            }
            receivers
        };
        try_join_all(receivers)
            .await
            .context(error::WaitDumpIndexSnafu)?;
        let bytes = encoder.finish()?;
        let mut writer = self
            .operator
            .writer(&self.path)
            .await
            .context(error::CreateWriterSnafu)?;
        writer.write(bytes).await.context(error::WriteIndexSnafu)?;
        writer.close().await.context(error::WriteIndexSnafu)?;

        Ok(())
    }

    /// The background task performs two main operations:
    /// - Persists the WAL index to the specified `path` at every `dump_index_interval`.
    /// - Updates the latest index ID for each WAL provider at every `checkpoint_interval`.
    fn run(self) -> CollectionTaskHandle {
        let mut dump_index_interval = tokio::time::interval(self.dump_index_interval);
        let running = self.running.clone();
        let moved_self = self.clone();
        common_runtime::spawn_global(async move {
            loop {
                if !running.load(Ordering::Relaxed) {
                    info!("shutdown the index collection task");
                    break;
                }
                select! {
                    _ = dump_index_interval.tick() => {
                        if let Err(err) = moved_self.dump_index().await {
                            error!(err; "Failed to persist the WAL index");
                        }
                    },
                }
            }
        });
        CollectionTaskHandle {
            running: self.running.clone(),
        }
    }
}

impl Drop for CollectionTaskHandle {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Relaxed);
    }
}

#[derive(Debug, Default)]
struct CollectionTaskHandle {
    running: Arc<AtomicBool>,
}

impl GlobalIndexCollector {
    /// Constructs a [`GlobalIndexCollector`].
    ///
    /// This method initializes a `GlobalIndexCollector` instance and starts a background task
    /// for managing WAL (Write-Ahead Logging) indexes.
    ///
    /// The background task persists the WAL index to the specified `path` at every `dump_index_interval`.
    pub fn new(
        dump_index_interval: Duration,
        operator: object_store::ObjectStore,
        path: String,
    ) -> Self {
        let providers: Arc<TokioMutex<HashMap<Arc<KafkaProvider>, Sender<WorkerRequest>>>> =
            Arc::new(Default::default());
        let task = CollectionTask {
            providers: providers.clone(),
            dump_index_interval,
            operator: operator.clone(),
            path,
            running: Arc::new(AtomicBool::new(true)),
            dump_lock: Arc::new(TokioMutex::new(())),
        };
        let handle = task.clone().run();
        Self {
            providers,
            operator,
            task,
            _handle: handle,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(operator: object_store::ObjectStore) -> Self {
        let providers: Arc<TokioMutex<HashMap<Arc<KafkaProvider>, Sender<WorkerRequest>>>> =
            Default::default();
        let task = CollectionTask {
            providers: providers.clone(),
            dump_index_interval: Duration::from_secs(60),
            operator: operator.clone(),
            path: default_index_file(0),
            running: Arc::new(AtomicBool::new(false)),
            dump_lock: Arc::new(TokioMutex::new(())),
        };
        Self {
            providers,
            operator,
            task,
            _handle: Default::default(),
        }
    }
}

impl GlobalIndexCollector {
    /// Retrieve [`EntryId`]s for a specified `region_id` in `datanode_id`
    /// that are greater than or equal to a given `entry_id`.
    pub(crate) async fn read_remote_region_index(
        &self,
        location_id: u64,
        provider: &KafkaProvider,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<Option<(BTreeSet<EntryId>, EntryId)>> {
        let path = default_index_file(location_id);

        let bytes = match self.operator.read(&path).await {
            Ok(bytes) => bytes.to_vec(),
            Err(err) => {
                if err.kind() == ErrorKind::NotFound {
                    return Ok(None);
                } else {
                    return Err(err).context(error::ReadIndexSnafu { path });
                }
            }
        };

        match DatanodeWalIndexes::decode(&bytes)?.provider(provider) {
            Some(indexes) => {
                let last_index = indexes.last_index();
                let indexes = indexes
                    .region(region_id)
                    .unwrap_or_default()
                    .split_off(&entry_id);

                Ok(Some((indexes, last_index)))
            }
            None => Ok(None),
        }
    }

    /// Creates a new [`ProviderLevelIndexCollector`] for a specified provider.
    ///
    /// This is called when initializing a provider client, not for every WAL operation. It restores
    /// the provider's persisted indexes so a subsequent truncation preserves indexes belonging to
    /// other regions in the same topic. The dump lock keeps restoration consistent with concurrent
    /// index dumps.
    pub(crate) async fn provider_level_index_collector(
        &self,
        provider: Arc<KafkaProvider>,
        sender: Sender<WorkerRequest>,
    ) -> Result<Box<dyn IndexCollector>> {
        let indexes = {
            let _guard = self.task.dump_lock.lock().await;
            self.task
                .read_index()
                .await?
                .provider_region_indexes(&provider)
                .unwrap_or_default()
        };
        self.providers.lock().await.insert(provider.clone(), sender);
        Ok(Box::new(ProviderLevelIndexCollector { indexes, provider }))
    }

    /// Truncates the index for a specific region up to a given [`EntryId`].
    ///
    /// It removes all [`EntryId`]s smaller than `entry_id`.
    pub(crate) async fn truncate(
        &self,
        provider: &Arc<KafkaProvider>,
        region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<()> {
        if let Some(sender) = self.providers.lock().await.get(provider).cloned()
            && sender
                .send(WorkerRequest::TruncateIndex(TruncateIndexRequest::new(
                    region_id, entry_id,
                )))
                .await
                .is_err()
        {
            return error::OrderedBatchProducerStoppedSnafu {}.fail();
        }

        Ok(())
    }

    /// Removes all indexes for a specific region and waits until the provider worker applies it.
    pub(crate) async fn truncate_all(
        &self,
        provider: &Arc<KafkaProvider>,
        region_id: RegionId,
    ) -> Result<()> {
        let sender = self
            .providers
            .lock()
            .await
            .get(provider)
            .cloned()
            .context(error::OrderedBatchProducerStoppedSnafu {})?;
        let (request, receiver) = TruncateAllIndexRequest::new(region_id);
        if sender
            .send(WorkerRequest::TruncateAllIndex(request))
            .await
            .is_err()
        {
            return error::OrderedBatchProducerStoppedSnafu {}.fail();
        }
        receiver
            .await
            .context(error::WaitTruncateAllIndexSnafu {})?;
        self.task.dump_index().await?;
        Ok(())
    }
}

/// The [`RegionIndexes`] struct maintains indexes for a collection of regions.
/// Each region is identified by a `RegionId` and maps to a set of [`EntryId`]s,
/// representing the entries within that region. It also keeps track of the
/// latest [`EntryId`] across all regions.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegionIndexes {
    pub(crate) regions: HashMap<RegionId, BTreeSet<EntryId>>,
    pub(crate) latest_entry_id: EntryId,
}

impl RegionIndexes {
    fn append(&mut self, region_id: RegionId, entry_id: EntryId) {
        self.regions.entry(region_id).or_default().insert(entry_id);
        self.latest_entry_id = self.latest_entry_id.max(entry_id);
    }

    fn truncate(&mut self, region_id: RegionId, entry_id: EntryId) {
        if let Some(entry_ids) = self.regions.get_mut(&region_id) {
            *entry_ids = entry_ids.split_off(&entry_id);
            // The `RegionIndexes` can be empty, keeps to track the latest entry id.
            self.latest_entry_id = self.latest_entry_id.max(entry_id);
        }
    }

    fn truncate_all(&mut self, region_id: RegionId) {
        self.regions.remove(&region_id);
    }

    fn set_latest_entry_id(&mut self, entry_id: EntryId) {
        self.latest_entry_id = entry_id;
    }
}

/// The [`ProviderLevelIndexCollector`] struct is responsible for managing index entries
/// specific to a particular provider.
#[derive(Debug, Clone)]
pub struct ProviderLevelIndexCollector {
    indexes: RegionIndexes,
    provider: Arc<KafkaProvider>,
}

impl IndexCollector for ProviderLevelIndexCollector {
    fn append(&mut self, region_id: RegionId, entry_id: EntryId) {
        self.indexes.append(region_id, entry_id)
    }

    fn truncate(&mut self, region_id: RegionId, entry_id: EntryId) {
        self.indexes.truncate(region_id, entry_id)
    }

    fn truncate_all(&mut self, region_id: RegionId) {
        self.indexes.truncate_all(region_id)
    }

    fn set_latest_entry_id(&mut self, entry_id: EntryId) {
        self.indexes.set_latest_entry_id(entry_id);
    }

    fn dump(&mut self, encoder: &dyn IndexEncoder) {
        encoder.encode(&self.provider, &self.indexes)
    }
}

/// The [`NoopCollector`] struct implements the [`IndexCollector`] trait with no-op methods.
///
/// This collector effectively ignores all operations, making it suitable for cases
/// where index collection is not required or should be disabled.
pub struct NoopCollector;

impl IndexCollector for NoopCollector {
    fn append(&mut self, _region_id: RegionId, _entry_id: EntryId) {}

    fn truncate(&mut self, _region_id: RegionId, _entry_id: EntryId) {}

    fn truncate_all(&mut self, _region_id: RegionId) {}

    fn set_latest_entry_id(&mut self, _entry_id: EntryId) {}

    fn dump(&mut self, _encoder: &dyn IndexEncoder) {}
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};
    use std::sync::Arc;

    use store_api::logstore::provider::KafkaProvider;
    use store_api::storage::RegionId;
    use tokio::sync::mpsc;

    use crate::kafka::index::JsonIndexEncoder;
    use crate::kafka::index::collector::RegionIndexes;
    use crate::kafka::index::encoder::{DatanodeWalIndexes, IndexEncoder};
    use crate::kafka::worker::WorkerRequest;
    use crate::kafka::{GlobalIndexCollector, default_index_file};

    #[test]
    fn test_truncate_all_region_indexes() {
        let region_to_remove = RegionId::new(1, 1);
        let region_to_keep = RegionId::new(1, 2);
        let mut indexes = RegionIndexes {
            regions: HashMap::from([
                (region_to_remove, BTreeSet::from([1, 5, 15])),
                (region_to_keep, BTreeSet::from([2, 8, 20])),
            ]),
            latest_entry_id: 21,
        };

        indexes.truncate_all(region_to_remove);
        assert!(!indexes.regions.contains_key(&region_to_remove));
        assert_eq!(
            indexes.regions.get(&region_to_keep),
            Some(&BTreeSet::from([2, 8, 20]))
        );
        assert_eq!(21, indexes.latest_entry_id);

        indexes.truncate_all(region_to_remove);
        assert_eq!(21, indexes.latest_entry_id);
    }

    #[tokio::test]
    async fn test_global_collector_truncate_all_waits_for_worker() {
        let operator = object_store::ObjectStore::new(object_store::services::Memory::default())
            .unwrap()
            .finish();
        let collector = GlobalIndexCollector::new_for_test(operator);
        let provider = Arc::new(KafkaProvider::new("my_topic_0".to_string()));
        let (tx, mut rx) = mpsc::channel(8);
        let mut provider_collector = collector
            .provider_level_index_collector(provider.clone(), tx)
            .await
            .unwrap();
        let region_to_remove = RegionId::new(1, 1);
        let region_to_keep = RegionId::new(1, 2);
        provider_collector.append(region_to_remove, 1);
        provider_collector.append(region_to_remove, 5);
        provider_collector.append(region_to_keep, 2);
        provider_collector.set_latest_entry_id(6);

        let worker = tokio::spawn(async move {
            loop {
                match rx.recv().await.unwrap() {
                    WorkerRequest::TruncateAllIndex(request) => {
                        request.apply(provider_collector.as_mut())
                    }
                    WorkerRequest::DumpIndex(request) => {
                        request.apply(provider_collector.as_mut());
                        break;
                    }
                    _ => unreachable!(),
                }
            }
            provider_collector
        });

        collector
            .truncate_all(&provider, region_to_remove)
            .await
            .unwrap();

        let mut provider_collector = worker.await.unwrap();
        let encoder = JsonIndexEncoder::default();
        provider_collector.dump(&encoder);
        let indexes = DatanodeWalIndexes::decode(&encoder.finish().unwrap()).unwrap();
        let indexes = indexes.provider(&provider).unwrap();
        assert!(indexes.region(region_to_remove).is_none());
        assert_eq!(indexes.region(region_to_keep), Some(BTreeSet::from([2])));
        assert_eq!(indexes.last_index(), 6);
    }

    #[tokio::test]
    async fn test_global_collector_truncate_all_persists_shared_topic_indexes() {
        let operator = object_store::ObjectStore::new(object_store::services::Memory::default())
            .unwrap()
            .finish();
        let path = default_index_file(0);
        let provider = Arc::new(KafkaProvider::new("my_topic_0".to_string()));
        let region_to_remove = RegionId::new(1, 1);
        let region_to_keep = RegionId::new(1, 2);
        let encoder = JsonIndexEncoder::default();
        encoder.encode(
            &provider,
            &RegionIndexes {
                regions: HashMap::from([
                    (region_to_remove, BTreeSet::from([1, 5])),
                    (region_to_keep, BTreeSet::from([2, 6])),
                ]),
                latest_entry_id: 7,
            },
        );
        let mut writer = operator.writer(&path).await.unwrap();
        writer.write(encoder.finish().unwrap()).await.unwrap();
        writer.close().await.unwrap();

        let collector = GlobalIndexCollector::new_for_test(operator);
        let (tx, mut rx) = mpsc::channel(8);
        let mut provider_collector = collector
            .provider_level_index_collector(provider.clone(), tx)
            .await
            .unwrap();
        let worker = tokio::spawn(async move {
            loop {
                match rx.recv().await.unwrap() {
                    WorkerRequest::TruncateAllIndex(request) => {
                        request.apply(provider_collector.as_mut())
                    }
                    WorkerRequest::DumpIndex(request) => {
                        request.apply(provider_collector.as_mut());
                        break;
                    }
                    _ => unreachable!(),
                }
            }
        });

        collector
            .truncate_all(&provider, region_to_remove)
            .await
            .unwrap();
        worker.await.unwrap();

        let (removed, last_index) = collector
            .read_remote_region_index(0, &provider, region_to_remove, 0)
            .await
            .unwrap()
            .unwrap();
        let (kept, kept_last_index) = collector
            .read_remote_region_index(0, &provider, region_to_keep, 0)
            .await
            .unwrap()
            .unwrap();
        assert!(removed.is_empty());
        assert_eq!(kept, BTreeSet::from([2, 6]));
        assert_eq!(last_index, 7);
        assert_eq!(kept_last_index, 7);
    }

    #[tokio::test]
    async fn test_read_remote_region_index() {
        let operator = object_store::ObjectStore::new(object_store::services::Memory::default())
            .unwrap()
            .finish();

        let path = default_index_file(0);
        let encoder = JsonIndexEncoder::default();
        encoder.encode(
            &KafkaProvider::new("my_topic_0".to_string()),
            &RegionIndexes {
                regions: HashMap::from([(RegionId::new(1, 1), BTreeSet::from([1, 5, 15]))]),
                latest_entry_id: 20,
            },
        );
        let bytes = encoder.finish().unwrap();
        let mut writer = operator.writer(&path).await.unwrap();
        writer.write(bytes).await.unwrap();
        writer.close().await.unwrap();

        let collector = GlobalIndexCollector::new_for_test(operator.clone());
        // Index file doesn't exist
        let result = collector
            .read_remote_region_index(
                1,
                &KafkaProvider::new("my_topic_0".to_string()),
                RegionId::new(1, 1),
                1,
            )
            .await
            .unwrap();
        assert!(result.is_none());

        // RegionId doesn't exist
        let (indexes, last_index) = collector
            .read_remote_region_index(
                0,
                &KafkaProvider::new("my_topic_0".to_string()),
                RegionId::new(1, 2),
                5,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(indexes, BTreeSet::new());
        assert_eq!(last_index, 20);

        // RegionId(1, 1), Start EntryId: 5
        let (indexes, last_index) = collector
            .read_remote_region_index(
                0,
                &KafkaProvider::new("my_topic_0".to_string()),
                RegionId::new(1, 1),
                5,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(indexes, BTreeSet::from([5, 15]));
        assert_eq!(last_index, 20);

        // RegionId(1, 1), Start EntryId: 20
        let (indexes, last_index) = collector
            .read_remote_region_index(
                0,
                &KafkaProvider::new("my_topic_0".to_string()),
                RegionId::new(1, 1),
                20,
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(indexes, BTreeSet::new());
        assert_eq!(last_index, 20);
    }
}
