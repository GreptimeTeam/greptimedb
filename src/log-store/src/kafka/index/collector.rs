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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common_telemetry::{error, info};
use futures::future::try_join_all;
use object_store::ErrorKind;
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::logstore::provider::KafkaProvider;
use store_api::logstore::EntryId;
use store_api::storage::RegionId;
use tokio::select;
use tokio::sync::mpsc::Sender;
use tokio::sync::Mutex as TokioMutex;

use crate::error::{self, Result};
use crate::kafka::index::encoder::{DatanodeWalIndexes, IndexEncoder};
use crate::kafka::index::{default_index_file, JsonIndexEncoder};
use crate::kafka::worker::{DumpIndexRequest, TruncateIndexRequest, WorkerRequest};

/// The [`IndexCollector`] trait defines the operations for managing and collecting index entries.
pub trait IndexCollector: Send + Sync {
    /// Appends an [`EntryId`] for a specific region.
    fn append(&mut self, region_id: RegionId, entry_id: EntryId);

    /// Truncates the index for a specific region up to a given [`EntryId`].
    ///
    /// It removes all [`EntryId`]s smaller than `entry_id`.
    fn truncate(&mut self, region_id: RegionId, entry_id: EntryId);

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
    _handle: CollectionTaskHandle,
}

#[derive(Debug, Clone)]
pub struct CollectionTask {
    providers: Arc<TokioMutex<HashMap<Arc<KafkaProvider>, Sender<WorkerRequest>>>>,
    dump_index_interval: Duration,
    operator: object_store::ObjectStore,
    path: String,
    running: Arc<AtomicBool>,
}

impl CollectionTask {
    async fn dump_index(&self) -> Result<()> {
        let encoder = Arc::new(JsonIndexEncoder::default());
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
        };
        let handle = task.run();
        Self {
            providers,
            operator,
            _handle: handle,
        }
    }

    #[cfg(test)]
    pub fn new_for_test(operator: object_store::ObjectStore) -> Self {
        Self {
            providers: Default::default(),
            operator,
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
    pub(crate) async fn provider_level_index_collector(
        &self,
        provider: Arc<KafkaProvider>,
        sender: Sender<WorkerRequest>,
    ) -> Box<dyn IndexCollector> {
        self.providers.lock().await.insert(provider.clone(), sender);
        Box::new(ProviderLevelIndexCollector {
            indexes: Default::default(),
            provider,
        })
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
        if let Some(sender) = self.providers.lock().await.get(provider).cloned() {
            if sender
                .send(WorkerRequest::TruncateIndex(TruncateIndexRequest::new(
                    region_id, entry_id,
                )))
                .await
                .is_err()
            {
                return error::OrderedBatchProducerStoppedSnafu {}.fail();
            }
        }

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

    fn set_latest_entry_id(&mut self, _entry_id: EntryId) {}

    fn dump(&mut self, _encoder: &dyn IndexEncoder) {}
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use store_api::logstore::provider::KafkaProvider;
    use store_api::storage::RegionId;

    use crate::kafka::index::collector::RegionIndexes;
    use crate::kafka::index::encoder::IndexEncoder;
    use crate::kafka::index::JsonIndexEncoder;
    use crate::kafka::{default_index_file, GlobalIndexCollector};

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
