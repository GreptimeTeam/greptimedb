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

//! [`NatsLogStore`] — full [`LogStore`] implementation backed by NATS JetStream.

use std::collections::HashMap;
use std::sync::Arc;

use common_telemetry::{debug, info};
use common_wal::config::nats::DatanodeNatsConfig;
use dashmap::DashMap;
use futures::future::try_join_all;
use snafu::OptionExt;
use store_api::logstore::entry::{Entry, Id as EntryId};
use store_api::logstore::provider::{NatsProvider, Provider};
use store_api::logstore::{AppendBatchResponse, LogStore, SendableEntryStream, WalIndex};
use store_api::storage::RegionId;

use crate::error::{self, InvalidProviderSnafu, Result};
use crate::nats::client::{NatsClient, NatsClientRef};
use crate::nats::consumer::read_entries;
use crate::nats::producer::{NatsBatchProducer, NatsBatchProducerRef};
use crate::nats::purge_worker::PurgeWorker;
use crate::nats::record::convert_to_nats_records;
use crate::nats::util::build_nats_entry;

/// A log store backed by NATS JetStream.
#[derive(Debug)]
pub struct NatsLogStore {
    /// Shared NATS connection, JetStream context, and stream handle.
    client: NatsClientRef,

    /// Maximum bytes per publish batch (includes NATS header overhead).
    max_batch_bytes: usize,

    /// Timeout waiting for a NATS publish acknowledgement.
    publish_ack_timeout: std::time::Duration,

    /// Per-subject WAL timeout when fetching from a pull consumer.
    consumer_wait_timeout: std::time::Duration,

    /// Cache of the latest committed sequence number per provider / subject.
    ///
    /// Updated on every successful `append_batch`. Read by `latest_entry_id`.
    subject_seq_cache: Arc<DashMap<Arc<NatsProvider>, u64>>,

    /// Pending purge watermarks.  `obsolete()` writes here; the background
    /// [`PurgeWorker`] reads and executes the actual NATS purge calls.
    pending_purge: Arc<DashMap<Arc<NatsProvider>, u64>>,

    /// Per-subject background producers (lazy, created on first use).
    producers: Arc<DashMap<Arc<NatsProvider>, NatsBatchProducerRef>>,
}

impl NatsLogStore {
    /// Creates a new [`NatsLogStore`] and starts the background purge worker.
    pub async fn try_new(config: &DatanodeNatsConfig) -> Result<Self> {
        let client = NatsClient::try_new(config).await?;
        let pending_purge: Arc<DashMap<Arc<NatsProvider>, u64>> = Arc::new(DashMap::new());

        // Start the background purge worker.
        PurgeWorker::new(client.clone(), pending_purge.clone(), config.purge_interval).run();

        info!(
            "NatsLogStore started: stream={}, servers={:?}",
            client.stream_name, config.servers
        );

        Ok(Self {
            client,
            max_batch_bytes: config.max_batch_bytes.as_bytes() as usize,
            publish_ack_timeout: config.publish_ack_timeout,
            consumer_wait_timeout: config.consumer_wait_timeout,
            subject_seq_cache: Arc::new(DashMap::new()),
            pending_purge,
            producers: Arc::new(DashMap::new()),
        })
    }

    /// Returns (or creates) the [`NatsBatchProducer`] for the given provider.
    fn get_or_create_producer(&self, provider: Arc<NatsProvider>) -> NatsBatchProducerRef {
        let client = self.client.clone();
        let timeout = self.publish_ack_timeout;
        self.producers
            .entry(provider.clone())
            .or_insert_with(|| {
                Arc::new(NatsBatchProducer::new(
                    provider.topic.clone(),
                    client,
                    timeout,
                ))
            })
            .clone()
    }
}

// ---------------------------------------------------------------------------
// LogStore impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl LogStore for NatsLogStore {
    type Error = error::Error;

    /// Creates an [`Entry`], splitting into `MultiplePart` if data exceeds the
    /// per-record payload limit.
    fn entry(
        &self,
        data: Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry> {
        provider
            .as_nats_provider()
            .with_context(|| InvalidProviderSnafu {
                expected: NatsProvider::type_name(),
                actual: provider.type_name(),
            })?;

        Ok(build_nats_entry(
            data,
            entry_id,
            region_id,
            provider,
            self.max_batch_bytes,
        ))
    }

    /// Publishes a batch of entries to NATS, one subject per region.
    ///
    /// Returns a map of `region_id → last committed NATS sequence number`.
    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse> {
        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }

        // Group records by provider / subject.
        let mut grouped: HashMap<Arc<NatsProvider>, (NatsBatchProducerRef, Vec<_>)> =
            HashMap::new();
        let mut provider_for_region: HashMap<RegionId, Arc<NatsProvider>> = HashMap::new();

        for entry in entries {
            let provider = entry
                .provider()
                .as_nats_provider()
                .with_context(|| InvalidProviderSnafu {
                    expected: NatsProvider::type_name(),
                    actual: entry.provider().type_name(),
                })?
                .clone();

            provider_for_region.insert(entry.region_id(), provider.clone());
            let records = convert_to_nats_records(entry)?;

            match grouped.entry(provider.clone()) {
                std::collections::hash_map::Entry::Occupied(mut slot) => {
                    slot.get_mut().1.extend(records);
                }
                std::collections::hash_map::Entry::Vacant(slot) => {
                    let producer = self.get_or_create_producer(provider);
                    slot.insert((producer, records));
                }
            }
        }

        // Fire all subjects concurrently, collect result handles.
        let mut handles = Vec::with_capacity(grouped.len());
        for (provider, (producer, records)) in grouped {
            let handle = producer.produce(records).await?;
            handles.push((provider, handle));
        }

        // Await all acks.
        let results = try_join_all(handles.into_iter().map(|(provider, handle)| async move {
            handle.wait().await.map(|seq| (provider, seq))
        }))
        .await?;

        // Update sequence cache and build last_entry_ids (per region).
        let mut last_entry_ids: HashMap<RegionId, EntryId> = HashMap::new();
        for (provider, seq) in &results {
            self.subject_seq_cache
                .entry(provider.clone())
                .and_modify(|e| *e = (*e).max(*seq))
                .or_insert(*seq);
        }
        // Map sequence back to region.
        for (region_id, provider) in provider_for_region {
            if let Some((_, seq)) = results.iter().find(|(p, _)| p == &provider) {
                last_entry_ids.insert(region_id, *seq);
            }
        }

        debug!("NATS append_batch: last_entry_ids={:?}", last_entry_ids);
        Ok(AppendBatchResponse { last_entry_ids })
    }

    /// Reads all WAL entries for the given provider starting from `entry_id`.
    async fn read(
        &self,
        provider: &Provider,
        entry_id: EntryId,
        _index: Option<WalIndex>,
    ) -> Result<SendableEntryStream<'static, Entry, Self::Error>> {
        let nats_provider = provider
            .as_nats_provider()
            .with_context(|| InvalidProviderSnafu {
                expected: NatsProvider::type_name(),
                actual: provider.type_name(),
            })?
            .clone();

        let stream = self.client.stream().await?;
        let subject = nats_provider.topic.clone();
        let consumer_wait_timeout = self.consumer_wait_timeout;
        let provider_arc = nats_provider.clone();

        let entries = read_entries(
            &stream,
            provider_arc,
            subject,
            entry_id,
            consumer_wait_timeout,
        )
        .await?;

        // Wrap the collected Vec in a once stream.
        let stream = futures::stream::once(async move { Ok(entries) });
        Ok(Box::pin(stream))
    }

    /// No-op: NATS subjects are virtual and require no explicit creation.
    async fn create_namespace(&self, _provider: &Provider) -> Result<()> {
        Ok(())
    }

    /// No-op: NATS subjects are virtual.
    async fn delete_namespace(&self, _provider: &Provider) -> Result<()> {
        Ok(())
    }

    /// Returns an empty list (subjects are virtual, no registry maintained).
    async fn list_namespaces(&self) -> Result<Vec<Provider>> {
        Ok(vec![])
    }

    /// Records the purge watermark for `entry_id` on the provider's subject.
    ///
    /// The actual `stream.purge()` is deferred to the background [`PurgeWorker`].
    async fn obsolete(
        &self,
        provider: &Provider,
        _region_id: RegionId,
        entry_id: EntryId,
    ) -> Result<()> {
        let nats_provider = provider
            .as_nats_provider()
            .with_context(|| InvalidProviderSnafu {
                expected: NatsProvider::type_name(),
                actual: provider.type_name(),
            })?
            .clone();

        self.pending_purge
            .entry(nats_provider)
            .and_modify(|e| *e = (*e).max(entry_id))
            .or_insert(entry_id);

        Ok(())
    }

    /// Returns the latest committed NATS sequence for the provider's subject.
    ///
    /// Returns 0 if no entries have been appended in this session (cache miss).
    fn latest_entry_id(&self, provider: &Provider) -> Result<EntryId> {
        let nats_provider = provider
            .as_nats_provider()
            .with_context(|| InvalidProviderSnafu {
                expected: NatsProvider::type_name(),
                actual: provider.type_name(),
            })?;

        Ok(self
            .subject_seq_cache
            .get(nats_provider)
            .map(|v| *v)
            .unwrap_or(0))
    }

    /// No-op: the background worker is detached and will stop with the process.
    async fn stop(&self) -> Result<()> {
        info!("NatsLogStore stopped");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use common_telemetry::tracing::warn;
    use common_wal::config::nats::DatanodeNatsConfig;
    use futures::TryStreamExt;
    use store_api::logstore::LogStore;
    use store_api::logstore::provider::Provider;
    use store_api::storage::RegionId;

    use super::NatsLogStore;

    async fn make_logstore(servers: Vec<String>) -> NatsLogStore {
        let config = DatanodeNatsConfig {
            servers,
            cluster_name: "test".to_string(),
            ..Default::default()
        };
        NatsLogStore::try_new(&config).await.unwrap()
    }

    #[tokio::test]
    async fn test_append_and_read_basic() {
        let Ok(servers_str) = std::env::var("GT_NATS_SERVERS") else {
            warn!("GT_NATS_SERVERS not set — skipping test_append_and_read_basic");
            return;
        };
        let servers: Vec<String> = servers_str.split(',').map(|s| s.trim().to_string()).collect();
        let logstore = make_logstore(servers).await;

        let subject = format!("greptimedb_wal_subject.{}", uuid::Uuid::new_v4());
        let provider = Provider::nats_provider(subject.clone());
        let region_id = RegionId::new(1, 1);

        let mut entries = Vec::new();
        for _ in 0..20 {
            let data: Vec<u8> = (0..1024).map(|_| rand::random::<u8>()).collect();
            entries.push(logstore.entry(data, 0, region_id, &provider).unwrap());
        }

        let resp = logstore.append_batch(entries.clone()).await.unwrap();
        assert_eq!(resp.last_entry_ids.len(), 1);

        let got: Vec<_> = logstore
            .read(&provider, 1, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        assert_eq!(got.len(), 20);
        for (orig, read) in entries.iter().zip(got.iter()) {
            assert_eq!(orig.region_id(), read.region_id());
            assert_eq!(orig.clone().into_bytes(), read.clone().into_bytes());
        }
    }

    #[tokio::test]
    async fn test_append_large_entries() {
        let Ok(servers_str) = std::env::var("GT_NATS_SERVERS") else {
            warn!("GT_NATS_SERVERS not set — skipping test_append_large_entries");
            return;
        };
        let servers: Vec<String> = servers_str.split(',').map(|s| s.trim().to_string()).collect();
        let config = DatanodeNatsConfig {
            servers,
            max_batch_bytes: common_base::readable_size::ReadableSize::kb(8),
            cluster_name: "test".to_string(),
            ..Default::default()
        };
        let logstore = NatsLogStore::try_new(&config).await.unwrap();

        let subject = format!("greptimedb_wal_subject.{}", uuid::Uuid::new_v4());
        let provider = Provider::nats_provider(subject.clone());
        let region_id = RegionId::new(1, 2);

        // 16 KB entry → should split into MultiplePart
        let data: Vec<u8> = (0..16 * 1024).map(|_| rand::random::<u8>()).collect();
        let entry = logstore.entry(data.clone(), 0, region_id, &provider).unwrap();
        assert!(matches!(entry, store_api::logstore::entry::Entry::MultiplePart(_)));

        let resp = logstore.append_batch(vec![entry]).await.unwrap();
        assert_eq!(resp.last_entry_ids.len(), 1);

        let got: Vec<_> = logstore
            .read(&provider, 1, None)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect();

        assert_eq!(got.len(), 1);
        assert_eq!(got[0].clone().into_bytes(), data);
    }

    #[tokio::test]
    async fn test_latest_entry_id_updated_after_append() {
        let Ok(servers_str) = std::env::var("GT_NATS_SERVERS") else {
            warn!("GT_NATS_SERVERS not set — skipping test_latest_entry_id_updated_after_append");
            return;
        };
        let servers: Vec<String> = servers_str.split(',').map(|s| s.trim().to_string()).collect();
        let logstore = make_logstore(servers).await;

        let subject = format!("greptimedb_wal_subject.{}", uuid::Uuid::new_v4());
        let provider = Provider::nats_provider(subject.clone());
        let region_id = RegionId::new(1, 3);

        assert_eq!(logstore.latest_entry_id(&provider).unwrap(), 0);

        let entry = logstore
            .entry(vec![42u8; 64], 0, region_id, &provider)
            .unwrap();
        logstore.append_batch(vec![entry]).await.unwrap();

        assert!(logstore.latest_entry_id(&provider).unwrap() > 0);
    }
}
