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
use std::sync::Arc;

use common_telemetry::{debug, warn};
use common_wal::config::kafka::DatanodeKafkaConfig;
use futures::future::try_join_all;
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::partition::OffsetAt;
use rskafka::client::ClientBuilder;
use rskafka::BackoffConfig;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::entry::{
    Entry, Id as EntryId, MultiplePartEntry, MultiplePartHeader, NaiveEntry,
};
use store_api::logstore::provider::{KafkaProvider, Provider};
use store_api::logstore::{AppendBatchResponse, LogStore, SendableEntryStream};
use store_api::storage::RegionId;

use super::producer_registry::OrderedBatchProducerRef;
use super::util::record::convert_to_kafka_records;
use crate::error::{self, ConsumeRecordSnafu, Error, GetOffsetSnafu, InvalidProviderSnafu, Result};
use crate::kafka::client_manager::{ClientManager, ClientManagerRef};
use crate::kafka::producer_registry::ProducerRegistry;
use crate::kafka::util::offset::Offset;
use crate::kafka::util::record::{
    maybe_emit_entry, remaining_entries, Record, ESTIMATED_META_SIZE,
};
use crate::metrics;

/// A log store backed by Kafka.
#[derive(Debug)]
pub struct KafkaLogStore {
    config: DatanodeKafkaConfig,
    /// TODO(weny): remove it.
    /// Manages kafka clients through which the log store contact the Kafka cluster.
    client_manager: ClientManagerRef,

    producer_registry: ProducerRegistry,
}

impl KafkaLogStore {
    /// Tries to create a Kafka log store.
    pub async fn try_new(config: &DatanodeKafkaConfig) -> Result<Self> {
        let backoff_config = BackoffConfig {
            init_backoff: config.backoff.init,
            max_backoff: config.backoff.max,
            base: config.backoff.base as f64,
            deadline: config.backoff.deadline,
        };
        let broker_endpoints = common_wal::resolve_to_ipv4(&config.broker_endpoints)
            .await
            .context(error::ResolveKafkaEndpointSnafu)?;
        let client = ClientBuilder::new(broker_endpoints)
            .backoff_config(backoff_config)
            .build()
            .await
            .with_context(|_| error::BuildClientSnafu {
                broker_endpoints: config.broker_endpoints.clone(),
            })?;
        let producer_registry = ProducerRegistry::new(
            client,
            config.max_batch_size.as_bytes() as usize,
            config.compression,
        );

        Ok(Self {
            producer_registry,
            client_manager: Arc::new(ClientManager::try_new(config).await?),
            config: config.clone(),
        })
    }
}

fn build_entry(
    data: &mut Vec<u8>,
    entry_id: EntryId,
    region_id: RegionId,
    provider: &Provider,
    max_data_size: usize,
) -> Entry {
    if data.len() <= max_data_size {
        Entry::Naive(NaiveEntry {
            provider: provider.clone(),
            region_id,
            entry_id,
            data: std::mem::take(data),
        })
    } else {
        let parts = std::mem::take(data)
            .chunks(max_data_size)
            .map(|s| s.into())
            .collect::<Vec<_>>();
        let num_parts = parts.len();

        let mut headers = Vec::with_capacity(num_parts);
        headers.push(MultiplePartHeader::First);
        headers.extend((1..num_parts - 1).map(MultiplePartHeader::Middle));
        headers.push(MultiplePartHeader::Last);

        Entry::MultiplePart(MultiplePartEntry {
            provider: provider.clone(),
            region_id,
            entry_id,
            headers,
            parts,
        })
    }
}

#[async_trait::async_trait]
impl LogStore for KafkaLogStore {
    type Error = Error;

    /// Creates an [Entry].
    fn entry(
        &self,
        data: &mut Vec<u8>,
        entry_id: EntryId,
        region_id: RegionId,
        provider: &Provider,
    ) -> Result<Entry> {
        provider
            .as_kafka_provider()
            .with_context(|| InvalidProviderSnafu {
                expected: KafkaProvider::type_name(),
                actual: provider.type_name(),
            })?;

        let max_data_size =
            self.client_manager.config.max_batch_size.as_bytes() as usize - ESTIMATED_META_SIZE;
        Ok(build_entry(
            data,
            entry_id,
            region_id,
            provider,
            max_data_size,
        ))
    }

    /// Appends a batch of entries and returns a response containing a map where the key is a region id
    /// while the value is the id of the last successfully written entry of the region.
    async fn append_batch(&self, entries: Vec<Entry>) -> Result<AppendBatchResponse> {
        metrics::METRIC_KAFKA_APPEND_BATCH_CALLS_TOTAL.inc();
        metrics::METRIC_KAFKA_APPEND_BATCH_BYTES_TOTAL.inc_by(
            entries
                .iter()
                .map(|entry| entry.estimated_size())
                .sum::<usize>() as u64,
        );
        let _timer = metrics::METRIC_KAFKA_APPEND_BATCH_ELAPSED.start_timer();

        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }

        let mut region_grouped_records: HashMap<RegionId, Vec<_>> = HashMap::new();
        let mut region_grouped_producers: HashMap<RegionId, OrderedBatchProducerRef> =
            HashMap::new();
        for entry in entries {
            let provider = entry
                .provider()
                .as_kafka_provider()
                .context(error::InvalidProviderSnafu {
                    expected: KafkaProvider::type_name(),
                    actual: entry.provider().type_name(),
                })?
                .clone();

            let region_id = entry.region_id();

            if let hash_map::Entry::Vacant(e) = region_grouped_producers.entry(region_id) {
                let producer = self.producer_registry.get_or_register(&provider).await?;
                e.insert(producer);
            };
            region_grouped_records
                .entry(region_id)
                .or_default()
                .extend(convert_to_kafka_records(entry)?);
        }

        let mut region_ids = Vec::with_capacity(region_grouped_records.keys().len());
        let mut region_grouped_result_receivers =
            Vec::with_capacity(region_grouped_records.keys().len());
        for (region_id, records) in region_grouped_records {
            region_ids.push(region_id);
            let producer = region_grouped_producers.get(&region_id).unwrap();
            // Safety: `Record`'s `approximate_size` must be less or equal to `max_flush_size`.
            region_grouped_result_receivers.push(producer.produce(records).await?)
        }

        let region_grouped_offsets = try_join_all(
            region_grouped_result_receivers
                .into_iter()
                .map(|handle| handle.wait()),
        )
        .await?;

        let region_grouped_max_offset = region_grouped_offsets
            .into_iter()
            .map(|offsets| offsets.into_iter().max().unwrap() as u64);

        let last_entry_ids = region_ids
            .into_iter()
            .zip(region_grouped_max_offset)
            .collect::<HashMap<_, _>>();

        Ok(AppendBatchResponse { last_entry_ids })
    }

    /// Creates a new `EntryStream` to asynchronously generates `Entry` with entry ids.
    /// Returns entries belonging to `provider`, starting from `entry_id`.
    async fn read(
        &self,
        provider: &Provider,
        entry_id: EntryId,
    ) -> Result<SendableEntryStream<'static, Entry, Self::Error>> {
        let provider = provider
            .as_kafka_provider()
            .with_context(|| InvalidProviderSnafu {
                expected: KafkaProvider::type_name(),
                actual: provider.type_name(),
            })?;

        metrics::METRIC_KAFKA_READ_CALLS_TOTAL.inc();
        let _timer = metrics::METRIC_KAFKA_READ_ELAPSED.start_timer();

        // Gets the client associated with the topic.
        let client = self
            .client_manager
            .get_or_insert(&provider.topic)
            .await?
            .raw_client
            .clone();

        // Gets the offset of the latest record in the topic. Actually, it's the latest record of the single partition in the topic.
        // The read operation terminates when this record is consumed.
        // Warning: the `get_offset` returns the end offset of the latest record. For our usage, it should be decremented.
        // See: https://kafka.apache.org/36/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#endOffsets(java.util.Collection)
        let end_offset = client
            .get_offset(OffsetAt::Latest)
            .await
            .context(GetOffsetSnafu {
                topic: &provider.topic,
            })?
            - 1;
        // Reads entries with offsets in the range [start_offset, end_offset].
        let start_offset = Offset::try_from(entry_id)?.0;

        debug!(
            "Start reading entries in range [{}, {}] for ns {}",
            start_offset, end_offset, provider
        );

        // Abort if there're no new entries.
        // FIXME(niebayes): how come this case happens?
        if start_offset > end_offset {
            warn!(
                "No new entries for ns {} in range [{}, {}]",
                provider, start_offset, end_offset
            );
            return Ok(futures_util::stream::empty().boxed());
        }

        let mut stream_consumer = StreamConsumerBuilder::new(client, StartOffset::At(start_offset))
            .with_max_batch_size(self.config.max_batch_size.as_bytes() as i32)
            .with_max_wait_ms(self.config.consumer_wait_timeout.as_millis() as i32)
            .build();

        debug!(
            "Built a stream consumer for ns {} to consume entries in range [{}, {}]",
            provider, start_offset, end_offset
        );

        // A buffer is used to collect records to construct a complete entry.
        let mut entry_records: HashMap<RegionId, Vec<Record>> = HashMap::new();
        let provider = provider.clone();
        let stream = async_stream::stream!({
            while let Some(consume_result) = stream_consumer.next().await {
                // Each next on the stream consumer produces a `RecordAndOffset` and a high watermark offset.
                // The `RecordAndOffset` contains the record data and its start offset.
                // The high watermark offset is the offset of the last record plus one.
                let (record_and_offset, high_watermark) =
                    consume_result.context(ConsumeRecordSnafu {
                        topic: &provider.topic,
                    })?;
                let (kafka_record, offset) = (record_and_offset.record, record_and_offset.offset);

                metrics::METRIC_KAFKA_READ_RECORD_BYTES_TOTAL
                    .inc_by(kafka_record.approximate_size() as u64);

                debug!(
                    "Read a record at offset {} for topic {}, high watermark: {}",
                    offset, provider.topic, high_watermark
                );

                // Ignores no-op records.
                if kafka_record.value.is_none() {
                    if check_termination(offset, end_offset) {
                        if let Some(entries) = remaining_entries(&provider, &mut entry_records) {
                            yield Ok(entries);
                        }
                        break;
                    }
                    continue;
                }

                let record = Record::try_from(kafka_record)?;
                // Tries to construct an entry from records consumed so far.
                if let Some(mut entry) = maybe_emit_entry(&provider, record, &mut entry_records)? {
                    // We don't rely on the EntryId generated by mito2.
                    // Instead, we use the offset return from Kafka as EntryId.
                    // Therefore, we MUST overwrite the EntryId with RecordOffset.
                    entry.set_entry_id(offset as u64);
                    yield Ok(vec![entry]);
                }

                if check_termination(offset, end_offset) {
                    if let Some(entries) = remaining_entries(&provider, &mut entry_records) {
                        yield Ok(entries);
                    }
                    break;
                }
            }
        });
        Ok(Box::pin(stream))
    }

    /// Creates a new `Namespace` from the given ref.
    async fn create_namespace(&self, _provider: &Provider) -> Result<()> {
        Ok(())
    }

    /// Deletes an existing `Namespace` specified by the given ref.
    async fn delete_namespace(&self, _provider: &Provider) -> Result<()> {
        Ok(())
    }

    /// Lists all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Provider>> {
        Ok(vec![])
    }

    /// Marks all entries with ids `<=entry_id` of the given `namespace` as obsolete,
    /// so that the log store can safely delete those entries. This method does not guarantee
    /// that the obsolete entries are deleted immediately.
    async fn obsolete(&self, _provider: &Provider, _entry_id: EntryId) -> Result<()> {
        Ok(())
    }

    /// Stops components of the logstore.
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

fn check_termination(offset: i64, end_offset: i64) -> bool {
    // Terminates the stream if the entry with the end offset was read.
    if offset >= end_offset {
        debug!("Stream consumer terminates at offset {}", offset);
        // There must have no records when the stream terminates.
        true
    } else {
        false
    }
}

#[cfg(test)]
mod tests {

    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use common_base::readable_size::ReadableSize;
    use common_telemetry::info;
    use common_telemetry::tracing::warn;
    use common_wal::config::kafka::DatanodeKafkaConfig;
    use futures::TryStreamExt;
    use rand::prelude::SliceRandom;
    use rand::Rng;
    use store_api::logstore::entry::{Entry, MultiplePartEntry, MultiplePartHeader, NaiveEntry};
    use store_api::logstore::provider::Provider;
    use store_api::logstore::LogStore;
    use store_api::storage::RegionId;

    use super::build_entry;
    use crate::kafka::log_store::KafkaLogStore;

    #[test]
    fn test_build_naive_entry() {
        let provider = Provider::kafka_provider("my_topic".to_string());
        let region_id = RegionId::new(1, 1);
        let entry = build_entry(&mut vec![1; 100], 1, region_id, &provider, 120);

        assert_eq!(
            entry.into_naive_entry().unwrap(),
            NaiveEntry {
                provider,
                region_id,
                entry_id: 1,
                data: vec![1; 100]
            }
        )
    }

    #[test]
    fn test_build_into_multiple_part_entry() {
        let provider = Provider::kafka_provider("my_topic".to_string());
        let region_id = RegionId::new(1, 1);
        let entry = build_entry(&mut vec![1; 100], 1, region_id, &provider, 50);

        assert_eq!(
            entry.into_multiple_part_entry().unwrap(),
            MultiplePartEntry {
                provider: provider.clone(),
                region_id,
                entry_id: 1,
                headers: vec![MultiplePartHeader::First, MultiplePartHeader::Last],
                parts: vec![vec![1; 50], vec![1; 50]],
            }
        );

        let region_id = RegionId::new(1, 1);
        let entry = build_entry(&mut vec![1; 100], 1, region_id, &provider, 21);

        assert_eq!(
            entry.into_multiple_part_entry().unwrap(),
            MultiplePartEntry {
                provider,
                region_id,
                entry_id: 1,
                headers: vec![
                    MultiplePartHeader::First,
                    MultiplePartHeader::Middle(1),
                    MultiplePartHeader::Middle(2),
                    MultiplePartHeader::Middle(3),
                    MultiplePartHeader::Last
                ],
                parts: vec![
                    vec![1; 21],
                    vec![1; 21],
                    vec![1; 21],
                    vec![1; 21],
                    vec![1; 16]
                ],
            }
        )
    }

    fn generate_entries(
        logstore: &KafkaLogStore,
        provider: &Provider,
        num_entries: usize,
        region_id: RegionId,
        data_len: usize,
    ) -> Vec<Entry> {
        (0..num_entries)
            .map(|_| {
                let mut data: Vec<u8> = (0..data_len).map(|_| rand::random::<u8>()).collect();
                // Always set `entry_id` to 0, the real entry_id will be set during the read.
                logstore.entry(&mut data, 0, region_id, provider).unwrap()
            })
            .collect()
    }

    #[tokio::test]
    async fn test_append_batch_basic() {
        common_telemetry::init_default_ut_logging();
        let Ok(broker_endpoints) = std::env::var("GT_KAFKA_ENDPOINTS") else {
            warn!("The endpoints is empty, skipping the test 'test_append_batch_basic'");
            return;
        };
        let broker_endpoints = broker_endpoints
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>();
        let config = DatanodeKafkaConfig {
            broker_endpoints,
            max_batch_size: ReadableSize::kb(32),
            ..Default::default()
        };
        let logstore = KafkaLogStore::try_new(&config).await.unwrap();
        let topic_name = uuid::Uuid::new_v4().to_string();
        let provider = Provider::kafka_provider(topic_name);
        let region_entries = (0..5)
            .map(|i| {
                let region_id = RegionId::new(1, i);
                (
                    region_id,
                    generate_entries(&logstore, &provider, 20, region_id, 1024),
                )
            })
            .collect::<HashMap<RegionId, Vec<_>>>();

        let mut all_entries = region_entries
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        all_entries.shuffle(&mut rand::thread_rng());

        let response = logstore.append_batch(all_entries.clone()).await.unwrap();
        // 5 region
        assert_eq!(response.last_entry_ids.len(), 5);
        let got_entries = logstore
            .read(&provider, 0)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        for (region_id, _) in region_entries {
            let expected_entries = all_entries
                .iter()
                .filter(|entry| entry.region_id() == region_id)
                .cloned()
                .collect::<Vec<_>>();
            let mut actual_entries = got_entries
                .iter()
                .filter(|entry| entry.region_id() == region_id)
                .cloned()
                .collect::<Vec<_>>();
            actual_entries
                .iter_mut()
                .for_each(|entry| entry.set_entry_id(0));
            assert_eq!(expected_entries, actual_entries);
        }
    }

    #[tokio::test]
    async fn test_append_batch_basic_large() {
        common_telemetry::init_default_ut_logging();
        let Ok(broker_endpoints) = std::env::var("GT_KAFKA_ENDPOINTS") else {
            warn!("The endpoints is empty, skipping the test 'test_append_batch_basic_large'");
            return;
        };
        let data_size_kb = rand::thread_rng().gen_range(9..31usize);
        info!("Entry size: {}Ki", data_size_kb);
        let broker_endpoints = broker_endpoints
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>();
        let config = DatanodeKafkaConfig {
            broker_endpoints,
            max_batch_size: ReadableSize::kb(8),
            ..Default::default()
        };
        let logstore = KafkaLogStore::try_new(&config).await.unwrap();
        let topic_name = uuid::Uuid::new_v4().to_string();
        let provider = Provider::kafka_provider(topic_name);
        let region_entries = (0..5)
            .map(|i| {
                let region_id = RegionId::new(1, i);
                (
                    region_id,
                    generate_entries(&logstore, &provider, 20, region_id, data_size_kb * 1024),
                )
            })
            .collect::<HashMap<RegionId, Vec<_>>>();

        let mut all_entries = region_entries
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        assert_matches!(all_entries[0], Entry::MultiplePart(_));
        all_entries.shuffle(&mut rand::thread_rng());

        let response = logstore.append_batch(all_entries.clone()).await.unwrap();
        // 5 region
        assert_eq!(response.last_entry_ids.len(), 5);
        let got_entries = logstore
            .read(&provider, 0)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        for (region_id, _) in region_entries {
            let expected_entries = all_entries
                .iter()
                .filter(|entry| entry.region_id() == region_id)
                .cloned()
                .collect::<Vec<_>>();
            let mut actual_entries = got_entries
                .iter()
                .filter(|entry| entry.region_id() == region_id)
                .cloned()
                .collect::<Vec<_>>();
            actual_entries
                .iter_mut()
                .for_each(|entry| entry.set_entry_id(0));
            assert_eq!(expected_entries, actual_entries);
        }
    }
}
