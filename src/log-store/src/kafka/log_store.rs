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
use std::sync::Arc;

use common_config::wal::{KafkaConfig, WalOptions};
use common_telemetry::{debug, warn};
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::partition::OffsetAt;
use snafu::ResultExt;
use store_api::logstore::entry::Id as EntryId;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::Id as NamespaceId;
use store_api::logstore::{AppendBatchResponse, AppendResponse, LogStore};

use crate::error::{ConsumeRecordSnafu, Error, GetOffsetSnafu, IllegalSequenceSnafu, Result};
use crate::kafka::client_manager::{ClientManager, ClientManagerRef};
use crate::kafka::util::offset::Offset;
use crate::kafka::util::record::{maybe_emit_entry, Record, RecordProducer};
use crate::kafka::{EntryImpl, NamespaceImpl};

/// A log store backed by Kafka.
#[derive(Debug)]
pub struct KafkaLogStore {
    config: KafkaConfig,
    /// Manages kafka clients through which the log store contact the Kafka cluster.
    client_manager: ClientManagerRef,
}

impl KafkaLogStore {
    /// Tries to create a Kafka log store.
    pub async fn try_new(config: &KafkaConfig) -> Result<Self> {
        Ok(Self {
            client_manager: Arc::new(ClientManager::try_new(config).await?),
            config: config.clone(),
        })
    }
}

#[async_trait::async_trait]
impl LogStore for KafkaLogStore {
    type Error = Error;
    type Entry = EntryImpl;
    type Namespace = NamespaceImpl;

    /// Creates an entry of the associated Entry type.
    fn entry<D: AsRef<[u8]>>(
        &self,
        data: D,
        entry_id: EntryId,
        ns: Self::Namespace,
    ) -> Self::Entry {
        EntryImpl {
            data: data.as_ref().to_vec(),
            id: entry_id,
            ns,
        }
    }

    /// Appends an entry to the log store and returns a response containing the entry id of the appended entry.
    async fn append(&self, entry: Self::Entry) -> Result<AppendResponse> {
        let entry_id = RecordProducer::new(entry.ns.clone())
            .with_entries(vec![entry])
            .produce(&self.client_manager)
            .await
            .map(TryInto::try_into)??;
        Ok(AppendResponse {
            last_entry_id: entry_id,
        })
    }

    /// Appends a batch of entries and returns a response containing a map where the key is a region id
    /// while the value is the id of the last successfully written entry of the region.
    async fn append_batch(&self, entries: Vec<Self::Entry>) -> Result<AppendBatchResponse> {
        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }

        // Groups entries by region id and pushes them to an associated record producer.
        let mut producers = HashMap::with_capacity(entries.len());
        for entry in entries {
            producers
                .entry(entry.ns.region_id)
                .or_insert_with(|| RecordProducer::new(entry.ns.clone()))
                .push(entry);
        }

        // Produces entries for each region and gets the offset those entries written to.
        // The returned offset is then converted into an entry id.
        let last_entry_ids = futures::future::try_join_all(producers.into_iter().map(
            |(region_id, producer)| async move {
                let entry_id = producer
                    .produce(&self.client_manager)
                    .await
                    .map(TryInto::try_into)??;
                Ok((region_id, entry_id))
            },
        ))
        .await?
        .into_iter()
        .collect::<HashMap<_, _>>();

        Ok(AppendBatchResponse { last_entry_ids })
    }

    /// Creates a new `EntryStream` to asynchronously generates `Entry` with entry ids
    /// starting from `entry_id`. The generated entries will be filtered by the namespace.
    async fn read(
        &self,
        ns: &Self::Namespace,
        entry_id: EntryId,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>> {
        // Gets the client associated with the topic.
        let client = self
            .client_manager
            .get_or_insert(&ns.topic)
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
            .context(GetOffsetSnafu { ns: ns.clone() })?
            - 1;
        // Reads entries with offsets in the range [start_offset, end_offset].
        let start_offset = Offset::try_from(entry_id)?.0;

        debug!(
            "Start reading entries in range [{}, {}] for ns {}",
            start_offset, end_offset, ns
        );

        // Abort if there're no new entries.
        // FIXME(niebayes): how come this case happens?
        if start_offset > end_offset {
            warn!(
                "No new entries for ns {} in range [{}, {}]",
                ns, start_offset, end_offset
            );
            return Ok(futures_util::stream::empty().boxed());
        }

        let mut stream_consumer = StreamConsumerBuilder::new(client, StartOffset::At(start_offset))
            .with_max_batch_size(self.config.max_batch_size.as_bytes() as i32)
            .with_max_wait_ms(self.config.consumer_wait_timeout.as_millis() as i32)
            .build();

        debug!(
            "Built a stream consumer for ns {} to consume entries in range [{}, {}]",
            ns, start_offset, end_offset
        );

        // Key: entry id, Value: the records associated with the entry.
        let mut entry_records: HashMap<_, Vec<_>> = HashMap::new();
        let ns_clone = ns.clone();
        let stream = async_stream::stream!({
            while let Some(consume_result) = stream_consumer.next().await {
                // Each next on the stream consumer produces a `RecordAndOffset` and a high watermark offset.
                // The `RecordAndOffset` contains the record data and its start offset.
                // The high watermark offset is the offset of the last record plus one.
                let (record_and_offset, high_watermark) =
                    consume_result.with_context(|_| ConsumeRecordSnafu {
                        ns: ns_clone.clone(),
                    })?;
                let (kafka_record, offset) = (record_and_offset.record, record_and_offset.offset);

                debug!(
                    "Read a record at offset {} for ns {}, high watermark: {}",
                    offset, ns_clone, high_watermark
                );

                // Ignores no-op records.
                if kafka_record.value.is_none() {
                    if check_termination(offset, end_offset, &entry_records)? {
                        break;
                    }
                    continue;
                }

                // Filters records by namespace.
                let record = Record::try_from(kafka_record)?;
                if record.meta.ns != ns_clone {
                    if check_termination(offset, end_offset, &entry_records)? {
                        break;
                    }
                    continue;
                }

                // Tries to construct an entry from records consumed so far.
                if let Some(entry) = maybe_emit_entry(record, &mut entry_records)? {
                    yield Ok(vec![entry]);
                }

                if check_termination(offset, end_offset, &entry_records)? {
                    break;
                }
            }
        });
        Ok(Box::pin(stream))
    }

    /// Creates a namespace of the associated Namespace type.
    fn namespace(&self, ns_id: NamespaceId, wal_options: &WalOptions) -> Self::Namespace {
        // Safety: upon start, the datanode checks the consistency of the wal providers in the wal config of the
        // datanode and that of the metasrv. Therefore, the wal options passed into the kafka log store
        // must be of type WalOptions::Kafka.
        let WalOptions::Kafka(kafka_options) = wal_options else {
            unreachable!()
        };
        NamespaceImpl {
            region_id: ns_id,
            topic: kafka_options.topic.clone(),
        }
    }

    /// Creates a new `Namespace` from the given ref.
    async fn create_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    /// Deletes an existing `Namespace` specified by the given ref.
    async fn delete_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    /// Lists all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>> {
        Ok(vec![])
    }

    /// Marks all entries with ids `<=entry_id` of the given `namespace` as obsolete,
    /// so that the log store can safely delete those entries. This method does not guarantee
    /// that the obsolete entries are deleted immediately.
    async fn obsolete(&self, _ns: Self::Namespace, _entry_id: EntryId) -> Result<()> {
        Ok(())
    }

    /// Stops components of the logstore.
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

fn check_termination(
    offset: i64,
    end_offset: i64,
    entry_records: &HashMap<EntryId, Vec<Record>>,
) -> Result<bool> {
    // Terminates the stream if the entry with the end offset was read.
    if offset >= end_offset {
        debug!("Stream consumer terminates at offset {}", offset);
        // There must have no records when the stream terminates.
        if !entry_records.is_empty() {
            return IllegalSequenceSnafu {
                error: "Found records leftover",
            }
            .fail();
        }
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use common_base::readable_size::ReadableSize;
    use common_config::wal::KafkaWalTopic as Topic;
    use rand::seq::IteratorRandom;

    use super::*;
    use crate::test_util::kafka::{
        create_topics, entries_with_random_data, new_namespace, EntryBuilder,
    };

    // Stores test context for a region.
    struct RegionContext {
        ns: NamespaceImpl,
        entry_builder: EntryBuilder,
        expected: Vec<EntryImpl>,
        flushed_entry_id: EntryId,
    }

    /// Prepares for a test in that a log store is constructed and a collection of topics is created.
    async fn prepare(
        test_name: &str,
        num_topics: usize,
        broker_endpoints: Vec<String>,
    ) -> (KafkaLogStore, Vec<Topic>) {
        let topics = create_topics(
            num_topics,
            |i| format!("{test_name}_{}_{}", i, uuid::Uuid::new_v4()),
            &broker_endpoints,
        )
        .await;

        let config = KafkaConfig {
            broker_endpoints,
            max_batch_size: ReadableSize::kb(32),
            ..Default::default()
        };
        let logstore = KafkaLogStore::try_new(&config).await.unwrap();

        // Appends a no-op record to each topic.
        for topic in topics.iter() {
            let last_entry_id = logstore
                .append(EntryImpl {
                    data: vec![],
                    id: 0,
                    ns: new_namespace(topic, 0),
                })
                .await
                .unwrap()
                .last_entry_id;
            assert_eq!(last_entry_id, 0);
        }

        (logstore, topics)
    }

    /// Creates a vector containing indexes of all regions if the `all` is true.
    /// Otherwise, creates a subset of the indexes. The cardinality of the subset
    /// is nearly a quarter of that of the universe set.
    fn all_or_subset(all: bool, num_regions: usize) -> Vec<u64> {
        assert!(num_regions > 0);
        let amount = if all {
            num_regions
        } else {
            (num_regions / 4).max(1)
        };
        (0..num_regions as u64).choose_multiple(&mut rand::thread_rng(), amount)
    }

    /// Builds entries for regions specified by `which`. Builds large entries if `large` is true.
    /// Returns the aggregated entries.
    fn build_entries(
        region_contexts: &mut HashMap<u64, RegionContext>,
        which: &[u64],
        large: bool,
    ) -> Vec<EntryImpl> {
        let mut aggregated = Vec::with_capacity(which.len());
        for region_id in which {
            let ctx = region_contexts.get_mut(region_id).unwrap();
            // Builds entries for the region.
            ctx.expected = if !large {
                entries_with_random_data(3, &ctx.entry_builder)
            } else {
                // Builds a large entry of size 256KB which is way greater than the configured `max_batch_size` which is 32KB.
                let large_entry = ctx.entry_builder.with_data([b'1'; 256 * 1024]);
                vec![large_entry]
            };
            // Aggregates entries of all regions.
            aggregated.push(ctx.expected.clone());
        }
        aggregated.into_iter().flatten().collect()
    }

    /// Starts a test with:
    /// * `test_name` - The name of the test.
    /// * `num_topics` - Number of topics to be created in the preparation phase.
    /// * `num_regions` - Number of regions involved in the test.
    /// * `num_appends` - Number of append operations to be performed.
    /// * `all` - All regions will be involved in an append operation if `all` is true. Otherwise,
    /// an append operation will only randomly choose a subset of regions.
    /// * `large` - Builds large entries for each region is `large` is true.
    async fn test_with(
        test_name: &str,
        num_topics: usize,
        num_regions: usize,
        num_appends: usize,
        all: bool,
        large: bool,
    ) {
        let Ok(broker_endpoints) = std::env::var("GT_KAFKA_ENDPOINTS") else {
            warn!("The endpoints is empty, skipping the test {test_name}");
            return;
        };
        let broker_endpoints = broker_endpoints
            .split(',')
            .map(|s| s.trim().to_string())
            .collect::<Vec<_>>();

        let (logstore, topics) = prepare(test_name, num_topics, broker_endpoints).await;
        let mut region_contexts = (0..num_regions)
            .map(|i| {
                let topic = &topics[i % topics.len()];
                let ns = new_namespace(topic, i as u64);
                let entry_builder = EntryBuilder::new(ns.clone());
                (
                    i as u64,
                    RegionContext {
                        ns,
                        entry_builder,
                        expected: Vec::new(),
                        flushed_entry_id: 0,
                    },
                )
            })
            .collect();

        for _ in 0..num_appends {
            // Appends entries for a subset of regions.
            let which = all_or_subset(all, num_regions);
            let entries = build_entries(&mut region_contexts, &which, large);
            let last_entry_ids = logstore.append_batch(entries).await.unwrap().last_entry_ids;

            // Reads entries for regions and checks for each region that the gotten entries are identical with the expected ones.
            for region_id in which {
                let ctx = &region_contexts[&region_id];
                let stream = logstore
                    .read(&ctx.ns, ctx.flushed_entry_id + 1)
                    .await
                    .unwrap();
                let got = stream
                    .collect::<Vec<_>>()
                    .await
                    .into_iter()
                    .flat_map(|x| x.unwrap())
                    .collect::<Vec<_>>();
                assert_eq!(ctx.expected, got);
            }

            // Simulates a flush for regions.
            for (region_id, last_entry_id) in last_entry_ids {
                let ctx = region_contexts.get_mut(&region_id).unwrap();
                ctx.flushed_entry_id = last_entry_id;
            }
        }
    }

    /// Appends entries for one region and checks all entries can be read successfully.
    #[tokio::test]
    async fn test_one_region() {
        test_with("test_one_region", 1, 1, 1, true, false).await;
    }

    /// Appends entries for multiple regions and checks entries for each region can be read successfully.
    /// A topic is assigned only a single region.
    #[tokio::test]
    async fn test_multi_regions_disjoint() {
        test_with("test_multi_regions_disjoint", 5, 5, 1, true, false).await;
    }

    /// Appends entries for multiple regions and checks entries for each region can be read successfully.
    /// A topic is assigned multiple regions.
    #[tokio::test]
    async fn test_multi_regions_overlapped() {
        test_with("test_multi_regions_overlapped", 5, 20, 1, true, false).await;
    }

    /// Appends entries for multiple regions and checks entries for each region can be read successfully.
    /// A topic may be assigned multiple regions. The append operation repeats for a several iterations.
    /// Each append operation will only append entries for a subset of randomly chosen regions.
    #[tokio::test]
    async fn test_multi_appends() {
        test_with("test_multi_appends", 5, 20, 3, false, false).await;
    }

    /// Appends large entries for multiple regions and checks entries for each region can be read successfully.
    /// A topic may be assigned multiple regions.
    #[tokio::test]
    async fn test_append_large_entries() {
        test_with("test_append_large_entries", 5, 20, 3, true, true).await;
    }
}
