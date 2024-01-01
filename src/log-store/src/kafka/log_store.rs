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

use common_config::wal::{KafkaConfig, KafkaWalTopic as Topic, WalOptions};
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::partition::OffsetAt;
use snafu::ResultExt;
use store_api::logstore::entry::Id as EntryId;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::Id as NamespaceId;
use store_api::logstore::{AppendBatchResponse, AppendResponse, LogStore};

use crate::error::{ConsumeRecordSnafu, Error, GetOffsetSnafu, Result};
use crate::kafka::client_manager::{ClientManager, ClientManagerRef};
use crate::kafka::offset::Offset;
use crate::kafka::record_utils::{decode_from_record, RecordProducer};
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

    /// Gets the end offset of the last record in a Kafka topic.
    /// Warning: this method is intended to be used only in testing.
    // TODO(niebayes): use this to test that the initial offset is 1 for a Kafka log store in that
    // a no-op record is successfully appended into each topic.
    #[allow(unused)]
    pub async fn get_offset(&self, topic: &Topic) -> EntryId {
        let client = self
            .client_manager
            .get_or_insert(topic)
            .await
            .unwrap()
            .raw_client;
        client
            .get_offset(OffsetAt::Latest)
            .await
            .map(TryInto::try_into)
            .unwrap()
            .unwrap()
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
        println!("LogStore handles append_batch with entries {:?}", entries);

        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }

        // Groups entries by region id and pushes them to an associated record producer.
        let mut producers = HashMap::with_capacity(entries.len());
        for entry in entries {
            producers
                .entry(entry.ns.region_id)
                .or_insert(RecordProducer::new(entry.ns.clone()))
                .push(entry);
        }

        // Builds a record from entries belong to a region and produces them to kafka server.
        let (region_ids, tasks): (Vec<_>, Vec<_>) = producers
            .into_iter()
            .map(|(id, producer)| (id, producer.produce(&self.client_manager)))
            .unzip();
        // Each produce operation returns a kafka offset of the produced record.
        // The offsets are then converted to entry ids.
        let entry_ids = futures::future::try_join_all(tasks)
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        println!("The entries are appended at offsets {:?}", entry_ids);

        let last_entry_ids = region_ids
            .into_iter()
            .zip(entry_ids)
            .collect::<HashMap<_, _>>();
        for (region, last_entry_id) in last_entry_ids.iter() {
            println!(
                "The entries for region {} are appended at offset {}",
                region, last_entry_id
            );
        }

        Ok(AppendBatchResponse { last_entry_ids })
    }

    /// Creates a new `EntryStream` to asynchronously generates `Entry` with entry ids
    /// starting from `entry_id`. The generated entries will be filtered by the namespace.
    async fn read(
        &self,
        ns: &Self::Namespace,
        entry_id: EntryId,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>> {
        let topic = ns.topic.clone();
        let region_id = ns.region_id;

        // Gets the client associated with the topic.
        let client = self
            .client_manager
            .get_or_insert(&topic)
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

        println!(
            "Start reading entries in range [{}, {}] for ns {}",
            start_offset, end_offset, ns
        );

        // Abort if there're no new entries.
        // FIXME(niebayes): how come this case happens?
        if start_offset > end_offset {
            println!(
                "No new entries for ns {} in range [{}, {}]",
                ns, start_offset, end_offset
            );
            return Ok(futures_util::stream::empty().boxed());
        }

        let mut stream_consumer = StreamConsumerBuilder::new(client, StartOffset::At(start_offset))
            .with_max_batch_size(self.config.max_batch_size.as_bytes() as i32)
            .with_max_wait_ms(self.config.produce_record_timeout.as_millis() as i32)
            .build();

        println!(
            "Built a stream consumer for ns {} to consume entries in range [{}, {}]",
            ns, start_offset, end_offset
        );

        let ns_clone = ns.clone();
        let stream = async_stream::stream!({
            while let Some(consume_result) = stream_consumer.next().await {
                // Each next will prdoce a `RecordAndOffset` and a high watermark offset.
                // The `RecordAndOffset` contains the record data and its start offset.
                // The high watermark offset is the end offset of the latest record in the partition.
                let (record, high_watermark) = consume_result.context(ConsumeRecordSnafu {
                    ns: ns_clone.clone(),
                })?;
                let record_offset = record.offset;
                println!(
                    "Read a record at offset {} for ns {}, high watermark: {}",
                    record_offset, ns_clone, high_watermark
                );

                let entries = decode_from_record(record.record)?;

                // Filters entries by region id.
                if let Some(entry) = entries.first()
                    && entry.ns.region_id == region_id
                {
                    println!("{} entries are yielded for ns {}", entries.len(), ns_clone);
                    yield Ok(entries);
                } else {
                    println!(
                        "{} entries are filtered out for ns {}",
                        entries.len(),
                        ns_clone
                    );
                }

                // Terminates the stream if the entry with the end offset was read.
                if record_offset >= end_offset {
                    println!(
                        "Stream consumer for ns {} terminates at offset {}",
                        ns_clone, record_offset
                    );
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

#[cfg(test)]
mod tests {
    use common_test_util::wal::kafka::BROKER_ENDPOINTS_KEY;
    use rand::seq::IteratorRandom;

    use super::*;
    use crate::get_broker_endpoints_from_env;
    use crate::test_util::kafka::{
        create_topics, entries_with_random_data, Affix, EntryBuilder, TopicDecorator,
    };

    // Stores test context for a region.
    struct RegionContext {
        ns: NamespaceImpl,
        entry_builder: EntryBuilder,
        expected: Vec<EntryImpl>,
    }

    /// Prepares for a test in that a log store is constructed and a collection of topics is created.
    async fn prepare(test_name: &str, num_topics: usize) -> (KafkaLogStore, Vec<Topic>) {
        let broker_endpoints = get_broker_endpoints_from_env!(BROKER_ENDPOINTS_KEY);
        let decorator = TopicDecorator::default()
            .with_prefix(Affix::Fixed(test_name.to_string()))
            .with_suffix(Affix::TimeNow);
        let topics = create_topics(num_topics, decorator, &broker_endpoints, None).await;

        let config = KafkaConfig {
            broker_endpoints,
            ..Default::default()
        };
        let logstore = KafkaLogStore::try_new(&config).await.unwrap();

        (logstore, topics)
    }

    /// Builds a context for each region.
    fn build_contexts(num_regions: usize, topics: &[Topic]) -> Vec<RegionContext> {
        (0..num_regions)
            .map(|i| {
                let topic = &topics[i % topics.len()];
                let ns = NamespaceImpl {
                    region_id: i as u64,
                    topic: topic.to_string(),
                };
                let entry_builder = EntryBuilder::new(ns.clone());
                RegionContext {
                    ns,
                    entry_builder,
                    expected: Vec::new(),
                }
            })
            .collect()
    }

    /// Creates a vector containing indexes of all regions if the `all` is true.
    /// Otherwise, creates a subset of the indexes. The cardinality of the subset
    /// is nearly a quarter of that of the universe set.
    fn all_or_subset(all: bool, num_regions: usize) -> Vec<usize> {
        assert!(num_regions > 0);
        let amount = if all {
            num_regions
        } else {
            (num_regions / 4).max(1)
        };
        (0..num_regions).choose_multiple(&mut rand::thread_rng(), amount)
    }

    /// Builds entries for regions specified with `which`.
    /// For example, assume `which[i] = x`, then entries for region with id = x will be built.
    fn build_entries(region_contexts: &mut [RegionContext], which: Vec<usize>) -> Vec<EntryImpl> {
        let mut entries = Vec::with_capacity(which.len());
        for i in which {
            let ctx = &mut region_contexts[i];
            ctx.expected = entries_with_random_data(25, &ctx.entry_builder);
            entries.push(ctx.expected.clone());
        }
        entries.into_iter().flatten().collect()
    }

    /// Reads entries for regions and checks for each region that the gotten entries are identical with the expected ones.
    async fn check_entries(
        region_contexts: &[RegionContext],
        last_entry_ids: HashMap<u64, u64>,
        logstore: &KafkaLogStore,
        which: Vec<usize>,
    ) {
        for i in which {
            let ctx = &region_contexts[i];
            let start_offset = last_entry_ids[&ctx.ns.region_id];
            let stream = logstore.read(&ctx.ns, start_offset).await.unwrap();
            let got = stream
                .collect::<Vec<_>>()
                .await
                .into_iter()
                .flat_map(|x| x.unwrap())
                .collect::<Vec<_>>();
            assert_eq!(ctx.expected, got);
        }
    }

    /// Starts a test with:
    /// * `test_name` - The name of the test.
    /// * `num_topics` - Number of topics to be created in the preparation phase.
    /// * `num_regions` - Number of regions involved in the test.
    /// * `num_appends` - Number of append operations to be performed.
    /// * `all` - All regions will be involved in an append operation if `all` is true. Otherwise,
    /// an append operation will only randomly choose a subset of regions.
    async fn test_with(
        test_name: &str,
        num_topics: usize,
        num_regions: usize,
        num_appends: usize,
        all: bool,
    ) {
        let (logstore, topics) = prepare(test_name, num_topics).await;
        let mut region_contexts = build_contexts(num_regions, &topics);
        for _ in 0..num_appends {
            let which = all_or_subset(all, num_regions);
            let entries = build_entries(&mut region_contexts, which.clone());
            let last_entry_ids = logstore.append_batch(entries).await.unwrap().last_entry_ids;
            check_entries(&region_contexts, last_entry_ids, &logstore, which).await;
        }
    }

    /// Appends entries for one region and checks all entries can be read successfully.
    #[tokio::test]
    async fn test_one_region() {
        test_with("test_one_region", 1, 1, 1, true).await;
    }

    /// Appends entries for multiple regions and checks entries for each region can be read successfully.
    /// A topic is assigned only a single region.
    #[tokio::test]
    async fn test_multi_regions_disjoint() {
        test_with("test_multi_regions_disjoint", 10, 10, 1, true).await;
    }

    /// Appends entries for multiple regions and checks entries for each region can be read successfully.
    /// A topic is assigned multiple regions.
    #[tokio::test]
    async fn test_multi_regions_overlapped() {
        test_with("test_multi_regions_overlapped", 10, 50, 1, true).await;
    }

    /// Appends entries for multiple regions and checks entries for each region can be read successfully.
    /// A topic may be assigned multiple regions. The append operation repeats for a several iterations.
    /// Each append operation will only append entries for a subset of randomly chosen regions.
    #[tokio::test]
    async fn test_multi_appends() {
        test_with("test_multi_appends", 32, 256, 16, false).await;
    }

    // / Appends a way-too large entry and checks the log store could handle it correctly.
    // #[tokio::test]
    // async fn test_way_too_large() {}
}
