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

use common_config::wal::kafka::{KafkaOptions, KafkaTopic as Topic, TOPIC_KEY};
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use rskafka::client::error::Error as RsKafkaError;
use rskafka::record::{Record, RecordAndOffset};
use snafu::{OptionExt, ResultExt};
use store_api::logstore::entry::{Id as EntryId, Offset as EntryOffset};
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::Id as NamespaceId;
use store_api::logstore::{AppendBatchResponse, AppendResponse, LogStore, RegionWalOptions};

use crate::error::{
    ConvertEntryIdToOffsetSnafu, ConvertRsKafkaOffsetToEntryOffsetSnafu, EmptyKafkaOffsetsSnafu,
    Error, GetKafkaTopicClientSnafu, MissingEntryOffsetSnafu, MissingKafkaTopicSnafu,
    ReadRecordFromKafkaSnafu, Result, WriteEntriesToKafkaSnafu,
};
use crate::kafka::topic_client_manager::{TopicClientManager, TopicClientManagerRef};
use crate::kafka::{EntryImpl, NamespaceImpl};

#[derive(Debug)]
pub struct KafkaLogStore {
    opts: KafkaOptions,
    topic_client_manager: TopicClientManagerRef,
}

impl KafkaLogStore {
    pub async fn try_new(kafka_opts: &KafkaOptions) -> Result<Self> {
        Ok(Self {
            opts: kafka_opts.clone(),
            topic_client_manager: Arc::new(TopicClientManager::try_new(kafka_opts).await?),
        })
    }

    /// Appends a batch of entries to the topic and returns the minimum start offsets of those entries grouped by region ids.
    async fn append_batch_to_topic(
        &self,
        entries: Vec<EntryImpl>,
        topic: Topic,
    ) -> Result<AppendBatchResponse> {
        // Safety: the caller ensures the input entries is not empty.
        assert!(!entries.is_empty());

        let region_ids = entries
            .iter()
            .map(|entry| entry.ns.region_id)
            .collect::<Vec<_>>();

        let topic_client = self
            .topic_client_manager
            .get_or_insert(&topic)
            .await
            .map_err(|_| GetKafkaTopicClientSnafu { topic: &topic }.build())?;
        let producer = &topic_client.producer;

        let produce_tasks = try_into_records(entries)?
            .into_iter()
            .map(|record| producer.produce(record))
            .collect::<Vec<_>>();
        let offsets = futures::future::try_join_all(produce_tasks)
            .await
            .context(WriteEntriesToKafkaSnafu { topic })?;

        let min_offset = offsets.into_iter().min().context(EmptyKafkaOffsetsSnafu)?;
        let min_offset: EntryOffset =
            min_offset
                .try_into()
                .context(ConvertRsKafkaOffsetToEntryOffsetSnafu {
                    rskafka_offset: min_offset,
                })?;

        // All regions belonging to a topic share the same entry offset
        // since that offset is the known minimum start offset for those regions.
        let num_region_ids = region_ids.len();
        let region_offset_map = region_ids
            .into_iter()
            .zip(vec![min_offset; num_region_ids])
            .collect();

        Ok(AppendBatchResponse {
            offsets: region_offset_map,
        })
    }
}

#[async_trait::async_trait]
impl LogStore for KafkaLogStore {
    type Error = Error;
    type Entry = EntryImpl;
    type Namespace = NamespaceImpl;

    /// Create an entry of the associate Entry type.
    fn entry<D: AsRef<[u8]>>(
        &self,
        data: D,
        entry_id: EntryId,
        ns: Self::Namespace,
    ) -> Self::Entry {
        EntryImpl::new(data.as_ref().to_vec(), entry_id, ns)
    }

    /// Append an `Entry` to WAL with given namespace and return append response containing
    /// the entry id.
    async fn append(&self, entry: Self::Entry) -> Result<AppendResponse> {
        let entry_id = entry.id;
        let region_id = entry.ns.region_id();
        let topic = entry.ns.topic.clone();

        let response = self
            .append_batch_to_topic(vec![entry], topic.clone())
            .await?;
        let offset = response
            .offsets
            .get(&region_id)
            .context(MissingEntryOffsetSnafu {
                entry_id,
                region_id,
                topic,
            })?;

        Ok(AppendResponse {
            entry_id,
            offset: Some(*offset),
        })
    }

    /// For a batch of log entries belonging to multiple regions, each assigned to a specific topic,
    /// we need to determine the minimum log offset returned for each region in this batch.
    /// During replay, we use this offset to fetch log entries for a region from its assigned topic.
    /// After fetching, we filter the entries to obtain log entries relevant to that specific region.
    async fn append_batch(&self, entries: Vec<Self::Entry>) -> Result<AppendBatchResponse> {
        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }

        // Groups entries by topic.
        let mut topic_entries: HashMap<String, Vec<_>> = HashMap::new();
        for entry in entries {
            topic_entries
                .entry(entry.ns.topic().clone())
                .or_default()
                .push(entry)
        }

        // Appends each group of entries to the corresponding topic.
        let append_tasks = topic_entries
            .into_iter()
            .map(|(topic, entries)| self.append_batch_to_topic(entries, topic))
            .collect::<Vec<_>>();
        let responses = futures::future::try_join_all(append_tasks).await?;

        // Each append task appends a batch of entries to a specific topic. Since each region is assigned only a single topic,
        // the returned `response.offsets` of different tasks are guaranteed to not have any overlapping. Therefore, a simple `extend` operation
        // is enough to find the known minimum start offset of the appended entries for each region.
        let mut region_offset_map = HashMap::new();
        for response in responses {
            region_offset_map.extend(response.offsets);
        }

        Ok(AppendBatchResponse {
            offsets: region_offset_map,
        })
    }

    /// Create a new `EntryStream` to asynchronously generates `Entry` with ids
    /// starting from `id`. The generated entries will be filtered by the namespace.
    async fn read(
        &self,
        ns: &Self::Namespace,
        entry_id: EntryId,
    ) -> Result<SendableEntryStream<Self::Entry, Self::Error>> {
        let topic = ns.topic().clone();
        let region_id = ns.region_id();
        let offset = try_into_offset(entry_id)?;

        let raw_client = self
            .topic_client_manager
            .get_or_insert(&topic)
            .await?
            .raw_client
            .clone();
        let mut stream_consumer = StreamConsumerBuilder::new(raw_client, StartOffset::At(offset))
            .with_max_batch_size(self.opts.max_batch_size.as_bytes() as i32)
            .with_max_wait_ms(self.opts.max_wait_time.as_millis() as i32)
            .build();

        // TODO(niebayes): May also need a buffering mechanism on fetching entries since the mapping between
        // entries and records is one-one currently.
        let stream = async_stream::stream!({
            while let Some(fetch_result) = stream_consumer.next().await {
                yield handle_fetch_result(fetch_result, &topic, region_id, offset);
            }
        });
        Ok(Box::pin(stream))
    }

    /// Create a namespace of the associate Namespace type
    fn namespace(
        &self,
        ns_id: NamespaceId,
        region_wal_options: &RegionWalOptions,
    ) -> Result<Self::Namespace> {
        let region_id = ns_id;
        let topic = region_wal_options
            .get(TOPIC_KEY)
            .context(MissingKafkaTopicSnafu {
                ns_id,
                region_wal_options: region_wal_options.clone(),
            })?;
        Ok(NamespaceImpl::new(region_id, topic.clone()))
    }

    /// Create a new `Namespace`.
    async fn create_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    /// Delete an existing `Namespace` with given ref.
    async fn delete_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    /// List all existing namespaces.
    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>> {
        Ok(vec![])
    }

    /// Mark all entry ids `<=id` of given `namespace` as obsolete so that logstore can safely delete
    /// the log files if all entries inside are obsolete. This method may not delete log
    /// files immediately.
    async fn obsolete(&self, _ns: Self::Namespace, _entry_id: EntryId) -> Result<()> {
        Ok(())
    }

    /// Stop components of logstore.
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}

// The mapping between entries and records is one-one currently.
// TODO(niebayes): Shall limit a record to be within the max batch size and group entries into several records if necessary.
fn try_into_records(entries: Vec<EntryImpl>) -> Result<Vec<Record>> {
    entries
        .into_iter()
        .map(|entry| entry.try_into())
        .collect::<Result<Vec<_>>>()
}

/// Try to convert the given entry id into an offset used by Kafka.
fn try_into_offset(entry_id: EntryId) -> Result<i64> {
    entry_id
        .try_into()
        .map_err(|_| ConvertEntryIdToOffsetSnafu { entry_id }.build())
}

fn try_from_record(record: Record) -> Result<Vec<EntryImpl>> {
    let entry = record.try_into()?;
    Ok(vec![entry])
}

fn handle_fetch_result(
    fetch_result: std::result::Result<(RecordAndOffset, i64), RsKafkaError>,
    topic: &Topic,
    region_id: u64,
    start_offset: i64,
) -> Result<Vec<EntryImpl>> {
    match fetch_result {
        Ok((record_and_offset, _)) => {
            let entries = try_from_record(record_and_offset.record)?
                .into_iter()
                .filter(|entry| entry.ns.region_id() == region_id)
                .collect();
            Ok(entries)
        }
        Err(e) => Err(e).context(ReadRecordFromKafkaSnafu {
            start_offset,
            topic,
            region_id,
        }),
    }
}

// TODO(niebayes): Add necessary unit tests.
