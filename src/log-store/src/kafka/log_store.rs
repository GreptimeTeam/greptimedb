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
use rskafka::record::{Record, RecordAndOffset};
use snafu::{OptionExt, ResultExt};
use store_api::logstore::entry::{Id as EntryId, Offset as EntryOffset};
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::Id as NamespaceId;
use store_api::logstore::{AppendBatchResponse, AppendResponse, LogStore};

use crate::error::{
    CastOffsetSnafu, ConsumeRecordSnafu, EmptyOffsetsSnafu, Error, GetClientSnafu,
    ProduceEntriesSnafu, Result,
};
use crate::kafka::client_manager::{ClientManager, ClientManagerRef};
use crate::kafka::{EntryImpl, NamespaceImpl};

type ConsumeResult = std::result::Result<(RecordAndOffset, i64), rskafka::client::error::Error>;

/// A log store backed by Kafka.
#[derive(Debug)]
pub struct KafkaLogStore {
    config: KafkaConfig,
    /// Manages rskafka clients through which the log store contact the Kafka cluster.
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

    /// Appends a batch of entries to a topic. The entries may come from multiple regions.
    /// Returns a tuple where the first element is the topic while the second is the minimum
    /// start offset of the entries appended to the topic.
    async fn append_batch_to_topic(
        &self,
        entries: Vec<EntryImpl>,
        topic: Topic,
    ) -> Result<(Topic, EntryOffset)> {
        // Safety: the caller ensures the input entries is not empty.
        assert!(!entries.is_empty());

        // Gets the client associated with the topic.
        let client = self
            .client_manager
            .get_or_insert(&topic)
            .await
            .map_err(|e| {
                GetClientSnafu {
                    topic: &topic,
                    error: e.to_string(),
                }
                .build()
            })?;

        // Convert entries to records and produce them to Kafka.
        let mut tasks = Vec::with_capacity(entries.len());
        for entry in entries {
            let record: Record = entry.try_into()?;
            let task = client.producer.produce(record);
            tasks.push(task);
        }
        // Each produce task will return an offset to represent the minimum start offset of the entries produced by the task.
        let offsets = futures::future::try_join_all(tasks)
            .await
            .context(ProduceEntriesSnafu { topic: &topic })?;

        // Since the task completion order is not deterministic, a `min` operation is required to find the minimum offset.
        let min_offset = offsets
            .into_iter()
            .min()
            .context(EmptyOffsetsSnafu { topic: &topic })?;
        let min_offset: EntryOffset = min_offset
            .try_into()
            .map_err(|_| CastOffsetSnafu { offset: min_offset }.build())?;
        Ok((topic, min_offset))
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
        EntryImpl::new(data.as_ref().to_vec(), entry_id, ns)
    }

    /// Appends an entry to the log store and returns a response containing the entry id and an optional entry offset.
    async fn append(&self, entry: Self::Entry) -> Result<AppendResponse> {
        let entry_id = entry.id;
        let topic = entry.ns.topic.clone();

        let offset = self.append_batch_to_topic(vec![entry], topic).await?.1;
        Ok(AppendResponse {
            entry_id,
            offset: Some(offset),
        })
    }

    /// For a batch of entries belonging to multiple regions, each assigned to a specific topic,
    /// we need to determine the minimum start offset returned for each region in this batch.
    /// During replay a region, we use this offset to fetch entries for the region from its assigned topic.
    /// After fetching, we filter the entries to obtain entries relevant to the region.
    async fn append_batch(&self, entries: Vec<Self::Entry>) -> Result<AppendBatchResponse> {
        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }

        // The entries are grouped by topic since the number of regions might be very large
        // while the number of topics are well controlled,
        let mut topic_entries: HashMap<_, Vec<_>> = HashMap::new();
        // A utility map used to construct the result response.
        let mut topic_regions: HashMap<_, Vec<_>> = HashMap::new();
        for entry in entries {
            let topic = entry.ns.topic.clone();
            let region_id = entry.ns.region_id;
            topic_entries.entry(topic.clone()).or_default().push(entry);
            topic_regions.entry(topic).or_default().push(region_id);
        }

        // Appends each group of entries to the corresponding topic.
        let tasks = topic_entries
            .into_iter()
            .map(|(topic, entries)| self.append_batch_to_topic(entries, topic))
            .collect::<Vec<_>>();
        let topic_offset: HashMap<_, _> = futures::future::try_join_all(tasks)
            .await?
            .into_iter()
            .collect();

        let mut region_offset = HashMap::new();
        for (topic, regions) in topic_regions {
            let offset = topic_offset[&topic];
            regions.into_iter().for_each(|region| {
                region_offset.insert(region, offset);
            });
        }
        Ok(AppendBatchResponse {
            offsets: region_offset,
        })
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
        let offset = try_get_offset(entry_id)?;

        let raw_client = self
            .client_manager
            .get_or_insert(&topic)
            .await?
            .raw_client
            .clone();
        // Reads the entries starting from exactly the specified offset.
        let mut stream_consumer = StreamConsumerBuilder::new(raw_client, StartOffset::At(offset))
            .with_max_batch_size(self.config.max_batch_size.as_bytes() as i32)
            .with_max_wait_ms(self.config.max_wait_time.as_millis() as i32)
            .build();
        let stream = async_stream::stream!({
            while let Some(consume_result) = stream_consumer.next().await {
                yield handle_consume_result(consume_result, &topic, region_id, offset);
            }
        });
        Ok(Box::pin(stream))
    }

    /// Create a namespace of the associate Namespace type
    fn namespace(&self, ns_id: NamespaceId, wal_options: &WalOptions) -> Self::Namespace {
        // Warning: we assume the database manager, not the database itself, is responsible for ensuring that
        // the wal config for metasrv and that for datanode are consistent, i.e. the wal provider should be identical.
        // With such an assumption, the unreachable is safe here.
        let WalOptions::Kafka(kafka_options) = wal_options else {
            unreachable!()
        };
        NamespaceImpl {
            region_id: ns_id,
            topic: kafka_options.topic.clone(),
        }
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

/// Tries to get the physical offset of the entry with the given entry id.
// TODO(niebayes): a mapping between entry id and entry offset needs to maintained at somewhere.
// One solution is to store the mapping at a specific Kafka topic. Each time the mapping is updated,
// a new record is constructed and appended to the topic. On initializing the log store, the latest
// record is pulled from Kafka cluster.
//
// Another solution is to store the mapping at the kv backend. We design a dedicated key, e.g. ID_TO_OFFSET_MAP_KEY.
// Each time the mapping is updated, the map is serialized into a vector of bytes and stored into the kv backend at the given key.
// On initializing the log store, the map is deserialized from the kv backend.
fn try_get_offset(entry_id: EntryId) -> Result<i64> {
    let _ = entry_id;
    todo!()
}

fn handle_consume_result(
    result: ConsumeResult,
    topic: &Topic,
    region_id: u64,
    offset: i64,
) -> Result<Vec<EntryImpl>> {
    match result {
        Ok((record_and_offset, _)) => {
            let entry = EntryImpl::try_from(record_and_offset.record)?;
            // Only produces entries belonging to the region with the given region id.
            if entry.ns.region_id == region_id {
                Ok(vec![entry])
            } else {
                Ok(vec![])
            }
        }
        Err(e) => Err(e).context(ConsumeRecordSnafu {
            offset,
            topic,
            region_id,
        }),
    }
}
