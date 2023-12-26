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
        debug!("LogStore handles append_batch with entries {:?}", entries);

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
        let region_ids = producers.keys().cloned().collect::<Vec<_>>();

        let tasks = producers
            .into_values()
            .map(|producer| producer.produce(&self.client_manager))
            .collect::<Vec<_>>();
        // Each produce operation returns a kafka offset of the produced record.
        // The offsets are then converted to entry ids.
        let entry_ids = futures::future::try_join_all(tasks)
            .await?
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        debug!("The entries are appended at offsets {:?}", entry_ids);

        Ok(AppendBatchResponse {
            last_entry_ids: region_ids.into_iter().zip(entry_ids).collect(),
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
        // Reads entries with offsets in the range [start_offset, end_offset).
        let start_offset = Offset::try_from(entry_id)?.0;

        // Abort if there're no new entries.
        // FIXME(niebayes): how come this case happens?
        if start_offset > end_offset {
            warn!(
                "No new entries for ns {} in range [{}, {})",
                ns, start_offset, end_offset
            );
            return Ok(futures_util::stream::empty().boxed());
        }

        let mut stream_consumer = StreamConsumerBuilder::new(client, StartOffset::At(start_offset))
            .with_max_batch_size(self.config.max_batch_size.as_bytes() as i32)
            .with_max_wait_ms(self.config.produce_record_timeout.as_millis() as i32)
            .build();

        debug!(
            "Built a stream consumer for ns {} to consume entries in range [{}, {})",
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
                debug!(
                    "Read a record at offset {} for ns {}, high watermark: {}",
                    record_offset, ns_clone, high_watermark
                );

                let entries = decode_from_record(record.record)?;

                // Filters entries by region id.
                if let Some(entry) = entries.first()
                    && entry.ns.region_id == region_id
                {
                    yield Ok(entries);
                } else {
                    yield Ok(vec![]);
                }

                // Terminates the stream if the entry with the end offset was read.
                if record_offset >= end_offset {
                    debug!(
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
