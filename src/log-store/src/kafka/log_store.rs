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
use futures_util::StreamExt;
use rskafka::client::consumer::{StartOffset, StreamConsumerBuilder};
use store_api::logstore::entry::Id as EntryId;
use store_api::logstore::entry_stream::SendableEntryStream;
use store_api::logstore::namespace::Id as NamespaceId;
use store_api::logstore::{AppendBatchResponse, AppendResponse, LogStore};

use crate::error::{Error, Result};
use crate::kafka::client_manager::{ClientManager, ClientManagerRef};
use crate::kafka::offset::Offset;
use crate::kafka::record_utils::{handle_consume_result, RecordProducer};
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

    /// Creates an entry.
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
        Ok(AppendResponse { entry_id })
    }

    /// Appends a batch of entries to the log store. The response contains a map where the key
    /// is a region id while the value if the id of the entry, the first entry of the entries belong to the region,
    /// written into the log store.
    async fn append_batch(&self, entries: Vec<Self::Entry>) -> Result<AppendBatchResponse> {
        if entries.is_empty() {
            return Ok(AppendBatchResponse::default());
        }

        // Groups entries by region id and push them to an associated record producer.
        let mut producers: HashMap<_, RecordProducer> = HashMap::with_capacity(entries.len());
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
        Ok(AppendBatchResponse {
            entry_ids: region_ids.into_iter().zip(entry_ids).collect(),
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

        // Reads the entries starting from exactly the specified offset.
        let offset = Offset::try_from(entry_id)?.0;
        let mut stream_consumer = StreamConsumerBuilder::new(client, StartOffset::At(offset))
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

    /// Creates a namespace.
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

    async fn create_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    async fn delete_namespace(&self, _ns: &Self::Namespace) -> Result<()> {
        Ok(())
    }

    async fn list_namespaces(&self) -> Result<Vec<Self::Namespace>> {
        Ok(vec![])
    }

    /// Marks all entry ids `<=id` of given `namespace` as obsolete so that logstore can safely delete
    /// the log files if all entries inside are obsolete. This method may not delete log
    /// files immediately.
    async fn obsolete(&self, _ns: Self::Namespace, _entry_id: EntryId) -> Result<()> {
        Ok(())
    }

    /// Stops components of the logstore.
    async fn stop(&self) -> Result<()> {
        Ok(())
    }
}
