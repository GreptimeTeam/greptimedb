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

use std::sync::Arc;
use std::time::Duration;

use common_config::wal::{KafkaConfig, KafkaWalTopic as Topic};
use dashmap::mapref::entry::Entry as DashMapEntry;
use dashmap::DashMap;
use rskafka::client::partition::{PartitionClient, UnknownTopicHandling};
use rskafka::client::producer::aggregator::RecordAggregator;
use rskafka::client::producer::{BatchProducer, BatchProducerBuilder};
use rskafka::client::{Client as RsKafkaClient, ClientBuilder};
use rskafka::BackoffConfig;
use snafu::ResultExt;

use crate::error::{BuildClientSnafu, BuildPartitionClientSnafu, Result};

// Each topic only has one partition for now.
// The `DEFAULT_PARTITION` refers to the index of the partition.
const DEFAULT_PARTITION: i32 = 0;

/// Arc wrapper of Client.
pub(super) type ClientRef = Arc<Client>;
/// Arc wrapper of ClientManager.
pub(super) type ClientManagerRef = Arc<ClientManager>;

/// A client through which to contact Kafka cluster. Each client associates with one partition of a topic.
/// Since a topic only has one partition in our design, the mapping between clients and topics are one-one.
#[derive(Debug)]
pub(super) struct Client {
    /// A raw client used to construct a batch producer and/or a stream consumer for a specific topic.
    pub(super) raw_client: Arc<PartitionClient>,
    /// A producer used to buffer log entries for a specific topic before sending them in a batching manner.
    pub(super) producer: Arc<BatchProducer<RecordAggregator>>,
}

impl Client {
    /// Creates a Client from the raw client.
    pub(super) fn new(raw_client: Arc<PartitionClient>, config: &KafkaConfig) -> Self {
        let record_aggregator = RecordAggregator::new(config.max_batch_size.as_bytes() as usize);
        let batch_producer = BatchProducerBuilder::new(raw_client.clone())
            .with_compression(config.compression)
            .with_linger(config.linger)
            .build(record_aggregator);

        Self {
            raw_client,
            producer: Arc::new(batch_producer),
        }
    }
}

/// Manages client construction and accesses.
#[derive(Debug)]
pub(super) struct ClientManager {
    config: KafkaConfig,
    /// Top-level client in rskafka. All clients are constructed by this client.
    client_factory: RsKafkaClient,
    /// A pool maintaining a collection of clients.
    /// Key: a topic. Value: the associated client of the topic.
    client_pool: DashMap<Topic, ClientRef>,
}

impl ClientManager {
    /// Tries to create a ClientManager.
    pub(super) async fn try_new(config: &KafkaConfig) -> Result<Self> {
        // Sets backoff config for the top-level rskafka client and all clients constructed by it.
        let backoff_config = BackoffConfig {
            init_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(10),
            base: 2.,
            // Stop reconnecting if the total wait time reaches the deadline.
            deadline: Some(Duration::from_secs(60 * 5)),
        };
        let client = ClientBuilder::new(config.broker_endpoints.clone())
            .backoff_config(backoff_config)
            .build()
            .await
            .with_context(|_| BuildClientSnafu {
                broker_endpoints: config.broker_endpoints.clone(),
            })?;

        Ok(Self {
            config: config.clone(),
            client_factory: client,
            client_pool: DashMap::with_capacity(config.num_topics),
        })
    }

    /// Gets the client associated with the topic. If the client does not exist, a new one will
    /// be created and returned.
    pub(super) async fn get_or_insert(&self, topic: &Topic) -> Result<ClientRef> {
        match self.client_pool.entry(topic.to_string()) {
            DashMapEntry::Occupied(entry) => Ok(entry.get().clone()),
            DashMapEntry::Vacant(entry) => {
                let topic_client = self.try_create_client(topic).await?;
                Ok(entry.insert(topic_client).clone())
            }
        }
    }

    async fn try_create_client(&self, topic: &Topic) -> Result<ClientRef> {
        // Sets to Retry to retry connecting if the kafka cluter replies with an UnknownTopic error.
        // That's because the topic is believed to exist as the metasrv is expected to create required topics upon start.
        // The reconnecting won't stop until succeed or a different error returns.
        let raw_client = self
            .client_factory
            .partition_client(topic, DEFAULT_PARTITION, UnknownTopicHandling::Retry)
            .await
            .context(BuildPartitionClientSnafu {
                topic,
                partition: DEFAULT_PARTITION,
            })
            .map(Arc::new)?;

        Ok(Arc::new(Client::new(raw_client, &self.config)))
    }
}
