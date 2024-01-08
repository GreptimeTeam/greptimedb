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

use common_config::wal::{KafkaConfig, KafkaWalTopic as Topic};
use rskafka::client::partition::{PartitionClient, UnknownTopicHandling};
use rskafka::client::producer::aggregator::RecordAggregator;
use rskafka::client::producer::{BatchProducer, BatchProducerBuilder};
use rskafka::client::{Client as RsKafkaClient, ClientBuilder};
use rskafka::BackoffConfig;
use snafu::ResultExt;
use tokio::sync::RwLock;

use crate::error::{BuildClientSnafu, BuildPartitionClientSnafu, Result};

// Each topic only has one partition for now.
// The `DEFAULT_PARTITION` refers to the index of the partition.
const DEFAULT_PARTITION: i32 = 0;

/// Arc wrapper of ClientManager.
pub(crate) type ClientManagerRef = Arc<ClientManager>;

/// A client through which to contact Kafka cluster. Each client associates with one partition of a topic.
/// Since a topic only has one partition in our design, the mapping between clients and topics are one-one.
#[derive(Debug, Clone)]
pub(crate) struct Client {
    /// A raw client used to construct a batch producer and/or a stream consumer for a specific topic.
    pub(crate) raw_client: Arc<PartitionClient>,
    /// A producer used to buffer log entries for a specific topic before sending them in a batching manner.
    pub(crate) producer: Arc<BatchProducer<RecordAggregator>>,
}

impl Client {
    /// Creates a Client from the raw client.
    pub(crate) fn new(raw_client: Arc<PartitionClient>, config: &KafkaConfig) -> Self {
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
pub(crate) struct ClientManager {
    pub(crate) config: KafkaConfig,
    /// Top-level client in kafka. All clients are constructed by this client.
    client_factory: RsKafkaClient,
    /// A pool maintaining a collection of clients.
    /// Key: a topic. Value: the associated client of the topic.
    client_pool: RwLock<HashMap<Topic, Client>>,
}

impl ClientManager {
    /// Tries to create a ClientManager.
    pub(crate) async fn try_new(config: &KafkaConfig) -> Result<Self> {
        // Sets backoff config for the top-level kafka client and all clients constructed by it.
        let backoff_config = BackoffConfig {
            init_backoff: config.backoff.init,
            max_backoff: config.backoff.max,
            base: config.backoff.base as f64,
            deadline: config.backoff.deadline,
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
            client_pool: RwLock::new(HashMap::new()),
        })
    }

    /// Gets the client associated with the topic. If the client does not exist, a new one will
    /// be created and returned.
    pub(crate) async fn get_or_insert(&self, topic: &Topic) -> Result<Client> {
        {
            let client_pool = self.client_pool.read().await;
            if let Some(client) = client_pool.get(topic) {
                return Ok(client.clone());
            }
        }

        let mut client_pool = self.client_pool.write().await;
        match client_pool.get(topic) {
            Some(client) => Ok(client.clone()),
            None => {
                let client = self.try_create_client(topic).await?;
                client_pool.insert(topic.clone(), client.clone());
                Ok(client)
            }
        }
    }

    async fn try_create_client(&self, topic: &Topic) -> Result<Client> {
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

        Ok(Client::new(raw_client, &self.config))
    }
}

#[cfg(test)]
mod tests {
    use common_meta::wal::kafka::test_util::run_test_with_kafka_wal;
    use tokio::sync::Barrier;

    use super::*;
    use crate::test_util::kafka::create_topics;

    /// Prepares for a test in that a collection of topics and a client manager are created.
    async fn prepare(
        test_name: &str,
        num_topics: usize,
        broker_endpoints: Vec<String>,
    ) -> (ClientManager, Vec<Topic>) {
        let topics = create_topics(
            num_topics,
            |i| format!("{test_name}_{}_{}", i, uuid::Uuid::new_v4()),
            &broker_endpoints,
        )
        .await;

        let config = KafkaConfig {
            broker_endpoints,
            ..Default::default()
        };
        let manager = ClientManager::try_new(&config).await.unwrap();

        (manager, topics)
    }

    /// Sends `get_or_insert` requests sequentially to the client manager, and checks if it could handle them correctly.
    #[tokio::test]
    async fn test_sequential() {
        run_test_with_kafka_wal(|broker_endpoints| {
            Box::pin(async {
                let (manager, topics) = prepare("test_sequential", 128, broker_endpoints).await;
                // Assigns multiple regions to a topic.
                let region_topic = (0..512)
                    .map(|region_id| (region_id, &topics[region_id % topics.len()]))
                    .collect::<HashMap<_, _>>();

                // Gets all clients sequentially.
                for (_, topic) in region_topic {
                    manager.get_or_insert(topic).await.unwrap();
                }

                // Ensures all clients exist.
                let client_pool = manager.client_pool.read().await;
                let all_exist = topics.iter().all(|topic| client_pool.contains_key(topic));
                assert!(all_exist);
            })
        })
        .await;
    }

    /// Sends `get_or_insert` requests in parallel to the client manager, and checks if it could handle them correctly.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_parallel() {
        run_test_with_kafka_wal(|broker_endpoints| {
            Box::pin(async {
                let (manager, topics) = prepare("test_parallel", 128, broker_endpoints).await;
                // Assigns multiple regions to a topic.
                let region_topic = (0..512)
                    .map(|region_id| (region_id, topics[region_id % topics.len()].clone()))
                    .collect::<HashMap<_, _>>();

                // Gets all clients in parallel.
                let manager = Arc::new(manager);
                let barrier = Arc::new(Barrier::new(region_topic.len()));
                let tasks = region_topic
                    .into_values()
                    .map(|topic| {
                        let manager = manager.clone();
                        let barrier = barrier.clone();
                        tokio::spawn(async move {
                            barrier.wait().await;
                            assert!(manager.get_or_insert(&topic).await.is_ok());
                        })
                    })
                    .collect::<Vec<_>>();
                futures::future::try_join_all(tasks).await.unwrap();

                // Ensures all clients exist.
                let client_pool = manager.client_pool.read().await;
                let all_exist = topics.iter().all(|topic| client_pool.contains_key(topic));
                assert!(all_exist);
            })
        })
        .await;
    }
}
