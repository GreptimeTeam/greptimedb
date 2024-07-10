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

use common_wal::config::kafka::DatanodeKafkaConfig;
use rskafka::client::partition::{Compression, PartitionClient, UnknownTopicHandling};
use rskafka::client::ClientBuilder;
use rskafka::BackoffConfig;
use snafu::ResultExt;
use store_api::logstore::provider::KafkaProvider;
use tokio::sync::{Mutex, RwLock};

use super::producer::OrderedBatchProducer;
use crate::error::{
    BuildClientSnafu, BuildPartitionClientSnafu, ResolveKafkaEndpointSnafu, Result,
};
use crate::kafka::producer::OrderedBatchProducerRef;

// Each topic only has one partition for now.
// The `DEFAULT_PARTITION` refers to the index of the partition.
const DEFAULT_PARTITION: i32 = 0;

// Max batch size for a `OrderedBatchProducer` to handle requests.
const REQUEST_BATCH_SIZE: usize = 64;

/// Arc wrapper of ClientManager.
pub(crate) type ClientManagerRef = Arc<ClientManager>;

/// Topic client.
#[derive(Debug, Clone)]
pub(crate) struct Client {
    client: Arc<PartitionClient>,
    producer: OrderedBatchProducerRef,
}

impl Client {
    pub(crate) fn client(&self) -> &Arc<PartitionClient> {
        &self.client
    }

    pub(crate) fn producer(&self) -> &OrderedBatchProducerRef {
        &self.producer
    }
}

/// Manages client construction and accesses.
#[derive(Debug)]
pub(crate) struct ClientManager {
    client: rskafka::client::Client,
    /// Used to initialize a new [Client].
    mutex: Mutex<()>,
    instances: RwLock<HashMap<Arc<KafkaProvider>, Client>>,

    producer_channel_size: usize,
    producer_request_batch_size: usize,
    flush_batch_size: usize,
    compression: Compression,
}

impl ClientManager {
    /// Tries to create a ClientManager.
    pub(crate) async fn try_new(config: &DatanodeKafkaConfig) -> Result<Self> {
        // Sets backoff config for the top-level kafka client and all clients constructed by it.
        let backoff_config = BackoffConfig {
            init_backoff: config.backoff.init,
            max_backoff: config.backoff.max,
            base: config.backoff.base as f64,
            deadline: config.backoff.deadline,
        };
        let broker_endpoints = common_wal::resolve_to_ipv4(&config.broker_endpoints)
            .await
            .context(ResolveKafkaEndpointSnafu)?;
        let client = ClientBuilder::new(broker_endpoints)
            .backoff_config(backoff_config)
            .build()
            .await
            .with_context(|_| BuildClientSnafu {
                broker_endpoints: config.broker_endpoints.clone(),
            })?;

        Ok(Self {
            client,
            mutex: Mutex::new(()),
            instances: RwLock::new(HashMap::new()),
            producer_channel_size: REQUEST_BATCH_SIZE * 2,
            producer_request_batch_size: REQUEST_BATCH_SIZE,
            flush_batch_size: config.max_batch_bytes.as_bytes() as usize,
            compression: Compression::Lz4,
        })
    }

    async fn try_insert(&self, provider: &Arc<KafkaProvider>) -> Result<Client> {
        let _guard = self.mutex.lock().await;

        let client = self.instances.read().await.get(provider).cloned();
        match client {
            Some(client) => Ok(client),
            None => {
                let client = self.try_create_client(provider).await?;
                self.instances
                    .write()
                    .await
                    .insert(provider.clone(), client.clone());
                Ok(client)
            }
        }
    }

    /// Gets the client associated with the topic. If the client does not exist, a new one will
    /// be created and returned.
    pub(crate) async fn get_or_insert(&self, provider: &Arc<KafkaProvider>) -> Result<Client> {
        let client = self.instances.read().await.get(provider).cloned();
        match client {
            Some(client) => Ok(client),
            None => self.try_insert(provider).await,
        }
    }

    async fn try_create_client(&self, provider: &Arc<KafkaProvider>) -> Result<Client> {
        // Sets to Retry to retry connecting if the kafka cluter replies with an UnknownTopic error.
        // That's because the topic is believed to exist as the metasrv is expected to create required topics upon start.
        // The reconnecting won't stop until succeed or a different error returns.
        let client = self
            .client
            .partition_client(
                provider.topic.as_str(),
                DEFAULT_PARTITION,
                UnknownTopicHandling::Retry,
            )
            .await
            .context(BuildPartitionClientSnafu {
                topic: &provider.topic,
                partition: DEFAULT_PARTITION,
            })
            .map(Arc::new)?;

        let producer = Arc::new(OrderedBatchProducer::new(
            client.clone(),
            self.compression,
            self.producer_channel_size,
            self.producer_request_batch_size,
            self.flush_batch_size,
        ));

        Ok(Client { client, producer })
    }
}

#[cfg(test)]
mod tests {
    use common_wal::test_util::run_test_with_kafka_wal;
    use tokio::sync::Barrier;

    use super::*;

    /// Creates `num_topiocs` number of topics each will be decorated by the given decorator.
    pub async fn create_topics<F>(
        num_topics: usize,
        decorator: F,
        broker_endpoints: &[String],
    ) -> Vec<String>
    where
        F: Fn(usize) -> String,
    {
        assert!(!broker_endpoints.is_empty());
        let client = ClientBuilder::new(broker_endpoints.to_vec())
            .build()
            .await
            .unwrap();
        let ctrl_client = client.controller_client().unwrap();
        let (topics, tasks): (Vec<_>, Vec<_>) = (0..num_topics)
            .map(|i| {
                let topic = decorator(i);
                let task = ctrl_client.create_topic(topic.clone(), 1, 1, 500);
                (topic, task)
            })
            .unzip();
        futures::future::try_join_all(tasks).await.unwrap();
        topics
    }

    /// Prepares for a test in that a collection of topics and a client manager are created.
    async fn prepare(
        test_name: &str,
        num_topics: usize,
        broker_endpoints: Vec<String>,
    ) -> (ClientManager, Vec<String>) {
        let topics = create_topics(
            num_topics,
            |i| format!("{test_name}_{}_{}", i, uuid::Uuid::new_v4()),
            &broker_endpoints,
        )
        .await;

        let config = DatanodeKafkaConfig {
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
                    let provider = Arc::new(KafkaProvider::new(topic.to_string()));
                    manager.get_or_insert(&provider).await.unwrap();
                }

                // Ensures all clients exist.
                let client_pool = manager.instances.read().await;
                let all_exist = topics.iter().all(|topic| {
                    let provider = Arc::new(KafkaProvider::new(topic.to_string()));
                    client_pool.contains_key(&provider)
                });
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
                            let provider = Arc::new(KafkaProvider::new(topic));
                            assert!(manager.get_or_insert(&provider).await.is_ok());
                        })
                    })
                    .collect::<Vec<_>>();
                futures::future::try_join_all(tasks).await.unwrap();

                // Ensures all clients exist.
                let client_pool = manager.instances.read().await;
                let all_exist = topics.iter().all(|topic| {
                    let provider = Arc::new(KafkaProvider::new(topic.to_string()));
                    client_pool.contains_key(&provider)
                });
                assert!(all_exist);
            })
        })
        .await;
    }
}
