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

use common_telemetry::{error, info};
use common_wal::config::kafka::MetasrvKafkaConfig;
use common_wal::TopicSelectorType;
use rskafka::client::controller::ControllerClient;
use rskafka::client::error::Error as RsKafkaError;
use rskafka::client::error::ProtocolError::TopicAlreadyExists;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::client::{Client, ClientBuilder};
use rskafka::record::Record;
use rskafka::BackoffConfig;
use snafu::{ensure, ResultExt};

use crate::error::{
    BuildKafkaClientSnafu, BuildKafkaCtrlClientSnafu, BuildKafkaPartitionClientSnafu,
    CreateKafkaWalTopicSnafu, InvalidNumTopicsSnafu, ProduceRecordSnafu, ResolveKafkaEndpointSnafu,
    Result, TlsConfigSnafu,
};
use crate::key::topic::TopicPool;
use crate::kv_backend::KvBackendRef;
use crate::wal_options_allocator::kafka::topic_selector::{
    RoundRobinTopicSelector, TopicSelectorRef,
};

// Each topic only has one partition for now.
// The `DEFAULT_PARTITION` refers to the index of the partition.
const DEFAULT_PARTITION: i32 = 0;

/// Manages topic initialization and selection.
pub struct TopicManager {
    config: MetasrvKafkaConfig,
    pub(crate) topic_pool: TopicPool,
    pub(crate) topic_selector: TopicSelectorRef,
    kv_backend: KvBackendRef,
}

impl TopicManager {
    /// Creates a new topic manager.
    pub fn new(config: MetasrvKafkaConfig, kv_backend: KvBackendRef) -> Self {
        let selector = match config.kafka_topic.selector_type {
            TopicSelectorType::RoundRobin => RoundRobinTopicSelector::with_shuffle(),
        };
        let num_topics = config.kafka_topic.num_topics;
        let prefix = config.kafka_topic.topic_name_prefix.clone();

        Self {
            config,
            topic_pool: TopicPool::new(num_topics, prefix),
            topic_selector: Arc::new(selector),
            kv_backend,
        }
    }

    /// Tries to initialize the topic manager.
    /// The initializer first tries to restore persisted topics from the kv backend.
    /// If not enough topics retrieved, the initializer will try to contact the Kafka cluster and request creating more topics.
    pub async fn start(&self) -> Result<()> {
        // Skip creating topics.
        if !self.config.auto_create_topics {
            return Ok(());
        }
        let num_topics = self.config.kafka_topic.num_topics;
        ensure!(num_topics > 0, InvalidNumTopicsSnafu { num_topics });

        let topics_to_be_created = self.topic_pool.to_be_created(&self.kv_backend).await?;

        if !topics_to_be_created.is_empty() {
            self.try_create_topics(&topics_to_be_created).await?;
            self.topic_pool.persist(&self.kv_backend).await?;
        }
        Ok(())
    }

    /// Tries to create topics specified by indexes in `to_be_created`.
    async fn try_create_topics(&self, topics: &[&String]) -> Result<()> {
        // Builds an kafka controller client for creating topics.
        let backoff_config = BackoffConfig {
            init_backoff: self.config.backoff.init,
            max_backoff: self.config.backoff.max,
            base: self.config.backoff.base as f64,
            deadline: self.config.backoff.deadline,
        };
        let broker_endpoints =
            common_wal::resolve_to_ipv4(&self.config.connection.broker_endpoints)
                .await
                .context(ResolveKafkaEndpointSnafu)?;
        let mut builder = ClientBuilder::new(broker_endpoints).backoff_config(backoff_config);
        if let Some(sasl) = &self.config.connection.sasl {
            builder = builder.sasl_config(sasl.config.clone().into_sasl_config());
        };
        if let Some(tls) = &self.config.connection.tls {
            builder = builder.tls_config(tls.to_tls_config().await.context(TlsConfigSnafu)?)
        };
        let client = builder
            .build()
            .await
            .with_context(|_| BuildKafkaClientSnafu {
                broker_endpoints: self.config.connection.broker_endpoints.clone(),
            })?;

        let control_client = client
            .controller_client()
            .context(BuildKafkaCtrlClientSnafu)?;

        // Try to create missing topics.
        let tasks = topics
            .iter()
            .map(|topic| async {
                self.try_create_topic(topic, &control_client).await?;
                self.try_append_noop_record(topic, &client).await?;
                Ok(())
            })
            .collect::<Vec<_>>();
        futures::future::try_join_all(tasks).await.map(|_| ())
    }

    /// Selects one topic from the topic pool through the topic selector.
    pub fn select(&self) -> Result<&String> {
        self.topic_selector.select(&self.topic_pool.topics)
    }

    /// Selects a batch of topics from the topic pool through the topic selector.
    pub fn select_batch(&self, num_topics: usize) -> Result<Vec<&String>> {
        (0..num_topics)
            .map(|_| self.topic_selector.select(&self.topic_pool.topics))
            .collect()
    }

    async fn try_append_noop_record(&self, topic: &String, client: &Client) -> Result<()> {
        let partition_client = client
            .partition_client(topic, DEFAULT_PARTITION, UnknownTopicHandling::Retry)
            .await
            .context(BuildKafkaPartitionClientSnafu {
                topic,
                partition: DEFAULT_PARTITION,
            })?;

        partition_client
            .produce(
                vec![Record {
                    key: None,
                    value: None,
                    timestamp: chrono::Utc::now(),
                    headers: Default::default(),
                }],
                Compression::Lz4,
            )
            .await
            .context(ProduceRecordSnafu { topic })?;

        Ok(())
    }

    async fn try_create_topic(&self, topic: &String, client: &ControllerClient) -> Result<()> {
        match client
            .create_topic(
                topic.clone(),
                self.config.kafka_topic.num_partitions,
                self.config.kafka_topic.replication_factor,
                self.config.kafka_topic.create_topic_timeout.as_millis() as i32,
            )
            .await
        {
            Ok(_) => {
                info!("Successfully created topic {}", topic);
                Ok(())
            }
            Err(e) => {
                if Self::is_topic_already_exist_err(&e) {
                    info!("The topic {} already exists", topic);
                    Ok(())
                } else {
                    error!("Failed to create a topic {}, error {:?}", topic, e);
                    Err(e).context(CreateKafkaWalTopicSnafu)
                }
            }
        }
    }

    fn is_topic_already_exist_err(e: &RsKafkaError) -> bool {
        matches!(
            e,
            &RsKafkaError::ServerError {
                protocol_error: TopicAlreadyExists,
                ..
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use common_wal::config::kafka::common::{KafkaConnectionConfig, KafkaTopicConfig};
    use common_wal::test_util::run_test_with_kafka_wal;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;

    /// Tests that the topic manager could allocate topics correctly.
    #[tokio::test]
    async fn test_alloc_topics() {
        run_test_with_kafka_wal(|broker_endpoints| {
            Box::pin(async {
                // Constructs topics that should be created.
                let topics = (0..256)
                    .map(|i| format!("test_alloc_topics_{}_{}", i, uuid::Uuid::new_v4()))
                    .collect::<Vec<_>>();

                // Creates a topic manager.
                let kafka_topic = KafkaTopicConfig {
                    replication_factor: broker_endpoints.len() as i16,
                    ..Default::default()
                };
                let config = MetasrvKafkaConfig {
                    connection: KafkaConnectionConfig {
                        broker_endpoints,
                        ..Default::default()
                    },
                    kafka_topic,
                    ..Default::default()
                };
                let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
                let mut manager = TopicManager::new(config.clone(), kv_backend);
                // Replaces the default topic pool with the constructed topics.
                manager.topic_pool.topics.clone_from(&topics);
                // Replaces the default selector with a round-robin selector without shuffled.
                manager.topic_selector = Arc::new(RoundRobinTopicSelector::default());
                manager.start().await.unwrap();

                // Selects exactly the number of `num_topics` topics one by one.
                let got = (0..topics.len())
                    .map(|_| manager.select().unwrap())
                    .cloned()
                    .collect::<Vec<_>>();
                assert_eq!(got, topics);

                // Selects exactly the number of `num_topics` topics in a batching manner.
                let got = manager
                    .select_batch(topics.len())
                    .unwrap()
                    .into_iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                assert_eq!(got, topics);

                // Selects more than the number of `num_topics` topics.
                let got = manager
                    .select_batch(2 * topics.len())
                    .unwrap()
                    .into_iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                let expected = vec![topics.clone(); 2]
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();
                assert_eq!(got, expected);
            })
        })
        .await;
    }
}
