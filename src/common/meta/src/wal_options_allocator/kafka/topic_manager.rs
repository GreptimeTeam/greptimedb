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

use std::collections::HashSet;
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
    CreateKafkaWalTopicSnafu, DecodeJsonSnafu, EncodeJsonSnafu, InvalidNumTopicsSnafu,
    ProduceRecordSnafu, ResolveKafkaEndpointSnafu, Result,
};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::PutRequest;
use crate::wal_options_allocator::kafka::topic_selector::{
    RoundRobinTopicSelector, TopicSelectorRef,
};

const CREATED_TOPICS_KEY: &str = "__created_wal_topics/kafka/";

// Each topic only has one partition for now.
// The `DEFAULT_PARTITION` refers to the index of the partition.
const DEFAULT_PARTITION: i32 = 0;

/// Manages topic initialization and selection.
pub struct TopicManager {
    config: MetasrvKafkaConfig,
    pub(crate) topic_pool: Vec<String>,
    pub(crate) topic_selector: TopicSelectorRef,
    kv_backend: KvBackendRef,
}

impl TopicManager {
    /// Creates a new topic manager.
    pub fn new(config: MetasrvKafkaConfig, kv_backend: KvBackendRef) -> Self {
        // Topics should be created.
        let topics = (0..config.kafka_topic.num_topics)
            .map(|topic_id| format!("{}_{topic_id}", config.kafka_topic.topic_name_prefix))
            .collect::<Vec<_>>();

        let selector = match config.kafka_topic.selector_type {
            TopicSelectorType::RoundRobin => RoundRobinTopicSelector::with_shuffle(),
        };

        Self {
            config,
            topic_pool: topics,
            topic_selector: Arc::new(selector),
            kv_backend,
        }
    }

    /// Tries to initialize the topic manager.
    /// The initializer first tries to restore persisted topics from the kv backend.
    /// If not enough topics retrieved, the initializer will try to contact the Kafka cluster and request creating more topics.
    pub async fn start(&self) -> Result<()> {
        let num_topics = self.config.kafka_topic.num_topics;
        ensure!(num_topics > 0, InvalidNumTopicsSnafu { num_topics });

        // Topics should be created.
        let topics = &self.topic_pool;

        // Topics already created.
        // There may have extra topics created but it's okay since those topics won't break topic allocation.
        let created_topics = Self::restore_created_topics(&self.kv_backend)
            .await?
            .into_iter()
            .collect::<HashSet<String>>();

        // Creates missing topics.
        let to_be_created = topics
            .iter()
            .enumerate()
            .filter_map(|(i, topic)| {
                if created_topics.contains(topic) {
                    return None;
                }
                Some(i)
            })
            .collect::<Vec<_>>();

        if !to_be_created.is_empty() {
            self.try_create_topics(topics, &to_be_created).await?;
            Self::persist_created_topics(topics, &self.kv_backend).await?;
        }
        Ok(())
    }

    /// Tries to create topics specified by indexes in `to_be_created`.
    async fn try_create_topics(&self, topics: &[String], to_be_created: &[usize]) -> Result<()> {
        // Builds an kafka controller client for creating topics.
        let backoff_config = BackoffConfig {
            init_backoff: self.config.backoff.init,
            max_backoff: self.config.backoff.max,
            base: self.config.backoff.base as f64,
            deadline: self.config.backoff.deadline,
        };
        let broker_endpoints = common_wal::resolve_to_ipv4(&self.config.broker_endpoints)
            .await
            .context(ResolveKafkaEndpointSnafu)?;
        let client = ClientBuilder::new(broker_endpoints)
            .backoff_config(backoff_config)
            .build()
            .await
            .with_context(|_| BuildKafkaClientSnafu {
                broker_endpoints: self.config.broker_endpoints.clone(),
            })?;

        let control_client = client
            .controller_client()
            .context(BuildKafkaCtrlClientSnafu)?;

        // Try to create missing topics.
        let tasks = to_be_created
            .iter()
            .map(|i| async {
                self.try_create_topic(&topics[*i], &control_client).await?;
                self.try_append_noop_record(&topics[*i], &client).await?;
                Ok(())
            })
            .collect::<Vec<_>>();
        futures::future::try_join_all(tasks).await.map(|_| ())
    }

    /// Selects one topic from the topic pool through the topic selector.
    pub fn select(&self) -> Result<&String> {
        self.topic_selector.select(&self.topic_pool)
    }

    /// Selects a batch of topics from the topic pool through the topic selector.
    pub fn select_batch(&self, num_topics: usize) -> Result<Vec<&String>> {
        (0..num_topics)
            .map(|_| self.topic_selector.select(&self.topic_pool))
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

    async fn restore_created_topics(kv_backend: &KvBackendRef) -> Result<Vec<String>> {
        kv_backend
            .get(CREATED_TOPICS_KEY.as_bytes())
            .await?
            .map_or_else(
                || Ok(vec![]),
                |key_value| serde_json::from_slice(&key_value.value).context(DecodeJsonSnafu),
            )
    }

    async fn persist_created_topics(topics: &[String], kv_backend: &KvBackendRef) -> Result<()> {
        let raw_topics = serde_json::to_vec(topics).context(EncodeJsonSnafu)?;
        kv_backend
            .put(PutRequest {
                key: CREATED_TOPICS_KEY.as_bytes().to_vec(),
                value: raw_topics,
                prev_kv: false,
            })
            .await
            .map(|_| ())
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
    use common_wal::config::kafka::common::KafkaTopicConfig;
    use common_wal::test_util::run_test_with_kafka_wal;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;

    // Tests that topics can be successfully persisted into the kv backend and can be successfully restored from the kv backend.
    #[tokio::test]
    async fn test_restore_persisted_topics() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let topic_name_prefix = "greptimedb_wal_topic";
        let num_topics = 16;

        // Constructs mock topics.
        let topics = (0..num_topics)
            .map(|topic| format!("{topic_name_prefix}{topic}"))
            .collect::<Vec<_>>();

        // Persists topics to kv backend.
        TopicManager::persist_created_topics(&topics, &kv_backend)
            .await
            .unwrap();

        // Restores topics from kv backend.
        let restored_topics = TopicManager::restore_created_topics(&kv_backend)
            .await
            .unwrap();

        assert_eq!(topics, restored_topics);
    }

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
                    broker_endpoints,
                    kafka_topic,
                    ..Default::default()
                };
                let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
                let mut manager = TopicManager::new(config.clone(), kv_backend);
                // Replaces the default topic pool with the constructed topics.
                manager.topic_pool.clone_from(&topics);
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
