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

mod kafka_topic_manager;

use std::sync::Arc;

use common_wal::config::kafka::MetasrvKafkaConfig;
use common_wal::TopicSelectorType;
use snafu::ensure;

use crate::error::{InvalidNumTopicsSnafu, Result};
use crate::kv_backend::KvBackendRef;
use crate::wal_options_allocator::kafka_topic_pool::kafka_topic_manager::TopicKafkaManager;
use crate::wal_options_allocator::kvbackend::TopicKvBackendManager;
use crate::wal_options_allocator::selector::{RoundRobinTopicSelector, TopicSelectorRef};

/// Topic pool for kafka remote wal.
/// Responsible for:
/// 1. Persists topics in kvbackend.
/// 2. Creates topics in kafka.
/// 3. Selects topic
pub struct KafkaTopicPool {
    topics: Vec<String>,
    topic_kvbackend_manager: TopicKvBackendManager,
    topic_kafka_manager: TopicKafkaManager,
    selector: TopicSelectorRef,
}

impl KafkaTopicPool {
    pub fn new(config: MetasrvKafkaConfig, kvbackend: KvBackendRef) -> Self {
        let num_topics = config.kafka_topic.num_topics;

        let prefix = config.kafka_topic.topic_name_prefix.clone();
        let topics = (0..num_topics)
            .map(|i| format!("{}_{}", prefix, i))
            .collect();

        let selector = match config.kafka_topic.selector_type {
            TopicSelectorType::RoundRobin => RoundRobinTopicSelector::with_shuffle(),
        };

        let kafka_topic_manager = TopicKafkaManager::new(config.clone());
        let kvbackend_topic_manager = TopicKvBackendManager::new(kvbackend.clone());

        Self {
            topics,
            topic_kvbackend_manager: kvbackend_topic_manager,
            topic_kafka_manager: kafka_topic_manager,
            selector: Arc::new(selector),
        }
    }

    /// Tries to initialize the topic manager.
    /// The initializer first tries to restore persisted topics from the kv backend.
    /// If not enough topics retrieved, the initializer will try to contact the Kafka cluster and request creating more topics.
    pub async fn init(&self) -> Result<()> {
        if !self.topic_kafka_manager.config.auto_create_topics {
            return Ok(());
        }

        let num_topics = self.topic_kafka_manager.config.kafka_topic.num_topics;
        ensure!(num_topics > 0, InvalidNumTopicsSnafu { num_topics });

        let topics_to_be_created = self
            .topic_kvbackend_manager
            .to_be_created(&self.topics)
            .await?;

        if !topics_to_be_created.is_empty() {
            self.topic_kafka_manager
                .try_create_topics(&topics_to_be_created)
                .await?;
            self.topic_kvbackend_manager.persist(&self.topics).await?;
        }
        Ok(())
    }

    /// Selects one topic from the topic pool through the topic selector.
    pub fn select(&self) -> Result<&String> {
        self.selector.select(&self.topics)
    }

    /// Selects a batch of topics from the topic pool through the topic selector.
    pub fn select_batch(&self, num_topics: usize) -> Result<Vec<&String>> {
        (0..num_topics)
            .map(|_| self.selector.select(&self.topics))
            .collect()
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
                let topic_pool = KafkaTopicPool::new(config.clone(), kv_backend);
                topic_pool.init().await.unwrap();

                // Selects exactly the number of `num_topics` topics one by one.
                let got = (0..topics.len())
                    .map(|_| topic_pool.select().unwrap())
                    .cloned()
                    .collect::<Vec<_>>();
                assert_eq!(got, topics);

                // Selects exactly the number of `num_topics` topics in a batching manner.
                let got = topic_pool
                    .select_batch(topics.len())
                    .unwrap()
                    .into_iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>();
                assert_eq!(got, topics);

                // Selects more than the number of `num_topics` topics.
                let got = topic_pool
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
