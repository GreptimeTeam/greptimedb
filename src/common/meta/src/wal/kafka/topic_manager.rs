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
use std::time::Duration;

use common_telemetry::debug;
use rskafka::client::ClientBuilder;
use rskafka::BackoffConfig;
use snafu::{ensure, ResultExt};

use crate::error::{
    BuildClientSnafu, BuildCtrlClientSnafu, CreateKafkaWalTopicSnafu, DecodeJsonSnafu,
    EncodeJsonSnafu, InvalidNumTopicsSnafu, Result,
};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::PutRequest;
use crate::wal::kafka::topic::Topic;
use crate::wal::kafka::topic_selector::{RoundRobinTopicSelector, SelectorType, TopicSelectorRef};
use crate::wal::kafka::KafkaConfig;

const CREATE_TOPIC_TIMEOUT: i32 = 5_000; // 5,000 ms.
const CREATED_TOPICS_KEY: &str = "__created_kafka_wal_topics";

/// Manages topic initialization and selection.
pub struct TopicManager {
    config: KafkaConfig,
    topic_pool: Vec<Topic>,
    topic_selector: TopicSelectorRef,
    kv_backend: KvBackendRef,
}

impl TopicManager {
    /// Creates a new topic manager.
    pub fn new(config: KafkaConfig, kv_backend: KvBackendRef) -> Self {
        let selector = match config.selector_type {
            SelectorType::RoundRobin => RoundRobinTopicSelector::with_shuffle(),
        };

        Self {
            config,
            topic_pool: Vec::new(),
            topic_selector: Arc::new(selector),
            kv_backend,
        }
    }

    /// Tries to initialize the topic manager.
    /// The initializer first tries to restore persisted topics from the kv backend.
    /// If not enough topics retrieved, the initializer will try to contact the Kafka cluster and request creating more topics.
    pub async fn try_init(&mut self) -> Result<()> {
        let num_topics = self.config.num_topics;
        ensure!(num_topics > 0, InvalidNumTopicsSnafu { num_topics });

        // Builds an rskafka controller client for creating topics if necessary.
        let broker_endpoints = self.config.broker_endpoints.clone();
        let backoff_config = BackoffConfig {
            init_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(10),
            base: 2.0,
            // Stop reconnecting if the total wait time reaches the deadline.
            deadline: Some(Duration::from_secs(60 * 5)), // 5 mins.
        };
        let client = ClientBuilder::new(broker_endpoints.clone())
            .backoff_config(backoff_config)
            .build()
            .await
            .context(BuildClientSnafu { broker_endpoints })?
            .controller_client()
            .context(BuildCtrlClientSnafu)?;

        // Topics should be created.
        let topics = (0..num_topics)
            .map(|topic_id| format!("{}_{topic_id}", self.config.topic_name_prefix))
            .collect::<Vec<_>>();
        // Topics already created.
        // There may have extra topics created but it's okay since those topics won't break topic allocation.
        let created_topics = Self::restore_created_topics(&self.kv_backend)
            .await?
            .into_iter()
            .collect::<HashSet<Topic>>();

        // Spawns tokio tasks for creating missing topics.
        let tasks = topics
            .iter()
            .filter_map(|topic| {
                if created_topics.contains(topic) {
                    debug!("Topic {} was created", topic);
                    return None;
                }

                debug!("Tries to create topic {}", topic);
                Some(client.create_topic(
                    topic,
                    self.config.num_partitions,
                    self.config.replication_factor,
                    CREATE_TOPIC_TIMEOUT,
                ))
            })
            .collect::<Vec<_>>();
        // TODO(niebayes): Determine how rskafka handles an already-exist topic. Check if an error would be raised.
        futures::future::try_join_all(tasks)
            .await
            .context(CreateKafkaWalTopicSnafu)?;

        // Persists created topics.
        Self::persist_created_topics(&topics, &self.kv_backend).await?;

        Ok(())
    }

    /// Selects one topic from the topic pool through the topic selector.
    pub fn select(&self) -> &Topic {
        self.topic_selector.select(&self.topic_pool)
    }

    /// Selects a batch of topics from the topic pool through the topic selector.
    pub fn select_batch(&self, num_topics: usize) -> Vec<&Topic> {
        (0..num_topics)
            .map(|_| self.topic_selector.select(&self.topic_pool))
            .collect()
    }

    async fn restore_created_topics(kv_backend: &KvBackendRef) -> Result<Vec<Topic>> {
        kv_backend
            .get(CREATED_TOPICS_KEY.as_bytes())
            .await?
            .map(|key_value| serde_json::from_slice(&key_value.value).context(DecodeJsonSnafu))
            .unwrap_or_else(|| Ok(vec![]))
    }

    async fn persist_created_topics(topics: &[Topic], kv_backend: &KvBackendRef) -> Result<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;

    // Tests that topics can be successfully persisted into the kv backend and can be successfully restored from the kv backend.
    #[tokio::test]
    async fn test_restore_persisted_topics() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let topic_name_prefix = "__test_";
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
}
