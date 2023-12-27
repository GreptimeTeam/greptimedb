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

use common_config::wal::kafka::TopicSelectorType;
use common_telemetry::{debug, error, info};
use rskafka::client::controller::ControllerClient;
use rskafka::client::error::Error as RsKafkaError;
use rskafka::client::error::ProtocolError::TopicAlreadyExists;
use rskafka::client::ClientBuilder;
use rskafka::BackoffConfig;
use snafu::{ensure, AsErrorSource, ResultExt};

use crate::error::{
    BuildKafkaClientSnafu, BuildKafkaCtrlClientSnafu, CreateKafkaWalTopicSnafu, DecodeJsonSnafu,
    EncodeJsonSnafu, InvalidNumTopicsSnafu, Result,
};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::PutRequest;
use crate::wal::kafka::topic::Topic;
use crate::wal::kafka::topic_selector::{RoundRobinTopicSelector, TopicSelectorRef};
use crate::wal::kafka::KafkaConfig;

const CREATED_TOPICS_KEY: &str = "__created_wal_topics/kafka/";

/// Manages topic initialization and selection.
pub struct TopicManager {
    config: KafkaConfig,
    // TODO(niebayes): maybe add a guard to ensure all topics in the topic pool are created.
    topic_pool: Vec<Topic>,
    topic_selector: TopicSelectorRef,
    kv_backend: KvBackendRef,
}

impl TopicManager {
    /// Creates a new topic manager.
    pub fn new(config: KafkaConfig, kv_backend: KvBackendRef) -> Self {
        // Topics should be created.
        let topics = (0..config.num_topics)
            .map(|topic_id| format!("{}_{topic_id}", config.topic_name_prefix))
            .collect::<Vec<_>>();

        let selector = match config.selector_type {
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
        let num_topics = self.config.num_topics;
        ensure!(num_topics > 0, InvalidNumTopicsSnafu { num_topics });

        // Topics should be created.
        let topics = &self.topic_pool;

        // Topics already created.
        // There may have extra topics created but it's okay since those topics won't break topic allocation.
        let created_topics = Self::restore_created_topics(&self.kv_backend)
            .await?
            .into_iter()
            .collect::<HashSet<Topic>>();

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
    async fn try_create_topics(&self, topics: &[Topic], to_be_created: &[usize]) -> Result<()> {
        // Builds an kafka controller client for creating topics.
        let backoff_config = BackoffConfig {
            init_backoff: self.config.backoff.init,
            max_backoff: self.config.backoff.max,
            base: self.config.backoff.base as f64,
            deadline: self.config.backoff.deadline,
        };
        let client = ClientBuilder::new(self.config.broker_endpoints.clone())
            .backoff_config(backoff_config)
            .build()
            .await
            .with_context(|_| BuildKafkaClientSnafu {
                broker_endpoints: self.config.broker_endpoints.clone(),
            })?
            .controller_client()
            .context(BuildKafkaCtrlClientSnafu)?;

        // Try to create missing topics.
        let tasks = to_be_created
            .iter()
            .map(|i| self.try_create_topic(&topics[*i], &client))
            .collect::<Vec<_>>();
        futures::future::try_join_all(tasks).await.map(|_| ())
    }

    /// Selects one topic from the topic pool through the topic selector.
    pub fn select(&self) -> Result<&Topic> {
        self.topic_selector.select(&self.topic_pool)
    }

    /// Selects a batch of topics from the topic pool through the topic selector.
    pub fn select_batch(&self, num_topics: usize) -> Result<Vec<&Topic>> {
        (0..num_topics)
            .map(|_| self.topic_selector.select(&self.topic_pool))
            .collect()
    }

    async fn try_create_topic(&self, topic: &Topic, client: &ControllerClient) -> Result<()> {
        match client
            .create_topic(
                topic.clone(),
                self.config.num_partitions,
                self.config.replication_factor,
                self.config.create_topic_timeout.as_millis() as i32,
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

    async fn restore_created_topics(kv_backend: &KvBackendRef) -> Result<Vec<Topic>> {
        kv_backend
            .get(CREATED_TOPICS_KEY.as_bytes())
            .await?
            .map_or_else(
                || Ok(vec![]),
                |key_value| serde_json::from_slice(&key_value.value).context(DecodeJsonSnafu),
            )
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
    use std::env;

    use common_telemetry::info;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::{self};

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

    #[tokio::test]
    async fn test_topic_manager() {
        let endpoints = env::var("GT_KAFKA_ENDPOINTS").unwrap_or_default();
        common_telemetry::init_default_ut_logging();

        if endpoints.is_empty() {
            info!("The endpoints is empty, skipping the test.");
            return;
        }
        // TODO: supports topic prefix
        let kv_backend = Arc::new(MemoryKvBackend::new());
        let config = KafkaConfig {
            replication_factor: 1,
            broker_endpoints: endpoints
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<_>>(),
            ..Default::default()
        };
        let manager = TopicManager::new(config, kv_backend);
        manager.start().await.unwrap();
    }
}
