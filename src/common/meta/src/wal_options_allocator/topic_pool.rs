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

use std::fmt::{self, Formatter};
use std::sync::Arc;

use common_telemetry::info;
use common_wal::config::kafka::MetasrvKafkaConfig;
use common_wal::TopicSelectorType;
use snafu::ensure;

use crate::error::{InvalidNumTopicsSnafu, Result};
use crate::kv_backend::KvBackendRef;
use crate::wal_options_allocator::selector::{RoundRobinTopicSelector, TopicSelectorRef};
use crate::wal_options_allocator::topic_creator::KafkaTopicCreator;
use crate::wal_options_allocator::topic_manager::KafkaTopicManager;

/// Topic pool for kafka remote wal.
/// Responsible for:
/// 1. Persists topics in kvbackend.
/// 2. Creates topics in kafka.
/// 3. Selects topics for regions.
pub struct KafkaTopicPool {
    pub(crate) topics: Vec<String>,
    // Manages topics in kvbackend.
    topic_manager: KafkaTopicManager,
    // Creates topics in kafka.
    topic_creator: KafkaTopicCreator,
    pub(crate) selector: TopicSelectorRef,
    auto_create_topics: bool,
}

impl fmt::Debug for KafkaTopicPool {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("KafkaTopicPool")
            .field("topics", &self.topics)
            .field("auto_create_topics", &self.auto_create_topics)
            .finish()
    }
}

impl KafkaTopicPool {
    pub fn new(
        config: &MetasrvKafkaConfig,
        kvbackend: KvBackendRef,
        topic_creator: KafkaTopicCreator,
    ) -> Self {
        let num_topics = config.kafka_topic.num_topics;
        let prefix = config.kafka_topic.topic_name_prefix.clone();
        let topics = (0..num_topics)
            .map(|i| format!("{}_{}", prefix, i))
            .collect();

        let selector = match config.kafka_topic.selector_type {
            TopicSelectorType::RoundRobin => RoundRobinTopicSelector::with_shuffle(),
        };

        let topic_manager = KafkaTopicManager::new(kvbackend);

        Self {
            topics,
            topic_manager,
            topic_creator,
            selector: Arc::new(selector),
            auto_create_topics: config.auto_create_topics,
        }
    }

    /// Tries to activate the topic manager when metasrv becomes the leader.
    ///
    /// First tries to restore persisted topics from the kv backend.
    /// If there are unprepared topics (topics that exist in the configuration but not in the kv backend),
    /// it will create these topics in Kafka if `auto_create_topics` is enabled.
    ///
    /// Then it prepares all unprepared topics by appending a noop record if the topic is empty,
    /// and persists them in the kv backend for future use.
    pub async fn activate(&self) -> Result<()> {
        let num_topics = self.topics.len();
        ensure!(num_topics > 0, InvalidNumTopicsSnafu { num_topics });

        let unprepared_topics = self.topic_manager.unprepare_topics(&self.topics).await?;

        if !unprepared_topics.is_empty() {
            if self.auto_create_topics {
                info!("Creating {} topics", unprepared_topics.len());
                self.topic_creator.create_topics(&unprepared_topics).await?;
            } else {
                info!("Auto create topics is disabled, skipping topic creation.");
            }
            self.topic_creator
                .prepare_topics(&unprepared_topics)
                .await?;
            self.topic_manager
                .persist_prepared_topics(&unprepared_topics)
                .await?;
        }
        info!("Activated topic pool with {} topics", self.topics.len());

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
impl KafkaTopicPool {
    pub(crate) fn topic_manager(&self) -> &KafkaTopicManager {
        &self.topic_manager
    }

    pub(crate) fn topic_creator(&self) -> &KafkaTopicCreator {
        &self.topic_creator
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_wal::maybe_skip_kafka_integration_test;
    use common_wal::test_util::get_kafka_endpoints;

    use super::*;
    use crate::error::Error;
    use crate::test_util::test_kafka_topic_pool;
    use crate::wal_options_allocator::selector::RoundRobinTopicSelector;

    #[tokio::test]
    async fn test_pool_invalid_number_topics_err() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let endpoints = get_kafka_endpoints();

        let pool = test_kafka_topic_pool(endpoints.clone(), 0, false, None).await;
        let err = pool.activate().await.unwrap_err();
        assert_matches!(err, Error::InvalidNumTopics { .. });

        let pool = test_kafka_topic_pool(endpoints, 0, true, None).await;
        let err = pool.activate().await.unwrap_err();
        assert_matches!(err, Error::InvalidNumTopics { .. });
    }

    #[tokio::test]
    async fn test_pool_activate_unknown_topics_err() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let pool =
            test_kafka_topic_pool(get_kafka_endpoints(), 1, false, Some("unknown_topic")).await;
        let err = pool.activate().await.unwrap_err();
        assert_matches!(err, Error::KafkaPartitionClient { .. });
    }

    #[tokio::test]
    async fn test_pool_activate() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let pool =
            test_kafka_topic_pool(get_kafka_endpoints(), 2, true, Some("pool_activate")).await;
        // clean up the topics before test
        let topic_creator = pool.topic_creator();
        topic_creator.delete_topics(&pool.topics).await.unwrap();

        let topic_manager = pool.topic_manager();
        pool.activate().await.unwrap();
        let topics = topic_manager.list_topics().await.unwrap();
        assert_eq!(topics.len(), 2);
    }

    #[tokio::test]
    async fn test_pool_activate_with_existing_topics() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let prefix = "pool_activate_with_existing_topics";
        let pool = test_kafka_topic_pool(get_kafka_endpoints(), 2, true, Some(prefix)).await;
        let topic_creator = pool.topic_creator();
        topic_creator.delete_topics(&pool.topics).await.unwrap();

        let topic_manager = pool.topic_manager();
        // persists one topic info, then pool.activate() will create new topics that not persisted.
        topic_manager
            .persist_prepared_topics(&pool.topics[0..1])
            .await
            .unwrap();

        pool.activate().await.unwrap();
        let topics = topic_manager.list_topics().await.unwrap();
        assert_eq!(topics.len(), 2);

        let client = pool.topic_creator().client();
        let topics = client
            .list_topics()
            .await
            .unwrap()
            .into_iter()
            .filter(|t| t.name.starts_with(prefix))
            .collect::<Vec<_>>();
        assert_eq!(topics.len(), 1);
    }

    /// Tests that the topic manager could allocate topics correctly.
    #[tokio::test]
    async fn test_alloc_topics() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let num_topics = 5;
        let mut topic_pool = test_kafka_topic_pool(
            get_kafka_endpoints(),
            num_topics,
            true,
            Some("test_allocator_with_kafka"),
        )
        .await;
        topic_pool.selector = Arc::new(RoundRobinTopicSelector::default());
        let topics = topic_pool.topics.clone();
        // clean up the topics before test
        let topic_creator = topic_pool.topic_creator();
        topic_creator.delete_topics(&topics).await.unwrap();

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
    }
}
