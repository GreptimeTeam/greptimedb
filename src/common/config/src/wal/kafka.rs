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

use std::time::Duration;

use common_base::readable_size::ReadableSize;
use rskafka::client::partition::Compression as RsKafkaCompression;
use serde::{Deserialize, Serialize};
use serde_with::with_prefix;

/// Topic name prefix.
pub const TOPIC_NAME_PREFIX: &str = "greptimedb_wal_topic";
/// Kafka wal topic.
pub type Topic = String;

/// The type of the topic selector, i.e. with which strategy to select a topic.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TopicSelectorType {
    #[default]
    RoundRobin,
}

/// Configurations for kafka wal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KafkaConfig {
    /// The broker endpoints of the Kafka cluster.
    pub broker_endpoints: Vec<String>,
    /// The compression algorithm used to compress log entries.
    #[serde(skip)]
    pub compression: RsKafkaCompression,
    /// The max size of a single producer batch.
    pub max_batch_size: ReadableSize,
    /// The linger duration of a kafka batch producer.
    #[serde(with = "humantime_serde")]
    pub linger: Duration,
    /// The consumer wait timeout.
    #[serde(with = "humantime_serde")]
    pub consumer_wait_timeout: Duration,
    /// The backoff config.
    #[serde(flatten, with = "kafka_backoff")]
    pub backoff: KafkaBackoffConfig,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            broker_endpoints: vec!["127.0.0.1:9092".to_string()],
            compression: RsKafkaCompression::NoCompression,
            // Warning: Kafka has a default limit of 1MB per message in a topic.
            max_batch_size: ReadableSize::mb(1),
            linger: Duration::from_millis(200),
            consumer_wait_timeout: Duration::from_millis(100),
            backoff: KafkaBackoffConfig::default(),
        }
    }
}

with_prefix!(pub kafka_backoff "backoff_");

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KafkaBackoffConfig {
    /// The initial backoff delay.
    #[serde(with = "humantime_serde")]
    pub init: Duration,
    /// The maximum backoff delay.
    #[serde(with = "humantime_serde")]
    pub max: Duration,
    /// Exponential backoff rate, i.e. next backoff = base * current backoff.
    pub base: u32,
    /// The deadline of retries. `None` stands for no deadline.
    #[serde(with = "humantime_serde")]
    pub deadline: Option<Duration>,
}

impl Default for KafkaBackoffConfig {
    fn default() -> Self {
        Self {
            init: Duration::from_millis(500),
            max: Duration::from_secs(10),
            base: 2,
            deadline: Some(Duration::from_secs(60 * 5)), // 5 mins
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StandaloneKafkaConfig {
    #[serde(flatten)]
    pub base: KafkaConfig,
    /// Number of topics to be created upon start.
    pub num_topics: usize,
    /// The type of the topic selector with which to select a topic for a region.
    pub selector_type: TopicSelectorType,
    /// Topic name prefix.
    pub topic_name_prefix: String,
    /// Number of partitions per topic.
    pub num_partitions: i32,
    /// The replication factor of each topic.
    pub replication_factor: i16,
    /// The timeout of topic creation.
    #[serde(with = "humantime_serde")]
    pub create_topic_timeout: Duration,
}

impl Default for StandaloneKafkaConfig {
    fn default() -> Self {
        let base = KafkaConfig::default();
        let replication_factor = base.broker_endpoints.len() as i16;

        Self {
            base,
            num_topics: 64,
            selector_type: TopicSelectorType::RoundRobin,
            topic_name_prefix: "greptimedb_wal_topic".to_string(),
            num_partitions: 1,
            replication_factor,
            create_topic_timeout: Duration::from_secs(30),
        }
    }
}

/// Kafka wal options allocated to a region.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaOptions {
    /// Kafka wal topic.
    pub topic: Topic,
}
