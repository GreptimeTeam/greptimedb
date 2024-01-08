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

#[cfg(any(test, feature = "testing"))]
pub mod test_util;
pub mod topic;
pub mod topic_manager;
pub mod topic_selector;

use std::time::Duration;

use common_config::wal::kafka::{kafka_backoff, KafkaBackoffConfig, TopicSelectorType};
use serde::{Deserialize, Serialize};

pub use crate::wal::kafka::topic::Topic;
pub use crate::wal::kafka::topic_manager::TopicManager;

/// Configurations for kafka wal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct KafkaConfig {
    /// The broker endpoints of the Kafka cluster.
    pub broker_endpoints: Vec<String>,
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
    /// The backoff config.
    #[serde(flatten, with = "kafka_backoff")]
    pub backoff: KafkaBackoffConfig,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        let broker_endpoints = vec!["127.0.0.1:9092".to_string()];
        let replication_factor = broker_endpoints.len() as i16;

        Self {
            broker_endpoints,
            num_topics: 64,
            selector_type: TopicSelectorType::RoundRobin,
            topic_name_prefix: "greptimedb_wal_topic".to_string(),
            num_partitions: 1,
            replication_factor,
            create_topic_timeout: Duration::from_secs(30),
            backoff: KafkaBackoffConfig::default(),
        }
    }
}
