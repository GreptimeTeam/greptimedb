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
use serde::{Deserialize, Serialize};

use crate::config::kafka::common::{backoff_prefix, BackoffConfig};
use crate::{TopicSelectorType, BROKER_ENDPOINT, TOPIC_NAME_PREFIX};

/// Kafka wal configurations for standalone.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct StandaloneKafkaConfig {
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
    /// TODO(weny): Remove the alias once we release v0.9.
    /// The max size of a single producer batch.
    #[serde(alias = "max_batch_size")]
    pub max_batch_bytes: ReadableSize,
    /// The consumer wait timeout.
    #[serde(with = "humantime_serde")]
    pub consumer_wait_timeout: Duration,
    /// The backoff config.
    #[serde(flatten, with = "backoff_prefix")]
    pub backoff: BackoffConfig,
}

impl Default for StandaloneKafkaConfig {
    fn default() -> Self {
        let broker_endpoints = vec![BROKER_ENDPOINT.to_string()];
        let replication_factor = broker_endpoints.len() as i16;
        Self {
            broker_endpoints,
            num_topics: 64,
            selector_type: TopicSelectorType::RoundRobin,
            topic_name_prefix: TOPIC_NAME_PREFIX.to_string(),
            num_partitions: 1,
            replication_factor,
            create_topic_timeout: Duration::from_secs(30),
            // Warning: Kafka has a default limit of 1MB per message in a topic.
            max_batch_bytes: ReadableSize::mb(1),
            consumer_wait_timeout: Duration::from_millis(100),
            backoff: BackoffConfig::default(),
        }
    }
}
