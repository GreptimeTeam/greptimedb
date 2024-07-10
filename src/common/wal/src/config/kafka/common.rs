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

use serde::{Deserialize, Serialize};
use serde_with::with_prefix;

use crate::{TopicSelectorType, TOPIC_NAME_PREFIX};

with_prefix!(pub backoff_prefix "backoff_");

/// Backoff configurations for kafka clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct BackoffConfig {
    /// The initial backoff delay.
    #[serde(with = "humantime_serde")]
    pub init: Duration,
    /// The maximum backoff delay.
    #[serde(with = "humantime_serde")]
    pub max: Duration,
    /// The exponential backoff rate, i.e. next backoff = base * current backoff.
    pub base: u32,
    /// The deadline of retries. `None` stands for no deadline.
    #[serde(with = "humantime_serde")]
    pub deadline: Option<Duration>,
}

impl Default for BackoffConfig {
    fn default() -> Self {
        Self {
            init: Duration::from_millis(500),
            max: Duration::from_secs(10),
            base: 2,
            deadline: Some(Duration::from_secs(60 * 5)), // 5 mins
        }
    }
}

/// Topic configurations for kafka clients.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KafkaTopicConfig {
    /// Number of topics to be created upon start.
    pub num_topics: usize,
    /// Number of partitions per topic.
    pub num_partitions: i32,
    /// The type of the topic selector with which to select a topic for a region.
    pub selector_type: TopicSelectorType,
    /// The replication factor of each topic.
    pub replication_factor: i16,
    /// The timeout of topic creation.
    #[serde(with = "humantime_serde")]
    pub create_topic_timeout: Duration,
    /// Topic name prefix.
    pub topic_name_prefix: String,
}

impl Default for KafkaTopicConfig {
    fn default() -> Self {
        Self {
            num_topics: 64,
            num_partitions: 1,
            selector_type: TopicSelectorType::RoundRobin,
            replication_factor: 1,
            create_topic_timeout: Duration::from_secs(30),
            topic_name_prefix: TOPIC_NAME_PREFIX.to_string(),
        }
    }
}
