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

use crate::config::kafka::common::{backoff_prefix, BackoffConfig};
use crate::{TopicSelectorType, BROKER_ENDPOINT, TOPIC_NAME_PREFIX};

/// Kafka wal configurations for metasrv.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetasrvKafkaConfig {
    /// The broker endpoints of the Kafka cluster.
    pub broker_endpoints: Vec<String>,
    /// The number of topics to be created upon start.
    pub num_topics: usize,
    /// The type of the topic selector with which to select a topic for a region.
    pub selector_type: TopicSelectorType,
    /// Topic name prefix.
    pub topic_name_prefix: String,
    /// The number of partitions per topic.
    pub num_partitions: i32,
    /// The replication factor of each topic.
    pub replication_factor: i16,
    /// The timeout of topic creation.
    #[serde(with = "humantime_serde")]
    pub create_topic_timeout: Duration,
    /// The backoff config.
    #[serde(flatten, with = "backoff_prefix")]
    pub backoff: BackoffConfig,
}

impl Default for MetasrvKafkaConfig {
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
            backoff: BackoffConfig::default(),
        }
    }
}
