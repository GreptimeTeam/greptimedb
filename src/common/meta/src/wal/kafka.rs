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

pub mod topic;
pub mod topic_manager;
pub mod topic_selector;

use serde::{Deserialize, Serialize};

use crate::wal::kafka::topic::Topic;
use crate::wal::kafka::topic_selector::SelectorType as TopicSelectorType;

/// Configurations for bootstraping a kafka wal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
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
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            broker_endpoints: vec!["127.0.0.1:9090".to_string()],
            num_topics: 64,
            selector_type: TopicSelectorType::RoundRobin,
            topic_name_prefix: "greptimedb_kafka_wal".to_string(),
            num_partitions: 1,
            replication_factor: 3,
        }
    }
}

/// Kafka wal options allocated to a region.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct KafkaOptions {
    /// Kafka wal topic.
    /// Publishers publish log entries to the topic while subscribers pull log entries from the topic.
    pub topic: Topic,
}
