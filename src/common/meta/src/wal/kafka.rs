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

pub mod topic_manager;
mod topic_selector;

use serde::{Deserialize, Serialize};
pub use topic_manager::{Topic as KafkaTopic, TopicManager as KafkaTopicManager};

use crate::wal::kafka::topic_selector::TopicSelectorType;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct KafkaOptions {
    /// The broker endpoints of the Kafka cluster.
    broker_endpoints: Vec<String>,
    /// Number of topics to be created upon start.
    num_topics: usize,
    /// The type of the topic selector with which to select a topic for a region.
    selector_type: TopicSelectorType,
    /// Topic name prefix.
    topic_name_prefix: String,
    /// Number of partitions per topic.
    num_partitions: i32,
    /// The replication factor of each topic.
    replication_factor: i16,
}

impl Default for KafkaOptions {
    fn default() -> Self {
        Self {
            broker_endpoints: vec!["127.0.0.1:9090".to_string()],
            num_topics: 64,
            selector_type: TopicSelectorType::RoundRobin,
            topic_name_prefix: "greptime_wal".to_string(),
            num_partitions: 1,
            replication_factor: 3,
        }
    }
}
