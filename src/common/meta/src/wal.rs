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

pub mod kafka;
pub mod options_allocator;

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_with::with_prefix;

use crate::error::Result;
use crate::wal::kafka::KafkaConfig;
pub use crate::wal::kafka::Topic as KafkaWalTopic;
pub use crate::wal::options_allocator::{
    allocate_region_wal_options, WalOptionsAllocator, WalOptionsAllocatorRef,
};

/// Wal config for metasrv.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "provider")]
pub enum WalConfig {
    #[default]
    #[serde(rename = "raft_engine")]
    RaftEngine,
    #[serde(rename = "kafka")]
    Kafka(KafkaConfig),
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::wal::kafka::topic_selector::SelectorType as KafkaTopicSelectorType;

    #[test]
    fn test_serde_wal_config() {
        // Test serde raft-engine wal config with none other wal config.
        let toml_str = r#"
            provider = "raft_engine"
        "#;
        let wal_config: WalConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(wal_config, WalConfig::RaftEngine);

        // Test serde raft-engine wal config with extra other wal config.
        let toml_str = r#"
            provider = "raft_engine"
            broker_endpoints = ["127.0.0.1:9090"]
            num_topics = 32
        "#;
        let wal_config: WalConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(wal_config, WalConfig::RaftEngine);

        // Test serde kafka wal config.
        let toml_str = r#"
            provider = "kafka"
            broker_endpoints = ["127.0.0.1:9090"]
            num_topics = 32
            selector_type = "round_robin"
            topic_name_prefix = "greptimedb_wal_kafka"
            num_partitions = 1
            replication_factor = 3
            create_topic_timeout = "30s"
            backoff_init = "500ms"
            backoff_max = "10s"
            backoff_base = 2.0
            backoff_deadline = "5mins"
        "#;
        let wal_config: WalConfig = toml::from_str(toml_str).unwrap();
        let expected_kafka_config = KafkaConfig {
            broker_endpoints: vec!["127.0.0.1:9090".to_string()],
            num_topics: 32,
            selector_type: KafkaTopicSelectorType::RoundRobin,
            topic_name_prefix: "greptimedb_wal_kafka".to_string(),
            num_partitions: 1,
            replication_factor: 3,
            create_topic_timeout: Duration::from_secs(30),
            backoff_init: Duration::from_millis(500),
            backoff_max: Duration::from_secs(10),
            backoff_base: 2.0,
            backoff_deadline: Some(Duration::from_secs(60 * 5)),
        };
        assert_eq!(wal_config, WalConfig::Kafka(expected_kafka_config));
    }

    // TODO(niebayes): the integrate test needs to test that the example config file can be successfully parsed.
}
