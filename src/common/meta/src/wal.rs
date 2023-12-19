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
pub use crate::wal::kafka::{KafkaOptions as KafkaWalOptions, Topic as KafkaWalTopic};
pub use crate::wal::options_allocator::WalOptionsAllocator;

/// An encoded wal options will be wrapped into a (WAL_OPTIONS_KEY, encoded wal options) key-value pair
/// and inserted into the options of a `RegionCreateRequest`.
pub const WAL_OPTIONS_KEY: &str = "wal_options";

/// Wal configurations for bootstrapping metasrv.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "provider")]
pub enum WalConfig {
    #[default]
    #[serde(rename = "raft_engine")]
    RaftEngine,
    #[serde(rename = "kafka")]
    Kafka(KafkaConfig),
}

/// Wal options allocated to a region.
/// A wal options is encoded by metasrv into a `String` with `serde_json::to_string`.
/// It's then decoded by datanode to a `HashMap<String, String>` with `serde_json::from_str`.
/// Such a encoding/decoding scheme is inspired by the encoding/decoding of `RegionOptions`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "wal.provider")]
pub enum WalOptions {
    #[default]
    #[serde(rename = "raft_engine")]
    RaftEngine,
    #[serde(rename = "kafka")]
    #[serde(with = "prefix_wal_kafka")]
    Kafka(KafkaWalOptions),
}

with_prefix!(prefix_wal_kafka "wal.kafka.");

#[cfg(test)]
mod tests {
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
            topic_name_prefix = "greptimedb_kafka_wal"
            num_partitions = 1
            replication_factor = 3
        "#;
        let wal_config: WalConfig = toml::from_str(toml_str).unwrap();
        let expected_kafka_config = KafkaConfig {
            broker_endpoints: vec!["127.0.0.1:9090".to_string()],
            num_topics: 32,
            selector_type: KafkaTopicSelectorType::RoundRobinBased,
            topic_name_prefix: "greptimedb_kafka_wal".to_string(),
            num_partitions: 1,
            replication_factor: 3,
        };
        assert_eq!(wal_config, WalConfig::Kafka(expected_kafka_config));
    }

    #[test]
    fn test_serde_wal_options() {
        // Test serde raft-engine wal options.
        let wal_options = WalOptions::RaftEngine;
        let encoded = serde_json::to_string(&wal_options).unwrap();
        let expected = r#"{"wal.provider":"raft_engine"}"#;
        assert_eq!(&encoded, expected);

        let decoded: WalOptions = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, wal_options);

        // Test serde kafka wal options.
        let wal_options = WalOptions::Kafka(KafkaWalOptions {
            topic: "test_topic".to_string(),
        });
        let encoded = serde_json::to_string(&wal_options).unwrap();
        let expected = r#"{"wal.provider":"kafka","wal.kafka.topic":"test_topic"}"#;
        assert_eq!(&encoded, expected);

        let decoded: WalOptions = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, wal_options);
    }
}
