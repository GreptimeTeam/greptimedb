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
use store_api::storage::RegionNumber;

use crate::error::Result;
use crate::wal::kafka::{KafkaConfig, KafkaOptions};
pub use crate::wal::options_allocator::WalOptionsAllocator;

/// Wal configurations for bootstraping meta srv.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "provider")]
pub enum WalConfig {
    #[default]
    #[serde(rename = "raft-engine")]
    RaftEngine,
    #[serde(rename = "kafka")]
    Kafka(KafkaConfig),
}

/// Wal options for a region.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(tag = "provider")]
pub enum WalOptions {
    #[default]
    RaftEngine,
    Kafka(KafkaOptions),
}

// TODO(niebayes): determine how to encode/decode wal options.
pub type EncodedWalOptions = HashMap<String, String>;

impl From<WalOptions> for EncodedWalOptions {
    fn from(value: WalOptions) -> Self {
        EncodedWalOptions::default()
    }
}

impl TryFrom<EncodedWalOptions> for WalOptions {
    type Error = crate::error::Error;

    fn try_from(value: EncodedWalOptions) -> Result<Self> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::kafka::topic_selector::SelectorType as KafkaTopicSelectorType;

    #[test]
    fn test_serde_wal_config() {
        // Test serde raft-engine wal config with none other wal config.
        let toml_str = r#"
            provider = "raft-engine"
        "#;
        let wal_config: WalConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(wal_config, WalConfig::RaftEngine);

        // Test serde raft-engine wal config with extra other wal config.
        let toml_str = r#"
            provider = "raft-engine"
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
            selector_type = "round-robin"
            topic_name_prefix = "greptimedb_kafka_wal"
            num_partitions = 1
            replication_factor = 3
        "#;
        let wal_config: WalConfig = toml::from_str(toml_str).unwrap();
        let expected_kafka_wal_config = KafkaConfig {
            broker_endpoints: vec!["127.0.0.1:9090".to_string()],
            num_topics: 32,
            selector_type: KafkaTopicSelectorType::RoundRobin,
            topic_name_prefix: "greptimedb_kafka_wal".to_string(),
            num_partitions: 1,
            replication_factor: 3,
        };
        assert_eq!(wal_config, WalConfig::Kafka(expected_kafka_wal_config));
    }

    fn test_serde_wal_options() {}
}
