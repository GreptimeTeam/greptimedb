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

use common_config::wal::StandaloneWalConfig;
use common_config::WAL_OPTIONS_KEY;
use serde::{Deserialize, Serialize};
use store_api::storage::{RegionId, RegionNumber};

use crate::wal::kafka::KafkaConfig;
pub use crate::wal::kafka::Topic as KafkaWalTopic;
pub use crate::wal::options_allocator::{
    allocate_region_wal_options, WalOptionsAllocator, WalOptionsAllocatorRef,
};

/// Wal config for metasrv.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum WalConfig {
    #[default]
    RaftEngine,
    Kafka(KafkaConfig),
}

impl From<StandaloneWalConfig> for WalConfig {
    fn from(value: StandaloneWalConfig) -> Self {
        match value {
            StandaloneWalConfig::RaftEngine(_) => WalConfig::RaftEngine,
            StandaloneWalConfig::Kafka(config) => WalConfig::Kafka(KafkaConfig {
                broker_endpoints: config.base.broker_endpoints,
                num_topics: config.num_topics,
                selector_type: config.selector_type,
                topic_name_prefix: config.topic_name_prefix,
                num_partitions: config.num_partitions,
                replication_factor: config.replication_factor,
                create_topic_timeout: config.create_topic_timeout,
                backoff: config.base.backoff,
            }),
        }
    }
}

pub fn prepare_wal_option(
    options: &mut HashMap<String, String>,
    region_id: RegionId,
    region_wal_options: &HashMap<RegionNumber, String>,
) {
    if let Some(wal_options) = region_wal_options.get(&region_id.region_number()) {
        options.insert(WAL_OPTIONS_KEY.to_string(), wal_options.clone());
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_config::wal::kafka::{KafkaBackoffConfig, TopicSelectorType};

    use super::*;

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
            broker_endpoints = ["127.0.0.1:9092"]
            num_topics = 32
        "#;
        let wal_config: WalConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(wal_config, WalConfig::RaftEngine);

        // Test serde kafka wal config.
        let toml_str = r#"
            provider = "kafka"
            broker_endpoints = ["127.0.0.1:9092"]
            num_topics = 32
            selector_type = "round_robin"
            topic_name_prefix = "greptimedb_wal_topic"
            num_partitions = 1
            replication_factor = 1
            create_topic_timeout = "30s"
            backoff_init = "500ms"
            backoff_max = "10s"
            backoff_base = 2
            backoff_deadline = "5mins"
        "#;
        let wal_config: WalConfig = toml::from_str(toml_str).unwrap();
        let expected_kafka_config = KafkaConfig {
            broker_endpoints: vec!["127.0.0.1:9092".to_string()],
            num_topics: 32,
            selector_type: TopicSelectorType::RoundRobin,
            topic_name_prefix: "greptimedb_wal_topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
            create_topic_timeout: Duration::from_secs(30),
            backoff: KafkaBackoffConfig {
                init: Duration::from_millis(500),
                max: Duration::from_secs(10),
                base: 2,
                deadline: Some(Duration::from_secs(60 * 5)),
            },
        };
        assert_eq!(wal_config, WalConfig::Kafka(expected_kafka_config));
    }
}
