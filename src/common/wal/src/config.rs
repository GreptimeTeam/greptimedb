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
pub mod raft_engine;

use serde::{Deserialize, Serialize};

use crate::config::kafka::{DatanodeKafkaConfig, MetasrvKafkaConfig, StandaloneKafkaConfig};
use crate::config::raft_engine::RaftEngineConfig;

/// Wal configurations for metasrv.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum MetasrvWalConfig {
    #[default]
    RaftEngine,
    Kafka(MetasrvKafkaConfig),
}

/// Wal configurations for datanode.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum DatanodeWalConfig {
    RaftEngine(RaftEngineConfig),
    Kafka(DatanodeKafkaConfig),
}

impl Default for DatanodeWalConfig {
    fn default() -> Self {
        Self::RaftEngine(RaftEngineConfig::default())
    }
}

/// Wal configurations for standalone.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum StandaloneWalConfig {
    RaftEngine(RaftEngineConfig),
    Kafka(StandaloneKafkaConfig),
}

impl Default for StandaloneWalConfig {
    fn default() -> Self {
        Self::RaftEngine(RaftEngineConfig::default())
    }
}

impl From<StandaloneWalConfig> for MetasrvWalConfig {
    fn from(config: StandaloneWalConfig) -> Self {
        match config {
            StandaloneWalConfig::RaftEngine(_) => Self::RaftEngine,
            StandaloneWalConfig::Kafka(config) => Self::Kafka(MetasrvKafkaConfig {
                broker_endpoints: config.broker_endpoints,
                num_topics: config.num_topics,
                selector_type: config.selector_type,
                topic_name_prefix: config.topic_name_prefix,
                num_partitions: config.num_partitions,
                replication_factor: config.replication_factor,
                create_topic_timeout: config.create_topic_timeout,
                backoff: config.backoff,
            }),
        }
    }
}

impl From<MetasrvWalConfig> for StandaloneWalConfig {
    fn from(config: MetasrvWalConfig) -> Self {
        match config {
            MetasrvWalConfig::RaftEngine => Self::RaftEngine(RaftEngineConfig::default()),
            MetasrvWalConfig::Kafka(config) => Self::Kafka(StandaloneKafkaConfig {
                broker_endpoints: config.broker_endpoints,
                num_topics: config.num_topics,
                selector_type: config.selector_type,
                topic_name_prefix: config.topic_name_prefix,
                num_partitions: config.num_partitions,
                replication_factor: config.replication_factor,
                create_topic_timeout: config.create_topic_timeout,
                backoff: config.backoff,
                ..Default::default()
            }),
        }
    }
}

impl From<StandaloneWalConfig> for DatanodeWalConfig {
    fn from(config: StandaloneWalConfig) -> Self {
        match config {
            StandaloneWalConfig::RaftEngine(config) => Self::RaftEngine(config),
            StandaloneWalConfig::Kafka(config) => Self::Kafka(DatanodeKafkaConfig {
                broker_endpoints: config.broker_endpoints,
                compression: config.compression,
                max_batch_bytes: config.max_batch_bytes,
                consumer_wait_timeout: config.consumer_wait_timeout,
                backoff: config.backoff,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_base::readable_size::ReadableSize;
    use rskafka::client::partition::Compression;

    use super::*;
    use crate::config::kafka::common::BackoffConfig;
    use crate::config::{DatanodeKafkaConfig, MetasrvKafkaConfig, StandaloneKafkaConfig};
    use crate::TopicSelectorType;

    #[test]
    fn test_toml_raft_engine() {
        // With none configs.
        let toml_str = r#"
            provider = "raft_engine"
        "#;
        let metasrv_wal_config: MetasrvWalConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(metasrv_wal_config, MetasrvWalConfig::RaftEngine);

        let datanode_wal_config: DatanodeWalConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            datanode_wal_config,
            DatanodeWalConfig::RaftEngine(RaftEngineConfig::default())
        );

        // With useless configs.
        let toml_str = r#"
            provider = "raft_engine"
            broker_endpoints = ["127.0.0.1:9092"]
            num_topics = 32
        "#;
        let datanode_wal_config: DatanodeWalConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            datanode_wal_config,
            DatanodeWalConfig::RaftEngine(RaftEngineConfig::default())
        );

        // With some useful configs.
        let toml_str = r#"
            provider = "raft_engine"
            file_size = "4MB"
            purge_threshold = "1GB"
            purge_interval = "5mins"
        "#;
        let datanode_wal_config: DatanodeWalConfig = toml::from_str(toml_str).unwrap();
        let expected = RaftEngineConfig {
            file_size: ReadableSize::mb(4),
            purge_threshold: ReadableSize::gb(1),
            purge_interval: Duration::from_secs(5 * 60),
            ..Default::default()
        };
        assert_eq!(datanode_wal_config, DatanodeWalConfig::RaftEngine(expected));
    }

    #[test]
    fn test_toml_kafka() {
        let toml_str = r#"
            provider = "kafka"
            broker_endpoints = ["127.0.0.1:9092"]
            num_topics = 32
            selector_type = "round_robin"
            topic_name_prefix = "greptimedb_wal_topic"
            replication_factor = 1
            create_topic_timeout = "30s"
            max_batch_bytes = "1MB"
            linger = "200ms"
            consumer_wait_timeout = "100ms"
            backoff_init = "500ms"
            backoff_max = "10s"
            backoff_base = 2
            backoff_deadline = "5mins"
        "#;

        // Deserialized to MetasrvWalConfig.
        let metasrv_wal_config: MetasrvWalConfig = toml::from_str(toml_str).unwrap();
        let expected = MetasrvKafkaConfig {
            broker_endpoints: vec!["127.0.0.1:9092".to_string()],
            num_topics: 32,
            selector_type: TopicSelectorType::RoundRobin,
            topic_name_prefix: "greptimedb_wal_topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
            create_topic_timeout: Duration::from_secs(30),
            backoff: BackoffConfig {
                init: Duration::from_millis(500),
                max: Duration::from_secs(10),
                base: 2,
                deadline: Some(Duration::from_secs(60 * 5)),
            },
        };
        assert_eq!(metasrv_wal_config, MetasrvWalConfig::Kafka(expected));

        // Deserialized to DatanodeWalConfig.
        let datanode_wal_config: DatanodeWalConfig = toml::from_str(toml_str).unwrap();
        let expected = DatanodeKafkaConfig {
            broker_endpoints: vec!["127.0.0.1:9092".to_string()],
            compression: Compression::NoCompression,
            max_batch_bytes: ReadableSize::mb(1),
            consumer_wait_timeout: Duration::from_millis(100),
            backoff: BackoffConfig {
                init: Duration::from_millis(500),
                max: Duration::from_secs(10),
                base: 2,
                deadline: Some(Duration::from_secs(60 * 5)),
            },
        };
        assert_eq!(datanode_wal_config, DatanodeWalConfig::Kafka(expected));

        // Deserialized to StandaloneWalConfig.
        let standalone_wal_config: StandaloneWalConfig = toml::from_str(toml_str).unwrap();
        let expected = StandaloneKafkaConfig {
            broker_endpoints: vec!["127.0.0.1:9092".to_string()],
            num_topics: 32,
            selector_type: TopicSelectorType::RoundRobin,
            topic_name_prefix: "greptimedb_wal_topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
            create_topic_timeout: Duration::from_secs(30),
            compression: Compression::NoCompression,
            max_batch_bytes: ReadableSize::mb(1),
            consumer_wait_timeout: Duration::from_millis(100),
            backoff: BackoffConfig {
                init: Duration::from_millis(500),
                max: Duration::from_secs(10),
                base: 2,
                deadline: Some(Duration::from_secs(60 * 5)),
            },
        };
        assert_eq!(standalone_wal_config, StandaloneWalConfig::Kafka(expected));
    }
}
