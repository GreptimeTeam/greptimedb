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

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::config::kafka::{DatanodeKafkaConfig, MetasrvKafkaConfig};
use crate::config::raft_engine::RaftEngineConfig;

/// Wal configurations for metasrv.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
#[serde(tag = "provider", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum MetasrvWalConfig {
    #[default]
    RaftEngine,
    Kafka(MetasrvKafkaConfig),
}

#[allow(clippy::large_enum_variant)]
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

impl From<DatanodeWalConfig> for MetasrvWalConfig {
    fn from(config: DatanodeWalConfig) -> Self {
        match config {
            DatanodeWalConfig::RaftEngine(_) => Self::RaftEngine,
            DatanodeWalConfig::Kafka(config) => Self::Kafka(MetasrvKafkaConfig {
                connection: config.connection,
                kafka_topic: config.kafka_topic,
                auto_create_topics: config.auto_create_topics,
                auto_prune_interval: config.auto_prune_interval,
                trigger_flush_threshold: config.trigger_flush_threshold,
                auto_prune_parallelism: config.auto_prune_parallelism,
            }),
        }
    }
}

impl MetasrvWalConfig {
    /// Returns if active wal pruning is enabled.
    pub fn enable_active_wal_pruning(&self) -> bool {
        match self {
            MetasrvWalConfig::RaftEngine => false,
            MetasrvWalConfig::Kafka(config) => config.auto_prune_interval > Duration::ZERO,
        }
    }

    /// Gets the kafka connection config.
    pub fn remote_wal_options(&self) -> Option<&MetasrvKafkaConfig> {
        match self {
            MetasrvWalConfig::RaftEngine => None,
            MetasrvWalConfig::Kafka(config) => Some(config),
        }
    }
}

impl From<MetasrvWalConfig> for DatanodeWalConfig {
    fn from(config: MetasrvWalConfig) -> Self {
        match config {
            MetasrvWalConfig::RaftEngine => Self::RaftEngine(RaftEngineConfig::default()),
            MetasrvWalConfig::Kafka(config) => Self::Kafka(DatanodeKafkaConfig {
                connection: config.connection,
                kafka_topic: config.kafka_topic,
                ..Default::default()
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_base::readable_size::ReadableSize;
    use kafka::common::{
        KafkaClientSasl, KafkaClientSaslConfig, KafkaClientTls, KafkaConnectionConfig,
    };
    use tests::kafka::common::KafkaTopicConfig;

    use super::*;
    use crate::config::{DatanodeKafkaConfig, MetasrvKafkaConfig};
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
            max_batch_bytes = "1MB"
            consumer_wait_timeout = "100ms"
            num_topics = 32
            num_partitions = 1
            selector_type = "round_robin"
            replication_factor = 1
            create_topic_timeout = "30s"
            topic_name_prefix = "greptimedb_wal_topic"
            [tls]
            server_ca_cert_path = "/path/to/server.pem"
            [sasl]
            type = "SCRAM-SHA-512"
            username = "hi"
            password = "test"
        "#;

        // Deserialized to MetasrvWalConfig.
        let metasrv_wal_config: MetasrvWalConfig = toml::from_str(toml_str).unwrap();
        let expected = MetasrvKafkaConfig {
            connection: KafkaConnectionConfig {
                broker_endpoints: vec!["127.0.0.1:9092".to_string()],
                sasl: Some(KafkaClientSasl {
                    config: KafkaClientSaslConfig::ScramSha512 {
                        username: "hi".to_string(),
                        password: "test".to_string(),
                    },
                }),
                tls: Some(KafkaClientTls {
                    server_ca_cert_path: Some("/path/to/server.pem".to_string()),
                    client_cert_path: None,
                    client_key_path: None,
                }),
            },
            kafka_topic: KafkaTopicConfig {
                num_topics: 32,
                selector_type: TopicSelectorType::RoundRobin,
                topic_name_prefix: "greptimedb_wal_topic".to_string(),
                num_partitions: 1,
                replication_factor: 1,
                create_topic_timeout: Duration::from_secs(30),
            },
            auto_create_topics: true,
            auto_prune_interval: Duration::from_secs(0),
            trigger_flush_threshold: 0,
            auto_prune_parallelism: 10,
        };
        assert_eq!(metasrv_wal_config, MetasrvWalConfig::Kafka(expected));

        // Deserialized to DatanodeWalConfig.
        let datanode_wal_config: DatanodeWalConfig = toml::from_str(toml_str).unwrap();
        let expected = DatanodeKafkaConfig {
            connection: KafkaConnectionConfig {
                broker_endpoints: vec!["127.0.0.1:9092".to_string()],
                sasl: Some(KafkaClientSasl {
                    config: KafkaClientSaslConfig::ScramSha512 {
                        username: "hi".to_string(),
                        password: "test".to_string(),
                    },
                }),
                tls: Some(KafkaClientTls {
                    server_ca_cert_path: Some("/path/to/server.pem".to_string()),
                    client_cert_path: None,
                    client_key_path: None,
                }),
            },
            max_batch_bytes: ReadableSize::mb(1),
            consumer_wait_timeout: Duration::from_millis(100),
            kafka_topic: KafkaTopicConfig {
                num_topics: 32,
                selector_type: TopicSelectorType::RoundRobin,
                topic_name_prefix: "greptimedb_wal_topic".to_string(),
                num_partitions: 1,
                replication_factor: 1,
                create_topic_timeout: Duration::from_secs(30),
            },
            ..Default::default()
        };
        assert_eq!(datanode_wal_config, DatanodeWalConfig::Kafka(expected));
    }
}
