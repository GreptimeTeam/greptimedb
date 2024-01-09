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
use serde_with::with_prefix;

pub use crate::wal::kafka::{
    KafkaConfig, KafkaOptions as KafkaWalOptions, StandaloneKafkaConfig, Topic as KafkaWalTopic,
};
pub use crate::wal::raft_engine::RaftEngineConfig;

/// An encoded wal options will be wrapped into a (WAL_OPTIONS_KEY, encoded wal options) key-value pair
/// and inserted into the options of a `RegionCreateRequest`.
pub const WAL_OPTIONS_KEY: &str = "wal_options";

/// Wal config for datanode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum WalConfig {
    RaftEngine(RaftEngineConfig),
    Kafka(KafkaConfig),
}

impl From<StandaloneWalConfig> for WalConfig {
    fn from(value: StandaloneWalConfig) -> Self {
        match value {
            StandaloneWalConfig::RaftEngine(config) => WalConfig::RaftEngine(config),
            StandaloneWalConfig::Kafka(config) => WalConfig::Kafka(config.base),
        }
    }
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig::RaftEngine(RaftEngineConfig::default())
    }
}

/// Wal config for datanode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "provider", rename_all = "snake_case")]
pub enum StandaloneWalConfig {
    RaftEngine(RaftEngineConfig),
    Kafka(StandaloneKafkaConfig),
}

impl Default for StandaloneWalConfig {
    fn default() -> Self {
        StandaloneWalConfig::RaftEngine(RaftEngineConfig::default())
    }
}

/// Wal options allocated to a region.
/// A wal options is encoded by metasrv with `serde_json::to_string`, and then decoded
/// by datanode with `serde_json::from_str`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "wal.provider", rename_all = "snake_case")]
pub enum WalOptions {
    #[default]
    RaftEngine,
    #[serde(with = "prefix_wal_kafka")]
    Kafka(KafkaWalOptions),
}

with_prefix!(prefix_wal_kafka "wal.kafka.");

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_base::readable_size::ReadableSize;
    use rskafka::client::partition::Compression as RsKafkaCompression;

    use crate::wal::kafka::KafkaBackoffConfig;
    use crate::wal::{KafkaConfig, KafkaWalOptions, WalOptions};

    #[test]
    fn test_serde_kafka_config() {
        // With all fields.
        let toml_str = r#"
            broker_endpoints = ["127.0.0.1:9092"]
            max_batch_size = "1MB"
            linger = "200ms"
            consumer_wait_timeout = "100ms"
            backoff_init = "500ms"
            backoff_max = "10s"
            backoff_base = 2
            backoff_deadline = "5mins"
        "#;
        let decoded: KafkaConfig = toml::from_str(toml_str).unwrap();
        let expected = KafkaConfig {
            broker_endpoints: vec!["127.0.0.1:9092".to_string()],
            compression: RsKafkaCompression::default(),
            max_batch_size: ReadableSize::mb(1),
            linger: Duration::from_millis(200),
            consumer_wait_timeout: Duration::from_millis(100),
            backoff: KafkaBackoffConfig {
                init: Duration::from_millis(500),
                max: Duration::from_secs(10),
                base: 2,
                deadline: Some(Duration::from_secs(60 * 5)),
            },
        };
        assert_eq!(decoded, expected);

        // With some fields missing.
        let toml_str = r#"
            broker_endpoints = ["127.0.0.1:9092"]
            linger = "200ms"
        "#;
        let decoded: KafkaConfig = toml::from_str(toml_str).unwrap();
        let expected = KafkaConfig {
            broker_endpoints: vec!["127.0.0.1:9092".to_string()],
            linger: Duration::from_millis(200),
            ..Default::default()
        };
        assert_eq!(decoded, expected);
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
