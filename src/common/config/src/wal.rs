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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_with::with_prefix;

pub use crate::wal::kafka::{KafkaConfig, KafkaOptions as KafkaWalOptions, Topic as KafkaWalTopic};
pub use crate::wal::raft_engine::RaftEngineConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "provider")]
pub enum WalConfig {
    #[serde(rename = "raft-engine")]
    RaftEngine(RaftEngineConfig),
    #[serde(rename = "kafka")]
    Kafka(KafkaConfig),
}

impl Default for WalConfig {
    fn default() -> Self {
        WalConfig::RaftEngine(RaftEngineConfig::default())
    }
}

/// Wal options allocated to a region.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "wal.provider")]
pub enum WalOptions {
    #[default]
    #[serde(rename = "raft-engine")]
    RaftEngine,
    #[serde(rename = "kafka")]
    #[serde(with = "prefix_wal_kafka")]
    Kafka(KafkaWalOptions),
}

with_prefix!(prefix_wal_kafka "wal.kafka.");

/// The encoded wal options.
pub type EncodedWalOptions = HashMap<String, String>;

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use crate::wal::{KafkaWalOptions, WalOptions};

    fn make_json_string(items: &[(&str, &str)]) -> String {
        let body = items
            .iter()
            .map(|(k, v)| format!("\"{k}\":\"{v}\""))
            .join(",");
        "{".to_string() + &body + "}"
    }

    #[test]
    fn test_serde_raft_engine_wal_options() {
        let wal_options = WalOptions::RaftEngine;
        let encoded = serde_json::to_string(&wal_options).unwrap();
        let expected_encoded = make_json_string(&[("wal.provider", "raft-engine")]);
        assert_eq!(encoded, expected_encoded);

        let decoded: WalOptions = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, wal_options);
    }

    #[test]
    fn test_serde_kafka_wal_options() {
        let wal_options = WalOptions::Kafka(KafkaWalOptions {
            topic: "test_topic".to_string(),
        });
        let encoded = serde_json::to_string(&wal_options).unwrap();
        let expected_encoded =
            make_json_string(&[("wal.provider", "kafka"), ("wal.kafka.topic", "test_topic")]);
        assert_eq!(encoded, expected_encoded);

        let decoded: WalOptions = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, wal_options);
    }
}
