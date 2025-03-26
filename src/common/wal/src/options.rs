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

use serde::{Deserialize, Serialize};
use serde_with::with_prefix;

pub use crate::options::kafka::KafkaWalOptions;

/// An encoded wal options will be wrapped into a (WAL_OPTIONS_KEY, encoded wal options) key-value pair
/// and inserted into the options of a `RegionCreateRequest`.
pub const WAL_OPTIONS_KEY: &str = "wal_options";

/// Wal options allocated to a region.
/// A wal options is encoded by metasrv with `serde_json::to_string`, and then decoded
/// by datanode with `serde_json::from_str`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(tag = "wal.provider", rename_all = "snake_case")]
pub enum WalOptions {
    #[default]
    RaftEngine,
    #[serde(with = "kafka_prefix")]
    Kafka(KafkaWalOptions),
    Noop,
}

with_prefix!(kafka_prefix "wal.kafka.");

#[cfg(test)]
mod tests {
    use super::*;

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

        // Test serde noop wal options.
        let wal_options = WalOptions::Noop;
        let encoded = serde_json::to_string(&wal_options).unwrap();
        let expected = r#"{"wal.provider":"noop"}"#;
        assert_eq!(&encoded, expected);

        let decoded: WalOptions = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, wal_options);
    }
}
