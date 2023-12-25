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

use std::time::Duration;

use common_base::readable_size::ReadableSize;
use rskafka::client::partition::Compression as RsKafkaCompression;
use serde::{Deserialize, Serialize};

/// Topic name prefix.
pub const TOPIC_NAME_PREFIX: &str = "greptimedb_wal_topic";
/// Kafka wal topic.
pub type Topic = String;

/// Configurations for kafka wal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KafkaConfig {
    /// The broker endpoints of the Kafka cluster.
    pub broker_endpoints: Vec<String>,
    /// The compression algorithm used to compress log entries.
    #[serde(skip)]
    #[serde(default)]
    pub compression: RsKafkaCompression,
    /// The maximum log size a kakfa batch producer could buffer.
    pub max_batch_size: ReadableSize,
    /// The linger duration of a kafka batch producer.
    #[serde(with = "humantime_serde")]
    pub linger: Duration,
    /// The maximum amount of time (in milliseconds) to wait for Kafka records to be returned.
    #[serde(with = "humantime_serde")]
    pub max_wait_time: Duration,
    /// The initial backoff for kafka clients.
    #[serde(with = "humantime_serde")]
    pub backoff_init: Duration,
    /// The maximum backoff for kafka clients.
    #[serde(with = "humantime_serde")]
    pub backoff_max: Duration,
    /// Exponential backoff rate, i.e. next backoff = base * current backoff.
    // Sets to u32 type since some structs containing the KafkaConfig need to derive the Eq trait.
    pub backoff_base: u32,
    /// Stop reconnecting if the total wait time reaches the deadline.
    /// If it's None, the reconnecting won't terminate.
    #[serde(with = "humantime_serde")]
    pub backoff_deadline: Option<Duration>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            broker_endpoints: vec!["127.0.0.1:9092".to_string()],
            compression: RsKafkaCompression::NoCompression,
            max_batch_size: ReadableSize::mb(4),
            linger: Duration::from_millis(200),
            max_wait_time: Duration::from_millis(100),
            backoff_init: Duration::from_millis(500),
            backoff_max: Duration::from_secs(10),
            backoff_base: 2,
            backoff_deadline: Some(Duration::from_secs(60 * 5)), // 5 mins
        }
    }
}

/// Kafka wal options allocated to a region.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaOptions {
    /// Kafka wal topic.
    pub topic: Topic,
}
