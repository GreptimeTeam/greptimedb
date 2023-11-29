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

pub type KafkaTopic = String;
pub const TOPIC_NAME_PREFIX: &str = "greptime_topic";
pub const TOPIC_KEY: &str = "kafka_topic";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Compression {
    NoCompression,
    Gzip,
    Lz4,
    Snappy,
    Zstd,
}

impl From<Compression> for RsKafkaCompression {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::NoCompression => RsKafkaCompression::NoCompression,
            Compression::Gzip => RsKafkaCompression::Gzip,
            Compression::Lz4 => RsKafkaCompression::Lz4,
            Compression::Snappy => RsKafkaCompression::Snappy,
            Compression::Zstd => RsKafkaCompression::Zstd,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KafkaOptions {
    /// The broker endpoints of the Kafka cluster.
    pub broker_endpoints: Vec<String>,
    /// Number of topics shall be created beforehand.
    pub num_topics: usize,
    /// Topic name prefix.
    pub topic_name_prefix: String,
    /// Number of partitions per topic.
    pub num_partitions: i32,
    /// The compression algorithm used to compress log entries.
    pub compression: Compression,
    /// The maximum log size an rskakfa batch producer could buffer.
    pub max_batch_size: ReadableSize,
    /// The linger duration of an rskafka batch producer.
    #[serde(with = "humantime_serde")]
    pub linger: Duration,
    /// The maximum amount of time (in milliseconds) to wait for Kafka records to be returned.
    #[serde(with = "humantime_serde")]
    pub max_wait_time: Duration,
}

impl Default for KafkaOptions {
    fn default() -> Self {
        Self {
            broker_endpoints: vec!["127.0.0.1:9090".to_string()],
            num_topics: 64,
            topic_name_prefix: TOPIC_NAME_PREFIX.to_string(),
            num_partitions: 1,
            compression: Compression::NoCompression,
            max_batch_size: ReadableSize::mb(4),       // 4MB.
            linger: Duration::from_millis(200),        // 200ms.
            max_wait_time: Duration::from_millis(100), // 100ms.
        }
    }
}

// TODO(niebayes): Add tests on serializing and deserializing kafka options.
