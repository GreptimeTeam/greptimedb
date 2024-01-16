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
use rskafka::client::partition::Compression;
use serde::{Deserialize, Serialize};

use crate::config::kafka::common::{backoff_prefix, BackoffConfig};
use crate::BROKER_ENDPOINT;

/// Configurations for kafka wal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct DatanodeKafkaConfig {
    /// The broker endpoints of the Kafka cluster.
    pub broker_endpoints: Vec<String>,
    /// The compression algorithm used to compress kafka records.
    #[serde(skip)]
    pub compression: Compression,
    /// The max size of a single producer batch.
    pub max_batch_size: ReadableSize,
    /// The linger duration of a kafka batch producer.
    #[serde(with = "humantime_serde")]
    pub linger: Duration,
    /// The consumer wait timeout.
    #[serde(with = "humantime_serde")]
    pub consumer_wait_timeout: Duration,
    /// The backoff config.
    #[serde(flatten, with = "backoff_prefix")]
    pub backoff: BackoffConfig,
}

impl Default for DatanodeKafkaConfig {
    fn default() -> Self {
        Self {
            broker_endpoints: vec![BROKER_ENDPOINT.to_string()],
            compression: Compression::NoCompression,
            // Warning: Kafka has a default limit of 1MB per message in a topic.
            max_batch_size: ReadableSize::mb(1),
            linger: Duration::from_millis(200),
            consumer_wait_timeout: Duration::from_millis(100),
            backoff: BackoffConfig::default(),
        }
    }
}
