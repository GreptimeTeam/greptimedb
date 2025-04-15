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
use serde::{Deserialize, Serialize};

use crate::config::kafka::common::{
    KafkaConnectionConfig, KafkaTopicConfig, DEFAULT_ACTIVE_PRUNE_INTERVAL,
    DEFAULT_ACTIVE_PRUNE_TASK_LIMIT, DEFAULT_TRIGGER_FLUSH_THRESHOLD,
};

/// Kafka wal configurations for datanode.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct DatanodeKafkaConfig {
    /// The kafka connection config.
    #[serde(flatten)]
    pub connection: KafkaConnectionConfig,
    /// TODO(weny): Remove the alias once we release v0.9.
    /// The max size of a single producer batch.
    #[serde(alias = "max_batch_size")]
    pub max_batch_bytes: ReadableSize,
    /// The consumer wait timeout.
    #[serde(with = "humantime_serde")]
    pub consumer_wait_timeout: Duration,
    /// The kafka topic config.
    #[serde(flatten)]
    pub kafka_topic: KafkaTopicConfig,
    // Automatically create topics for WAL.
    pub auto_create_topics: bool,
    // Create index for WAL.
    pub create_index: bool,
    #[serde(with = "humantime_serde")]
    pub dump_index_interval: Duration,
    /// Ignore missing entries during read WAL.
    pub overwrite_entry_start_id: bool,
    // Active WAL pruning.
    pub auto_prune_topic_records: bool,
    // Interval of WAL pruning.
    pub auto_prune_interval: Duration,
    // Threshold for sending flush request when pruning remote WAL.
    // `None` stands for never sending flush request.
    pub trigger_flush_threshold: u64,
    // Limit of concurrent active pruning procedures.
    pub auto_prune_parallelism: usize,
}

impl Default for DatanodeKafkaConfig {
    fn default() -> Self {
        Self {
            connection: KafkaConnectionConfig::default(),
            // Warning: Kafka has a default limit of 1MB per message in a topic.
            max_batch_bytes: ReadableSize::mb(1),
            consumer_wait_timeout: Duration::from_millis(100),
            kafka_topic: KafkaTopicConfig::default(),
            auto_create_topics: true,
            create_index: true,
            dump_index_interval: Duration::from_secs(60),
            overwrite_entry_start_id: false,
            auto_prune_topic_records: false,
            auto_prune_interval: DEFAULT_ACTIVE_PRUNE_INTERVAL,
            trigger_flush_threshold: DEFAULT_TRIGGER_FLUSH_THRESHOLD,
            auto_prune_parallelism: DEFAULT_ACTIVE_PRUNE_TASK_LIMIT,
        }
    }
}
