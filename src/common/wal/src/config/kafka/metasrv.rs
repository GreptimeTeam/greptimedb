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
    DEFAULT_AUTO_PRUNE_INTERVAL, DEFAULT_AUTO_PRUNE_LOGICAL_DELETE, DEFAULT_AUTO_PRUNE_PARALLELISM,
    DEFAULT_CHECKPOINT_TRIGGER_SIZE, DEFAULT_FLUSH_TRIGGER_SIZE,
    DEFAULT_PERIODIC_CHECKPOINT_PERSIST_INTERVAL, DEFAULT_REGION_FLUSH_TRIGGER_INTERVAL,
    KafkaConnectionConfig, KafkaTopicConfig,
};

/// Kafka wal configurations for metasrv.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct MetasrvKafkaConfig {
    /// The kafka connection config.
    #[serde(flatten)]
    pub connection: KafkaConnectionConfig,
    /// The kafka config.
    #[serde(flatten)]
    pub kafka_topic: KafkaTopicConfig,
    // Automatically create topics for WAL.
    pub auto_create_topics: bool,
    // Interval of WAL pruning.
    #[serde(with = "humantime_serde")]
    pub auto_prune_interval: Duration,
    // Whether auto WAL pruning only updates metadata and skips Kafka DeleteRecords.
    pub auto_prune_logical_delete: bool,
    // Limit of concurrent active pruning procedures.
    pub auto_prune_parallelism: usize,
    // The size of WAL to trigger flush.
    pub flush_trigger_size: ReadableSize,
    // The checkpoint trigger size.
    pub checkpoint_trigger_size: ReadableSize,
    /// Internal interval of remote WAL region flush trigger.
    #[serde(with = "humantime_serde")]
    pub region_flush_trigger_interval: Duration,
    /// Internal interval to periodically persist remote WAL checkpoints.
    #[serde(with = "humantime_serde")]
    pub periodic_checkpoint_persist_interval: Duration,
}

impl Default for MetasrvKafkaConfig {
    fn default() -> Self {
        Self {
            connection: Default::default(),
            kafka_topic: Default::default(),
            auto_create_topics: true,
            auto_prune_interval: DEFAULT_AUTO_PRUNE_INTERVAL,
            auto_prune_logical_delete: DEFAULT_AUTO_PRUNE_LOGICAL_DELETE,
            auto_prune_parallelism: DEFAULT_AUTO_PRUNE_PARALLELISM,
            flush_trigger_size: DEFAULT_FLUSH_TRIGGER_SIZE,
            checkpoint_trigger_size: DEFAULT_CHECKPOINT_TRIGGER_SIZE,
            region_flush_trigger_interval: DEFAULT_REGION_FLUSH_TRIGGER_INTERVAL,
            periodic_checkpoint_persist_interval: DEFAULT_PERIODIC_CHECKPOINT_PERSIST_INTERVAL,
        }
    }
}
