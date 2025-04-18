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

use serde::{Deserialize, Serialize};

use crate::config::kafka::common::{
    KafkaConnectionConfig, KafkaTopicConfig, DEFAULT_ACTIVE_PRUNE_INTERVAL,
    DEFAULT_ACTIVE_PRUNE_TASK_LIMIT, DEFAULT_TRIGGER_FLUSH_THRESHOLD,
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
    pub auto_prune_interval: Duration,
    // Threshold for sending flush request when pruning remote WAL.
    // `None` stands for never sending flush request.
    pub trigger_flush_threshold: u64,
    // Limit of concurrent active pruning procedures.
    pub auto_prune_parallelism: usize,
}

impl Default for MetasrvKafkaConfig {
    fn default() -> Self {
        Self {
            connection: Default::default(),
            kafka_topic: Default::default(),
            auto_create_topics: true,
            auto_prune_interval: DEFAULT_ACTIVE_PRUNE_INTERVAL,
            trigger_flush_threshold: DEFAULT_TRIGGER_FLUSH_THRESHOLD,
            auto_prune_parallelism: DEFAULT_ACTIVE_PRUNE_TASK_LIMIT,
        }
    }
}
