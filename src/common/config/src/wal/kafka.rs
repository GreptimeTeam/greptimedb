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

use serde::{Deserialize, Serialize};

/// Kafka wal topic.
pub type Topic = String;

/// Configurations for kafka wal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaConfig;

/// Kafka wal options allocated to a region.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct KafkaOptions {
    /// Kafka wal topic.
    pub topic: Topic,
}

impl Default for KafkaOptions {
    fn default() -> Self {
        Self {
            // To indicates a default deserialized topic is invalid.
            topic: "invalid_topic".to_string(),
        }
    }
}
