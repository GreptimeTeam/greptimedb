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

/// Kafka wal options allocated to a region.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KafkaWalOptions {
    /// Kafka wal topic.
    pub topic: String,
    /// Initial pruned entry id of the topic when this option is allocated.
    ///
    /// This is a create-time hint for initializing a new region's flushed entry id,
    /// not the authoritative latest pruned entry id of the topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_pruned_entry_id: Option<u64>,
}

impl KafkaWalOptions {
    /// Creates kafka WAL options with the topic only.
    pub fn new(topic: String) -> Self {
        Self {
            topic,
            initial_pruned_entry_id: None,
        }
    }
}
