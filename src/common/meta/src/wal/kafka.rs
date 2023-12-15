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

pub mod topic;
pub mod topic_manager;
mod topic_selector;

use serde::{Deserialize, Serialize};

use crate::wal::kafka::topic::Topic;

/// Configurations for bootstraping a kafka wal.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Default)]
pub struct KafkaConfig;

/// Kafka wal options allocated to a region.
#[derive(Serialize, Deserialize, Default, Debug, Clone, PartialEq)]
pub struct KafkaWalOptions {
    /// Kafka wal topic.
    /// Publishers publish log entries to the topic while subscribers pull log entries from the topic.
    pub topic: Topic,
}
