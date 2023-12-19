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

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::wal::kafka::topic::Topic;

/// The type of the topic selector, i.e. with which strategy to select a topic.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum SelectorType {
    #[default]
    RoundRobin,
}

/// Controls topic selection.
pub(super) trait TopicSelector: Send + Sync {
    /// Selects a topic from the topic pool.
    fn select(&self, topic_pool: &[Topic]) -> &Topic;
}

pub(super) type TopicSelectorRef = Arc<dyn TopicSelector>;

/// A topic selector with the round-robin strategy, i.e. selects topics in a round-robin manner.
pub(super) struct RoundRobinTopicSelector;

impl RoundRobinTopicSelector {
    /// Creates a new round-robin topic selector.
    pub(super) fn new() -> Self {
        todo!()
    }
}

impl TopicSelector for RoundRobinTopicSelector {
    fn select(&self, topic_pool: &[Topic]) -> &Topic {
        todo!()
    }
}
