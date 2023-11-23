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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::wal::kafka::topic_manager::Topic;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum TopicSelectorType {
    RoundRobin,
}

pub trait TopicSelector: Send + Sync {
    fn select(&self, topic_pool: &[Topic]) -> Topic;
}

pub type TopicSelectorRef = Arc<dyn TopicSelector>;

#[derive(Default)]
pub struct RoundRobinTopicSelector {
    cursor: AtomicUsize,
}

impl TopicSelector for RoundRobinTopicSelector {
    fn select(&self, topic_pool: &[Topic]) -> Topic {
        // Safety: the caller ensures the topic pool is not empty and hence the modulo operation is safe.
        let which = self.cursor.fetch_add(1, Ordering::Relaxed) % topic_pool.len();
        // Safety: the modulo operation ensures the indexing is safe.
        topic_pool[which].clone()
    }
}

pub fn build_topic_selector(selector_type: &TopicSelectorType) -> TopicSelectorRef {
    match selector_type {
        TopicSelectorType::RoundRobin => Arc::new(RoundRobinTopicSelector::default()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_topic_selector() {
        let topic_pool: Vec<_> = [0, 1, 2].into_iter().map(|v| v.to_string()).collect();
        let selector = RoundRobinTopicSelector::default();

        assert_eq!(selector.select(&topic_pool), "0");
        assert_eq!(selector.select(&topic_pool), "1");
        assert_eq!(selector.select(&topic_pool), "2");
        assert_eq!(selector.select(&topic_pool), "0");
    }
}
