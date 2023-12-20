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

use rand::Rng;
use serde::{Deserialize, Serialize};

use crate::wal::kafka::topic::Topic;

/// The type of the topic selector, i.e. with which strategy to select a topic.
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SelectorType {
    #[default]
    #[serde(rename = "round_robin")]
    RoundRobin,
}

/// Controls topic selection.
pub(super) trait TopicSelector: Send + Sync {
    /// Selects a topic from the topic pool.
    fn select<'a>(&'a self, topic_pool: &'a [Topic]) -> &Topic;
}

/// Arc wrapper of TopicSelector.
pub(super) type TopicSelectorRef = Arc<dyn TopicSelector>;

/// A topic selector with the round-robin strategy, i.e. selects topics in a round-robin manner.
#[derive(Default)]
pub(super) struct RoundRobinTopicSelector {
    cursor: AtomicUsize,
}

impl RoundRobinTopicSelector {
    // The cursor in the round-robin selector is not persisted which may break the round-robin strategy cross crashes.
    // Introducing a shuffling strategy may help mitigate this issue.
    pub fn with_shuffle() -> Self {
        let mut this = Self::default();
        let offset = rand::thread_rng().gen::<usize>() % usize::MAX;
        // It's ok when an overflow happens since `fetch_add` automatically wraps around.
        this.cursor.fetch_add(offset, Ordering::Relaxed);
        this
    }
}

impl TopicSelector for RoundRobinTopicSelector {
    fn select<'a>(&'a self, topic_pool: &'a [Topic]) -> &Topic {
        // Safety: the caller ensures the topic pool is not empty and hence the modulo operation is safe.
        let which = self.cursor.fetch_add(1, Ordering::Relaxed) % topic_pool.len();
        // Safety: the modulo operation ensures the index operation is safe.
        topic_pool.get(which).unwrap()
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

        // Creates a round-robin selector with shuffle.
        let selector = RoundRobinTopicSelector::with_shuffle();
        let topic = selector.select(&topic_pool);
        assert!(topic_pool.contains(topic));
    }
}
