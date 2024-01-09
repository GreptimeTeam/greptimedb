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
use snafu::ensure;

use crate::error::{EmptyTopicPoolSnafu, Result};
use crate::wal::kafka::topic::Topic;

/// Controls topic selection.
pub(crate) trait TopicSelector: Send + Sync {
    /// Selects a topic from the topic pool.
    fn select<'a>(&self, topic_pool: &'a [Topic]) -> Result<&'a Topic>;
}

/// Arc wrapper of TopicSelector.
pub(crate) type TopicSelectorRef = Arc<dyn TopicSelector>;

/// A topic selector with the round-robin strategy, i.e. selects topics in a round-robin manner.
#[derive(Default)]
pub(crate) struct RoundRobinTopicSelector {
    cursor: AtomicUsize,
}

impl RoundRobinTopicSelector {
    // The cursor in the round-robin selector is not persisted which may break the round-robin strategy cross crashes.
    // Introducing a shuffling strategy may help mitigate this issue.
    pub fn with_shuffle() -> Self {
        let offset = rand::thread_rng().gen_range(0..64);
        Self {
            cursor: AtomicUsize::new(offset),
        }
    }
}

impl TopicSelector for RoundRobinTopicSelector {
    fn select<'a>(&self, topic_pool: &'a [Topic]) -> Result<&'a Topic> {
        ensure!(!topic_pool.is_empty(), EmptyTopicPoolSnafu);
        let which = self.cursor.fetch_add(1, Ordering::Relaxed) % topic_pool.len();
        Ok(&topic_pool[which])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests that a selector behaves as expected when the given topic pool is empty.
    #[test]
    fn test_empty_topic_pool() {
        let topic_pool = vec![];
        let selector = RoundRobinTopicSelector::default();
        assert!(selector.select(&topic_pool).is_err());
    }

    #[test]
    fn test_round_robin_topic_selector() {
        let topic_pool: Vec<_> = [0, 1, 2].into_iter().map(|v| v.to_string()).collect();
        let selector = RoundRobinTopicSelector::default();

        assert_eq!(selector.select(&topic_pool).unwrap(), "0");
        assert_eq!(selector.select(&topic_pool).unwrap(), "1");
        assert_eq!(selector.select(&topic_pool).unwrap(), "2");
        assert_eq!(selector.select(&topic_pool).unwrap(), "0");

        // Creates a round-robin selector with shuffle.
        let selector = RoundRobinTopicSelector::with_shuffle();
        let topic = selector.select(&topic_pool).unwrap();
        assert!(topic_pool.contains(topic));
    }
}
