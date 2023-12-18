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

use crate::error::Result;
use crate::kv_backend::KvBackendRef;
use crate::wal::kafka::topic::Topic;
use crate::wal::kafka::topic_selector::{RoundRobinTopicSelector, TopicSelectorRef};
use crate::wal::kafka::KafkaConfig;

/// Manages topic initialization and selection.
pub struct TopicManager {
    topic_pool: Vec<Topic>,
    topic_selector: TopicSelectorRef,
    kv_backend: KvBackendRef,
}

impl TopicManager {
    /// Creates a new topic manager.
    pub fn new(config: &KafkaConfig, kv_backend: KvBackendRef) -> Self {
        Self {
            topic_pool: Vec::new(),
            topic_selector: Arc::new(RoundRobinTopicSelector::new()),
            kv_backend,
        }
    }

    /// Tries to initialize the topic pool.
    /// The initializer first tries to restore persisted topics from the kv backend.
    /// If not enough topics retrieved, the initializer would try to contact with Kafka cluster and request more topics.
    pub async fn try_init(&mut self) -> Result<()> {
        todo!()
    }

    /// Selects one topic from the topic pool through the topic selector.
    pub fn select(&self) -> &Topic {
        self.topic_selector.select(&self.topic_pool)
    }

    /// Selects a batch of topics from the topic pool through the topic selector.
    pub fn select_batch(&self, num_topics: usize) -> Vec<Topic> {
        // TODO(niebayes): calls `select` to select a collection of topics in a batching manner.
        vec!["tmp_topic".to_string(); num_topics]
    }
}
