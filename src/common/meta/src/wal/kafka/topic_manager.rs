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

use crate::error::Result;
use crate::kv_backend::KvBackendRef;
use crate::wal::kafka::topic_selector::TopicSelectorRef;
use crate::wal::kafka::Topic;
use crate::wal::KafkaOptions;

/// Manages topic initialization and selection.
pub struct TopicManager {
    topic_pool: Vec<Topic>,
    topic_selector: TopicSelectorRef,
    kv_backend: KvBackendRef,
}

impl TopicManager {
    /// Creates a new topic manager.
    pub fn new(kafka_opts: &KafkaOptions, kv_backend: KvBackendRef) -> Self {
        todo!()
    }

    /// Tries to initialize the topic pool and persist the topics into the kv backend.
    pub async fn try_init_topic_pool(&mut self) -> Result<()> {
        todo!()
    }

    /// Selects one topic from the topic pool through the topic selector.
    pub fn select(&self) -> Topic {
        self.topic_selector.select(&self.topic_pool)
    }

    /// Selects a batch of topics from the topic pool through the topic selector.
    pub fn select_batch(&self, num_topics: usize) -> Vec<Topic> {
        todo!()
    }
}
