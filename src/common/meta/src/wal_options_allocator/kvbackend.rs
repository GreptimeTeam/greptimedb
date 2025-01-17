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

use std::collections::HashSet;

use crate::error::Result;
use crate::key::topic_name::{TopicNameKey, TopicNameKeyManager};
use crate::kv_backend::KvBackendRef;

/// Manages topics in kvbackend.
/// Responsible for:
/// 1. Restores and persisting topics in kvbackend.
/// 2. Clears topics in legacy format and restores them in the new format.
pub struct KvBackendTopicManager {
    manager: TopicNameKeyManager,
}

impl KvBackendTopicManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self {
            manager: TopicNameKeyManager::new(kv_backend),
        }
    }

    async fn restore(&self) -> Result<Vec<String>> {
        self.manager.update_legacy_topics().await?;
        let topics = self.manager.range().await?;
        Ok(topics)
    }

    /// Restores topics from kvbackend and return the topics that are not stored in kvbackend.
    pub async fn to_be_created<'a>(&self, all_topics: &'a [String]) -> Result<Vec<&'a String>> {
        let topics = self.restore().await?;
        let topic_set = topics.iter().collect::<HashSet<_>>();
        let mut to_be_created = Vec::with_capacity(all_topics.len());
        for topic in all_topics {
            if !topic_set.contains(topic) {
                to_be_created.push(topic);
            }
        }
        Ok(to_be_created)
    }

    /// Persists topics into kvbackend.
    pub async fn persist(&self, topics: &[String]) -> Result<()> {
        self.manager
            .batch_put(
                topics
                    .iter()
                    .map(|topic| TopicNameKey::new(topic))
                    .collect(),
            )
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::key::LEGACY_TOPIC_KEY_PREFIX;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::rpc::store::PutRequest;

    #[tokio::test]
    async fn test_restore_legacy_persisted_topics() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let topic_kvbackend_manager = KvBackendTopicManager::new(kv_backend.clone());

        let all_topics = (0..16)
            .map(|i| format!("greptimedb_wal_topic_{}", i))
            .collect::<Vec<_>>();

        // No legacy topics.
        let mut topics_to_be_created = topic_kvbackend_manager
            .to_be_created(&all_topics)
            .await
            .unwrap();
        topics_to_be_created.sort();
        let mut expected = all_topics.iter().collect::<Vec<_>>();
        expected.sort();
        assert_eq!(expected, topics_to_be_created);

        // A topic pool with 16 topics stored in kvbackend in legacy format.
        let topics = "[\"greptimedb_wal_topic_0\",\"greptimedb_wal_topic_1\",\"greptimedb_wal_topic_2\",\"greptimedb_wal_topic_3\",\"greptimedb_wal_topic_4\",\"greptimedb_wal_topic_5\",\"greptimedb_wal_topic_6\",\"greptimedb_wal_topic_7\",\"greptimedb_wal_topic_8\",\"greptimedb_wal_topic_9\",\"greptimedb_wal_topic_10\",\"greptimedb_wal_topic_11\",\"greptimedb_wal_topic_12\",\"greptimedb_wal_topic_13\",\"greptimedb_wal_topic_14\",\"greptimedb_wal_topic_15\"]";
        let put_req = PutRequest {
            key: LEGACY_TOPIC_KEY_PREFIX.as_bytes().to_vec(),
            value: topics.as_bytes().to_vec(),
            prev_kv: true,
        };
        let res = kv_backend.put(put_req).await.unwrap();
        assert!(res.prev_kv.is_none());

        let topics_to_be_created = topic_kvbackend_manager
            .to_be_created(&all_topics)
            .await
            .unwrap();
        assert!(topics_to_be_created.is_empty());

        // Legacy topics should be deleted after restoring.
        let legacy_topics = kv_backend
            .get(LEGACY_TOPIC_KEY_PREFIX.as_bytes())
            .await
            .unwrap();
        assert!(legacy_topics.is_none());

        // Then we can restore it from the new format.
        let mut restored_topics = topic_kvbackend_manager.restore().await.unwrap();
        restored_topics.sort();
        let mut expected = all_topics.clone();
        expected.sort();
        assert_eq!(expected, restored_topics);
    }

    // Tests that topics can be successfully persisted into the kv backend and can be successfully restored from the kv backend.
    #[tokio::test]
    async fn test_restore_persisted_topics() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let topic_name_prefix = "greptimedb_wal_topic";
        let num_topics = 16;

        let all_topics = (0..num_topics)
            .map(|i| format!("{}_{}", topic_name_prefix, i))
            .collect::<Vec<_>>();

        // Constructs mock topics.
        let topic_kvbackend_manager = KvBackendTopicManager::new(kv_backend);

        let mut topics_to_be_created = topic_kvbackend_manager
            .to_be_created(&all_topics)
            .await
            .unwrap();
        topics_to_be_created.sort();
        let mut expected = all_topics.iter().collect::<Vec<_>>();
        expected.sort();
        assert_eq!(expected, topics_to_be_created);

        // Persists topics to kv backend.
        topic_kvbackend_manager.persist(&all_topics).await.unwrap();
        let topics_to_be_created = topic_kvbackend_manager
            .to_be_created(&all_topics)
            .await
            .unwrap();
        assert!(topics_to_be_created.is_empty());
    }
}
