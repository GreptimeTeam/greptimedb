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
use std::fmt::{self, Display};

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::error::{DecodeJsonSnafu, Error, InvalidMetadataSnafu, Result};
use crate::key::{
    MetadataKey, KAFKA_TOPIC_KEY_PATTERN, KAFKA_TOPIC_KEY_PREFIX, LEGACY_TOPIC_KEY_PREFIX,
};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{BatchPutRequest, RangeRequest};
use crate::rpc::KeyValue;

#[derive(Debug, Clone, PartialEq)]
pub struct TopicNameKey<'a> {
    pub topic: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicNameValue;

impl<'a> TopicNameKey<'a> {
    pub fn new(topic: &'a str) -> Self {
        Self { topic }
    }

    pub fn gen_with_id_and_prefix(id: usize, prefix: &'a str) -> String {
        format!("{}_{}", prefix, id)
    }

    pub fn range_start_key(prefix: &'a str) -> String {
        format!("{}/{}", KAFKA_TOPIC_KEY_PREFIX, prefix)
    }
}

impl<'a> MetadataKey<'a, TopicNameKey<'a>> for TopicNameKey<'_> {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<TopicNameKey<'a>> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "TopicNameKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        TopicNameKey::try_from(key)
    }
}

impl Display for TopicNameKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", KAFKA_TOPIC_KEY_PREFIX, self.topic)
    }
}

impl<'a> TryFrom<&'a str> for TopicNameKey<'a> {
    type Error = Error;

    fn try_from(value: &'a str) -> Result<TopicNameKey<'a>> {
        let captures = KAFKA_TOPIC_KEY_PATTERN
            .captures(value)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid topic name key: {}", value),
            })?;

        // Safety: pass the regex check above
        Ok(TopicNameKey {
            topic: captures.get(1).unwrap().as_str(),
        })
    }
}

/// Convert a key-value pair to a topic name.
fn topic_decoder(kv: &KeyValue) -> Result<String> {
    let key = TopicNameKey::from_bytes(&kv.key)?;
    Ok(key.topic.to_string())
}

pub struct TopicPool {
    prefix: String,
    pub(crate) topics: Vec<String>,
}

impl TopicPool {
    pub fn new(num_topics: usize, prefix: String) -> Self {
        let topics = (0..num_topics)
            .map(|i| TopicNameKey::gen_with_id_and_prefix(i, &prefix))
            .collect();
        Self { prefix, topics }
    }

    /// Legacy restore for compatibility.
    /// Returns the topics in legacy format and the transaction to update to the new format if needed.
    async fn legacy_restore(&self, kv_backend: &KvBackendRef) -> Result<()> {
        if let Some(kv) = kv_backend.get(LEGACY_TOPIC_KEY_PREFIX.as_bytes()).await? {
            let topics =
                serde_json::from_slice::<Vec<String>>(&kv.value).context(DecodeJsonSnafu)?;
            // Should remove the legacy topics and update to the new format.
            kv_backend
                .delete(LEGACY_TOPIC_KEY_PREFIX.as_bytes(), false)
                .await?;
            let batch_put_req = BatchPutRequest {
                kvs: topics
                    .iter()
                    .map(|topic| KeyValue {
                        key: TopicNameKey::new(topic).to_bytes(),
                        value: vec![],
                    })
                    .collect(),
                prev_kv: false,
            };
            kv_backend.batch_put(batch_put_req).await?;
        }
        Ok(())
    }

    async fn restore(&self, kv_backend: &KvBackendRef) -> Result<Vec<String>> {
        // Topics in legacy format should be updated to the new format.
        self.legacy_restore(kv_backend).await?;
        let req =
            RangeRequest::new().with_prefix(TopicNameKey::range_start_key(&self.prefix).as_bytes());
        let resp = kv_backend.range(req).await?;
        let topics = resp
            .kvs
            .iter()
            .map(topic_decoder)
            .collect::<Result<Vec<String>>>()?;
        Ok(topics)
    }

    /// Restores topics from kvbackend and return the topics that are not stored in kvbackend.
    pub async fn to_be_created(&self, kv_backend: &KvBackendRef) -> Result<Vec<&String>> {
        let topics = self.restore(kv_backend).await?;
        let mut topics_set = HashSet::with_capacity(topics.len());
        topics_set.extend(topics);

        Ok(self
            .topics
            .iter()
            .filter(|topic| !topics_set.contains(*topic))
            .collect::<Vec<_>>())
    }

    /// Persists topics into kvbackend.
    pub async fn persist(&self, kv_backend: &KvBackendRef) -> Result<()> {
        let topic_name_keys = self
            .topics
            .iter()
            .map(|topic| TopicNameKey::new(topic))
            .collect::<Vec<_>>();
        let req = BatchPutRequest {
            kvs: topic_name_keys
                .iter()
                .map(|key| KeyValue {
                    key: key.to_bytes(),
                    value: vec![],
                })
                .collect(),
            prev_kv: false,
        };
        kv_backend.batch_put(req).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::rpc::store::PutRequest;

    #[tokio::test]
    async fn test_restore_legacy_persisted_topics() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let topic_pool = TopicPool::new(16, "greptimedb_wal_topic".to_string());

        // No legacy topics.
        let mut topics_to_be_created = topic_pool.to_be_created(&kv_backend).await.unwrap();
        topics_to_be_created.sort();
        let mut expected = topic_pool.topics.iter().collect::<Vec<_>>();
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

        let topics_to_be_created = topic_pool.to_be_created(&kv_backend).await.unwrap();
        assert!(topics_to_be_created.is_empty());

        // Legacy topics should be deleted after restoring.
        let legacy_topics = kv_backend
            .get(LEGACY_TOPIC_KEY_PREFIX.as_bytes())
            .await
            .unwrap();
        assert!(legacy_topics.is_none());

        // Then we can restore it from the new format.
        let mut restored_topics = topic_pool.restore(&kv_backend).await.unwrap();
        restored_topics.sort();
        let mut expected = topic_pool.topics.clone();
        expected.sort();
        assert_eq!(expected, restored_topics);
    }

    // Tests that topics can be successfully persisted into the kv backend and can be successfully restored from the kv backend.
    #[tokio::test]
    async fn test_restore_persisted_topics() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let topic_name_prefix = "greptimedb_wal_topic";
        let num_topics = 16;

        // Constructs mock topics.
        let topic_pool = TopicPool::new(num_topics, topic_name_prefix.to_string());

        let mut topics_to_be_created = topic_pool.to_be_created(&kv_backend).await.unwrap();
        topics_to_be_created.sort();
        let mut expected = topic_pool.topics.iter().collect::<Vec<_>>();
        expected.sort();
        assert_eq!(expected, topics_to_be_created);

        // Persists topics to kv backend.
        topic_pool.persist(&kv_backend).await.unwrap();
        let topics_to_be_created = topic_pool.to_be_created(&kv_backend).await.unwrap();
        assert!(topics_to_be_created.is_empty());
    }
}
