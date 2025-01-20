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

use std::fmt::{self, Display};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};

use crate::error::{DecodeJsonSnafu, Error, InvalidMetadataSnafu, Result};
use crate::key::{
    MetadataKey, KAFKA_TOPIC_KEY_PATTERN, KAFKA_TOPIC_KEY_PREFIX, LEGACY_TOPIC_KEY_PREFIX,
};
use crate::kv_backend::memory::MemoryKvBackend;
use crate::kv_backend::txn::{Txn, TxnOp};
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

    pub fn range_start_key() -> String {
        KAFKA_TOPIC_KEY_PREFIX.to_string()
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

pub struct TopicNameManager {
    kv_backend: KvBackendRef,
}

impl Default for TopicNameManager {
    fn default() -> Self {
        Self::new(Arc::new(MemoryKvBackend::default()))
    }
}

impl TopicNameManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Update the topics in legacy format to the new format.
    pub async fn update_legacy_topics(&self) -> Result<()> {
        if let Some(kv) = self
            .kv_backend
            .get(LEGACY_TOPIC_KEY_PREFIX.as_bytes())
            .await?
        {
            let topics =
                serde_json::from_slice::<Vec<String>>(&kv.value).context(DecodeJsonSnafu)?;
            let mut reqs = topics
                .iter()
                .map(|topic| {
                    let key = TopicNameKey::new(topic);
                    TxnOp::Put(key.to_bytes(), vec![])
                })
                .collect::<Vec<_>>();
            let delete_req = TxnOp::Delete(LEGACY_TOPIC_KEY_PREFIX.as_bytes().to_vec());
            reqs.push(delete_req);
            let txn = Txn::new().and_then(reqs);
            self.kv_backend.txn(txn).await?;
        }
        Ok(())
    }

    /// Range query for topics.
    /// Caution: this method returns keys as String instead of values of range query since the topics are stored in keys.
    pub async fn range(&self) -> Result<Vec<String>> {
        let prefix = TopicNameKey::range_start_key();
        let raw_prefix = prefix.as_bytes();
        let req = RangeRequest::new().with_prefix(raw_prefix);
        let resp = self.kv_backend.range(req).await?;
        resp.kvs
            .iter()
            .map(topic_decoder)
            .collect::<Result<Vec<String>>>()
    }

    /// Put topics into kvbackend.
    pub async fn batch_put(&self, topic_name_keys: Vec<TopicNameKey<'_>>) -> Result<()> {
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
        self.kv_backend.batch_put(req).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::KvBackend;
    use crate::rpc::store::PutRequest;

    #[tokio::test]
    async fn test_topic_name_key_manager() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicNameManager::new(kv_backend.clone());

        let mut all_topics = (0..16)
            .map(|i| format!("{}/{}", KAFKA_TOPIC_KEY_PREFIX, i))
            .collect::<Vec<_>>();
        all_topics.sort();
        let topic_name_keys = all_topics
            .iter()
            .map(|topic| TopicNameKey::new(topic))
            .collect::<Vec<_>>();

        manager.batch_put(topic_name_keys.clone()).await.unwrap();

        let topics = manager.range().await.unwrap();
        assert_eq!(topics, all_topics);

        kv_backend
            .put(PutRequest {
                key: LEGACY_TOPIC_KEY_PREFIX.as_bytes().to_vec(),
                value: serde_json::to_vec(&all_topics).unwrap(),
                prev_kv: false,
            })
            .await
            .unwrap();
        manager.update_legacy_topics().await.unwrap();
        let res = kv_backend
            .get(LEGACY_TOPIC_KEY_PREFIX.as_bytes())
            .await
            .unwrap();
        assert!(res.is_none());
        let topics = manager.range().await.unwrap();
        assert_eq!(topics, all_topics);

        let topics = manager.range().await.unwrap();
        assert_eq!(topics, all_topics);
    }
}
