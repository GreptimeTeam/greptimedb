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

use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};

use crate::ensure_values;
use crate::error::{self, DecodeJsonSnafu, Error, InvalidMetadataSnafu, Result, UnexpectedSnafu};
use crate::key::txn_helper::TxnOpGetResponseSet;
use crate::key::{
    DeserializedValueWithBytes, MetadataKey, MetadataValue, KAFKA_TOPIC_KEY_PATTERN,
    KAFKA_TOPIC_KEY_PREFIX, LEGACY_TOPIC_KEY_PREFIX,
};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{BatchPutRequest, RangeRequest};
use crate::rpc::KeyValue;

#[derive(Debug, Clone, PartialEq)]
pub struct TopicNameKey<'a> {
    pub topic: &'a str,
}

/// The value of the topic name key.
/// `pruned_entry_id`: The offset already pruned in remote WAL. Region using this topic should replay from at least this offset + 1.
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct TopicNameValue {
    pub pruned_entry_id: u64,
}

impl TopicNameValue {
    pub fn new(pruned_entry_id: u64) -> Self {
        Self { pruned_entry_id }
    }
}

impl MetadataValue for TopicNameValue {
    fn try_from_raw_value(raw_value: &[u8]) -> Result<Self> {
        let value = serde_json::from_slice::<TopicNameValue>(raw_value).context(DecodeJsonSnafu)?;
        Ok(value)
    }

    fn try_as_raw_value(&self) -> Result<Vec<u8>> {
        let raw_value = serde_json::to_vec(self).context(DecodeJsonSnafu)?;
        Ok(raw_value)
    }
}

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
            let mut reqs = Vec::with_capacity(topics.len() + 1);
            for topic in topics {
                let topic_name_key = TopicNameKey::new(&topic);
                let topic_name_value = TopicNameValue::new(0);
                let put_req = TxnOp::Put(
                    topic_name_key.to_bytes(),
                    topic_name_value.try_as_raw_value()?,
                );
                reqs.push(put_req);
            }
            let delete_req = TxnOp::Delete(LEGACY_TOPIC_KEY_PREFIX.as_bytes().to_vec());
            reqs.push(delete_req);
            let txn = Txn::new().and_then(reqs);
            self.kv_backend.txn(txn).await?;
        }
        Ok(())
    }

    /// Range query for topics. Only the keys are returned.
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

    /// Put topics into kvbackend. The value is set to 0 by default.
    pub async fn batch_put(&self, topic_name_keys: Vec<TopicNameKey<'_>>) -> Result<()> {
        let mut kvs = Vec::with_capacity(topic_name_keys.len());
        for topic_name_key in &topic_name_keys {
            let topic_name_value = TopicNameValue::new(0);
            let kv = KeyValue {
                key: topic_name_key.to_bytes(),
                value: topic_name_value.try_as_raw_value()?,
            };
            kvs.push(kv);
        }
        let req = BatchPutRequest {
            kvs,
            prev_kv: false,
        };
        self.kv_backend.batch_put(req).await?;
        Ok(())
    }

    /// Get value for a specific topic.
    pub async fn get(
        &self,
        topic: &str,
    ) -> Result<Option<DeserializedValueWithBytes<TopicNameValue>>> {
        let key = TopicNameKey::new(topic);
        let raw_key = key.to_bytes();
        self.kv_backend
            .get(&raw_key)
            .await?
            .map(|x| DeserializedValueWithBytes::from_inner_slice(&x.value))
            .transpose()
    }

    /// Update the topic name key and value in the kv backend.
    pub async fn update(
        &self,
        topic: &str,
        pruned_entry_id: u64,
        prev: Option<DeserializedValueWithBytes<TopicNameValue>>,
    ) -> Result<()> {
        let key = TopicNameKey::new(topic);
        let raw_key = key.to_bytes();
        let value = TopicNameValue::new(pruned_entry_id);
        let new_raw_value = value.try_as_raw_value()?;
        let raw_value = prev.map(|v| v.get_raw_bytes()).unwrap_or_default();

        let txn = Txn::compare_and_put(raw_key.clone(), raw_value, new_raw_value.clone());
        let mut r = self.kv_backend.txn(txn).await?;

        if !r.succeeded {
            let mut set = TxnOpGetResponseSet::from(&mut r.responses);
            let raw_value = TxnOpGetResponseSet::filter(raw_key)(&mut set)
                .context(UnexpectedSnafu {
                    err_msg: "Reads the empty topic name value in comparing operation while updating TopicNameValue",
                })?;

            let op_name = "updating TopicNameValue";
            ensure_values!(raw_value, new_raw_value, op_name);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
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

        for topic in &topics {
            let value = manager.get(topic).await.unwrap().unwrap();
            assert_eq!(value.pruned_entry_id, 0);
            manager.update(topic, 1, Some(value.clone())).await.unwrap();
            let new_value = manager.get(topic).await.unwrap().unwrap();
            assert_eq!(new_value.pruned_entry_id, 1);
            // Update twice, nothing changed
            manager.update(topic, 1, Some(value.clone())).await.unwrap();
            let new_value = manager.get(topic).await.unwrap().unwrap();
            assert_eq!(new_value.pruned_entry_id, 1);
            // Bad cas, emit error
            let err = manager.update(topic, 3, Some(value)).await.unwrap_err();
            assert_matches!(err, error::Error::Unexpected { .. });
        }
    }
}
