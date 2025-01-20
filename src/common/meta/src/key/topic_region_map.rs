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
use snafu::OptionExt;
use store_api::storage::RegionId;

use crate::error::{Error, InvalidMetadataSnafu, Result};
use crate::key::{MetadataKey, TOPIC_REGION_MAP_PATTERN, TOPIC_REGION_MAP_PREFIX};
use crate::kv_backend::memory::MemoryKvBackend;
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{BatchPutRequest, PutRequest, RangeRequest};
use crate::rpc::KeyValue;

#[derive(Debug, Clone, PartialEq)]
pub struct TopicRegionMapKey<'a> {
    pub region_id: RegionId,
    pub topic: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicRegionMapValue;

impl<'a> TopicRegionMapKey<'a> {
    pub fn new(region_id: RegionId, topic: &'a str) -> Self {
        Self { region_id, topic }
    }

    pub fn range_start_key() -> String {
        TOPIC_REGION_MAP_PREFIX.to_string()
    }
}

impl<'a> MetadataKey<'a, TopicRegionMapKey<'a>> for TopicRegionMapKey<'a> {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<TopicRegionMapKey<'a>> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "RegionTopicMapKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        TopicRegionMapKey::try_from(key)
    }
}

impl Display for TopicRegionMapKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}_{}",
            TOPIC_REGION_MAP_PREFIX,
            self.topic,
            self.region_id.as_u64()
        )
    }
}

impl<'a> TryFrom<&'a str> for TopicRegionMapKey<'a> {
    type Error = Error;

    /// Value is of format `{prefix}/{topic}_{region_id}`
    fn try_from(value: &'a str) -> Result<TopicRegionMapKey<'a>> {
        let captures = TOPIC_REGION_MAP_PATTERN
            .captures(value)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid region topic map key: {}", value),
            })?;
        let topic = captures
            .get(1)
            .map(|m| m.as_str())
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid topic in region topic map key: {}", value),
            })?;
        let region_id = captures[2].parse::<u64>().map_err(|_| {
            InvalidMetadataSnafu {
                err_msg: format!("Invalid region id in region topic map key: {}", value),
            }
            .build()
        })?;
        Ok(TopicRegionMapKey {
            region_id: RegionId::from_u64(region_id),
            topic,
        })
    }
}

fn topic_region_map_decoder(value: &KeyValue) -> Result<TopicRegionMapKey<'_>> {
    let key = TopicRegionMapKey::from_bytes(&value.key)?;
    Ok(key)
}

/// Manages map of topics and regions in kvbackend.
pub struct TopicRegionMapManager {
    kv_backend: KvBackendRef,
}

impl Default for TopicRegionMapManager {
    fn default() -> Self {
        Self::new(Arc::new(MemoryKvBackend::default()))
    }
}

impl TopicRegionMapManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    pub async fn put(&self, key: TopicRegionMapKey<'_>) -> Result<()> {
        let put_req = PutRequest {
            key: key.to_bytes(),
            value: vec![],
            prev_kv: false,
        };
        self.kv_backend.put(put_req).await?;
        Ok(())
    }

    pub async fn batch_put(&self, keys: Vec<TopicRegionMapKey<'_>>) -> Result<()> {
        let req = BatchPutRequest {
            kvs: keys
                .into_iter()
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

    /// Returns all topic-region mappings to avoid parsing again.
    pub async fn range(&self) -> Result<Vec<(RegionId, String)>> {
        let prefix = TopicRegionMapKey::range_start_key();
        let range_req = RangeRequest::new().with_prefix(prefix.as_bytes());
        let resp = self.kv_backend.range(range_req).await?;
        resp.kvs
            .iter()
            .map(|kv| {
                let key = topic_region_map_decoder(kv)?;
                Ok((key.region_id, key.topic.to_string()))
            })
            .collect::<Result<Vec<_>>>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_topic_region_map_manager() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicRegionMapManager::new(kv_backend.clone());

        let topics = (0..16).map(|i| format!("topic_{}", i)).collect::<Vec<_>>();
        let keys = (0..64)
            .map(|i| TopicRegionMapKey::new(RegionId::from_u64(i), &topics[(i % 16) as usize]))
            .collect::<Vec<_>>();

        manager.batch_put(keys.clone()).await.unwrap();

        let mut key_values = manager.range().await.unwrap();
        let expected = keys
            .iter()
            .map(|key| (key.region_id, key.topic.to_string()))
            .collect::<Vec<_>>();
        key_values.sort_by(|a, b| a.0.as_u64().cmp(&b.0.as_u64()).then(a.1.cmp(&b.1)));
        assert_eq!(key_values, expected);
    }
}
