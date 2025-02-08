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

use std::collections::HashMap;
use std::fmt::{self, Display};

use common_wal::options::WalOptions;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

use crate::ddl::utils::parse_region_wal_options;
use crate::error::{Error, InvalidMetadataSnafu, Result};
use crate::key::{MetadataKey, TOPIC_REGION_PATTERN, TOPIC_REGION_PREFIX};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::rpc::store::{BatchDeleteRequest, BatchPutRequest, PutRequest, RangeRequest};
use crate::rpc::KeyValue;

// The TopicRegionKey is a key for the topic-region mapping in the kvbackend.
// The layout of the key is `__topic_region/{topic_name}/{region_id}`.
#[derive(Debug, Clone, PartialEq)]
pub struct TopicRegionKey<'a> {
    pub region_id: RegionId,
    pub topic: &'a str,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicRegionValue;

impl<'a> TopicRegionKey<'a> {
    pub fn new(region_id: RegionId, topic: &'a str) -> Self {
        Self { region_id, topic }
    }

    pub fn range_topic_key(topic: &str) -> String {
        format!("{}/{}/", TOPIC_REGION_PREFIX, topic)
    }
}

impl<'a> MetadataKey<'a, TopicRegionKey<'a>> for TopicRegionKey<'a> {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<TopicRegionKey<'a>> {
        let key = std::str::from_utf8(bytes).map_err(|e| {
            InvalidMetadataSnafu {
                err_msg: format!(
                    "TopicRegionKey '{}' is not a valid UTF8 string: {e}",
                    String::from_utf8_lossy(bytes)
                ),
            }
            .build()
        })?;
        TopicRegionKey::try_from(key)
    }
}

impl Display for TopicRegionKey<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}",
            Self::range_topic_key(self.topic),
            self.region_id.as_u64()
        )
    }
}

impl<'a> TryFrom<&'a str> for TopicRegionKey<'a> {
    type Error = Error;

    /// Value is of format `{prefix}/{topic}/{region_id}`
    fn try_from(value: &'a str) -> Result<TopicRegionKey<'a>> {
        let captures = TOPIC_REGION_PATTERN
            .captures(value)
            .context(InvalidMetadataSnafu {
                err_msg: format!("Invalid TopicRegionKey: {}", value),
            })?;
        let topic = captures.get(1).map(|m| m.as_str()).unwrap();
        let region_id = captures[2].parse::<u64>().map_err(|_| {
            InvalidMetadataSnafu {
                err_msg: format!("Invalid region id in TopicRegionKey: {}", value),
            }
            .build()
        })?;
        Ok(TopicRegionKey {
            region_id: RegionId::from_u64(region_id),
            topic,
        })
    }
}

fn topic_region_decoder(value: &KeyValue) -> Result<TopicRegionKey<'_>> {
    let key = TopicRegionKey::from_bytes(&value.key)?;
    Ok(key)
}

/// Manages map of topics and regions in kvbackend.
pub struct TopicRegionManager {
    kv_backend: KvBackendRef,
}

impl TopicRegionManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    pub async fn put(&self, key: TopicRegionKey<'_>) -> Result<()> {
        let put_req = PutRequest {
            key: key.to_bytes(),
            value: vec![],
            prev_kv: false,
        };
        self.kv_backend.put(put_req).await?;
        Ok(())
    }

    pub async fn batch_put(&self, keys: Vec<TopicRegionKey<'_>>) -> Result<()> {
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

    pub fn build_create_txn(
        &self,
        table_id: TableId,
        region_wal_options: &HashMap<RegionNumber, String>,
    ) -> Result<Txn> {
        let region_wal_options = parse_region_wal_options(region_wal_options)?;
        let topic_region_mapping = self.get_topic_region_mapping(table_id, &region_wal_options);
        let topic_region_keys = topic_region_mapping
            .iter()
            .map(|(topic, region_id)| TopicRegionKey::new(*topic, region_id))
            .collect::<Vec<_>>();
        let operations = topic_region_keys
            .into_iter()
            .map(|key| TxnOp::Put(key.to_bytes(), vec![]))
            .collect::<Vec<_>>();
        Ok(Txn::new().and_then(operations))
    }

    /// Returns the list of region ids using specified topic.
    pub async fn regions(&self, topic: &str) -> Result<Vec<RegionId>> {
        let prefix = TopicRegionKey::range_topic_key(topic);
        let req = RangeRequest::new().with_prefix(prefix.as_bytes());
        let resp = self.kv_backend.range(req).await?;
        let region_ids = resp
            .kvs
            .iter()
            .map(topic_region_decoder)
            .collect::<Result<Vec<_>>>()?;
        Ok(region_ids.iter().map(|key| key.region_id).collect())
    }

    pub async fn delete(&self, key: TopicRegionKey<'_>) -> Result<()> {
        let raw_key = key.to_bytes();
        self.kv_backend.delete(&raw_key, false).await?;
        Ok(())
    }

    pub async fn batch_delete(&self, keys: Vec<TopicRegionKey<'_>>) -> Result<()> {
        let raw_keys = keys.iter().map(|key| key.to_bytes()).collect::<Vec<_>>();
        let req = BatchDeleteRequest {
            keys: raw_keys,
            prev_kv: false,
        };
        self.kv_backend.batch_delete(req).await?;
        Ok(())
    }

    /// Retrieves a mapping of [`RegionId`]s to their corresponding topics name
    /// based on the provided table ID and WAL options.
    ///
    /// # Returns
    /// A vector of tuples, where each tuple contains a [`RegionId`] and its corresponding topic name.
    pub fn get_topic_region_mapping<'a>(
        &self,
        table_id: TableId,
        region_wal_options: &'a HashMap<RegionNumber, WalOptions>,
    ) -> Vec<(RegionId, &'a str)> {
        region_wal_options
            .keys()
            .filter_map(
                |region_number| match region_wal_options.get(region_number) {
                    Some(WalOptions::Kafka(kafka)) => {
                        let region_id = RegionId::new(table_id, *region_number);
                        Some((region_id, kafka.topic.as_str()))
                    }
                    Some(WalOptions::RaftEngine) => None,
                    None => None,
                },
            )
            .collect::<Vec<_>>()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_wal::options::KafkaWalOptions;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;

    #[tokio::test]
    async fn test_topic_region_manager() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicRegionManager::new(kv_backend.clone());

        let topics = (0..16).map(|i| format!("topic_{}", i)).collect::<Vec<_>>();
        let keys = (0..64)
            .map(|i| TopicRegionKey::new(RegionId::from_u64(i), &topics[(i % 16) as usize]))
            .collect::<Vec<_>>();

        manager.batch_put(keys.clone()).await.unwrap();

        let mut key_values = manager.regions(&topics[0]).await.unwrap();
        let expected = keys
            .iter()
            .filter_map(|key| {
                if key.topic == topics[0] {
                    Some(key.region_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        key_values.sort_by_key(|id| id.as_u64());
        assert_eq!(key_values, expected);

        let key = TopicRegionKey::new(RegionId::from_u64(0), "topic_0");
        manager.delete(key.clone()).await.unwrap();
        let mut key_values = manager.regions(&topics[0]).await.unwrap();
        let expected = keys
            .iter()
            .filter_map(|key| {
                if key.topic == topics[0] && key.region_id != RegionId::from_u64(0) {
                    Some(key.region_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        key_values.sort_by_key(|id| id.as_u64());
        assert_eq!(key_values, expected);
    }

    #[test]
    fn test_topic_region_map() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicRegionManager::new(kv_backend.clone());

        let table_id = 1;
        let region_wal_options = (0..64)
            .map(|i| {
                let region_number = i;
                let wal_options = if i % 2 == 0 {
                    WalOptions::Kafka(KafkaWalOptions {
                        topic: format!("topic_{}", i),
                    })
                } else {
                    WalOptions::RaftEngine
                };
                (region_number, serde_json::to_string(&wal_options).unwrap())
            })
            .collect::<HashMap<_, _>>();

        let region_wal_options = parse_region_wal_options(&region_wal_options).unwrap();
        let mut topic_region_mapping =
            manager.get_topic_region_mapping(table_id, &region_wal_options);
        let mut expected = (0..64)
            .filter_map(|i| {
                if i % 2 == 0 {
                    Some((RegionId::new(table_id, i), format!("topic_{}", i)))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        topic_region_mapping.sort_by_key(|(region_id, _)| region_id.as_u64());
        let topic_region_map = topic_region_mapping
            .iter()
            .map(|(region_id, topic)| (*region_id, topic.to_string()))
            .collect::<Vec<_>>();
        expected.sort_by_key(|(region_id, _)| region_id.as_u64());
        assert_eq!(topic_region_map, expected);
    }
}
