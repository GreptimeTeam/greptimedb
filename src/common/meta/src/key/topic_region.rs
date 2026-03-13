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
use crate::key::{MetadataKey, MetadataValue, TOPIC_REGION_PATTERN, TOPIC_REGION_PREFIX};
use crate::kv_backend::KvBackendRef;
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::rpc::KeyValue;
use crate::rpc::store::{
    BatchDeleteRequest, BatchGetRequest, BatchPutRequest, PutRequest, RangeRequest,
};

// The TopicRegionKey is a key for the topic-region mapping in the kvbackend.
// The layout of the key is `__topic_region/{topic_name}/{region_id}`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TopicRegionKey<'a> {
    pub region_id: RegionId,
    pub topic: &'a str,
}

/// Represents additional information for a region when using a shared WAL.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct TopicRegionValue {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<ReplayCheckpoint>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReplayCheckpoint {
    #[serde(default)]
    pub entry_id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_entry_id: Option<u64>,
}

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

impl ReplayCheckpoint {
    /// Creates a new [`ReplayCheckpoint`] with the given entry id and metadata entry id.
    pub fn new(entry_id: u64, metadata_entry_id: Option<u64>) -> Self {
        Self {
            entry_id,
            metadata_entry_id,
        }
    }
}

impl TopicRegionValue {
    /// Creates a new [`TopicRegionValue`] with the given checkpoint.
    pub fn new(checkpoint: Option<ReplayCheckpoint>) -> Self {
        Self { checkpoint }
    }

    /// Returns the minimum entry id of the region.
    ///
    /// If the metadata entry id is not set, it returns the entry id.
    pub fn min_entry_id(&self) -> Option<u64> {
        match self.checkpoint {
            Some(ReplayCheckpoint {
                entry_id,
                metadata_entry_id,
            }) => match metadata_entry_id {
                Some(metadata_entry_id) => Some(entry_id.min(metadata_entry_id)),
                None => Some(entry_id),
            },
            None => None,
        }
    }
}

fn topic_region_decoder(value: &KeyValue) -> Result<(TopicRegionKey<'_>, TopicRegionValue)> {
    let key = TopicRegionKey::from_bytes(&value.key)?;
    let value = if value.value.is_empty() {
        TopicRegionValue::default()
    } else {
        TopicRegionValue::try_from_raw_value(&value.value)?
    };
    Ok((key, value))
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

    pub async fn batch_get(
        &self,
        keys: Vec<TopicRegionKey<'_>>,
    ) -> Result<HashMap<RegionId, TopicRegionValue>> {
        let raw_keys = keys.iter().map(|key| key.to_bytes()).collect::<Vec<_>>();
        let req = BatchGetRequest { keys: raw_keys };
        let resp = self.kv_backend.batch_get(req).await?;

        let v = resp
            .kvs
            .into_iter()
            .map(|kv| topic_region_decoder(&kv).map(|(key, value)| (key.region_id, value)))
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(v)
    }

    pub async fn get(&self, key: TopicRegionKey<'_>) -> Result<Option<TopicRegionValue>> {
        let key_bytes = key.to_bytes();
        let resp = self.kv_backend.get(&key_bytes).await?;
        let value = resp
            .map(|kv| topic_region_decoder(&kv).map(|(_, value)| value))
            .transpose()?;

        Ok(value)
    }

    pub async fn batch_put(
        &self,
        keys: &[(TopicRegionKey<'_>, Option<TopicRegionValue>)],
    ) -> Result<()> {
        let req = BatchPutRequest {
            kvs: keys
                .iter()
                .map(|(key, value)| {
                    let value = value
                        .map(|v| v.try_as_raw_value())
                        .transpose()?
                        .unwrap_or_default();

                    Ok(KeyValue {
                        key: key.to_bytes(),
                        value,
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            prev_kv: false,
        };
        self.kv_backend.batch_put(req).await?;
        Ok(())
    }

    /// Build a create topic region mapping transaction. It only executes while the primary keys comparing successes.
    pub fn build_create_txn(
        &self,
        table_id: TableId,
        region_wal_options: &HashMap<RegionNumber, String>,
    ) -> Result<Txn> {
        let region_wal_options = parse_region_wal_options(region_wal_options)?;
        let topic_region_mapping = self.get_topic_region_mapping(table_id, &region_wal_options);
        let topic_region_keys = topic_region_mapping
            .iter()
            .map(|(region_id, topic)| TopicRegionKey::new(*region_id, topic))
            .collect::<Vec<_>>();
        let operations = topic_region_keys
            .into_iter()
            .map(|key| TxnOp::Put(key.to_bytes(), vec![]))
            .collect::<Vec<_>>();
        Ok(Txn::new().and_then(operations))
    }

    /// Build a update topic region mapping transaction.
    pub fn build_update_txn(
        &self,
        table_id: TableId,
        old_region_wal_options: &HashMap<RegionNumber, String>,
        new_region_wal_options: &HashMap<RegionNumber, String>,
    ) -> Result<Txn> {
        let old_wal_options_parsed = parse_region_wal_options(old_region_wal_options)?;
        let new_wal_options_parsed = parse_region_wal_options(new_region_wal_options)?;
        let old_mapping = self.get_topic_region_mapping(table_id, &old_wal_options_parsed);
        let new_mapping = self.get_topic_region_mapping(table_id, &new_wal_options_parsed);

        // Convert to HashMap for easier lookup: RegionId -> Topic
        let old_map: HashMap<RegionId, &str> = old_mapping.into_iter().collect();
        let new_map: HashMap<RegionId, &str> = new_mapping.into_iter().collect();
        let mut ops = Vec::new();

        // Check for deletes (in old but not in new, or topic changed)
        for (region_id, old_topic) in &old_map {
            match new_map.get(region_id) {
                Some(new_topic) if *new_topic == *old_topic => {
                    // Same topic, do nothing (preserve checkpoint)
                }
                _ => {
                    // Removed or topic changed -> Delete old
                    let key = TopicRegionKey::new(*region_id, old_topic);
                    ops.push(TxnOp::Delete(key.to_bytes()));
                }
            }
        }

        // Check for adds (in new but not in old, or topic changed)
        for (region_id, new_topic) in &new_map {
            match old_map.get(region_id) {
                Some(old_topic) if *old_topic == *new_topic => {
                    // Same topic, already handled (do nothing)
                }
                _ => {
                    // New or topic changed -> Put new
                    let key = TopicRegionKey::new(*region_id, new_topic);
                    // Initialize with empty value (default TopicRegionValue)
                    ops.push(TxnOp::Put(key.to_bytes(), vec![]));
                }
            }
        }

        Ok(Txn::new().and_then(ops))
    }

    /// Returns the map of [`RegionId`] to their corresponding topic [`TopicRegionValue`].
    pub async fn regions(&self, topic: &str) -> Result<HashMap<RegionId, TopicRegionValue>> {
        let prefix = TopicRegionKey::range_topic_key(topic);
        let req = RangeRequest::new().with_prefix(prefix.as_bytes());
        let resp = self.kv_backend.range(req).await?;
        let region_ids = resp
            .kvs
            .iter()
            .map(topic_region_decoder)
            .collect::<Result<Vec<_>>>()?;
        Ok(region_ids
            .into_iter()
            .map(|(key, value)| (key.region_id, value))
            .collect())
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
                    Some(WalOptions::Noop) => None,
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
            .map(|i| {
                (
                    TopicRegionKey::new(RegionId::from_u64(i), &topics[(i % 16) as usize]),
                    None,
                )
            })
            .collect::<Vec<_>>();

        manager.batch_put(&keys).await.unwrap();
        let mut key_values = manager
            .regions(&topics[0])
            .await
            .unwrap()
            .into_keys()
            .collect::<Vec<_>>();
        let expected = keys
            .iter()
            .filter_map(|(key, _)| {
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
        let mut key_values = manager
            .regions(&topics[0])
            .await
            .unwrap()
            .into_keys()
            .collect::<Vec<_>>();
        let expected = keys
            .iter()
            .filter_map(|(key, _)| {
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

    #[test]
    fn test_topic_region_key_is_match() {
        let key = "__topic_region/6f153a64-7fac-4cf6-8b0b-a7967dd73879_2/4410931412992";
        let topic_region_key = TopicRegionKey::try_from(key).unwrap();
        assert_eq!(
            topic_region_key.topic,
            "6f153a64-7fac-4cf6-8b0b-a7967dd73879_2"
        );
        assert_eq!(
            topic_region_key.region_id,
            RegionId::from_u64(4410931412992)
        );
    }

    #[test]
    fn test_build_create_txn() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicRegionManager::new(kv_backend.clone());
        let table_id = 1;
        let region_wal_options = vec![
            (
                0,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_0".to_string(),
                }),
            ),
            (
                1,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_1".to_string(),
                }),
            ),
            (2, WalOptions::RaftEngine), // Should be ignored
        ]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();

        let txn = manager
            .build_create_txn(table_id, &region_wal_options)
            .unwrap();

        // Verify the transaction contains correct operations
        // Should create mappings for region 0 and 1, but not region 2 (RaftEngine)
        let ops = txn.req().success.clone();
        assert_eq!(ops.len(), 2);

        let keys: Vec<_> = ops
            .iter()
            .filter_map(|op| {
                if let TxnOp::Put(key, _) = op {
                    TopicRegionKey::from_bytes(key).ok()
                } else {
                    None
                }
            })
            .collect();

        assert_eq!(keys.len(), 2);
        let region_ids: Vec<_> = keys.iter().map(|k| k.region_id).collect();
        assert!(region_ids.contains(&RegionId::new(table_id, 0)));
        assert!(region_ids.contains(&RegionId::new(table_id, 1)));
        assert!(!region_ids.contains(&RegionId::new(table_id, 2)));

        // Verify topics are correct
        for key in keys {
            match key.region_id.region_number() {
                0 => assert_eq!(key.topic, "topic_0"),
                1 => assert_eq!(key.topic, "topic_1"),
                _ => panic!("Unexpected region number"),
            }
        }
    }

    #[test]
    fn test_build_update_txn_add_new_region() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicRegionManager::new(kv_backend.clone());
        let table_id = 1;
        let old_region_wal_options = vec![(
            0,
            WalOptions::Kafka(KafkaWalOptions {
                topic: "topic_0".to_string(),
            }),
        )]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let new_region_wal_options = vec![
            (
                0,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_0".to_string(),
                }),
            ),
            (
                1,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_1".to_string(),
                }),
            ),
        ]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let txn = manager
            .build_update_txn(table_id, &old_region_wal_options, &new_region_wal_options)
            .unwrap();
        let ops = txn.req().success.clone();
        // Should only have Put for new region 1 (region 0 unchanged)
        assert_eq!(ops.len(), 1);
        if let TxnOp::Put(key, _) = &ops[0] {
            let topic_key = TopicRegionKey::from_bytes(key).unwrap();
            assert_eq!(topic_key.region_id, RegionId::new(table_id, 1));
            assert_eq!(topic_key.topic, "topic_1");
        } else {
            panic!("Expected Put operation");
        }
    }

    #[test]
    fn test_build_update_txn_remove_region() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicRegionManager::new(kv_backend.clone());
        let table_id = 1;
        let old_region_wal_options = vec![
            (
                0,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_0".to_string(),
                }),
            ),
            (
                1,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_1".to_string(),
                }),
            ),
        ]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let new_region_wal_options = vec![(
            0,
            WalOptions::Kafka(KafkaWalOptions {
                topic: "topic_0".to_string(),
            }),
        )]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let txn = manager
            .build_update_txn(table_id, &old_region_wal_options, &new_region_wal_options)
            .unwrap();
        let ops = txn.req().success.clone();
        // Should only have Delete for removed region 1 (region 0 unchanged)
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            TxnOp::Delete(key) => {
                let topic_key = TopicRegionKey::from_bytes(key).unwrap();
                assert_eq!(topic_key.region_id, RegionId::new(table_id, 1));
                assert_eq!(topic_key.topic, "topic_1");
            }
            TxnOp::Put(_, _) | TxnOp::Get(_) => {
                panic!("Expected Delete operation");
            }
        }
    }

    #[test]
    fn test_build_update_txn_change_topic() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicRegionManager::new(kv_backend.clone());
        let table_id = 1;
        let old_region_wal_options = vec![(
            0,
            WalOptions::Kafka(KafkaWalOptions {
                topic: "topic_0".to_string(),
            }),
        )]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let new_region_wal_options = vec![(
            0,
            WalOptions::Kafka(KafkaWalOptions {
                topic: "topic_0_new".to_string(),
            }),
        )]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let txn = manager
            .build_update_txn(table_id, &old_region_wal_options, &new_region_wal_options)
            .unwrap();
        let ops = txn.req().success.clone();
        // Should have Delete for old topic and Put for new topic
        assert_eq!(ops.len(), 2);

        let mut delete_found = false;
        let mut put_found = false;
        for op in ops {
            match op {
                TxnOp::Delete(key) => {
                    let topic_key = TopicRegionKey::from_bytes(&key).unwrap();
                    assert_eq!(topic_key.region_id, RegionId::new(table_id, 0));
                    assert_eq!(topic_key.topic, "topic_0");
                    delete_found = true;
                }
                TxnOp::Put(key, _) => {
                    let topic_key = TopicRegionKey::from_bytes(&key).unwrap();
                    assert_eq!(topic_key.region_id, RegionId::new(table_id, 0));
                    assert_eq!(topic_key.topic, "topic_0_new");
                    put_found = true;
                }
                TxnOp::Get(_) => {
                    // Get operations shouldn't appear in this context
                    panic!("Unexpected Get operation in update transaction");
                }
            }
        }
        assert!(delete_found, "Expected Delete operation for old topic");
        assert!(put_found, "Expected Put operation for new topic");
    }

    #[test]
    fn test_build_update_txn_no_change() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicRegionManager::new(kv_backend.clone());
        let table_id = 1;
        let region_wal_options = vec![
            (
                0,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_0".to_string(),
                }),
            ),
            (
                1,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_1".to_string(),
                }),
            ),
        ]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let txn = manager
            .build_update_txn(table_id, &region_wal_options, &region_wal_options)
            .unwrap();
        // Should have no operations when nothing changes (preserves checkpoint)
        let ops = txn.req().success.clone();
        assert_eq!(ops.len(), 0);
    }

    #[test]
    fn test_build_update_txn_mixed_scenarios() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicRegionManager::new(kv_backend.clone());
        let table_id = 1;
        let old_region_wal_options = vec![
            (
                0,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_0".to_string(),
                }),
            ),
            (
                1,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_1".to_string(),
                }),
            ),
            (
                2,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_2".to_string(),
                }),
            ),
        ]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let new_region_wal_options = vec![
            (
                0,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_0".to_string(), // Unchanged
                }),
            ),
            (
                1,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_1_new".to_string(), // Topic changed
                }),
            ),
            // Region 2 removed
            (
                3,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_3".to_string(), // New region
                }),
            ),
        ]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let txn = manager
            .build_update_txn(table_id, &old_region_wal_options, &new_region_wal_options)
            .unwrap();

        let ops = txn.req().success.clone();
        // Should have:
        // - Delete for region 2 (removed)
        // - Delete for region 1 old topic (topic changed)
        // - Put for region 1 new topic (topic changed)
        // - Put for region 3 (new)
        // Region 0 unchanged, so no operation
        assert_eq!(ops.len(), 4);

        let mut delete_ops = 0;
        let mut put_ops = 0;
        let mut delete_region_2 = false;
        let mut delete_region_1_old = false;
        let mut put_region_1_new = false;
        let mut put_region_3 = false;

        for op in ops {
            match op {
                TxnOp::Delete(key) => {
                    delete_ops += 1;
                    let topic_key = TopicRegionKey::from_bytes(&key).unwrap();
                    match topic_key.region_id.region_number() {
                        1 => {
                            assert_eq!(topic_key.topic, "topic_1");
                            delete_region_1_old = true;
                        }
                        2 => {
                            assert_eq!(topic_key.topic, "topic_2");
                            delete_region_2 = true;
                        }
                        _ => panic!("Unexpected delete operation for region"),
                    }
                }
                TxnOp::Put(key, _) => {
                    put_ops += 1;
                    let topic_key: TopicRegionKey<'_> = TopicRegionKey::from_bytes(&key).unwrap();
                    match topic_key.region_id.region_number() {
                        1 => {
                            assert_eq!(topic_key.topic, "topic_1_new");
                            put_region_1_new = true;
                        }
                        3 => {
                            assert_eq!(topic_key.topic, "topic_3");
                            put_region_3 = true;
                        }
                        _ => panic!("Unexpected put operation for region"),
                    }
                }
                TxnOp::Get(_) => {
                    panic!("Unexpected Get operation in update transaction");
                }
            }
        }

        assert_eq!(delete_ops, 2);
        assert_eq!(put_ops, 2);
        assert!(delete_region_2, "Expected delete for removed region 2");
        assert!(
            delete_region_1_old,
            "Expected delete for region 1 old topic"
        );
        assert!(put_region_1_new, "Expected put for region 1 new topic");
        assert!(put_region_3, "Expected put for new region 3");
    }

    #[test]
    fn test_build_update_txn_with_raft_engine() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let manager = TopicRegionManager::new(kv_backend.clone());
        let table_id = 1;
        let old_region_wal_options = vec![
            (
                0,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_0".to_string(),
                }),
            ),
            (1, WalOptions::RaftEngine), // Should be ignored
        ]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let new_region_wal_options = vec![
            (
                0,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_0".to_string(),
                }),
            ),
            (
                1,
                WalOptions::Kafka(KafkaWalOptions {
                    topic: "topic_1".to_string(), // Changed from RaftEngine to Kafka
                }),
            ),
        ]
        .into_iter()
        .map(|(num, opts)| (num, serde_json::to_string(&opts).unwrap()))
        .collect::<HashMap<_, _>>();
        let txn = manager
            .build_update_txn(table_id, &old_region_wal_options, &new_region_wal_options)
            .unwrap();
        let ops = txn.req().success.clone();
        // Should only have Put for region 1 (new Kafka topic)
        // Region 0 unchanged, so no operation
        // Region 1 was RaftEngine before (not tracked), so only Put needed
        assert_eq!(ops.len(), 1);
        match &ops[0] {
            TxnOp::Put(key, _) => {
                let topic_key = TopicRegionKey::from_bytes(key).unwrap();
                assert_eq!(topic_key.region_id, RegionId::new(table_id, 1));
                assert_eq!(topic_key.topic, "topic_1");
            }
            TxnOp::Delete(_) | TxnOp::Get(_) => {
                panic!("Expected Put operation for new Kafka region");
            }
        }
    }
}
