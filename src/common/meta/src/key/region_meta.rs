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

use futures::stream::BoxStream;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use store_api::storage::{RegionId, RegionNumber};
use table::metadata::TableId;

use crate::error::Result;
use crate::key::{TableMetaKey, REGION_META_KEY_PREFIX};
use crate::kv_backend::txn::{Txn, TxnOp};
use crate::kv_backend::KvBackendRef;
use crate::range_stream::{PaginationStream, DEFAULT_PAGE_SIZE};
use crate::rpc::store::RangeRequest;
use crate::rpc::KeyValue;

// TODO(niebayes): to be removed when `Kafka Remote Wal` is merged.
pub type KafkaTopic = String;

/// A region's unique metadata.
#[derive(Clone)]
pub struct RegionMeta {
    region_id: RegionId,
    topic: Option<KafkaTopic>,
}

impl RegionMeta {
    fn new(region_id: RegionId, topic: Option<KafkaTopic>) -> Self {
        Self { region_id, topic }
    }
}

// The table id is included to support efficiently scan metadata of all regions of a table.
pub struct RegionMetaKey {
    table_id: TableId,
    region_number: RegionNumber,
}

impl RegionMetaKey {
    pub fn new(table_id: TableId, region_number: RegionNumber) -> Self {
        Self {
            table_id,
            region_number,
        }
    }

    fn range_start_key(table_id: TableId) -> Vec<u8> {
        format!("{}/{}/", REGION_META_KEY_PREFIX, table_id).into_bytes()
    }
}

impl TableMetaKey for RegionMetaKey {
    fn as_raw_key(&self) -> Vec<u8> {
        format!(
            "{}/{}/{}",
            REGION_META_KEY_PREFIX, self.table_id, self.region_number
        )
        .into_bytes()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RegionMetaValue {
    topic: Option<KafkaTopic>,
}

impl RegionMetaValue {
    pub fn new(topic: Option<KafkaTopic>) -> Self {
        Self { topic }
    }
}

pub struct RegionMetaManager {
    kv_backend: KvBackendRef,
}

impl RegionMetaManager {
    pub fn new(kv_backend: KvBackendRef) -> Self {
        Self { kv_backend }
    }

    /// Gets the region meta value associated with the given region meta key.
    /// Returns None if the key does not exist.
    pub async fn get(&self, key: &RegionMetaKey) -> Result<Option<RegionMetaValue>> {
        self.kv_backend
            .get(&key.as_raw_key())
            .await?
            .map(|kv| RegionMetaValue::try_from_raw_value(&kv.value))
            .transpose()
    }

    /// Gets metadata of all regions in a table with the given table id.
    /// Returns a stream of region meta values.
    pub fn region_metas(&self, table_id: TableId) -> BoxStream<'static, Result<RegionMetaValue>> {
        let start_key = RegionMetaKey::range_start_key(table_id);
        let range_request = RangeRequest::new().with_prefix(start_key);

        fn region_meta_value_decoder(kv: KeyValue) -> Result<((), RegionMetaValue)> {
            let value = RegionMetaValue::try_from_raw_value(&kv.value)?;
            Ok(((), value))
        }

        let raw_stream = PaginationStream::new(
            self.kv_backend.clone(),
            range_request,
            DEFAULT_PAGE_SIZE,
            Arc::new(region_meta_value_decoder),
        );
        let stream = raw_stream
            .map(|decode_result| decode_result.map(|(_, region_meta_value)| region_meta_value));

        Box::pin(stream)
    }

    /// Builds a txn to create a sequence of region metadata.
    pub(crate) fn build_create_txn(&self, region_metas: Vec<RegionMeta>) -> Result<Txn> {
        let txns = region_metas
            .into_iter()
            .map(|region_meta| {
                let table_id = region_meta.region_id.table_id();
                let region_number = region_meta.region_id.region_number();
                let topic = region_meta.topic;

                let key = RegionMetaKey::new(table_id, region_number).as_raw_key();
                let value = RegionMetaValue::new(topic).try_as_raw_value()?;

                Ok(TxnOp::Put(key, value))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Txn::new().and_then(txns))
    }

    /// Builds a txn to delete region metadata of the regions identified by the given region ids.
    pub(crate) fn build_delete_txn(&self, region_ids: Vec<RegionId>) -> Result<Txn> {
        let txns = region_ids
            .into_iter()
            .map(|region_id| {
                let table_id = region_id.table_id();
                let region_number = region_id.region_number();

                let key = RegionMetaKey::new(table_id, region_number).as_raw_key();

                TxnOp::Delete(key)
            })
            .collect::<Vec<_>>();

        Ok(Txn::new().and_then(txns))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::kv_backend::txn::{TxnOpResponse, TxnService};

    #[test]
    fn test_key_serde() {
        let table_id = 22;
        let region_number = 33;
        let key = RegionMetaKey::new(table_id, region_number);
        let got = key.as_raw_key();
        let expected = format!("{}/{}/{}", REGION_META_KEY_PREFIX, 22, 33).into_bytes();
        assert_eq!(got, expected);
    }

    #[test]
    fn test_value_serde() {
        let value = RegionMetaValue { topic: None };
        let raw_value = value.try_as_raw_value().unwrap();
        let parsed_value = RegionMetaValue::try_from_raw_value(&raw_value).unwrap();
        assert_eq!(value, parsed_value);

        let value = RegionMetaValue {
            topic: Some("test_topic".to_string()),
        };
        let raw_value = value.try_as_raw_value().unwrap();
        let parsed_value = RegionMetaValue::try_from_raw_value(&raw_value).unwrap();
        assert_eq!(value, parsed_value);
    }

    #[tokio::test]
    async fn test_create_then_get() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let region_meta_manager = RegionMetaManager::new(kv_backend.clone());

        let region_id = RegionId::from_u64(1);
        let topic = Some("test_topic".to_string());
        let region_metas = vec![RegionMeta::new(region_id, topic.clone())];
        let create_txn = region_meta_manager.build_create_txn(region_metas).unwrap();
        let create_response = kv_backend.txn(create_txn).await.unwrap();
        assert!(create_response.succeeded);

        let region_meta_key = RegionMetaKey::new(region_id.table_id(), region_id.region_number());
        let region_meta_value = region_meta_manager.get(&region_meta_key).await.unwrap();
        assert_eq!(region_meta_value, Some(RegionMetaValue { topic }));
    }

    #[tokio::test]
    async fn test_create_then_range() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let region_meta_manager = RegionMetaManager::new(kv_backend.clone());

        let table_id = 42;
        let region_metas = vec![
            RegionMeta::new(RegionId::new(table_id, 1), Some("test_topic_1".to_string())),
            RegionMeta::new(RegionId::new(table_id, 2), Some("test_topic_2".to_string())),
            RegionMeta::new(RegionId::new(table_id, 3), Some("test_topic_3".to_string())),
        ];
        let create_txn = region_meta_manager
            .build_create_txn(region_metas.clone())
            .unwrap();
        let create_response = kv_backend.txn(create_txn).await.unwrap();
        assert!(create_response.succeeded);

        let region_meta_stream = region_meta_manager.region_metas(table_id);
        // FIXME(niebayes): Simplify the codes.
        let got = region_meta_stream
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|res| res.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(format!("{:?}", got).into_bytes(), b"[RegionMetaValue { topic: Some(\"test_topic_1\") }, RegionMetaValue { topic: Some(\"test_topic_2\") }, RegionMetaValue { topic: Some(\"test_topic_3\") }]");

        let expected = region_metas
            .into_iter()
            .map(|region_meta| RegionMetaValue::new(region_meta.topic))
            .collect::<Vec<_>>();
        assert_eq!(expected, got);
    }

    #[tokio::test]
    async fn test_create_then_delete() {
        let kv_backend = Arc::new(MemoryKvBackend::default());
        let region_meta_manager = RegionMetaManager::new(kv_backend.clone());

        let table_id = 42;
        let region_metas = vec![
            RegionMeta::new(RegionId::new(table_id, 1), Some("test_topic_1".to_string())),
            RegionMeta::new(RegionId::new(table_id, 2), Some("test_topic_2".to_string())),
            RegionMeta::new(RegionId::new(table_id, 3), Some("test_topic_3".to_string())),
        ];
        let create_txn = region_meta_manager
            .build_create_txn(region_metas.clone())
            .unwrap();
        let create_response = kv_backend.txn(create_txn).await.unwrap();
        assert!(create_response.succeeded);

        let region_ids = region_metas
            .into_iter()
            .map(|region_meta| region_meta.region_id)
            .collect::<Vec<_>>();
        let delete_txn = region_meta_manager.build_delete_txn(region_ids).unwrap();
        let delete_response = kv_backend.txn(delete_txn).await.unwrap();
        assert!(delete_response.succeeded);

        let num_deleted = delete_response
            .responses
            .into_iter()
            .map(|response| {
                let TxnOpResponse::ResponseDelete(inner) = response else {
                    panic!();
                };
                inner.deleted
            })
            .sum::<i64>();
        assert_eq!(num_deleted, 3);
    }
}
