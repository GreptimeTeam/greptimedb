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

use common_telemetry::debug;

use crate::error::Result;
use crate::key::datanode_table::DatanodeTableValue;
use crate::key::{MetadataValue, DATANODE_TABLE_KEY_PREFIX};
use crate::kv_backend::txn::{Compare, CompareOp, Txn, TxnOp};
use crate::utils::transformer::{MetadataAction, MetadataTransformer};
use crate::wal_options_allocator::{allocate_region_wal_options, WalOptionsAllocatorRef};

/// A processor responsible for reallocating WAL options for regions.
pub struct RellocateRegionWalOptions {
    // TODO(weny): refactor the f***king [`WalOptionsAllocatorRef`] ASAP.
    wal_options_allocator: WalOptionsAllocatorRef,
}

impl RellocateRegionWalOptions {
    /// Creates the [`RellocateRegionWalOptions`].
    pub fn new(wal_options_allocator: WalOptionsAllocatorRef) -> Self {
        Self {
            wal_options_allocator,
        }
    }
}

#[async_trait::async_trait]
impl MetadataTransformer for RellocateRegionWalOptions {
    fn accept(&self, key: &[u8]) -> bool {
        key.starts_with(DATANODE_TABLE_KEY_PREFIX.as_bytes())
    }

    async fn handle(&self, key: Vec<u8>, value: Vec<u8>) -> Result<MetadataAction> {
        let key_str = String::from_utf8_lossy(&key);
        let mut datanode_table_value = DatanodeTableValue::try_from_raw_value(&value)?;
        let regions = datanode_table_value.regions.clone();
        let new_region_wal_options =
            allocate_region_wal_options(regions, &self.wal_options_allocator)?;
        datanode_table_value.region_info.region_wal_options = new_region_wal_options;

        debug!("Updating key: {key_str}");
        let txn = Txn::new()
            .when(vec![Compare::with_value(
                key.clone(),
                CompareOp::Equal,
                value,
            )])
            .and_then(vec![TxnOp::Put(
                key,
                datanode_table_value.try_as_raw_value()?,
            )]);
        Ok(MetadataAction::Transform(txn))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use common_wal::options::WalOptions;
    use store_api::storage::RegionNumber;

    use super::*;
    use crate::key::datanode_table::{DatanodeTableKey, RegionInfo};
    use crate::key::MetadataKey;
    use crate::wal_options_allocator::WalOptionsAllocator;

    #[test]
    fn test_accept() {
        let processor = RellocateRegionWalOptions::new(Arc::new(WalOptionsAllocator::default()));
        assert!(processor.accept(format!("{}/1024", DATANODE_TABLE_KEY_PREFIX).as_bytes()));
        assert!(!processor.accept("hello".as_bytes()));
    }

    async fn test_handle(
        processor: &RellocateRegionWalOptions,
        key: Vec<u8>,
        value: Vec<u8>,
        expected: HashMap<RegionNumber, String>,
    ) {
        let txn = processor
            .handle(key.clone(), value.clone())
            .await
            .unwrap()
            .into_transform();
        assert_eq!(
            txn.req.compare,
            vec![Compare::with_value(key.clone(), CompareOp::Equal, value)]
        );
        assert_eq!(txn.req.success.len(), 1);
        let TxnOp::Put(txn_key, txn_value) = txn.req.success[0].clone() else {
            unreachable!()
        };
        assert_eq!(txn_key, key);
        let value = DatanodeTableValue::try_from_raw_value(&txn_value).unwrap();
        assert_eq!(value.region_info.region_wal_options, expected);
    }

    #[tokio::test]
    async fn test_handle_basic() {
        let processor = RellocateRegionWalOptions::new(Arc::new(WalOptionsAllocator::default()));
        assert!(processor.accept(format!("{}/1024", DATANODE_TABLE_KEY_PREFIX).as_bytes()));
        assert!(!processor.accept("hello".as_bytes()));
        let key = DatanodeTableKey::new(1, 1024);
        let value = DatanodeTableValue::new(
            1024,
            vec![1],
            RegionInfo {
                engine: Default::default(),
                region_storage_path: Default::default(),
                region_options: Default::default(),
                region_wal_options: HashMap::from([(1, "hello".to_string())]),
            },
        );
        let key = key.to_bytes();
        let value = value.try_as_raw_value().unwrap();
        test_handle(
            &processor,
            key,
            value,
            HashMap::from([(1, serde_json::to_string(&WalOptions::RaftEngine).unwrap())]),
        )
        .await;

        let key = DatanodeTableKey::new(1, 1024);
        let value = DatanodeTableValue::new(
            1024,
            vec![1],
            RegionInfo {
                engine: Default::default(),
                region_storage_path: Default::default(),
                region_options: Default::default(),
                region_wal_options: HashMap::default(),
            },
        );
        let key = key.to_bytes();
        let value = value.try_as_raw_value().unwrap();
        test_handle(
            &processor,
            key,
            value,
            HashMap::from([(1, serde_json::to_string(&WalOptions::RaftEngine).unwrap())]),
        )
        .await;
    }
}
