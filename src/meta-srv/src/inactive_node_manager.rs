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

use common_meta::rpc::store::{BatchGetRequest, PutRequest};
use store_api::storage::RegionNumber;

use crate::error::Result;
use crate::keys::InactiveNodeKey;
use crate::service::store::kv::ResettableKvStoreRef;

pub struct InactiveNodeManager<'a> {
    store: &'a ResettableKvStoreRef,
}

impl<'a> InactiveNodeManager<'a> {
    pub fn new(store: &'a ResettableKvStoreRef) -> Self {
        Self { store }
    }

    pub async fn register_inactive_region(
        &self,
        cluster_id: u64,
        node_id: u64,
        table_id: u32,
        region_number: RegionNumber,
    ) -> Result<()> {
        let key = InactiveNodeKey {
            cluster_id,
            node_id,
            table_id,
            region_number,
        };
        let req = PutRequest {
            key: key.into(),
            value: vec![],
            prev_kv: false,
        };
        self.store.put(req).await?;
        Ok(())
    }

    pub async fn deregister_inactive_region(
        &self,
        cluster_id: u64,
        node_id: u64,
        table_id: u32,
        region_number: RegionNumber,
    ) -> Result<()> {
        let key: Vec<u8> = InactiveNodeKey {
            cluster_id,
            node_id,
            table_id,
            region_number,
        }
        .into();
        self.store.delete(&key, false).await?;
        Ok(())
    }

    /// The input is a list of regions from a table on a specific node. If one or more
    /// regions have been set to inactive state by metasrv, the corresponding regions
    /// will be removed, then return the remaining regions.
    pub async fn retain_active_regions(
        &self,
        cluster_id: u64,
        node_id: u64,
        table_id: u32,
        region_numbers: &mut Vec<RegionNumber>,
    ) -> Result<()> {
        let key_region_numbers: Vec<(Vec<u8>, RegionNumber)> = region_numbers
            .iter()
            .map(|region_number| {
                (
                    InactiveNodeKey {
                        cluster_id,
                        node_id,
                        table_id,
                        region_number: *region_number,
                    }
                    .into(),
                    *region_number,
                )
            })
            .collect();
        let keys = key_region_numbers
            .iter()
            .map(|(key, _)| key.clone())
            .collect();
        let resp = self.store.batch_get(BatchGetRequest { keys }).await?;
        let kvs = resp.kvs;
        if kvs.is_empty() {
            return Ok(());
        }

        let inactive_keys = kvs.into_iter().map(|kv| kv.key).collect::<HashSet<_>>();
        let inactive_region_numbers = key_region_numbers
            .into_iter()
            .filter(|(key, _)| inactive_keys.contains(key))
            .map(|(_, region_number)| region_number)
            .collect::<HashSet<_>>();
        region_numbers.retain(|region_number| !inactive_region_numbers.contains(region_number));

        Ok(())
    }
}
