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

use common_meta::rpc::store::{BatchGetRequest, DeleteRangeRequest, PutRequest, RangeRequest};
use common_meta::RegionIdent;

use crate::error::Result;
use crate::keys::InactiveRegionKey;
use crate::metrics::METRIC_META_INACTIVE_REGIONS;
use crate::service::store::kv::ResettableKvStoreRef;

pub struct InactiveRegionManager<'a> {
    store: &'a ResettableKvStoreRef,
}

impl<'a> InactiveRegionManager<'a> {
    pub fn new(store: &'a ResettableKvStoreRef) -> Self {
        Self { store }
    }

    pub async fn register_inactive_region(&self, region_ident: &RegionIdent) -> Result<()> {
        let region_id = region_ident.get_region_id().as_u64();
        let key = InactiveRegionKey {
            cluster_id: region_ident.cluster_id,
            node_id: region_ident.datanode_id,
            region_id,
        };
        let req = PutRequest {
            key: key.into(),
            value: vec![],
            prev_kv: false,
        };
        self.store.put(req).await?;

        METRIC_META_INACTIVE_REGIONS.inc();

        Ok(())
    }

    pub async fn deregister_inactive_region(&self, region_ident: &RegionIdent) -> Result<()> {
        let region_id = region_ident.get_region_id().as_u64();
        let key: Vec<u8> = InactiveRegionKey {
            cluster_id: region_ident.cluster_id,
            node_id: region_ident.datanode_id,
            region_id,
        }
        .into();
        self.store.delete(&key, false).await?;

        METRIC_META_INACTIVE_REGIONS.dec();

        Ok(())
    }

    /// The input is a list of regions on a specific node. If one or more regions have been
    /// set to inactive state by metasrv, the corresponding regions will be removed(update the
    /// `region_ids`), then returns the removed regions.
    pub async fn retain_active_regions(
        &self,
        cluster_id: u64,
        node_id: u64,
        region_ids: &mut Vec<u64>,
    ) -> Result<HashSet<u64>> {
        let key_region_ids = region_ids
            .iter()
            .map(|region_id| {
                (
                    InactiveRegionKey {
                        cluster_id,
                        node_id,
                        region_id: *region_id,
                    }
                    .into(),
                    *region_id,
                )
            })
            .collect::<Vec<(Vec<u8>, _)>>();
        let keys = key_region_ids.iter().map(|(key, _)| key.clone()).collect();
        let resp = self.store.batch_get(BatchGetRequest { keys }).await?;
        let kvs = resp.kvs;
        if kvs.is_empty() {
            return Ok(HashSet::new());
        }

        let inactive_keys = kvs.into_iter().map(|kv| kv.key).collect::<HashSet<_>>();
        let (active_region_ids, inactive_region_ids): (Vec<Option<u64>>, Vec<Option<u64>>) =
            key_region_ids
                .into_iter()
                .map(|(key, region_id)| {
                    let is_active = !inactive_keys.contains(&key);
                    if is_active {
                        (Some(region_id), None)
                    } else {
                        (None, Some(region_id))
                    }
                })
                .unzip();
        *region_ids = active_region_ids.into_iter().flatten().collect();

        Ok(inactive_region_ids.into_iter().flatten().collect())
    }

    /// Scan all inactive regions in the cluster.
    ///
    /// When will these data appear?
    /// Generally, it is because the corresponding Datanode is disconnected and
    /// did not respond to the `Failover` scheduling instructions of metasrv.
    pub async fn scan_all_inactive_regions(
        &self,
        cluster_id: u64,
    ) -> Result<Vec<InactiveRegionKey>> {
        let prefix = InactiveRegionKey::get_prefix_by_cluster(cluster_id);
        let request = RangeRequest::new().with_prefix(prefix);
        let resp = self.store.range(request).await?;
        let kvs = resp.kvs;
        kvs.into_iter()
            .map(|kv| InactiveRegionKey::try_from(kv.key))
            .collect::<Result<Vec<_>>>()
    }

    pub async fn clear_all_inactive_regions(&self, cluster_id: u64) -> Result<()> {
        let prefix = InactiveRegionKey::get_prefix_by_cluster(cluster_id);
        let request = DeleteRangeRequest::new().with_prefix(prefix);
        let _ = self.store.delete_range(request).await?;
        Ok(())
    }
}
