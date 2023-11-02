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

pub mod mito;
pub mod utils;

use std::collections::{HashMap, HashSet};

use common_meta::key::TableMetadataManagerRef;
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};

use self::mito::find_staled_leader_regions;
use crate::error::{self, Result};

pub struct RegionLeaseKeeper {
    table_metadata_manager: TableMetadataManagerRef,
}

impl RegionLeaseKeeper {
    pub fn new(table_metadata_manager: TableMetadataManagerRef) -> Self {
        Self {
            table_metadata_manager,
        }
    }
}

impl RegionLeaseKeeper {
    /// Returns staled [RegionRole::Leader](store_api::region_engine::RegionRole::Leader) regions.
    ///
    /// - It returns a region if the `datanode_id` isn't the corresponding leader peer in `region_routes`.
    ///     - Expected as [RegionRole::Follower](store_api::region_engine::RegionRole::Follower) regions.
    ///     - Unexpected [RegionRole::Leader](store_api::region_engine::RegionRole::Leader) regions.
    /// - It returns a region if the region's table metadata is not found.
    pub async fn find_staled_leader_regions(
        &self,
        _cluster_id: u64,
        datanode_id: u64,
        datanode_regions: &[RegionId],
    ) -> Result<HashSet<RegionId>> {
        let table_route_manager = self.table_metadata_manager.table_route_manager();

        let mut tables = HashMap::<TableId, Vec<RegionId>>::new();

        // Group by `table_id`.
        for region_id in datanode_regions.iter() {
            let table = tables.entry(region_id.table_id()).or_default();
            table.push(*region_id);
        }

        let table_ids = tables.keys().cloned().collect::<Vec<_>>();

        // The subset of all table metadata.
        // TODO: considers storing all active regions in meta's memory.
        let metadata_subset = table_route_manager
            .batch_get(&table_ids)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        let mut staled_regions = HashSet::new();

        for (table_id, regions) in &mut tables {
            if let Some(metadata) = metadata_subset.get(table_id) {
                let region_routes = &metadata.region_routes;

                staled_regions.extend(find_staled_leader_regions(
                    datanode_id,
                    regions,
                    region_routes,
                ));
            } else {
                // If table metadata is not found.
                staled_regions.extend(regions.drain(..));
            }
        }

        Ok(staled_regions)
    }

    #[cfg(test)]
    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::key::TableMetadataManager;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::RegionId;

    use super::RegionLeaseKeeper;
    use crate::service::store::kv::KvBackendAdapter;
    use crate::service::store::memory::MemStore;

    fn new_test_keeper() -> RegionLeaseKeeper {
        let store = KvBackendAdapter::wrap(Arc::new(MemStore::new()));

        let table_metadata_manager = Arc::new(TableMetadataManager::new(store));

        RegionLeaseKeeper::new(table_metadata_manager)
    }

    #[tokio::test]
    async fn test_empty_table_routes() {
        let datanode_id = 1;
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);

        let keeper = new_test_keeper();

        let datanode_regions = vec![region_id];

        let staled_regions = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert_eq!(staled_regions.len(), 1);
        assert!(staled_regions.contains(&region_id));
    }

    #[tokio::test]
    async fn test_find_staled_regions_simple() {
        let datanode_id = 1;
        let region_number = 1u32;
        let table_id = 10;
        let region_id = RegionId::new(table_id, region_number);
        let peer = Peer::empty(datanode_id);
        let table_info = new_test_table_info(table_id, vec![region_number]).into();

        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(peer.clone()),
            ..Default::default()
        }];

        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();

        table_metadata_manager
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        // `staled_regions` should be empty.
        let datanode_regions = vec![region_id];

        let staled_regions = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert!(staled_regions.is_empty());

        // `staled_regions` should be empty.
        let datanode_regions = vec![];

        let staled_regions = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert!(staled_regions.is_empty());
    }

    #[tokio::test]
    async fn test_find_staled_regions_2() {
        let datanode_id = 1;
        let region_number = 1u32;
        let table_id = 10;
        let region_id = RegionId::new(table_id, region_number);
        let another_region_id = RegionId::new(table_id, region_number + 1);
        let unknown_region_id = RegionId::new(table_id + 1, region_number);

        let peer = Peer::empty(datanode_id);
        let another_peer = Peer::empty(datanode_id + 1);

        let table_info =
            new_test_table_info(table_id, vec![region_number, region_number + 1]).into();

        let region_routes = vec![
            RegionRoute {
                region: Region::new_test(region_id),
                leader_peer: Some(peer.clone()),
                ..Default::default()
            },
            RegionRoute {
                region: Region::new_test(another_region_id),
                leader_peer: None,
                follower_peers: vec![another_peer.clone()],
            },
        ];

        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();

        table_metadata_manager
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        // Unexpected Leader region.
        // `staled_regions` should be vec![unknown_region_id].
        let datanode_regions = vec![region_id, unknown_region_id];

        let staled_regions = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert_eq!(staled_regions.len(), 1);
        assert!(staled_regions.contains(&unknown_region_id));

        // Expected as Follower region.
        // `staled_regions` should be vec![another_region_id], because the `another_region_id` is a active region of `another_peer`.
        let datanode_regions = vec![another_region_id];

        let staled_regions = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert_eq!(staled_regions.len(), 1);
        assert!(staled_regions.contains(&another_region_id));
    }
}
