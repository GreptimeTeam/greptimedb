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
use std::sync::{Arc, RwLock};

use common_meta::key::table_route::TableRouteValue;
use common_meta::key::TableMetadataManagerRef;
use common_meta::DatanodeId;
use snafu::ResultExt;
use store_api::storage::{RegionId, TableId};

use self::mito::find_staled_leader_regions;
use crate::error::{self, Result};
use crate::region::lease_keeper::utils::find_staled_follower_regions;

pub type RegionLeaseKeeperRef = Arc<RegionLeaseKeeper>;

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
    fn collect_tables(&self, datanode_regions: &[RegionId]) -> HashMap<TableId, Vec<RegionId>> {
        let mut tables = HashMap::<TableId, Vec<RegionId>>::new();

        // Group by `table_id`.
        for region_id in datanode_regions.iter() {
            let table = tables.entry(region_id.table_id()).or_default();
            table.push(*region_id);
        }

        tables
    }

    async fn collect_tables_metadata(
        &self,
        table_ids: &[TableId],
    ) -> Result<HashMap<TableId, TableRouteValue>> {
        let table_route_manager = self.table_metadata_manager.table_route_manager();

        // The subset of all table metadata.
        // TODO: considers storing all active regions in meta's memory.
        let metadata_subset = table_route_manager
            .batch_get(table_ids)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        Ok(metadata_subset)
    }

    /// Returns downgradable regions, and closable regions.
    ///
    /// - Downgradable regions:
    /// Region's peer(`datanode_id`) is the corresponding downgraded leader peer in `region_routes`.
    ///
    /// - Closable regions:
    /// - It returns a region if it's peer(`datanode_id`) isn't the corresponding leader peer in `region_routes`.
    ///     - Expected as [RegionRole::Follower](store_api::region_engine::RegionRole::Follower) regions.
    ///     - Unexpected [RegionRole::Leader](store_api::region_engine::RegionRole::Leader) regions.
    /// - It returns a region if the region's table metadata is not found.
    pub async fn find_staled_leader_regions(
        &self,
        _cluster_id: u64,
        datanode_id: u64,
        datanode_regions: &[RegionId],
    ) -> Result<(HashSet<RegionId>, HashSet<RegionId>)> {
        let tables = self.collect_tables(datanode_regions);
        let table_ids = tables.keys().copied().collect::<Vec<_>>();
        let metadata_subset = self.collect_tables_metadata(&table_ids).await?;

        let mut closable_set = HashSet::new();
        let mut downgradable_set = HashSet::new();

        for (table_id, regions) in tables {
            if let Some(metadata) = metadata_subset.get(&table_id) {
                let region_routes = &metadata.region_routes;

                let (downgradable, closable) =
                    find_staled_leader_regions(datanode_id, &regions, region_routes);

                downgradable_set.extend(downgradable);
                closable_set.extend(closable);
            } else {
                // If table metadata is not found.
                closable_set.extend(regions);
            }
        }

        Ok((downgradable_set, closable_set))
    }

    /// Returns upgradable regions, and closable regions.
    ///
    /// Upgradable regions:
    /// - Region's peer(`datanode_id`) is the corresponding leader peer in `region_routes`.
    ///
    /// Closable regions:
    /// - Region's peer(`datanode_id`) isn't the corresponding leader/follower peer in `region_routes`.
    /// - Region's table metadata is not found.
    pub async fn find_staled_follower_regions(
        &self,
        _cluster_id: u64,
        datanode_id: u64,
        datanode_regions: &[RegionId],
    ) -> Result<(HashSet<RegionId>, HashSet<RegionId>)> {
        let tables = self.collect_tables(datanode_regions);
        let table_ids = tables.keys().copied().collect::<Vec<_>>();
        let metadata_subset = self.collect_tables_metadata(&table_ids).await?;

        let mut upgradable_set = HashSet::new();
        let mut closable_set = HashSet::new();

        for (table_id, regions) in tables {
            if let Some(metadata) = metadata_subset.get(&table_id) {
                let region_routes = &metadata.region_routes;

                let (upgradable, closable) =
                    find_staled_follower_regions(datanode_id, &regions, region_routes);

                upgradable_set.extend(upgradable);
                closable_set.extend(closable);
            } else {
                // If table metadata is not found.
                closable_set.extend(regions);
            }
        }

        Ok((upgradable_set, closable_set))
    }

    #[cfg(test)]
    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }
}

#[derive(Debug, Clone)]
pub struct OpeningRegionGuard {
    datanode_id: DatanodeId,
    region_id: RegionId,
    inner: Arc<RwLock<HashSet<(DatanodeId, RegionId)>>>,
}

impl Drop for OpeningRegionGuard {
    fn drop(&mut self) {
        let mut inner = self.inner.write().unwrap();
        inner.remove(&(self.datanode_id, self.region_id));
    }
}

pub type OpeningRegionKeeperRef = Arc<OpeningRegionKeeper>;

#[derive(Debug, Clone)]
///  Tracks opening regions.
pub struct OpeningRegionKeeper {
    inner: Arc<RwLock<HashSet<(DatanodeId, RegionId)>>>,
}

impl OpeningRegionKeeper {
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    /// Returns [OpeningRegionGuard] if Region(`region_id`) on Peer(`datanode_id`) does not exist.
    pub fn register(
        &self,
        datanode_id: DatanodeId,
        region_id: RegionId,
    ) -> Option<OpeningRegionGuard> {
        let mut inner = self.inner.write().unwrap();

        if inner.insert((datanode_id, region_id)) {
            Some(OpeningRegionGuard {
                datanode_id,
                region_id,
                inner: self.inner.clone(),
            })
        } else {
            None
        }
    }

    /// Returns true if the keeper contains a (`datanoe_id`, `region_id`) tuple.
    pub fn contains(&self, datanode_id: DatanodeId, region_id: RegionId) -> bool {
        let inner = self.inner.read().unwrap();
        inner.contains(&(datanode_id, region_id))
    }

    /// Returns number of element in tracking set.
    pub fn len(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute, RegionStatus};
    use store_api::storage::RegionId;
    use table::metadata::RawTableInfo;

    use super::{OpeningRegionKeeper, RegionLeaseKeeper};

    fn new_test_keeper() -> RegionLeaseKeeper {
        let store = Arc::new(MemoryKvBackend::new());

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

        let (downgradable, closable) = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert_eq!(closable.len(), 1);
        assert!(closable.contains(&region_id));
        assert!(downgradable.is_empty());

        let (upgradable, closable) = keeper
            .find_staled_follower_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert!(upgradable.is_empty());
        assert_eq!(closable.len(), 1);
        assert!(closable.contains(&region_id));
    }

    #[tokio::test]
    async fn test_find_closable_regions_simple() {
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

        // `closable` should be empty.
        let datanode_regions = vec![region_id];

        let (downgradable, closable) = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert!(closable.is_empty());
        assert!(downgradable.is_empty());

        // `closable` should be empty.
        let datanode_regions = vec![];

        let (downgradable, closable) = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert!(closable.is_empty());
        assert!(downgradable.is_empty());
    }

    #[tokio::test]
    async fn test_find_closable_regions_2() {
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
                leader_status: None,
            },
        ];

        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();

        table_metadata_manager
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        // Unexpected Leader region.
        // `closable` should be vec![unknown_region_id].
        let datanode_regions = vec![region_id, unknown_region_id];

        let (downgradable, closable) = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert_eq!(closable.len(), 1);
        assert!(closable.contains(&unknown_region_id));
        assert!(downgradable.is_empty());

        // Expected as Follower region.
        // `closable` should be vec![another_region_id], because the `another_region_id` is a active region of `another_peer`.
        let datanode_regions = vec![another_region_id];

        let (downgradable, closable) = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert_eq!(closable.len(), 1);
        assert!(closable.contains(&another_region_id));
        assert!(downgradable.is_empty());
    }

    #[tokio::test]
    async fn test_find_staled_leader_region_downgraded() {
        let datanode_id = 1;
        let region_number = 1u32;
        let table_id = 10;
        let region_id = RegionId::new(table_id, region_number);
        let another_region_id = RegionId::new(table_id, region_number + 1);
        let peer = Peer::empty(datanode_id);
        let table_info: RawTableInfo = new_test_table_info(table_id, vec![region_number]).into();

        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(peer.clone()),
            leader_status: Some(RegionStatus::Downgraded),
            ..Default::default()
        }];
        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();
        table_metadata_manager
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        // `upgradable` should be empty, `closable` should be empty.
        let datanode_regions = vec![region_id, another_region_id];

        let (downgradable, closable) = keeper
            .find_staled_leader_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert_eq!(closable.len(), 1);
        assert!(closable.contains(&another_region_id));
        assert_eq!(downgradable.len(), 1);
        assert!(downgradable.contains(&region_id));
    }

    #[tokio::test]
    async fn test_find_staled_follower_regions() {
        let datanode_id = 1;
        let region_number = 1u32;
        let table_id = 10;
        let region_id = RegionId::new(table_id, region_number);
        let peer = Peer::empty(datanode_id);
        let table_info: RawTableInfo = new_test_table_info(table_id, vec![region_number]).into();

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

        // `upgradable` should be vec![region_id], `closable` should be empty.
        let datanode_regions = vec![region_id];

        let (upgradable, closable) = keeper
            .find_staled_follower_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();

        assert!(closable.is_empty());
        assert_eq!(upgradable.len(), 1);
        assert!(upgradable.contains(&region_id));

        // `upgradable` should be empty, `closable` should be vec![region_id].
        let datanode_regions = vec![region_id];

        let (upgradable, closable) = keeper
            .find_staled_follower_regions(0, datanode_id + 1, &datanode_regions)
            .await
            .unwrap();

        assert!(upgradable.is_empty());
        assert_eq!(closable.len(), 1);
        assert!(closable.contains(&region_id));
    }

    #[tokio::test]
    async fn test_find_staled_region_downgraded() {
        let datanode_id = 1;
        let region_number = 1u32;
        let table_id = 10;
        let region_id = RegionId::new(table_id, region_number);
        let peer = Peer::empty(datanode_id);
        let table_info: RawTableInfo = new_test_table_info(table_id, vec![region_number]).into();

        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(peer.clone()),
            leader_status: Some(RegionStatus::Downgraded),
            ..Default::default()
        }];

        let datanode_regions = vec![region_id];
        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();
        table_metadata_manager
            .create_table_metadata(table_info, region_routes)
            .await
            .unwrap();

        let (upgradable, closable) = keeper
            .find_staled_follower_regions(0, datanode_id, &datanode_regions)
            .await
            .unwrap();
        assert!(upgradable.is_empty());
        assert!(closable.is_empty());
    }

    #[test]
    fn test_opening_region_keeper() {
        let keeper = OpeningRegionKeeper::new();

        let guard = keeper.register(1, RegionId::from_u64(1)).unwrap();
        assert!(keeper.register(1, RegionId::from_u64(1)).is_none());
        let guard2 = keeper.register(1, RegionId::from_u64(2)).unwrap();

        assert_eq!(keeper.len(), 2);
        drop(guard);

        assert_eq!(keeper.len(), 1);

        assert!(keeper.contains(1, RegionId::from_u64(2)));
        drop(guard2);

        assert!(keeper.is_empty());
    }
}
