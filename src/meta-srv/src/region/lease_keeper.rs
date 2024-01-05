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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_meta::key::table_route::TableRouteValue;
use common_meta::key::TableMetadataManagerRef;
use common_meta::region_keeper::MemoryRegionKeeperRef;
use common_meta::rpc::router::RegionRoute;
use common_meta::DatanodeId;
use snafu::ResultExt;
use store_api::region_engine::RegionRole;
use store_api::storage::{RegionId, TableId};

use crate::error::{self, Result};

pub type RegionLeaseKeeperRef = Arc<RegionLeaseKeeper>;

/// Renews lease of regions.
pub struct RegionLeaseKeeper {
    table_metadata_manager: TableMetadataManagerRef,
    memory_region_keeper: MemoryRegionKeeperRef,
}

/// The result of region lease renewal,
/// contains the renewed region leases and [RegionId] of non-existing regions.
pub struct RenewRegionLeasesResponse {
    pub renewed: HashMap<RegionId, RegionRole>,
    pub non_exists: HashSet<RegionId>,
}

impl RegionLeaseKeeper {
    pub fn new(
        table_metadata_manager: TableMetadataManagerRef,
        memory_region_keeper: MemoryRegionKeeperRef,
    ) -> Self {
        Self {
            table_metadata_manager,
            memory_region_keeper,
        }
    }
}

fn renew_region_lease_via_region_route(
    region_route: &RegionRoute,
    datanode_id: DatanodeId,
    region_id: RegionId,
) -> Option<(RegionId, RegionRole)> {
    // If it's a leader region on this datanode.
    if let Some(leader) = &region_route.leader_peer {
        if leader.id == datanode_id {
            let region_role = if region_route.is_leader_downgraded() {
                RegionRole::Follower
            } else {
                RegionRole::Leader
            };

            return Some((region_id, region_role));
        }
    }

    // If it's a follower region on this datanode.
    if region_route
        .follower_peers
        .iter()
        .any(|peer| peer.id == datanode_id)
    {
        return Some((region_id, RegionRole::Follower));
    }

    // The region doesn't belong to this datanode.
    None
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

    /// Returns [None] if:
    /// - The region doesn't belong to the datanode.
    /// - The region belongs to a logical table.
    fn renew_region_lease(
        &self,
        table_metadata: &HashMap<TableId, TableRouteValue>,
        datanode_id: DatanodeId,
        region_id: RegionId,
        role: RegionRole,
    ) -> Option<(RegionId, RegionRole)> {
        // Renews the lease if it's a opening region or deleting region.
        if self.memory_region_keeper.contains(datanode_id, region_id) {
            return Some((region_id, role));
        }

        if let Some(table_route) = table_metadata.get(&region_id.table_id()) {
            if let Ok(Some(region_route)) = table_route.region_route(region_id) {
                return renew_region_lease_via_region_route(&region_route, datanode_id, region_id);
            }
        }

        None
    }

    /// Renews the lease of regions for specific datanode.
    ///
    /// The lease of regions will be renewed if:
    /// -  The region of the specific datanode exists in [TableRouteValue].
    /// -  The region of the specific datanode is opening.
    ///
    /// Otherwise the lease of regions will not be renewed,
    /// and corresponding regions will be added to `non_exists` of [RenewRegionLeasesResponse].
    pub async fn renew_region_leases(
        &self,
        _cluster_id: u64,
        datanode_id: DatanodeId,
        regions: &[(RegionId, RegionRole)],
    ) -> Result<RenewRegionLeasesResponse> {
        let region_ids = regions
            .iter()
            .map(|(region_id, _)| *region_id)
            .collect::<Vec<_>>();
        let tables = self.collect_tables(&region_ids);
        let table_ids = tables.keys().copied().collect::<Vec<_>>();
        let table_metadata = self.collect_tables_metadata(&table_ids).await?;

        let mut renewed = HashMap::new();
        let mut non_exists = HashSet::new();

        for &(region, role) in regions {
            match self.renew_region_lease(&table_metadata, datanode_id, region, role) {
                Some((region, renewed_role)) => {
                    renewed.insert(region, renewed_role);
                }
                None => {
                    non_exists.insert(region);
                }
            }
        }

        Ok(RenewRegionLeasesResponse {
            renewed,
            non_exists,
        })
    }

    #[cfg(test)]
    pub fn table_metadata_manager(&self) -> &TableMetadataManagerRef {
        &self.table_metadata_manager
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use common_meta::key::table_route::{LogicalTableRouteValue, TableRouteValue};
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::peer::Peer;
    use common_meta::region_keeper::MemoryRegionKeeper;
    use common_meta::rpc::router::{Region, RegionRouteBuilder, RegionStatus};
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;
    use table::metadata::RawTableInfo;

    use super::{renew_region_lease_via_region_route, RegionLeaseKeeper};
    use crate::region::lease_keeper::RenewRegionLeasesResponse;

    fn new_test_keeper() -> RegionLeaseKeeper {
        let store = Arc::new(MemoryKvBackend::new());

        let table_metadata_manager = Arc::new(TableMetadataManager::new(store));

        let opening_region_keeper = Arc::new(MemoryRegionKeeper::default());
        RegionLeaseKeeper::new(table_metadata_manager, opening_region_keeper)
    }

    #[test]
    fn test_renew_region_lease_via_region_route() {
        let region_id = RegionId::new(1024, 1);
        let leader_peer_id = 1024;
        let follower_peer_id = 2048;
        let mut region_route = RegionRouteBuilder::default()
            .region(Region::new_test(region_id))
            .leader_peer(Peer::empty(leader_peer_id))
            .follower_peers(vec![Peer::empty(follower_peer_id)])
            .build()
            .unwrap();

        // The region doesn't belong to the datanode.
        for region_id in [RegionId::new(1024, 2), region_id] {
            assert!(renew_region_lease_via_region_route(&region_route, 1, region_id).is_none());
        }

        // The leader region on the datanode.
        assert_eq!(
            renew_region_lease_via_region_route(&region_route, leader_peer_id, region_id),
            Some((region_id, RegionRole::Leader))
        );
        // The follower region on the datanode.
        assert_eq!(
            renew_region_lease_via_region_route(&region_route, follower_peer_id, region_id),
            Some((region_id, RegionRole::Follower))
        );

        region_route.leader_status = Some(RegionStatus::Downgraded);
        // The downgraded leader region on the datanode.
        assert_eq!(
            renew_region_lease_via_region_route(&region_route, leader_peer_id, region_id),
            Some((region_id, RegionRole::Follower))
        );
    }

    #[tokio::test]
    async fn test_renew_region_leases_non_exists_regions() {
        let keeper = new_test_keeper();

        let RenewRegionLeasesResponse {
            non_exists,
            renewed,
        } = keeper
            .renew_region_leases(
                0,
                1,
                &[
                    (RegionId::new(1024, 1), RegionRole::Follower),
                    (RegionId::new(1024, 2), RegionRole::Leader),
                ],
            )
            .await
            .unwrap();

        assert!(renewed.is_empty());
        assert_eq!(
            non_exists,
            HashSet::from([RegionId::new(1024, 1), RegionId::new(1024, 2)])
        );
    }

    #[tokio::test]
    async fn test_renew_region_leases_basic() {
        let region_number = 1u32;
        let table_id = 1024;
        let table_info: RawTableInfo = new_test_table_info(table_id, vec![region_number]).into();

        let region_id = RegionId::new(table_id, 1);
        let leader_peer_id = 1024;
        let follower_peer_id = 2048;
        let region_route = RegionRouteBuilder::default()
            .region(Region::new_test(region_id))
            .leader_peer(Peer::empty(leader_peer_id))
            .follower_peers(vec![Peer::empty(follower_peer_id)])
            .build()
            .unwrap();

        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();
        table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(vec![region_route]),
                HashMap::default(),
            )
            .await
            .unwrap();

        // The region doesn't belong to the datanode.
        for region_id in [RegionId::new(1024, 2), region_id] {
            let RenewRegionLeasesResponse {
                non_exists,
                renewed,
            } = keeper
                .renew_region_leases(0, 1, &[(region_id, RegionRole::Follower)])
                .await
                .unwrap();
            assert!(renewed.is_empty());
            assert_eq!(non_exists, HashSet::from([region_id]));
        }

        // The leader region on the datanode.
        for role in [RegionRole::Leader, RegionRole::Follower] {
            let RenewRegionLeasesResponse {
                non_exists,
                renewed,
            } = keeper
                .renew_region_leases(0, leader_peer_id, &[(region_id, role)])
                .await
                .unwrap();

            assert!(non_exists.is_empty());
            assert_eq!(renewed, HashMap::from([(region_id, RegionRole::Leader)]));
        }

        // The follower region on the datanode.
        for role in [RegionRole::Leader, RegionRole::Follower] {
            let RenewRegionLeasesResponse {
                non_exists,
                renewed,
            } = keeper
                .renew_region_leases(0, follower_peer_id, &[(region_id, role)])
                .await
                .unwrap();

            assert!(non_exists.is_empty());
            assert_eq!(renewed, HashMap::from([(region_id, RegionRole::Follower)]));
        }

        let opening_region_id = RegionId::new(2048, 1);
        let _guard = keeper
            .memory_region_keeper
            .register(leader_peer_id, opening_region_id)
            .unwrap();

        // The opening region on the datanode.
        // NOTES: The procedure lock will ensure only one opening leader.
        for role in [RegionRole::Leader, RegionRole::Follower] {
            let RenewRegionLeasesResponse {
                non_exists,
                renewed,
            } = keeper
                .renew_region_leases(0, leader_peer_id, &[(opening_region_id, role)])
                .await
                .unwrap();

            assert!(non_exists.is_empty());
            assert_eq!(renewed, HashMap::from([(opening_region_id, role)]));
        }
    }

    #[tokio::test]
    async fn test_renew_unexpected_logic_table() {
        let region_number = 1u32;
        let table_id = 1024;
        let table_info: RawTableInfo = new_test_table_info(table_id, vec![region_number]).into();

        let region_id = RegionId::new(table_id, 1);
        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();
        table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::Logical(LogicalTableRouteValue::new(table_id, vec![region_id])),
                HashMap::default(),
            )
            .await
            .unwrap();

        for region_id in [region_id, RegionId::new(1024, 2)] {
            let RenewRegionLeasesResponse {
                non_exists,
                renewed,
            } = keeper
                .renew_region_leases(
                    0,
                    1,
                    &[
                        (region_id, RegionRole::Follower),
                        (region_id, RegionRole::Leader),
                    ],
                )
                .await
                .unwrap();
            assert!(renewed.is_empty());
            assert_eq!(non_exists, HashSet::from([region_id]));
        }
    }

    #[tokio::test]
    async fn test_renew_region_leases_with_downgrade_leader() {
        let region_number = 1u32;
        let table_id = 1024;
        let table_info: RawTableInfo = new_test_table_info(table_id, vec![region_number]).into();

        let region_id = RegionId::new(table_id, 1);
        let leader_peer_id = 1024;
        let follower_peer_id = 2048;
        let region_route = RegionRouteBuilder::default()
            .region(Region::new_test(region_id))
            .leader_peer(Peer::empty(leader_peer_id))
            .follower_peers(vec![Peer::empty(follower_peer_id)])
            .leader_status(RegionStatus::Downgraded)
            .build()
            .unwrap();

        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();
        table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(vec![region_route]),
                HashMap::default(),
            )
            .await
            .unwrap();

        // The leader region on the datanode.
        for role in [RegionRole::Leader, RegionRole::Follower] {
            let RenewRegionLeasesResponse {
                non_exists,
                renewed,
            } = keeper
                .renew_region_leases(0, follower_peer_id, &[(region_id, role)])
                .await
                .unwrap();

            assert!(non_exists.is_empty());
            assert_eq!(renewed, HashMap::from([(region_id, RegionRole::Follower)]));
        }
    }
}
