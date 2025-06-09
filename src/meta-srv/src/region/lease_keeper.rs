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
use common_telemetry::warn;
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
    pub renewed: HashMap<RegionId, RegionLeaseInfo>,
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
            let region_role = if region_route.is_leader_downgrading() {
                RegionRole::DowngradingLeader
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

    warn!(
        "Denied to renew region lease for datanode: {datanode_id}, region_id: {region_id}, region_routes: {:?}",
        region_route
    );
    // The region doesn't belong to this datanode.
    None
}

/// The information of region lease.
#[derive(Debug, PartialEq, Eq)]
pub struct RegionLeaseInfo {
    pub region_id: RegionId,
    /// Whether the region is operating.
    ///
    /// The region is under dropping or opening / migration operation.
    pub is_operating: bool,
    /// The role of region.
    pub role: RegionRole,
}

impl RegionLeaseInfo {
    /// Creates a new [RegionLeaseInfo] with the given region id and role with operating status.
    pub fn operating(region_id: RegionId, role: RegionRole) -> Self {
        Self {
            region_id,
            is_operating: true,
            role,
        }
    }
}

impl From<(RegionId, RegionRole)> for RegionLeaseInfo {
    fn from((region_id, role): (RegionId, RegionRole)) -> Self {
        Self {
            region_id,
            is_operating: false,
            role,
        }
    }
}

impl RegionLeaseKeeper {
    async fn collect_table_metadata(
        &self,
        table_ids: &[TableId],
    ) -> Result<HashMap<TableId, TableRouteValue>> {
        let table_route_manager = self.table_metadata_manager.table_route_manager();

        // The subset of all table metadata.
        // TODO: considers storing all active regions in meta's memory.
        let table_routes = table_route_manager
            .table_route_storage()
            .batch_get(table_ids)
            .await
            .context(error::TableMetadataManagerSnafu)?;

        let metadata_subset = table_routes
            .into_iter()
            .zip(table_ids)
            .filter_map(|(route, id)| route.map(|route| (*id, route)))
            .collect::<HashMap<_, _>>();

        Ok(metadata_subset)
    }

    /// Returns [None] if:
    /// - The region doesn't belong to the datanode.
    /// - The region belongs to a logical table.
    fn renew_region_lease(
        &self,
        table_metadata: &HashMap<TableId, TableRouteValue>,
        operating_regions: &HashSet<RegionId>,
        datanode_id: DatanodeId,
        region_id: RegionId,
        role: RegionRole,
    ) -> Option<RegionLeaseInfo> {
        if operating_regions.contains(&region_id) {
            let region_lease_info = RegionLeaseInfo::operating(region_id, role);
            return Some(region_lease_info);
        }

        if let Some(table_route) = table_metadata.get(&region_id.table_id()) {
            if let Ok(Some(region_route)) = table_route.region_route(region_id) {
                return renew_region_lease_via_region_route(&region_route, datanode_id, region_id)
                    .map(RegionLeaseInfo::from);
            } else {
                warn!(
                    "Denied to renew region lease for datanode: {datanode_id}, region_id: {region_id}, region route is not found in table({})",
                    region_id.table_id()
                );
            }
        } else {
            warn!(
                "Denied to renew region lease for datanode: {datanode_id}, region_id: {region_id}, table({}) is not found",
                region_id.table_id()
            );
        }
        None
    }

    async fn collect_metadata(
        &self,
        datanode_id: DatanodeId,
        mut region_ids: HashSet<RegionId>,
    ) -> Result<(HashMap<TableId, TableRouteValue>, HashSet<RegionId>)> {
        // Filters out operating region first, improves the cache hit rate(reduce expensive remote fetches).
        let operating_regions = self
            .memory_region_keeper
            .extract_operating_regions(datanode_id, &mut region_ids);
        let table_ids = region_ids
            .into_iter()
            .map(|region_id| region_id.table_id())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let table_metadata = self.collect_table_metadata(&table_ids).await?;
        Ok((table_metadata, operating_regions))
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
        datanode_id: DatanodeId,
        regions: &[(RegionId, RegionRole)],
    ) -> Result<RenewRegionLeasesResponse> {
        let region_ids = regions
            .iter()
            .map(|(region_id, _)| *region_id)
            .collect::<HashSet<_>>();
        let (table_metadata, operating_regions) =
            self.collect_metadata(datanode_id, region_ids).await?;
        let mut renewed = HashMap::new();
        let mut non_exists = HashSet::new();

        for &(region, role) in regions {
            match self.renew_region_lease(
                &table_metadata,
                &operating_regions,
                datanode_id,
                region,
                role,
            ) {
                Some(region_lease_info) => {
                    renewed.insert(region_lease_info.region_id, region_lease_info);
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
    use common_meta::rpc::router::{LeaderState, Region, RegionRouteBuilder};
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;
    use table::metadata::RawTableInfo;

    use super::{renew_region_lease_via_region_route, RegionLeaseKeeper};
    use crate::region::lease_keeper::{RegionLeaseInfo, RenewRegionLeasesResponse};

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

        region_route.leader_state = Some(LeaderState::Downgrading);
        // The downgraded leader region on the datanode.
        assert_eq!(
            renew_region_lease_via_region_route(&region_route, leader_peer_id, region_id),
            Some((region_id, RegionRole::DowngradingLeader))
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
    async fn test_collect_metadata() {
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
        let opening_region_id = RegionId::new(1025, 1);
        let _guard = keeper
            .memory_region_keeper
            .register(leader_peer_id, opening_region_id)
            .unwrap();
        let another_opening_region_id = RegionId::new(1025, 2);
        let _guard2 = keeper
            .memory_region_keeper
            .register(follower_peer_id, another_opening_region_id)
            .unwrap();

        let (metadata, regions) = keeper
            .collect_metadata(
                leader_peer_id,
                HashSet::from([region_id, opening_region_id]),
            )
            .await
            .unwrap();
        assert_eq!(
            metadata.keys().cloned().collect::<Vec<_>>(),
            vec![region_id.table_id()]
        );
        assert!(regions.contains(&opening_region_id));
        assert_eq!(regions.len(), 1);
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
                .renew_region_leases(1, &[(region_id, RegionRole::Follower)])
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
                .renew_region_leases(leader_peer_id, &[(region_id, role)])
                .await
                .unwrap();

            assert!(non_exists.is_empty());
            assert_eq!(
                renewed,
                HashMap::from([(
                    region_id,
                    RegionLeaseInfo::from((region_id, RegionRole::Leader))
                )])
            );
        }

        // The follower region on the datanode.
        for role in [RegionRole::Leader, RegionRole::Follower] {
            let RenewRegionLeasesResponse {
                non_exists,
                renewed,
            } = keeper
                .renew_region_leases(follower_peer_id, &[(region_id, role)])
                .await
                .unwrap();

            assert!(non_exists.is_empty());
            assert_eq!(
                renewed,
                HashMap::from([(
                    region_id,
                    RegionLeaseInfo::from((region_id, RegionRole::Follower))
                )])
            );
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
                .renew_region_leases(leader_peer_id, &[(opening_region_id, role)])
                .await
                .unwrap();

            assert!(non_exists.is_empty());
            assert_eq!(
                renewed,
                HashMap::from([(
                    opening_region_id,
                    RegionLeaseInfo::operating(opening_region_id, role)
                )])
            );
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
            .leader_state(LeaderState::Downgrading)
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
                .renew_region_leases(follower_peer_id, &[(region_id, role)])
                .await
                .unwrap();

            assert!(non_exists.is_empty());
            assert_eq!(
                renewed,
                HashMap::from([(
                    region_id,
                    RegionLeaseInfo::from((region_id, RegionRole::Follower))
                )])
            );
        }
    }
}
