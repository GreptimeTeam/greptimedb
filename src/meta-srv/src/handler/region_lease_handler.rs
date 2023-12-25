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

use api::v1::meta::{HeartbeatRequest, RegionLease, Role};
use async_trait::async_trait;
use common_meta::key::TableMetadataManagerRef;
use common_meta::region_keeper::MemoryRegionKeeperRef;
use store_api::region_engine::GrantedRegion;

use crate::error::Result;
use crate::handler::{HandleControl, HeartbeatAccumulator, HeartbeatHandler};
use crate::metasrv::Context;
use crate::region::lease_keeper::{RegionLeaseKeeperRef, RenewRegionLeasesResponse};
use crate::region::RegionLeaseKeeper;

pub struct RegionLeaseHandler {
    region_lease_seconds: u64,
    region_lease_keeper: RegionLeaseKeeperRef,
}

impl RegionLeaseHandler {
    pub fn new(
        region_lease_seconds: u64,
        table_metadata_manager: TableMetadataManagerRef,
        memory_region_keeper: MemoryRegionKeeperRef,
    ) -> Self {
        let region_lease_keeper =
            RegionLeaseKeeper::new(table_metadata_manager, memory_region_keeper.clone());

        Self {
            region_lease_seconds,
            region_lease_keeper: Arc::new(region_lease_keeper),
        }
    }
}

#[async_trait]
impl HeartbeatHandler for RegionLeaseHandler {
    fn is_acceptable(&self, role: Role) -> bool {
        role == Role::Datanode
    }

    async fn handle(
        &self,
        req: &HeartbeatRequest,
        _ctx: &mut Context,
        acc: &mut HeartbeatAccumulator,
    ) -> Result<HandleControl> {
        let Some(stat) = acc.stat.as_ref() else {
            return Ok(HandleControl::Continue);
        };

        let regions = stat.regions();
        let cluster_id = stat.cluster_id;
        let datanode_id = stat.id;

        let RenewRegionLeasesResponse {
            non_exists,
            renewed,
        } = self
            .region_lease_keeper
            .renew_region_leases(cluster_id, datanode_id, &regions)
            .await?;

        let renewed = renewed
            .into_iter()
            .map(|(region_id, region_role)| {
                GrantedRegion {
                    region_id,
                    region_role,
                }
                .into()
            })
            .collect::<Vec<_>>();

        acc.region_lease = Some(RegionLease {
            regions: renewed,
            duration_since_epoch: req.duration_since_epoch,
            lease_seconds: self.region_lease_seconds,
            closeable_region_ids: non_exists.iter().map(|region| region.as_u64()).collect(),
        });
        acc.inactive_region_ids = non_exists;

        Ok(HandleControl::Continue)
    }
}

#[cfg(test)]
mod test {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    use common_meta::distributed_time_constants;
    use common_meta::key::table_route::TableRouteValue;
    use common_meta::key::test_utils::new_test_table_info;
    use common_meta::key::TableMetadataManager;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::peer::Peer;
    use common_meta::region_keeper::MemoryRegionKeeper;
    use common_meta::rpc::router::{Region, RegionRoute, RegionStatus};
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;

    use super::*;
    use crate::handler::node_stat::{RegionStat, Stat};
    use crate::metasrv::builder::MetaSrvBuilder;

    fn new_test_keeper() -> RegionLeaseKeeper {
        let store = Arc::new(MemoryKvBackend::new());

        let table_metadata_manager = Arc::new(TableMetadataManager::new(store));

        let memory_region_keeper = Arc::new(MemoryRegionKeeper::default());
        RegionLeaseKeeper::new(table_metadata_manager, memory_region_keeper)
    }

    fn new_empty_region_stat(region_id: RegionId, role: RegionRole) -> RegionStat {
        RegionStat {
            id: region_id,
            role,
            rcus: 0,
            wcus: 0,
            approximate_bytes: 0,
            approximate_rows: 0,
            engine: String::new(),
        }
    }

    #[tokio::test]
    async fn test_handle_upgradable_follower() {
        let datanode_id = 1;
        let region_number = 1u32;
        let table_id = 10;
        let region_id = RegionId::new(table_id, region_number);
        let another_region_id = RegionId::new(table_id, region_number + 1);
        let peer = Peer::empty(datanode_id);
        let follower_peer = Peer::empty(datanode_id + 1);
        let table_info = new_test_table_info(table_id, vec![region_number]).into();
        let cluster_id = 1;

        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(peer.clone()),
            follower_peers: vec![follower_peer.clone()],
            ..Default::default()
        }];

        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();

        table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(region_routes),
                HashMap::default(),
            )
            .await
            .unwrap();

        let builder = MetaSrvBuilder::new();
        let metasrv = builder.build().await.unwrap();
        let ctx = &mut metasrv.new_ctx();

        let acc = &mut HeartbeatAccumulator::default();

        acc.stat = Some(Stat {
            cluster_id,
            id: peer.id,
            region_stats: vec![
                new_empty_region_stat(region_id, RegionRole::Follower),
                new_empty_region_stat(another_region_id, RegionRole::Follower),
            ],
            ..Default::default()
        });

        let req = HeartbeatRequest {
            duration_since_epoch: 1234,
            ..Default::default()
        };

        let opening_region_keeper = Arc::new(MemoryRegionKeeper::default());

        let handler = RegionLeaseHandler::new(
            distributed_time_constants::REGION_LEASE_SECS,
            table_metadata_manager.clone(),
            opening_region_keeper.clone(),
        );

        handler.handle(&req, ctx, acc).await.unwrap();

        assert_region_lease(acc, vec![GrantedRegion::new(region_id, RegionRole::Leader)]);
        assert_eq!(acc.inactive_region_ids, HashSet::from([another_region_id]));
        assert_eq!(
            acc.region_lease.as_ref().unwrap().closeable_region_ids,
            vec![another_region_id]
        );

        let acc = &mut HeartbeatAccumulator::default();

        acc.stat = Some(Stat {
            cluster_id,
            id: follower_peer.id,
            region_stats: vec![
                new_empty_region_stat(region_id, RegionRole::Follower),
                new_empty_region_stat(another_region_id, RegionRole::Follower),
            ],
            ..Default::default()
        });

        handler.handle(&req, ctx, acc).await.unwrap();

        assert_eq!(
            acc.region_lease.as_ref().unwrap().lease_seconds,
            distributed_time_constants::REGION_LEASE_SECS
        );

        assert_region_lease(
            acc,
            vec![GrantedRegion::new(region_id, RegionRole::Follower)],
        );
        assert_eq!(acc.inactive_region_ids, HashSet::from([another_region_id]));
        assert_eq!(
            acc.region_lease.as_ref().unwrap().closeable_region_ids,
            vec![another_region_id]
        );

        let opening_region_id = RegionId::new(table_id, region_number + 2);
        let _guard = opening_region_keeper
            .register(follower_peer.id, opening_region_id)
            .unwrap();

        let acc = &mut HeartbeatAccumulator::default();

        acc.stat = Some(Stat {
            cluster_id,
            id: follower_peer.id,
            region_stats: vec![
                new_empty_region_stat(region_id, RegionRole::Follower),
                new_empty_region_stat(another_region_id, RegionRole::Follower),
                new_empty_region_stat(opening_region_id, RegionRole::Follower),
            ],
            ..Default::default()
        });

        handler.handle(&req, ctx, acc).await.unwrap();

        assert_eq!(
            acc.region_lease.as_ref().unwrap().lease_seconds,
            distributed_time_constants::REGION_LEASE_SECS
        );

        assert_region_lease(
            acc,
            vec![
                GrantedRegion::new(region_id, RegionRole::Follower),
                GrantedRegion::new(opening_region_id, RegionRole::Follower),
            ],
        );
        assert_eq!(acc.inactive_region_ids, HashSet::from([another_region_id]));
        assert_eq!(
            acc.region_lease.as_ref().unwrap().closeable_region_ids,
            vec![another_region_id]
        );
    }

    #[tokio::test]

    async fn test_handle_downgradable_leader() {
        let datanode_id = 1;
        let region_number = 1u32;
        let table_id = 10;
        let region_id = RegionId::new(table_id, region_number);
        let another_region_id = RegionId::new(table_id, region_number + 1);
        let no_exist_region_id = RegionId::new(table_id, region_number + 2);
        let peer = Peer::empty(datanode_id);
        let follower_peer = Peer::empty(datanode_id + 1);
        let table_info = new_test_table_info(table_id, vec![region_number]).into();
        let cluster_id = 1;

        let region_routes = vec![
            RegionRoute {
                region: Region::new_test(region_id),
                leader_peer: Some(peer.clone()),
                follower_peers: vec![follower_peer.clone()],
                leader_status: Some(RegionStatus::Downgraded),
            },
            RegionRoute {
                region: Region::new_test(another_region_id),
                leader_peer: Some(peer.clone()),
                ..Default::default()
            },
        ];

        let keeper = new_test_keeper();
        let table_metadata_manager = keeper.table_metadata_manager();

        table_metadata_manager
            .create_table_metadata(
                table_info,
                TableRouteValue::physical(region_routes),
                HashMap::default(),
            )
            .await
            .unwrap();

        let builder = MetaSrvBuilder::new();
        let metasrv = builder.build().await.unwrap();
        let ctx = &mut metasrv.new_ctx();

        let req = HeartbeatRequest {
            duration_since_epoch: 1234,
            ..Default::default()
        };

        let acc = &mut HeartbeatAccumulator::default();

        acc.stat = Some(Stat {
            cluster_id,
            id: peer.id,
            region_stats: vec![
                new_empty_region_stat(region_id, RegionRole::Leader),
                new_empty_region_stat(another_region_id, RegionRole::Leader),
                new_empty_region_stat(no_exist_region_id, RegionRole::Leader),
            ],
            ..Default::default()
        });

        let handler = RegionLeaseHandler::new(
            distributed_time_constants::REGION_LEASE_SECS,
            table_metadata_manager.clone(),
            Default::default(),
        );

        handler.handle(&req, ctx, acc).await.unwrap();

        assert_region_lease(
            acc,
            vec![
                GrantedRegion::new(region_id, RegionRole::Follower),
                GrantedRegion::new(another_region_id, RegionRole::Leader),
            ],
        );
        assert_eq!(acc.inactive_region_ids, HashSet::from([no_exist_region_id]));
    }

    fn assert_region_lease(acc: &HeartbeatAccumulator, expected: Vec<GrantedRegion>) {
        let region_lease = acc.region_lease.as_ref().unwrap().clone();
        let granted: Vec<GrantedRegion> = region_lease
            .regions
            .into_iter()
            .map(Into::into)
            .collect::<Vec<_>>();

        let granted = granted
            .into_iter()
            .map(|region| (region.region_id, region))
            .collect::<HashMap<_, _>>();

        let expected = expected
            .into_iter()
            .map(|region| (region.region_id, region))
            .collect::<HashMap<_, _>>();

        assert_eq!(granted, expected);
    }
}
