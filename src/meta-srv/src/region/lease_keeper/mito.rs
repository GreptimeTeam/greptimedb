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

use common_meta::rpc::router::{
    convert_to_region_leader_map, convert_to_region_leader_status_map, RegionRoute,
};
use store_api::storage::RegionId;

use super::utils::downgradable_leader_regions;
use crate::region::lease_keeper::utils::closable_leader_region;

/// Returns Downgradable regions and Closable regions.
///
/// - Downgradable regions:
/// Region's peer(`datanode_id`) is the corresponding downgraded leader peer in `region_routes`.
///
/// - Closable regions:
/// Region's peer(`datanode_id`) isn't the corresponding leader peer in `region_routes`.
///   - Expected as [RegionRole::Follower](store_api::region_engine::RegionRole::Follower) regions.
///   - Unexpected [RegionRole::Leader](store_api::region_engine::RegionRole::Leader) regions.
pub fn find_staled_leader_regions(
    datanode_id: u64,
    datanode_regions: &[RegionId],
    region_routes: &[RegionRoute],
) -> (HashSet<RegionId>, HashSet<RegionId>) {
    let region_leader_map = convert_to_region_leader_map(region_routes);
    let region_leader_status_map = convert_to_region_leader_status_map(region_routes);

    let (downgradable, closable): (HashSet<_>, HashSet<_>) = datanode_regions
        .iter()
        .map(|region_id| {
            (
                downgradable_leader_regions(
                    datanode_id,
                    *region_id,
                    &region_leader_map,
                    &region_leader_status_map,
                ),
                closable_leader_region(datanode_id, *region_id, &region_leader_map),
            )
        })
        .unzip();

    let downgradable = downgradable.into_iter().flatten().collect();
    let closable = closable.into_iter().flatten().collect();

    (downgradable, closable)
}

#[cfg(test)]
mod tests {

    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::storage::RegionId;

    use crate::region::lease_keeper::mito::find_staled_leader_regions;

    #[test]
    fn test_find_staled_regions() {
        let datanode_id = 1u64;
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);
        let peer = Peer::empty(datanode_id);
        let another_peer = Peer::empty(datanode_id + 1);

        let datanode_regions = vec![region_id];
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(peer.clone()),
            ..Default::default()
        }];

        // Grants lease.
        // `closable` should be empty, `region_id` is a active leader region of the `peer`
        let (downgradable, closable) =
            find_staled_leader_regions(datanode_id, &datanode_regions, &region_routes);

        assert!(closable.is_empty());
        assert!(downgradable.is_empty());

        // Unexpected Leader region.
        // `closable` should be vec![`region_id`];
        let (downgradable, closable) =
            find_staled_leader_regions(datanode_id, &datanode_regions, &[]);

        assert_eq!(closable.len(), 1);
        assert!(closable.contains(&region_id));
        assert!(downgradable.is_empty());

        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(another_peer.clone()),
            follower_peers: vec![peer.clone()],
            leader_status: None,
        }];

        let retained_active_regions = datanode_regions.clone();

        // Expected as Follower region.
        // `closable` should be vec![`region_id`], `region_id` is RegionRole::Leader.
        let (downgradable, closable) =
            find_staled_leader_regions(datanode_id, &retained_active_regions, &region_routes);

        assert!(downgradable.is_empty());
        assert_eq!(closable.len(), 1);
        assert!(closable.contains(&region_id));
    }
}
