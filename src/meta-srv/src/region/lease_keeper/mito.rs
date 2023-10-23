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
    convert_to_region_leader_map, convert_to_region_peers_map, RegionRoute,
};
use store_api::region_engine::RegionRole;
use store_api::storage::RegionId;

use crate::region::lease_keeper::utils::{inactive_follower_regions, inactive_leader_regions};

/// Retains active mito regions(`datanode_regions`), returns inactive regions.
///
/// It removes a leader region if the `node_id` isn't the corresponding leader peer in `region_routes`.
///
/// It removes a follower region if the `node_id` isn't one of the peers in `region_routes`.
pub fn retain_active_regions(
    node_id: u64,
    datanode_regions: &mut Vec<(RegionId, RegionRole)>,
    region_routes: &[RegionRoute],
) -> HashSet<RegionId> {
    let region_leader_map = convert_to_region_leader_map(region_routes);
    let region_peers_map = convert_to_region_peers_map(region_routes);

    let inactive_region_ids = datanode_regions
        .clone()
        .into_iter()
        .filter_map(|(region_id, role)| match role {
            RegionRole::Follower => {
                inactive_follower_regions(node_id, region_id, &region_peers_map)
            }
            RegionRole::Leader => inactive_leader_regions(node_id, region_id, &region_leader_map),
        })
        .collect::<HashSet<_>>();

    let _ = datanode_regions
        .extract_if(|(region_id, _)| inactive_region_ids.contains(region_id))
        .collect::<Vec<_>>();

    inactive_region_ids
}

#[cfg(test)]
mod tests {

    use common_meta::peer::Peer;
    use common_meta::rpc::router::{Region, RegionRoute};
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;

    use crate::region::lease_keeper::mito::retain_active_regions;

    #[test]
    fn test_retain_active_regions() {
        let node_id = 1u64;
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);
        let peer = Peer::empty(node_id);
        let another_peer = Peer::empty(node_id + 1);

        let datanode_regions = vec![(region_id, RegionRole::Leader)];
        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(peer.clone()),
            ..Default::default()
        }];

        // `inactive_regions` should be empty, `region_id` is a active leader region of the `peer`
        let inactive_regions =
            retain_active_regions(node_id, &mut datanode_regions.clone(), &region_routes);

        assert!(inactive_regions.is_empty());

        let mut retained_active_regions = datanode_regions.clone();

        // `inactive_regions` should be vec![`region_id`];
        let inactive_regions = retain_active_regions(node_id, &mut retained_active_regions, &[]);

        assert_eq!(inactive_regions.len(), 1);
        assert!(inactive_regions.contains(&region_id));
        assert!(retained_active_regions.is_empty());

        let region_routes = vec![RegionRoute {
            region: Region::new_test(region_id),
            leader_peer: Some(another_peer.clone()),
            follower_peers: vec![peer.clone()],
        }];

        let mut retained_active_regions = datanode_regions.clone();

        // `inactive_regions` should be vec![`region_id`], `region_id` is RegionRole::Leader.
        let inactive_regions =
            retain_active_regions(node_id, &mut retained_active_regions, &region_routes);

        assert_eq!(inactive_regions.len(), 1);
        assert!(inactive_regions.contains(&region_id));
        assert!(retained_active_regions.is_empty());

        // `inactive_regions` should be empty, `region_id` is a active follower region of the `peer`.
        let datanode_regions = vec![(region_id, RegionRole::Follower)];
        let inactive_regions =
            retain_active_regions(node_id, &mut datanode_regions.clone(), &region_routes);
        assert!(inactive_regions.is_empty());

        let another_region_id = RegionId::from_u64(region_number as u64 + 1);

        let region_routes = vec![
            RegionRoute {
                region: Region::new_test(region_id),
                leader_peer: Some(peer.clone()),
                ..Default::default()
            },
            RegionRoute {
                region: Region::new_test(another_region_id),
                leader_peer: None,
                follower_peers: vec![peer],
            },
        ];

        let datanode_regions = vec![
            (region_id, RegionRole::Leader),
            (another_region_id, RegionRole::Follower),
        ];

        let inactive_regions =
            retain_active_regions(node_id, &mut datanode_regions.clone(), &region_routes);

        assert!(inactive_regions.is_empty());

        // `inactive_regions` should be vec![another_region_id].
        let datanode_regions = vec![
            (another_region_id, RegionRole::Leader),
            (region_id, RegionRole::Follower),
        ];

        let mut retained_active_regions = datanode_regions.clone();

        let inactive_regions =
            retain_active_regions(node_id, &mut retained_active_regions, &region_routes);

        assert_eq!(inactive_regions.len(), 1);
        assert!(inactive_regions.contains(&another_region_id));

        assert_eq!(retained_active_regions.len(), 1);
        assert!(retained_active_regions.contains(&(region_id, RegionRole::Follower)));
    }
}
