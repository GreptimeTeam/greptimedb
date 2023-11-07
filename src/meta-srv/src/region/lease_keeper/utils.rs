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

use common_meta::peer::Peer;
use common_meta::rpc::router::{
    convert_to_region_leader_map, convert_to_region_leader_status_map, convert_to_region_peer_map,
    RegionRoute, RegionStatus,
};
use store_api::storage::{RegionId, RegionNumber};

/// Returns Some(region_id) if it's not a leader region in `region_route`.
///
/// It removes a leader region if its peer(`node_id`) isn't the corresponding leader peer in `region_routes`.
pub fn closable_leader_region(
    node_id: u64,
    region_id: RegionId,
    region_leader_map: &HashMap<RegionNumber, &Peer>,
) -> Option<RegionId> {
    let region_number = region_id.region_number();
    if let Some(peer) = region_leader_map.get(&region_number) {
        if peer.id == node_id {
            None
        } else {
            Some(region_id)
        }
    } else {
        Some(region_id)
    }
}

/// Returns Some(region_id) if its peer(`node_id`) a downgrade leader region peer in `region_route`.
pub fn downgradable_leader_regions(
    node_id: u64,
    region_id: RegionId,
    region_leader_map: &HashMap<RegionNumber, &Peer>,
    region_leader_status: &HashMap<RegionNumber, RegionStatus>,
) -> Option<RegionId> {
    let region_number = region_id.region_number();
    let leader_status = region_leader_status.get(&region_number);
    let downgraded = matches!(leader_status, Some(RegionStatus::Downgraded));

    if let Some(peer) = region_leader_map.get(&region_number) {
        if peer.id == node_id && downgraded {
            Some(region_id)
        } else {
            None
        }
    } else {
        None
    }
}

/// Returns upgradable regions, and closable regions.
///
/// Upgradable regions:
/// - Region's peer(`datanode_id`) is the corresponding leader peer in `region_routes`.
///
/// Closable regions:
/// - Region's peer(`datanode_id`) isn't the corresponding leader/follower peer in `region_routes`.
pub fn find_staled_follower_regions(
    datanode_id: u64,
    datanode_regions: &[RegionId],
    region_routes: &[RegionRoute],
) -> (HashSet<RegionId>, HashSet<RegionId>) {
    let region_leader_map = convert_to_region_leader_map(region_routes);
    let region_leader_status_map = convert_to_region_leader_status_map(region_routes);
    let region_peer_map = convert_to_region_peer_map(region_routes);

    let (upgradable, closable): (HashSet<Option<RegionId>>, HashSet<Option<RegionId>>) =
        datanode_regions
            .iter()
            .map(|region_id| {
                (
                    upgradable_follower_region(
                        datanode_id,
                        *region_id,
                        &region_leader_map,
                        &region_leader_status_map,
                    ),
                    closable_region(datanode_id, *region_id, &region_peer_map),
                )
            })
            .unzip();

    let upgradable = upgradable.into_iter().flatten().collect();
    let closable = closable.into_iter().flatten().collect();

    (upgradable, closable)
}

/// Returns Some(region) if its peer(`node_id`) a leader region peer in `region_routes`.
pub fn upgradable_follower_region(
    node_id: u64,
    region_id: RegionId,
    region_leader_map: &HashMap<RegionNumber, &Peer>,
    region_leader_status: &HashMap<RegionNumber, RegionStatus>,
) -> Option<RegionId> {
    let region_number = region_id.region_number();
    let leader_status = region_leader_status.get(&region_number);
    let downgraded = matches!(leader_status, Some(RegionStatus::Downgraded));

    if let Some(peer) = region_leader_map.get(&region_number) {
        if peer.id == node_id && !downgraded {
            Some(region_id)
        } else {
            None
        }
    } else {
        None
    }
}

/// Returns Some(region) if its peer(`node_id) is't a leader or follower region peer in `region_routes`.
pub fn closable_region(
    node_id: u64,
    region_id: RegionId,
    region_peer_map: &HashMap<RegionNumber, HashSet<u64>>,
) -> Option<RegionId> {
    if let Some(set) = region_peer_map.get(&region_id.region_number()) {
        if set.get(&node_id).is_some() {
            None
        } else {
            Some(region_id)
        }
    } else {
        Some(region_id)
    }
}

#[cfg(test)]
mod tests {

    use common_meta::peer::Peer;
    use store_api::storage::RegionId;

    use super::*;

    #[test]
    fn test_closable_leader_region() {
        let datanode_id = 1u64;
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);
        let peer = Peer::empty(datanode_id);

        let region_leader_map = [(region_number, &peer)].into();

        // Should be None, `region_id` is an active region of `peer`.
        assert_eq!(
            None,
            closable_leader_region(datanode_id, region_id, &region_leader_map,)
        );

        // Should be Some(`region_id`), incorrect datanode_id.
        assert_eq!(
            Some(region_id),
            closable_leader_region(datanode_id + 1, region_id, &region_leader_map,)
        );

        // Should be Some(`region_id`), the inactive_leader_regions is empty.
        assert_eq!(
            Some(region_id),
            closable_leader_region(datanode_id, region_id, &Default::default(),)
        );

        let another_peer = Peer::empty(datanode_id + 1);
        let region_leader_map = [(region_number, &another_peer)].into();

        // Should be Some(`region_id`), `region_id` is active region of `another_peer`.
        assert_eq!(
            Some(region_id),
            closable_leader_region(datanode_id, region_id, &region_leader_map,)
        );
    }

    #[test]
    fn test_downgradable_region() {
        let datanode_id = 1u64;
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);
        let peer = Peer::empty(datanode_id);

        let region_leader_map = [(region_number, &peer)].into();
        let region_leader_status_map = [(region_number, RegionStatus::Downgraded)].into();

        // Should be Some(region_id), `region_id` is a downgraded leader region.
        assert_eq!(
            Some(region_id),
            downgradable_leader_regions(
                datanode_id,
                region_id,
                &region_leader_map,
                &region_leader_status_map
            )
        );

        // Should be None, `region_id` is a leader region.
        assert_eq!(
            None,
            downgradable_leader_regions(
                datanode_id,
                region_id,
                &region_leader_map,
                &Default::default(),
            )
        );

        // Should be None, incorrect datanode_id.
        assert_eq!(
            None,
            downgradable_leader_regions(
                datanode_id + 1,
                region_id,
                &region_leader_map,
                &region_leader_status_map
            )
        );

        // Should be None, incorrect datanode_id.
        assert_eq!(
            None,
            downgradable_leader_regions(
                datanode_id + 1,
                region_id,
                &region_leader_map,
                &Default::default(),
            )
        );
    }

    #[test]
    fn test_closable_follower_region() {
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);
        let another_region_id = RegionId::from_u64(region_number as u64 + 1);
        let region_peer_map = [(region_number, HashSet::from([1, 2, 3]))].into();

        // Should be None.
        assert_eq!(None, closable_region(1, region_id, &region_peer_map));

        // Should be Some(`region_id`), incorrect `datanode_id`.
        assert_eq!(
            Some(region_id),
            closable_region(4, region_id, &region_peer_map)
        );

        // Should be Some(`another_region_id`), `another_region_id` doesn't exist.
        assert_eq!(
            Some(another_region_id),
            closable_region(1, another_region_id, &region_peer_map)
        );

        // Should be Some(`another_region_id`), `another_region_id` doesn't exist, incorrect `datanode_id`.
        assert_eq!(
            Some(another_region_id),
            closable_region(4, another_region_id, &region_peer_map)
        );
    }

    #[test]
    fn test_upgradable_follower_region() {
        let datanode_id = 1u64;
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);
        let another_region_id = RegionId::from_u64(region_number as u64 + 1);
        let peer = Peer::empty(datanode_id);

        let region_leader_map = [(region_number, &peer)].into();
        let region_leader_status = HashMap::new();

        // Should be Some(region_id), `region_id` is a leader region.
        assert_eq!(
            Some(region_id),
            upgradable_follower_region(
                datanode_id,
                region_id,
                &region_leader_map,
                &region_leader_status
            )
        );

        let downgraded_leader = [(region_number, RegionStatus::Downgraded)].into();

        // Should be None, `region_id` is a downgraded leader region.
        assert_eq!(
            None,
            upgradable_follower_region(
                datanode_id,
                region_id,
                &region_leader_map,
                &downgraded_leader
            )
        );

        // Should be None, incorrect `datanode_id`.
        assert_eq!(
            None,
            upgradable_follower_region(
                datanode_id + 1,
                region_id,
                &region_leader_map,
                &region_leader_status
            )
        );

        // Should be None, incorrect `datanode_id`, `another_region_id` doesn't exist.
        assert_eq!(
            None,
            upgradable_follower_region(
                datanode_id,
                another_region_id,
                &region_leader_map,
                &region_leader_status
            )
        );

        // Should be None, incorrect `datanode_id`, `another_region_id` doesn't exist, incorrect `datanode_id`.
        assert_eq!(
            None,
            upgradable_follower_region(
                datanode_id + 1,
                another_region_id,
                &region_leader_map,
                &region_leader_status
            )
        );
    }
}
