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
use store_api::storage::RegionId;

/// Returns Some(region_id) if it's a inactive leader region.
///
/// It removes a leader region if the `node_id` isn't the corresponding leader peer in `region_routes`.
pub fn inactive_leader_regions(
    node_id: u64,
    region_id: RegionId,
    region_leader_map: &HashMap<u32, &Peer>,
) -> Option<RegionId> {
    if let Some(peer) = region_leader_map.get(&region_id.region_number()) {
        // TODO(weny): treats the leader peer as inactive if it's readonly or downgraded.
        if peer.id == node_id {
            None
        } else {
            Some(region_id)
        }
    } else {
        Some(region_id)
    }
}

/// Returns Some(region_id) if it's a inactive follower region.
///
/// It removes a region if the `node_id` isn't one of the peers in `region_routes`.
pub fn inactive_follower_regions(
    node_id: u64,
    region_id: RegionId,
    region_peer_map: &HashMap<u32, HashSet<u64>>,
) -> Option<RegionId> {
    if let Some(peers) = region_peer_map.get(&region_id.region_number()) {
        if peers.contains(&node_id) {
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
    fn test_inactive_follower_regions() {
        let node_id = 1u64;
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);
        let peer = Peer::empty(node_id);

        let region_peers_map = [(region_number, HashSet::from([node_id]))].into();

        // Should be None, `region_id` is a active region of `peer`.
        assert_eq!(
            None,
            inactive_follower_regions(peer.id, region_id, &region_peers_map)
        );

        // Should be Some(`region_id`), region_followers_map is empty.
        assert_eq!(
            Some(region_id),
            inactive_follower_regions(peer.id, region_id, &Default::default())
        );

        let another_peer = Peer::empty(node_id + 1);

        let region_peers_map = [(region_number, HashSet::from([peer.id, another_peer.id]))].into();

        // Should be None, `region_id` is a active region of `another_peer`.
        assert_eq!(
            None,
            inactive_follower_regions(node_id, region_id, &region_peers_map)
        );
    }

    #[test]
    fn test_inactive_leader_regions() {
        let node_id = 1u64;
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);
        let peer = Peer::empty(node_id);

        let region_leader_map = [(region_number, &peer)].into();

        // Should be None, `region_id` is a active region of `peer`.
        assert_eq!(
            None,
            inactive_leader_regions(node_id, region_id, &region_leader_map)
        );

        // Should be Some(`region_id`), the inactive_leader_regions is empty.
        assert_eq!(
            Some(region_id),
            inactive_leader_regions(node_id, region_id, &Default::default())
        );

        let another_peer = Peer::empty(node_id + 1);
        let region_leader_map = [(region_number, &another_peer)].into();

        // Should be Some(`region_id`), `region_id` is active region of `another_peer`.
        assert_eq!(
            Some(region_id),
            inactive_leader_regions(node_id, region_id, &region_leader_map)
        );
    }
}
