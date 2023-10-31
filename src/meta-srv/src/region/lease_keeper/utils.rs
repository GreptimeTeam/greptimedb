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

use std::collections::HashMap;

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

#[cfg(test)]
mod tests {

    use common_meta::peer::Peer;
    use store_api::storage::RegionId;

    use super::*;

    #[test]
    fn test_inactive_leader_regions() {
        let datanode_id = 1u64;
        let region_number = 1u32;
        let region_id = RegionId::from_u64(region_number as u64);
        let peer = Peer::empty(datanode_id);

        let region_leader_map = [(region_number, &peer)].into();

        // Should be None, `region_id` is a active region of `peer`.
        assert_eq!(
            None,
            inactive_leader_regions(datanode_id, region_id, &region_leader_map)
        );

        // Should be Some(`region_id`), the inactive_leader_regions is empty.
        assert_eq!(
            Some(region_id),
            inactive_leader_regions(datanode_id, region_id, &Default::default())
        );

        let another_peer = Peer::empty(datanode_id + 1);
        let region_leader_map = [(region_number, &another_peer)].into();

        // Should be Some(`region_id`), `region_id` is active region of `another_peer`.
        assert_eq!(
            Some(region_id),
            inactive_leader_regions(datanode_id, region_id, &region_leader_map)
        );
    }
}
