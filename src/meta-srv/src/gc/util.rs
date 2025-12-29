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

use common_meta::key::table_route::PhysicalTableRouteValue;
use common_telemetry::warn;
use store_api::storage::RegionId;

use crate::gc::{Peer2Regions, Region2Peers};

pub fn table_route_to_region(
    table_route: &PhysicalTableRouteValue,
    table_regions: &[RegionId],
    region_to_peer: &mut Region2Peers,
    peer_to_regions: &mut Peer2Regions,
) {
    for &region_id in table_regions {
        let mut found = false;

        // Find the region in the table route
        for region_route in &table_route.region_routes {
            if region_route.region.id == region_id
                && let Some(leader_peer) = &region_route.leader_peer
            {
                region_to_peer.insert(
                    region_id,
                    (leader_peer.clone(), region_route.follower_peers.clone()),
                );
                peer_to_regions
                    .entry(leader_peer.clone())
                    .or_default()
                    .insert(region_id);
                found = true;
                break;
            }
        }

        if !found {
            warn!(
                "Failed to find region {} in table route or no leader peer found",
                region_id,
            );
        }
    }
}
