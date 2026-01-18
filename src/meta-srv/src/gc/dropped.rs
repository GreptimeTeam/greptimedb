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

//! Dropped region GC handling.
//!
//! This module handles garbage collection for "dropped regions". A dropped region is:
//! 1. Recorded in `table_repart` metadata (the only place that still remembers it)
//! 2. No longer exists in `table_route` metadata (removed from region routing)
//! 3. No longer present in any datanode's heartbeat (physically closed)
//!
//! This differs from "active regions" which exist in both metadata and heartbeats.
//! The `table_repart` entry serves as a tombstone that tracks which regions were
//! merged/split and need their files cleaned up.

use std::collections::{HashMap, HashSet};
use std::time::Instant;

use common_meta::key::table_repart::TableRepartValue;
use common_meta::peer::Peer;
use common_telemetry::{debug, warn};
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::error::Result;
use crate::gc::Region2Peers;
use crate::gc::ctx::SchedulerCtx;
use crate::gc::options::GcSchedulerOptions;
use crate::gc::tracker::RegionGcTracker;

/// Information about a dropped region ready for GC.
#[derive(Debug, Clone)]
pub(crate) struct DroppedRegionInfo {
    pub region_id: RegionId,
    pub table_id: TableId,
    /// The destination regions this region was split into (if any).
    #[allow(unused)]
    pub dst_regions: HashSet<RegionId>,
}

/// Result of collecting and assigning dropped regions to peers.
#[derive(Debug, Default)]
pub(crate) struct DroppedRegionAssignment {
    /// Dropped regions grouped by the peer responsible for GC.
    pub regions_by_peer: HashMap<Peer, Vec<DroppedRegionInfo>>,
    /// Regions that require full file listing (always true for dropped regions).
    pub force_full_listing: HashMap<Peer, HashSet<RegionId>>,
    /// Override region routes for dropped regions (they have no active route).
    pub region_routes_override: HashMap<Peer, Region2Peers>,
}

/// Collector for dropped regions that need GC.
pub(crate) struct DroppedRegionCollector<'a> {
    ctx: &'a dyn SchedulerCtx,
    config: &'a GcSchedulerOptions,
    tracker: &'a tokio::sync::Mutex<RegionGcTracker>,
}

impl<'a> DroppedRegionCollector<'a> {
    pub fn new(
        ctx: &'a dyn SchedulerCtx,
        config: &'a GcSchedulerOptions,
        tracker: &'a tokio::sync::Mutex<RegionGcTracker>,
    ) -> Self {
        Self {
            ctx,
            config,
            tracker,
        }
    }

    /// Collect and assign dropped regions for GC.
    ///
    /// This method:
    /// 1. Identifies regions in repartition metadata that are no longer active
    /// 2. Filters out regions still in cooldown period
    /// 3. Assigns each dropped region to an available peer for cleanup
    pub async fn collect_and_assign(
        &self,
        active_region_ids: &HashSet<RegionId>,
    ) -> Result<DroppedRegionAssignment> {
        let table_reparts = self.ctx.get_table_reparts().await?;
        let dropped_regions = self.identify_dropped_regions(table_reparts, active_region_ids);

        if dropped_regions.is_empty() {
            return Ok(DroppedRegionAssignment::default());
        }

        let dropped_regions = self.filter_by_cooldown(dropped_regions).await;

        if dropped_regions.is_empty() {
            return Ok(DroppedRegionAssignment::default());
        }

        self.assign_to_peers(dropped_regions).await
    }

    /// Identify dropped regions: regions in `table_repart` but not in heartbeats.
    /// The `assign_to_peers` step later verifies they're also absent from `table_route`.
    fn identify_dropped_regions(
        &self,
        table_reparts: Vec<(TableId, TableRepartValue)>,
        active_region_ids: &HashSet<RegionId>,
    ) -> HashMap<TableId, HashMap<RegionId, HashSet<RegionId>>> {
        let mut dropped_regions: HashMap<TableId, HashMap<RegionId, HashSet<RegionId>>> =
            HashMap::new();

        for (table_id, repart) in table_reparts {
            if repart.src_to_dst.is_empty() {
                continue;
            }

            let entry = dropped_regions.entry(table_id).or_default();
            for (src_region, dst_regions) in repart.src_to_dst {
                if !active_region_ids.contains(&src_region) {
                    entry.insert(src_region, dst_regions.into_iter().collect());
                }
            }
        }

        dropped_regions.retain(|_, regions| !regions.is_empty());
        dropped_regions
    }

    /// Filter out dropped regions that are still in their cooldown period.
    async fn filter_by_cooldown(
        &self,
        dropped_regions: HashMap<TableId, HashMap<RegionId, HashSet<RegionId>>>,
    ) -> HashMap<TableId, HashMap<RegionId, HashSet<RegionId>>> {
        let now = Instant::now();
        let tracker = self.tracker.lock().await;
        let mut filtered = HashMap::new();

        for (table_id, regions) in dropped_regions {
            let mut kept = HashMap::new();
            for (region_id, dst_regions) in regions {
                if let Some(gc_info) = tracker.get(&region_id) {
                    let elapsed = now.saturating_duration_since(gc_info.last_gc_time);
                    if elapsed < self.config.gc_cooldown_period {
                        debug!("Skipping dropped region {} due to cooldown", region_id);
                        continue;
                    }
                }
                kept.insert(region_id, dst_regions);
            }

            if !kept.is_empty() {
                filtered.insert(table_id, kept);
            }
        }

        filtered
    }

    /// Assign dropped regions to available peers for GC execution.
    ///
    /// For dropped regions, we need to:
    /// 1. Find an available peer from the table's current route
    /// 2. Use consistent hashing (region_id % peer_count) for load distribution
    /// 3. Create route overrides since dropped regions have no active route
    async fn assign_to_peers(
        &self,
        dropped_regions: HashMap<TableId, HashMap<RegionId, HashSet<RegionId>>>,
    ) -> Result<DroppedRegionAssignment> {
        let mut assignment = DroppedRegionAssignment::default();

        for (table_id, regions) in dropped_regions {
            let (phy_table_id, table_route) = match self.ctx.get_table_route(table_id).await {
                Ok(route) => route,
                Err(e) => {
                    warn!(
                        "Failed to get table route for table {}: {}, skipping dropped regions",
                        table_id, e
                    );
                    continue;
                }
            };

            if phy_table_id != table_id {
                continue;
            }

            let active_region_ids: HashSet<RegionId> = table_route
                .region_routes
                .iter()
                .map(|r| r.region.id)
                .collect();

            let mut leader_peers: Vec<Peer> = table_route
                .region_routes
                .iter()
                .filter_map(|r| r.leader_peer.clone())
                .collect();
            leader_peers.sort_by_key(|peer| peer.id);
            leader_peers.dedup_by_key(|peer| peer.id);

            if leader_peers.is_empty() {
                warn!(
                    "No leader peers found for table {}, skipping dropped regions",
                    table_id
                );
                continue;
            }

            for (region_id, dst_regions) in regions {
                if active_region_ids.contains(&region_id) {
                    debug!(
                        "Skipping dropped region {} since it still exists in table route",
                        region_id
                    );
                    continue;
                }

                let selected_idx = (region_id.as_u64() as usize) % leader_peers.len();
                let peer = leader_peers[selected_idx].clone();

                let info = DroppedRegionInfo {
                    region_id,
                    table_id,
                    dst_regions,
                };

                assignment
                    .regions_by_peer
                    .entry(peer.clone())
                    .or_default()
                    .push(info);

                assignment
                    .force_full_listing
                    .entry(peer.clone())
                    .or_default()
                    .insert(region_id);

                assignment
                    .region_routes_override
                    .entry(peer.clone())
                    .or_default()
                    .insert(region_id, (peer, Vec::new()));
            }
        }

        Ok(assignment)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dropped_region_info() {
        let info = DroppedRegionInfo {
            region_id: RegionId::new(1, 1),
            table_id: 1,
            dst_regions: HashSet::new(),
        };
        assert_eq!(info.region_id, RegionId::new(1, 1));
        assert_eq!(info.table_id, 1);
    }
}
