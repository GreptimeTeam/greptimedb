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
use std::time::Instant;

use common_telemetry::info;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::gc::scheduler::GcScheduler;

/// Tracks GC timing information for a region.
#[derive(Debug, Clone)]
pub(crate) struct RegionGcInfo {
    /// Last time a regular GC was performed on this region.
    pub(crate) last_gc_time: Instant,
    /// Last time a full file listing GC was performed on this region.
    pub(crate) last_full_listing_time: Option<Instant>,
}

impl RegionGcInfo {
    pub(crate) fn new(last_gc_time: Instant) -> Self {
        Self {
            last_gc_time,
            last_full_listing_time: None,
        }
    }
}

/// Tracks the last GC time for regions to implement cooldown.
pub(crate) type RegionGcTracker = HashMap<RegionId, RegionGcInfo>;

impl GcScheduler {
    /// Clean up stale entries from the region GC tracker if enough time has passed.
    /// This removes entries for regions that no longer exist in the current table routes.
    pub(crate) async fn cleanup_tracker_if_needed(&self) -> Result<()> {
        let mut last_cleanup = *self.last_tracker_cleanup.lock().await;
        let now = Instant::now();

        // Check if enough time has passed since last cleanup
        if now.saturating_duration_since(last_cleanup) < self.config.tracker_cleanup_interval {
            return Ok(());
        }

        info!("Starting region GC tracker cleanup");
        let cleanup_start = Instant::now();

        // Get all current region IDs from table routes
        let table_to_region_stats = self.ctx.get_table_to_region_stats().await?;
        let mut current_regions = HashSet::new();
        for region_stats in table_to_region_stats.values() {
            for region_stat in region_stats {
                current_regions.insert(region_stat.id);
            }
        }

        // Remove stale entries from tracker
        let mut tracker = self.region_gc_tracker.lock().await;
        let initial_count = tracker.len();
        tracker.retain(|region_id, _| current_regions.contains(region_id));
        let removed_count = initial_count - tracker.len();

        *self.last_tracker_cleanup.lock().await = now;

        info!(
            "Completed region GC tracker cleanup: removed {} stale entries out of {} total (retained {}). Duration: {:?}",
            removed_count,
            initial_count,
            tracker.len(),
            cleanup_start.elapsed()
        );

        Ok(())
    }

    /// Determine if full file listing should be used for a region based on the last full listing time.
    pub(crate) async fn should_use_full_listing(&self, region_id: RegionId) -> bool {
        let gc_tracker = self.region_gc_tracker.lock().await;
        let now = Instant::now();

        if let Some(gc_info) = gc_tracker.get(&region_id) {
            if let Some(last_full_listing) = gc_info.last_full_listing_time {
                let elapsed = now.saturating_duration_since(last_full_listing);
                elapsed >= self.config.full_file_listing_interval
            } else {
                // Never did full listing for this region, do it now
                true
            }
        } else {
            // First time GC for this region, do full listing
            true
        }
    }

    pub(crate) async fn update_full_listing_time(
        &self,
        region_id: RegionId,
        did_full_listing: bool,
    ) {
        let mut gc_tracker = self.region_gc_tracker.lock().await;
        let now = Instant::now();

        gc_tracker
            .entry(region_id)
            .and_modify(|info| {
                if did_full_listing {
                    info.last_full_listing_time = Some(now);
                }
                info.last_gc_time = now;
            })
            .or_insert_with(|| RegionGcInfo {
                last_gc_time: now,
                // prevent need to full listing on the first GC
                last_full_listing_time: Some(now),
            });
    }
}
