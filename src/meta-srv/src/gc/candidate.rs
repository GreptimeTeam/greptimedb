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
use std::time::Instant;

use common_meta::datanode::{RegionManifestInfo, RegionStat};
use common_telemetry::{debug, info};
use ordered_float::OrderedFloat;
use store_api::region_engine::RegionRole;
use store_api::storage::RegionId;
use table::metadata::TableId;

use crate::error::Result;
use crate::gc::scheduler::GcScheduler;

/// Represents a region candidate for GC with its priority score.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GcCandidate {
    pub(crate) region_id: RegionId,
    pub(crate) score: OrderedFloat<f64>,
    pub(crate) region_stat: RegionStat,
}

impl GcCandidate {
    fn new(region_id: RegionId, score: f64, region_stat: RegionStat) -> Self {
        Self {
            region_id,
            score: OrderedFloat(score),
            region_stat,
        }
    }

    #[allow(unused)]
    fn score_f64(&self) -> f64 {
        self.score.into_inner()
    }
}

impl GcScheduler {
    /// Calculate GC priority score for a region based on various metrics.
    fn calculate_gc_score(&self, region_stat: &RegionStat) -> f64 {
        let sst_count_score = region_stat.sst_num as f64 * self.config.sst_count_weight;

        let file_remove_cnt_score = match &region_stat.region_manifest {
            RegionManifestInfo::Mito {
                file_removed_cnt, ..
            } => *file_removed_cnt as f64 * self.config.file_removed_count_weight,
            // Metric engine doesn't have file_removal_rate, also this should be unreachable since metrics engine doesn't support gc
            RegionManifestInfo::Metric { .. } => 0.0,
        };

        sst_count_score + file_remove_cnt_score
    }

    /// Filter and score regions that are candidates for GC, grouped by table.
    ///
    /// `regions_per_table_threshold` is documented as the *maximum* number of
    /// regions selected per table, so a table gets at most `N = threshold` seats and
    /// every candidate is allocated inside that budget:
    ///
    /// 1. **The first `N - 1` seats go by score** — the best scoring eligible
    ///    regions, highest score first and region id only to make equal scores
    ///    deterministic, i.e. the original behaviour. A higher score is never
    ///    displaced, so "the tables with the most garbage are collected first" still
    ///    holds.
    /// 2. **The last seat goes by staleness** — the eligible region whose last GC
    ///    (`RegionGcInfo::last_gc_time`) is the oldest, with `None` — never GC'd —
    ///    treated as the oldest of all; equal ages fall back to the higher score and
    ///    then to the region id. The score-seated regions are excluded from that
    ///    pool, so the two groups are disjoint by construction.
    ///
    /// `N == 1` therefore degenerates to "always the least recently dispatched
    /// region": there is no score seat left and the single seat is the staleness
    /// one. When a table has fewer than `N` eligible regions, all of them are
    /// selected.
    ///
    /// The staleness seat is what keeps selection bounded: with
    /// `gc_cooldown_period` equal to the ticker interval the score seats alone
    /// re-select the same best-scoring regions whenever their cooldown clears, so a
    /// region ranked below them could be starved forever even though its own listing
    /// state is unboundedly stale.
    ///
    /// `last_gc_time` is the clock that makes that seat a rotation rather than a
    /// fixed second slate: `Self::update_full_listing_time` moves it forward after
    /// every dispatch attempt, fast listing included. The seated region is therefore
    /// the least recently dispatched one, and dispatching it sends it to the back of
    /// the queue, so the seat walks the regions ranked below the score seats in
    /// least-recently-used order. With `R` equally eligible regions ranked by score
    /// and `N` seats, each tick seats at most one new region of the tail while the
    /// score seats keep their `N - 1` places, so the region at rank `R` is dispatched
    /// within `1 + max(0, R - N)` ticks of first becoming eligible.
    /// (`last_full_listing_time` would not rotate — it only advances on a full
    /// listing, i.e. once per `full_file_listing_interval`, so a fast-listing steady
    /// state would seat the same regions every tick.)
    ///
    /// Both groups share the same eligibility filter below (leader, size, cooldown,
    /// positive score), so a staleness-seated candidate is subject to exactly the
    /// same cooldown and the same fast/full listing decision as a score-seated one.
    ///
    /// Eligibility is not the same thing as dispatchability: a region can report
    /// itself as a leader in its statistics while its table route carries no
    /// `leader_peer`, and such a region is dropped at aggregation time. Because the
    /// staleness key above only advances on an actual dispatch, a region that is
    /// selected but never dispatched would keep the staleness seat forever, so the
    /// caller (`GcScheduler::restrict_to_dispatchable_regions`) removes the regions
    /// the route cannot reach *before* they enter this pool.
    ///
    /// The `region_gc_tracker` is read here rather than passed in: the selector
    /// already locks it to apply the cooldown, so a region's ordering key is
    /// available without widening the signature or adding a second pass over the
    /// tracker.
    pub(crate) async fn select_gc_candidates(
        &self,
        table_to_region_stats: &HashMap<TableId, Vec<RegionStat>>,
    ) -> Result<HashMap<TableId, Vec<GcCandidate>>> {
        let mut table_candidates: HashMap<TableId, Vec<GcCandidate>> = HashMap::new();
        let now = Instant::now();
        let threshold = self.config.regions_per_table_threshold;

        for (table_id, region_stats) in table_to_region_stats {
            // Eligible regions paired with the staleness ordering key: the last GC
            // of the region, with `None` meaning "never GC'd".
            let mut candidates: Vec<(Option<Instant>, GcCandidate)> = Vec::new();
            let tracker = self.region_gc_tracker.lock().await;

            for region_stat in region_stats {
                if region_stat.role != RegionRole::Leader {
                    continue;
                }

                // Skip regions that are too small
                if region_stat.approximate_bytes < self.config.min_region_size_threshold {
                    continue;
                }

                // Skip regions that are in cooldown period
                if let Some(gc_info) = tracker.get(&region_stat.id)
                    && now.saturating_duration_since(gc_info.last_gc_time)
                        < self.config.gc_cooldown_period
                {
                    debug!("Skipping region {} due to cooldown", region_stat.id);
                    continue;
                }

                let score = self.calculate_gc_score(region_stat);

                debug!(
                    "Region {} (table {}) has GC score {:.4}",
                    region_stat.id, table_id, score
                );

                // Only consider regions with a meaningful score
                if score > 0.0 {
                    let last_gc_time = tracker
                        .get(&region_stat.id)
                        .map(|gc_info| gc_info.last_gc_time);
                    candidates.push((
                        last_gc_time,
                        GcCandidate::new(region_stat.id, score, region_stat.clone()),
                    ));
                }
            }
            drop(tracker);

            // Score order: highest score first, region id only to make equal scores
            // deterministic.
            candidates.sort_by(|a, b| {
                b.1.score
                    .cmp(&a.1.score)
                    .then_with(|| a.1.region_id.cmp(&b.1.region_id))
            });
            // The score seats are the best `threshold - 1` regions; everything past
            // them competes for the single staleness seat, so the total never exceeds
            // `regions_per_table_threshold`.
            let score_seats = threshold.saturating_sub(1).min(candidates.len());
            let mut stale_pool = candidates.split_off(score_seats);

            // Staleness order: oldest GC first, `None` (never GC'd) beating every
            // dispatched region; score and id only break ties so the sweep is
            // deterministic.
            stale_pool.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| b.1.score.cmp(&a.1.score))
                    .then_with(|| a.1.region_id.cmp(&b.1.region_id))
            });

            let score_selected = candidates.len();
            let mut selected: Vec<GcCandidate> = candidates.into_iter().map(|(_, c)| c).collect();
            // The staleness seat only exists when a seat is actually left over, i.e.
            // `threshold >= 1`, and when the pool below the score seats is not empty.
            let stale_seat = if score_seats < threshold {
                stale_pool.into_iter().next()
            } else {
                None
            };
            let stale_selected = usize::from(stale_seat.is_some());
            selected.extend(stale_seat.map(|(_, candidate)| candidate));

            if !selected.is_empty() {
                info!(
                    "Selected {} of at most {} GC candidate seats for table {} ({} by score, \
                     {} least recently GC'd)",
                    selected.len(),
                    threshold,
                    table_id,
                    score_selected,
                    stale_selected,
                );
                table_candidates.insert(*table_id, selected);
            }
        }

        info!(
            "Selected GC candidates for {} tables",
            table_candidates.len()
        );
        Ok(table_candidates)
    }
}
