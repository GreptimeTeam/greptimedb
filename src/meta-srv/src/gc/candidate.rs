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

    /// Upper bound on the number of extra "most overdue" candidates appended
    /// after the score-selected top-N of a table.
    ///
    /// Derived from `regions_per_table_threshold` instead of introducing another
    /// config option: one extra slice of the same size caps a table at
    /// `2 * regions_per_table_threshold` candidates per tick and bounds how long a
    /// region can wait to roughly `ceil(regions / threshold)` ticks. See
    /// [`Self::select_gc_candidates`].
    fn fallback_candidate_limit(&self) -> usize {
        self.config.regions_per_table_threshold
    }

    /// Filter and score regions that are candidates for GC, grouped by table.
    ///
    /// Each table's candidate vector has two channels, in this order:
    ///
    /// 1. **Score channel** — the `regions_per_table_threshold` best scoring
    ///    eligible regions, i.e. the original behaviour. It owns the first slots,
    ///    so a higher score is never displaced by the channel below.
    /// 2. **Time-fallback channel** — up to [`Self::fallback_candidate_limit`]
    ///    further eligible regions, ordered by how long ago they were last *fully*
    ///    listed (`RegionGcInfo::last_full_listing_time`, with `None` — never
    ///    fully listed — treated as the most overdue). A region picked by the score
    ///    channel is never appended a second time, and the two channels are always
    ///    disjoint.
    ///
    /// The fallback keeps selection bounded: with `gc_cooldown_period` equal to the
    /// ticker interval the score channel alone re-selects the same top-N whenever
    /// its cooldown clears, so a region ranked below N could be starved forever
    /// even though its own listing state is unboundedly stale.
    ///
    /// Both channels share the same eligibility filter below (leader, size,
    /// cooldown, positive score), so a fallback candidate is subject to exactly the
    /// same cooldown and the same fast/full listing decision as a score candidate.
    ///
    /// The `region_gc_tracker` is read here rather than passed in: the selector
    /// already locks it to apply the cooldown, so a region's staleness is available
    /// without widening the signature or adding a second pass over the tracker.
    pub(crate) async fn select_gc_candidates(
        &self,
        table_to_region_stats: &HashMap<TableId, Vec<RegionStat>>,
    ) -> Result<HashMap<TableId, Vec<GcCandidate>>> {
        let mut table_candidates: HashMap<TableId, Vec<GcCandidate>> = HashMap::new();
        let now = Instant::now();
        let threshold = self.config.regions_per_table_threshold;
        let fallback_limit = self.fallback_candidate_limit();

        for (table_id, region_stats) in table_to_region_stats {
            // Eligible regions paired with the staleness key of the time-fallback
            // channel: the last full listing, `None` meaning "never fully listed".
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
                    let last_full_listing = tracker
                        .get(&region_stat.id)
                        .and_then(|gc_info| gc_info.last_full_listing_time);
                    candidates.push((
                        last_full_listing,
                        GcCandidate::new(region_stat.id, score, region_stat.clone()),
                    ));
                }
            }
            drop(tracker);

            // Score channel: highest score first, region id only to make equal
            // scores deterministic.
            candidates.sort_by(|a, b| {
                b.1.score
                    .cmp(&a.1.score)
                    .then_with(|| a.1.region_id.cmp(&b.1.region_id))
            });
            // Everything past the threshold is the fallback pool, i.e. regions the
            // score channel did not have room for.
            let mut fallback_pool = if candidates.len() > threshold {
                candidates.split_off(threshold)
            } else {
                Vec::new()
            };

            // Time-fallback channel: `None` (never fully listed) sorts first, then
            // the oldest listing; score and id only break staleness ties so the
            // sweep is deterministic.
            fallback_pool.sort_by(|a, b| {
                a.0.cmp(&b.0)
                    .then_with(|| b.1.score.cmp(&a.1.score))
                    .then_with(|| a.1.region_id.cmp(&b.1.region_id))
            });

            let score_selected = candidates.len();
            let mut selected: Vec<GcCandidate> = candidates.into_iter().map(|(_, c)| c).collect();
            selected.extend(
                fallback_pool
                    .into_iter()
                    .take(fallback_limit)
                    .map(|(_, c)| c),
            );

            if !selected.is_empty() {
                info!(
                    "Selected {} GC candidates for table {} ({} by score, top {} out of all \
                     qualified; {} by listing staleness)",
                    selected.len(),
                    table_id,
                    score_selected,
                    threshold,
                    selected.len() - score_selected,
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
