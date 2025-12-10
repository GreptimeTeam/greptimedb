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
    pub(crate) async fn select_gc_candidates(
        &self,
        table_to_region_stats: &HashMap<TableId, Vec<RegionStat>>,
    ) -> Result<HashMap<TableId, Vec<GcCandidate>>> {
        let mut table_candidates: HashMap<TableId, Vec<GcCandidate>> = HashMap::new();
        let now = Instant::now();

        for (table_id, region_stats) in table_to_region_stats {
            let mut candidates = Vec::new();
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
                    candidates.push(GcCandidate::new(region_stat.id, score, region_stat.clone()));
                }
            }

            // Sort candidates by score in descending order and take top N
            candidates.sort_by(|a, b| b.score.cmp(&a.score));
            let top_candidates: Vec<GcCandidate> = candidates
                .into_iter()
                .take(self.config.regions_per_table_threshold)
                .collect();

            if !top_candidates.is_empty() {
                info!(
                    "Selected {} GC candidates for table {} (top {} out of all qualified)",
                    top_candidates.len(),
                    table_id,
                    self.config.regions_per_table_threshold
                );
                table_candidates.insert(*table_id, top_candidates);
            }
        }

        info!(
            "Selected GC candidates for {} tables",
            table_candidates.len()
        );
        Ok(table_candidates)
    }
}
