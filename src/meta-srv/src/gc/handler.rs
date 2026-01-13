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

use common_meta::peer::Peer;
use common_telemetry::{debug, error, info, warn};
use futures::StreamExt;
use itertools::Itertools;
use store_api::storage::{GcReport, RegionId};
use table::metadata::TableId;

use crate::error::Result;
use crate::gc::candidate::GcCandidate;
use crate::gc::scheduler::{GcJobReport, GcScheduler};
use crate::gc::tracker::RegionGcInfo;

impl GcScheduler {
    /// Iterate through all region stats, find region that might need gc, and send gc instruction to
    /// the corresponding datanode with improved parallel processing and retry logic.
    pub(crate) async fn trigger_gc(&self) -> Result<GcJobReport> {
        let start_time = Instant::now();
        info!("Starting GC cycle");

        // Step 1: Get all region statistics
        let table_to_region_stats = self.ctx.get_table_to_region_stats().await?;
        info!(
            "Fetched region stats for {} tables",
            table_to_region_stats.len()
        );

        // Step 2: Select GC candidates based on our scoring algorithm
        let per_table_candidates = self.select_gc_candidates(&table_to_region_stats).await?;

        if per_table_candidates.is_empty() {
            info!("No GC candidates found, skipping GC cycle");
            return Ok(Default::default());
        }

        // Step 3: Aggregate candidates by datanode
        let datanode_to_candidates = self
            .aggregate_candidates_by_datanode(per_table_candidates)
            .await?;

        // TODO(discord9): add deleted regions from repartition mapping

        if datanode_to_candidates.is_empty() {
            info!("No valid datanode candidates found, skipping GC cycle");
            return Ok(Default::default());
        }

        // Step 4: Process datanodes concurrently with limited parallelism
        let report = self
            .parallel_process_datanodes(datanode_to_candidates)
            .await;

        let duration = start_time.elapsed();
        info!(
            "Finished GC cycle. Processed {} datanodes ({} failed). Duration: {:?}",
            report.per_datanode_reports.len(), // Reuse field for datanode count
            report.failed_datanodes.len(),
            duration
        );
        debug!("Detailed GC Job Report: {report:#?}");

        Ok(report)
    }

    /// Aggregate GC candidates by their corresponding datanode peer.
    pub(crate) async fn aggregate_candidates_by_datanode(
        &self,
        per_table_candidates: HashMap<TableId, Vec<GcCandidate>>,
    ) -> Result<HashMap<Peer, Vec<(TableId, GcCandidate)>>> {
        let mut datanode_to_candidates: HashMap<Peer, Vec<(TableId, GcCandidate)>> = HashMap::new();

        for (table_id, candidates) in per_table_candidates {
            if candidates.is_empty() {
                continue;
            }

            // Get table route information to map regions to peers
            let (phy_table_id, table_peer) = self.ctx.get_table_route(table_id).await?;

            if phy_table_id != table_id {
                // Skip logical tables
                continue;
            }

            let region_to_peer = table_peer
                .region_routes
                .iter()
                .filter_map(|r| {
                    r.leader_peer
                        .as_ref()
                        .map(|peer| (r.region.id, peer.clone()))
                })
                .collect::<HashMap<RegionId, Peer>>();

            for candidate in candidates {
                if let Some(peer) = region_to_peer.get(&candidate.region_id) {
                    datanode_to_candidates
                        .entry(peer.clone())
                        .or_default()
                        .push((table_id, candidate));
                } else {
                    warn!(
                        "Skipping region {} for table {}: no leader peer found",
                        candidate.region_id, table_id
                    );
                }
            }
        }

        info!(
            "Aggregated GC candidates for {} datanodes",
            datanode_to_candidates.len()
        );
        Ok(datanode_to_candidates)
    }

    /// Process multiple datanodes concurrently with limited parallelism.
    pub(crate) async fn parallel_process_datanodes(
        &self,
        datanode_to_candidates: HashMap<Peer, Vec<(TableId, GcCandidate)>>,
    ) -> GcJobReport {
        let mut report = GcJobReport::default();

        // Create a stream of datanode GC tasks with limited concurrency
        let results: Vec<_> = futures::stream::iter(
            datanode_to_candidates
                .into_iter()
                .filter(|(_, candidates)| !candidates.is_empty()),
        )
        .map(|(peer, candidates)| {
            let scheduler = self;
            let peer_clone = peer.clone();
            async move {
                (
                    peer,
                    scheduler.process_datanode_gc(peer_clone, candidates).await,
                )
            }
        })
        .buffer_unordered(self.config.max_concurrent_tables) // Reuse table concurrency limit for datanodes
        .collect()
        .await;

        // Process all datanode GC results and collect regions that need retry from table reports
        for (peer, result) in results {
            match result {
                Ok(dn_report) => {
                    report.per_datanode_reports.insert(peer.id, dn_report);
                }
                Err(e) => {
                    error!("Failed to process datanode GC for peer {}: {:#?}", peer, e);
                    // Note: We don't have a direct way to map peer to table_id here,
                    // so we just log the error. The table_reports will contain individual region failures.
                    report.failed_datanodes.entry(peer.id).or_default().push(e);
                }
            }
        }

        report
    }

    /// Process GC for a single datanode with all its candidate regions.
    /// Returns the table reports for this datanode.
    ///
    /// When the number of candidates exceeds `max_regions_per_batch`, they are
    /// split into multiple batches to avoid overwhelming object storage.
    pub(crate) async fn process_datanode_gc(
        &self,
        peer: Peer,
        candidates: Vec<(TableId, GcCandidate)>,
    ) -> Result<GcReport> {
        info!(
            "Starting GC for datanode {} with {} candidate regions",
            peer,
            candidates.len()
        );

        if candidates.is_empty() {
            return Ok(Default::default());
        }

        let batch_size = self.config.max_regions_per_batch;
        let mut combined_report = GcReport::default();

        // Split candidates into batches to avoid overwhelming object storage
        for batch in candidates.chunks(batch_size) {
            let batch_report = self
                .process_datanode_gc_batch(peer.clone(), batch.to_vec())
                .await?;
            combined_report.merge(batch_report);
        }

        info!(
            "Completed GC for datanode {}: {} regions processed in {} batch(es)",
            peer,
            candidates.len(),
            (candidates.len() + batch_size - 1) / batch_size
        );

        Ok(combined_report)
    }

    /// Process a single batch of GC candidates for a datanode.
    async fn process_datanode_gc_batch(
        &self,
        peer: Peer,
        candidates: Vec<(TableId, GcCandidate)>,
    ) -> Result<GcReport> {
        if candidates.is_empty() {
            return Ok(Default::default());
        }

        let all_region_ids: Vec<RegionId> = candidates.iter().map(|(_, c)| c.region_id).collect();

        // Partition regions into full listing and fast listing in a single pass
        let batch_full_listing_decisions =
            self.batch_should_use_full_listing(&all_region_ids).await;

        let need_full_list_regions = batch_full_listing_decisions
            .iter()
            .filter_map(
                |(&region_id, &need_full)| {
                    if need_full { Some(region_id) } else { None }
                },
            )
            .collect_vec();
        let fast_list_regions = batch_full_listing_decisions
            .iter()
            .filter_map(
                |(&region_id, &need_full)| {
                    if !need_full { Some(region_id) } else { None }
                },
            )
            .collect_vec();

        let mut combined_report = GcReport::default();

        // First process regions that can fast list
        if !fast_list_regions.is_empty() {
            match self
                .ctx
                .gc_regions(&fast_list_regions, false, self.config.mailbox_timeout)
                .await
            {
                Ok(report) => combined_report.merge(report),
                Err(e) => {
                    error!(
                        "Failed to GC regions {:?} on datanode {}: {}",
                        fast_list_regions, peer, e
                    );

                    // Add to need_retry_regions since it failed
                    combined_report
                        .need_retry_regions
                        .extend(fast_list_regions.clone().into_iter());
                }
            }
        }

        if !need_full_list_regions.is_empty() {
            match self
                .ctx
                .gc_regions(&need_full_list_regions, true, self.config.mailbox_timeout)
                .await
            {
                Ok(report) => combined_report.merge(report),
                Err(e) => {
                    error!(
                        "Failed to GC regions {:?} on datanode {}: {}",
                        need_full_list_regions, peer, e
                    );

                    // Add to need_retry_regions since it failed
                    combined_report
                        .need_retry_regions
                        .extend(need_full_list_regions.clone());
                }
            }
        }

        let fully_listed_regions = need_full_list_regions
            .into_iter()
            .filter(|r| !combined_report.need_retry_regions.contains(r))
            .collect::<HashSet<_>>();

        // Update tracker for processed regions
        for region_id in &all_region_ids {
            self.update_full_listing_time(*region_id, fully_listed_regions.contains(region_id))
                .await;
        }

        Ok(combined_report)
    }

    async fn batch_should_use_full_listing(
        &self,
        region_ids: &[RegionId],
    ) -> HashMap<RegionId, bool> {
        let mut result = HashMap::new();
        let mut gc_tracker = self.region_gc_tracker.lock().await;
        let now = Instant::now();
        for &region_id in region_ids {
            let use_full_listing = {
                if let Some(gc_info) = gc_tracker.get(&region_id) {
                    if let Some(last_full_listing) = gc_info.last_full_listing_time {
                        // check if pass cooling down interval after last full listing
                        let elapsed = now.saturating_duration_since(last_full_listing);
                        elapsed >= self.config.full_file_listing_interval
                    } else {
                        // Never did full listing for this region, do it now
                        true
                    }
                } else {
                    // First time GC for this region, skip doing full listing, for this time
                    gc_tracker.insert(
                        region_id,
                        RegionGcInfo {
                            last_gc_time: now,
                            last_full_listing_time: Some(now),
                        },
                    );
                    false
                }
            };
            result.insert(region_id, use_full_listing);
        }
        result
    }
}
