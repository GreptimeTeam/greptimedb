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
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use common_meta::peer::Peer;
use common_telemetry::{debug, error, info, warn};
use futures::StreamExt;
use itertools::Itertools;
use snafu::OptionExt as _;
use store_api::storage::{FileRefsManifest, GcReport, RegionId};
use table::metadata::TableId;
use tokio::time::sleep;

use crate::error::{self, RegionRouteNotFoundSnafu, Result, UnexpectedSnafu};
use crate::gc::candidate::GcCandidate;
use crate::gc::scheduler::{GcJobReport, GcScheduler, TableGcReport};
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

        // Step 3: Process tables concurrently with limited parallelism
        let report = self.process_tables_concurrently(per_table_candidates).await;

        let duration = start_time.elapsed();
        info!(
            "Finished GC cycle. Processed {} tables ({} successful). Duration: {:?}",
            report.processed_tables,
            report.table_reports.len(),
            duration
        );
        debug!("Detailed GC Job Report: {report:#?}");

        Ok(report)
    }

    /// Process GC for a single table with all its candidate regions.
    pub(crate) async fn process_table_gc(
        &self,
        table_id: TableId,
        candidates: Vec<GcCandidate>,
    ) -> Result<TableGcReport> {
        info!(
            "Starting GC for table {} with {} candidate regions",
            table_id,
            candidates.len()
        );

        let mut report = TableGcReport {
            table_id,
            ..Default::default()
        };

        // Step 1: Get table route information
        // if is logic table, can simply pass.
        let (phy_table_id, table_peer) = self.ctx.get_table_route(table_id).await?;

        if phy_table_id != table_id {
            return Ok(report);
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

        // Step 2: Determine related regions for file reference fetching
        let candidate_region_ids: Vec<RegionId> = candidates.iter().map(|c| c.region_id).collect();
        let related_region_ids = self.find_related_regions(&candidate_region_ids).await?;

        // Step 3: Get file references for related regions
        let file_refs_manifest = self
            .ctx
            .get_file_references(
                &related_region_ids,
                &region_to_peer,
                self.config.mailbox_timeout,
            )
            .await?;

        // Step 4: Filter out candidates that don't have file references available
        let total_candidates = candidates.len();
        let mut valid_candidates = Vec::new();
        for candidate in candidates {
            // Check if we have file references for this region
            if file_refs_manifest
                .manifest_version
                .contains_key(&candidate.region_id)
            {
                // Check if this peer's addr were successfully obtained
                if region_to_peer.contains_key(&candidate.region_id) {
                    valid_candidates.push(candidate);
                } else {
                    UnexpectedSnafu {
                        violated: format!("Missing peer info for region {}", candidate.region_id),
                    }
                    .fail()?;
                }
            } else {
                error!(
                    "Missing file references entry for region {}",
                    candidate.region_id
                );
                UnexpectedSnafu {
                    violated: format!(
                        "Missing file references entry for region {}",
                        candidate.region_id
                    ),
                }
                .fail()?;
            }
        }

        // Step 5: Process each valid candidate region with retry logic
        let valid_candidates_count = valid_candidates.len();
        let successful_regions = Arc::new(AtomicUsize::new(0));
        let failed_regions = Arc::new(AtomicUsize::new(0));
        let reports = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let err_reports = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let file_refs_manifest = Arc::new(file_refs_manifest);
        let region_to_peer = Arc::new(region_to_peer);

        futures::stream::iter(valid_candidates)
            .for_each_concurrent(Some(self.config.region_gc_concurrency), |candidate| {
                let region_id = candidate.region_id;
                let file_refs_manifest = file_refs_manifest.clone();
                let region_to_peer = region_to_peer.clone();
                let successful_regions = successful_regions.clone();
                let failed_regions = failed_regions.clone();
                let region_gc_tracker = self.region_gc_tracker.clone();
                let reports = reports.clone();
                let err_reports = err_reports.clone();

                async move {
                    match self
                        .process_region_gc_with_retry(
                            candidate,
                            &file_refs_manifest,
                            &region_to_peer,
                        )
                        .await
                    {
                        Ok((report, used_full_listing)) => {
                            reports.lock().await.push(report);
                            successful_regions.fetch_add(1, Ordering::Relaxed);
                            // Update GC tracker
                            let mut gc_tracker = region_gc_tracker.lock().await;
                            let now = Instant::now();
                            let gc_info = gc_tracker
                                .entry(region_id)
                                .or_insert_with(|| RegionGcInfo::new(now));
                            gc_info.last_gc_time = now;
                            if used_full_listing {
                                gc_info.last_full_listing_time = Some(now);
                            }
                        }
                        Err(e) => {
                            failed_regions.fetch_add(1, Ordering::Relaxed);
                            error!("Failed to GC region {} after all retries: {}", region_id, e);
                            // TODO: collect errors into table gc report
                            err_reports
                                .lock()
                                .await
                                .push((region_id, format!("{:#?}", e)));
                        }
                    }
                }
            })
            .await;

        let successful_regions = successful_regions.load(Ordering::Relaxed);

        info!(
            "Completed GC for table {}: {}/{} regions successful ({} skipped due to missing file references)",
            table_id,
            successful_regions,
            valid_candidates_count,
            total_candidates - valid_candidates_count
        );

        report.success_regions = reports.lock().await.drain(..).collect();
        report.failed_regions = err_reports.lock().await.drain(..).collect();
        Ok(report)
    }

    /// Find related regions that might share files with the candidate regions.
    /// Currently returns the same regions since repartition is not implemented yet.
    /// TODO(discord9): When repartition is implemented, this should also find parent/child regions
    /// that might share files with the candidate regions.
    pub(crate) async fn find_related_regions(
        &self,
        candidate_region_ids: &[RegionId],
    ) -> Result<Vec<RegionId>> {
        Ok(candidate_region_ids.to_vec())
    }

    /// Process GC for a single region with retry logic.
    /// Returns the GC report and a boolean indicating whether full file listing was used.
    pub(crate) async fn process_region_gc_with_retry(
        &self,
        candidate: GcCandidate,
        file_refs_manifest: &FileRefsManifest,
        // TODO(discord9): maybe also refresh region_to_peer mapping if needed?
        region_to_peer: &HashMap<RegionId, Peer>,
    ) -> Result<(GcReport, bool)> {
        let region_id = candidate.region_id;

        // TODO(discord9): Select the best peer based on GC load
        // for now gc worker need to be run from datanode that hosts the region
        // this limit might be lifted in the future
        let mut peer = self.get_region_peer(region_id, region_to_peer)?;

        let mut retries = 0;
        let mut current_manifest = file_refs_manifest.clone();
        // Final report for recording all deleted files
        let mut final_report = GcReport::default();

        // Determine if we should use full file listing for this region
        let should_use_full_listing = self.should_use_full_listing(region_id).await;

        loop {
            match self
                .ctx
                .gc_regions(
                    peer.clone(),
                    region_id,
                    &current_manifest,
                    should_use_full_listing,
                    self.config.mailbox_timeout,
                )
                .await
            {
                Ok(report) => {
                    match self
                        .handle_gc_success(region_id, report, &mut final_report, region_to_peer)
                        .await?
                    {
                        None => return Ok((final_report, should_use_full_listing)),
                        Some(refreshed_manifest) => {
                            current_manifest = refreshed_manifest;
                        }
                    }
                }
                // Retryable errors: refresh file references and retry with backoff
                Err(e) if e.is_retryable() => {
                    // TODO(discord9): might do it on table level instead
                    let (refreshed_manifest, refreshed_peer) = self
                        .handle_gc_retry(region_id, &mut retries, e, region_to_peer)
                        .await?;
                    current_manifest = refreshed_manifest;
                    peer = refreshed_peer;
                }
                Err(e) => {
                    error!(
                        "Non-retryable error during GC for region {}: {}",
                        region_id, e
                    );
                    return Err(e);
                }
            }
        }
    }

    /// Handle successful GC report, checking if retry is needed for outdated regions.
    async fn handle_gc_success(
        &self,
        region_id: RegionId,
        report: GcReport,
        final_report: &mut GcReport,
        region_to_peer: &HashMap<RegionId, Peer>,
    ) -> Result<Option<FileRefsManifest>> {
        if report.need_retry_regions.is_empty() {
            final_report.merge(report);
            debug!(
                "Successfully completed GC for region {} with report: {final_report:?}",
                region_id
            );
            // note that need_retry_regions should be empty here
            // since no more outdated regions
            final_report.need_retry_regions.clear();
            Ok(None)
        } else {
            // retry outdated regions if needed
            let refreshed_manifest = self
                .refresh_file_refs_for(
                    &report.need_retry_regions.clone().into_iter().collect_vec(),
                    region_to_peer,
                )
                .await?;
            info!(
                "Retrying GC for regions {:?} due to outdated file references",
                &report.need_retry_regions
            );
            final_report.merge(report);
            Ok(Some(refreshed_manifest))
        }
    }

    /// Handle retryable GC error with backoff and manifest refresh.
    async fn handle_gc_retry(
        &self,
        region_id: RegionId,
        retries: &mut usize,
        error: error::Error,
        region_to_peer: &HashMap<RegionId, Peer>,
    ) -> Result<(FileRefsManifest, Peer)> {
        *retries += 1;
        if *retries >= self.config.max_retries_per_region {
            error!(
                "Failed to GC region {} after {} retries: {}",
                region_id, retries, error
            );
            return Err(error);
        }

        warn!(
            "GC failed for region {} (attempt {}/{}): {}. Retrying after backoff...",
            region_id, retries, self.config.max_retries_per_region, error
        );

        // Wait for backoff period
        sleep(self.config.retry_backoff_duration).await;

        let refreshed_manifest = self
            .refresh_file_refs_for(&[region_id], region_to_peer)
            .await?;

        // TODO(discord9): Select the best peer based on GC load
        // for now gc worker need to be run from datanode that hosts the region
        // this limit might be lifted in the future
        let peer = self.get_region_peer(region_id, region_to_peer)?;

        Ok((refreshed_manifest, peer))
    }

    /// Refresh file references for related regions, typically used before retrying GC.
    async fn refresh_file_refs_for(
        &self,
        regions: &[RegionId],
        region_to_peer: &HashMap<RegionId, Peer>,
    ) -> Result<FileRefsManifest> {
        let related_regions = self.find_related_regions(regions).await?;
        self.ctx
            .get_file_references(
                &related_regions,
                region_to_peer,
                self.config.mailbox_timeout,
            )
            .await
            .inspect_err(|e| {
                error!(
                    "Failed to refresh file references for regions {:?}: {}",
                    related_regions, e
                );
            })
    }

    /// Get the peer for a given region.
    fn get_region_peer(
        &self,
        region_id: RegionId,
        region_to_peer: &HashMap<RegionId, Peer>,
    ) -> Result<Peer> {
        region_to_peer
            .get(&region_id)
            .cloned()
            .with_context(|| RegionRouteNotFoundSnafu { region_id })
    }
}
