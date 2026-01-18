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

use common_meta::datanode::{RegionManifestInfo, RegionStat};
use common_meta::key::table_repart::TableRepartValue;
use common_meta::peer::Peer;
use common_telemetry::{debug, error, info, warn};
use futures::StreamExt;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use store_api::region_engine::RegionRole;
use store_api::storage::{GcReport, RegionId};
use table::metadata::TableId;

use crate::error::Result;
use crate::gc::Region2Peers;
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

        let active_region_ids = table_to_region_stats
            .values()
            .flat_map(|stats| stats.iter().map(|stat| stat.id))
            .collect::<HashSet<_>>();

        let table_reparts = self.ctx.get_table_reparts().await?;
        let dropped_regions = self.collect_dropped_regions(table_reparts, &active_region_ids);
        let dropped_regions = self
            .filter_dropped_regions_by_cooldown(dropped_regions)
            .await;

        if per_table_candidates.is_empty() && dropped_regions.is_empty() {
            info!("No GC candidates found, skipping GC cycle");
            return Ok(Default::default());
        }

        // Step 3: Aggregate candidates by datanode
        let mut datanode_to_candidates = self
            .aggregate_candidates_by_datanode(per_table_candidates)
            .await?;

        let (dropped_by_peer, force_full_listing_by_peer, region_routes_override_by_peer) =
            self.assign_dropped_regions(&dropped_regions).await?;
        self.merge_dropped_regions(&mut datanode_to_candidates, dropped_by_peer);

        if datanode_to_candidates.is_empty() {
            info!("No valid datanode candidates found, skipping GC cycle");
            return Ok(Default::default());
        }

        // Step 4: Process datanodes concurrently with limited parallelism
        let report = self
            .parallel_process_datanodes(
                datanode_to_candidates,
                force_full_listing_by_peer,
                region_routes_override_by_peer,
            )
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

    fn collect_dropped_regions(
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
                if active_region_ids.contains(&src_region) {
                    continue;
                }
                entry.insert(src_region, dst_regions.into_iter().collect());
            }
        }

        dropped_regions
    }

    /// Filter out dropped regions that are still in their cooldown period.
    async fn filter_dropped_regions_by_cooldown(
        &self,
        dropped_regions: HashMap<TableId, HashMap<RegionId, HashSet<RegionId>>>,
    ) -> HashMap<TableId, HashMap<RegionId, HashSet<RegionId>>> {
        if dropped_regions.is_empty() {
            return dropped_regions;
        }

        let now = Instant::now();
        let tracker = self.region_gc_tracker.lock().await;
        let mut filtered = HashMap::new();

        for (table_id, regions) in dropped_regions {
            let mut kept = HashMap::new();
            for (region_id, dst_regions) in regions {
                if let Some(gc_info) = tracker.get(&region_id)
                    && now.saturating_duration_since(gc_info.last_gc_time)
                        < self.config.gc_cooldown_period
                {
                    debug!("Skipping dropped region {} due to cooldown", region_id);
                    continue;
                }
                kept.insert(region_id, dst_regions);
            }

            if !kept.is_empty() {
                filtered.insert(table_id, kept);
            }
        }

        filtered
    }

    async fn assign_dropped_regions(
        &self,
        dropped_regions: &HashMap<TableId, HashMap<RegionId, HashSet<RegionId>>>,
    ) -> Result<(
        HashMap<Peer, Vec<(TableId, RegionId)>>,
        HashMap<Peer, HashSet<RegionId>>,
        HashMap<Peer, Region2Peers>,
    )> {
        let mut dropped_by_peer: HashMap<Peer, Vec<(u32, RegionId)>> = HashMap::new();
        let mut force_full_listing_by_peer: HashMap<Peer, HashSet<RegionId>> = HashMap::new();
        let mut region_routes_override_by_peer: HashMap<Peer, Region2Peers> = HashMap::new();

        for (table_id, regions) in dropped_regions {
            let (phy_table_id, table_route) = match self.ctx.get_table_route(*table_id).await {
                Ok(route) => route,
                Err(e) => {
                    warn!(
                        "Failed to get table route for table {}: {}, skipping dropped regions",
                        table_id, e
                    );
                    continue;
                }
            };

            if phy_table_id != *table_id {
                continue;
            }

            let active_region_ids = table_route
                .region_routes
                .iter()
                .map(|r| r.region.id)
                .collect::<HashSet<_>>();

            let mut leader_peers = table_route
                .region_routes
                .iter()
                .filter_map(|r| r.leader_peer.clone())
                .collect::<Vec<_>>();
            leader_peers.sort_by_key(|peer| peer.id);
            leader_peers.dedup_by_key(|peer| peer.id);

            if leader_peers.is_empty() {
                warn!(
                    "No leader peers found for table {}, skipping dropped regions",
                    table_id
                );
                continue;
            }

            for (region_id, _dst_regions) in regions {
                if active_region_ids.contains(region_id) {
                    debug!(
                        "Skipping dropped region {} since it still exists in table route",
                        region_id
                    );
                    continue;
                }

                let selected_idx = (region_id.as_u64() as usize) % leader_peers.len();
                let peer = leader_peers[selected_idx].clone();

                dropped_by_peer
                    .entry(peer.clone())
                    .or_default()
                    .push((*table_id, *region_id));
                force_full_listing_by_peer
                    .entry(peer.clone())
                    .or_default()
                    .insert(*region_id);
                region_routes_override_by_peer
                    .entry(peer.clone())
                    .or_default()
                    .insert(*region_id, (peer, Vec::new()));
            }
        }

        Ok((
            dropped_by_peer,
            force_full_listing_by_peer,
            region_routes_override_by_peer,
        ))
    }

    fn merge_dropped_regions(
        &self,
        datanode_to_candidates: &mut HashMap<Peer, Vec<(TableId, GcCandidate)>>,
        dropped_by_peer: HashMap<Peer, Vec<(TableId, RegionId)>>,
    ) {
        for (peer, regions) in dropped_by_peer {
            let entry = datanode_to_candidates.entry(peer).or_default();
            for (table_id, region_id) in regions {
                entry.push((table_id, dropped_candidate(region_id)));
            }
        }
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
        force_full_listing_by_peer: HashMap<Peer, HashSet<RegionId>>,
        region_routes_override_by_peer: HashMap<Peer, Region2Peers>,
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
            let force_full_listing = force_full_listing_by_peer
                .get(&peer)
                .cloned()
                .unwrap_or_default();
            let region_routes_override = region_routes_override_by_peer
                .get(&peer)
                .cloned()
                .unwrap_or_default();
            async move {
                (
                    peer,
                    scheduler
                        .process_datanode_gc(
                            peer_clone,
                            candidates,
                            force_full_listing,
                            region_routes_override,
                        )
                        .await,
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
    pub(crate) async fn process_datanode_gc(
        &self,
        peer: Peer,
        candidates: Vec<(TableId, GcCandidate)>,
        force_full_listing: HashSet<RegionId>,
        region_routes_override: Region2Peers,
    ) -> Result<GcReport> {
        info!(
            "Starting GC for datanode {} with {} candidate regions",
            peer,
            candidates.len()
        );

        if candidates.is_empty() {
            return Ok(Default::default());
        }

        let all_region_ids: Vec<RegionId> = candidates.iter().map(|(_, c)| c.region_id).collect();

        // Step 2: Run GC for all regions on this datanode in a single batch
        let (gc_report, fully_listed_regions) = {
            // Partition regions into full listing and fast listing in a single pass

            let batch_full_listing_decisions = self
                .batch_should_use_full_listing(&all_region_ids, &force_full_listing)
                .await;

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
                    .gc_regions(
                        &fast_list_regions,
                        false,
                        self.config.mailbox_timeout,
                        region_routes_override.clone(),
                    )
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
                    .gc_regions(
                        &need_full_list_regions,
                        true,
                        self.config.mailbox_timeout,
                        region_routes_override,
                    )
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

            (combined_report, fully_listed_regions)
        };

        // Step 3: Process the combined GC report and update table reports
        for region_id in &all_region_ids {
            self.update_full_listing_time(*region_id, fully_listed_regions.contains(region_id))
                .await;
        }

        info!(
            "Completed GC for datanode {}: {} regions processed",
            peer,
            all_region_ids.len()
        );

        Ok(gc_report)
    }

    async fn batch_should_use_full_listing(
        &self,
        region_ids: &[RegionId],
        force_full_listing: &HashSet<RegionId>,
    ) -> HashMap<RegionId, bool> {
        let mut result = HashMap::new();
        let mut gc_tracker = self.region_gc_tracker.lock().await;
        let now = Instant::now();
        for &region_id in region_ids {
            if force_full_listing.contains(&region_id) {
                gc_tracker
                    .entry(region_id)
                    .and_modify(|info| {
                        info.last_full_listing_time = Some(now);
                        info.last_gc_time = now;
                    })
                    .or_insert_with(|| RegionGcInfo {
                        last_gc_time: now,
                        last_full_listing_time: Some(now),
                    });
                result.insert(region_id, true);
                continue;
            }
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

fn dropped_candidate(region_id: RegionId) -> GcCandidate {
    GcCandidate {
        region_id,
        score: OrderedFloat(0.0),
        region_stat: dropped_region_stat(region_id),
    }
}

fn dropped_region_stat(region_id: RegionId) -> RegionStat {
    RegionStat {
        id: region_id,
        rcus: 0,
        wcus: 0,
        approximate_bytes: 0,
        engine: "mito".to_string(),
        role: RegionRole::Leader,
        num_rows: 0,
        memtable_size: 0,
        manifest_size: 0,
        sst_size: 0,
        sst_num: 0,
        index_size: 0,
        region_manifest: RegionManifestInfo::Mito {
            manifest_version: 0,
            flushed_entry_id: 0,
            file_removed_cnt: 0,
        },
        written_bytes: 0,
        data_topic_latest_entry_id: 0,
        metadata_topic_latest_entry_id: 0,
    }
}
