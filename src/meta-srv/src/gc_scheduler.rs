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
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::meta::MailboxMessage;
use common_meta::datanode::{GcStat, RegionManifestInfo, RegionStat};
use common_meta::instruction::{
    GcRegions, GetFileRefs, GetFileRefsReply, Instruction, InstructionReply,
};
use common_meta::key::TableMetadataManagerRef;
use common_meta::peer::Peer;
use common_telemetry::{debug, error, info, warn};
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt as _, ResultExt};
use store_api::storage::{FileRefsManifest, GcReport, RegionId};
use table::metadata::TableId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::sleep;

use crate::cluster::MetaPeerClientRef;
use crate::define_ticker;
use crate::error::{self, RegionRouteNotFoundSnafu, Result, TableMetadataManagerSnafu};
use crate::handler::HeartbeatMailbox;
use crate::service::mailbox::{Channel, MailboxRef};

/// The interval of the gc ticker.
#[allow(unused)]
const TICKER_INTERVAL: Duration = Duration::from_secs(60 * 5);

/// Configuration for GC operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct GcSchedulerOptions {
    /// Whether GC is enabled. Default to true. If set to false, no GC will be performed, and potentially some
    /// files from datanodes will never be deleted.
    ///
    /// TODO(discord9): If `enabled` is set to false, datanode side should also use `LocalFilePurger` instead of `ObjectStoreFilePurger`, maybe add some check?
    pub enabled: bool,
    /// Maximum number of tables to process concurrently.
    pub max_concurrent_tables: usize,
    /// Maximum number of retries per region when GC fails.
    pub max_retries_per_region: usize,
    /// Backoff duration between retries.
    pub retry_backoff_duration: Duration,
    /// Minimum region size threshold for GC (in bytes).
    pub min_region_size_threshold: u64,
    /// Weight for SST file count in GC scoring.
    pub sst_count_weight: f64,
    /// Weight for file removal rate in GC scoring.
    pub file_removal_rate_weight: f64,
    /// Cooldown period between GC operations on the same region.
    pub gc_cooldown_period: Duration,
    /// Maximum number of regions to select for GC per table.
    pub regions_per_table_threshold: usize,
}

impl Default for GcSchedulerOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            max_concurrent_tables: 10,
            max_retries_per_region: 3,
            retry_backoff_duration: Duration::from_secs(5),
            min_region_size_threshold: 100 * 1024 * 1024, // 100MB
            sst_count_weight: 1.0,
            file_removal_rate_weight: 0.5,
            gc_cooldown_period: Duration::from_secs(60 * 30), // 30 minutes
            regions_per_table_threshold: 20,                  // Select top 20 regions per table
        }
    }
}

/// Represents a region candidate for GC with its priority score.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GcCandidate {
    region_id: RegionId,
    score: OrderedFloat<f64>,
    region_stat: RegionStat,
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

/// Tracks the last GC time for regions to implement cooldown.
type RegionGcTracker = HashMap<RegionId, Instant>;

/// [`Event`] represents various types of events that can be processed by the gc ticker.
///
/// Variants:
/// - `Tick`: This event is used to trigger gc periodically.
pub(crate) enum Event {
    Tick,
}

#[allow(unused)]
pub(crate) type GcTickerRef = Arc<GcTicker>;

define_ticker!(
    /// [GcTicker] is used to trigger gc periodically.
    GcTicker,
    event_type = Event,
    event_value = Event::Tick
);

/// [`GcScheduler`] is used to periodically trigger garbage collection on datanodes.
pub struct GcScheduler {
    /// The metadata manager.
    table_metadata_manager: TableMetadataManagerRef,
    /// For getting `RegionStats`.
    meta_peer_client: MetaPeerClientRef,
    /// The mailbox to send messages.
    mailbox: MailboxRef,
    /// The server address.
    server_addr: String,
    /// The receiver of events.
    receiver: Receiver<Event>,
    /// GC configuration.
    config: GcSchedulerOptions,
    /// Tracks the last GC time for regions.
    region_gc_tracker: Arc<tokio::sync::Mutex<RegionGcTracker>>,
}

impl GcScheduler {
    /// Creates a new [`GcScheduler`].
    #[allow(unused)]
    pub(crate) fn new(
        table_metadata_manager: TableMetadataManagerRef,
        meta_peer_client: MetaPeerClientRef,
        mailbox: MailboxRef,
        server_addr: String,
    ) -> (Self, GcTicker) {
        Self::new_with_config(
            table_metadata_manager,
            meta_peer_client,
            mailbox,
            server_addr,
            GcSchedulerOptions::default(),
        )
    }

    /// Creates a new [`GcScheduler`] with custom configuration.
    pub(crate) fn new_with_config(
        table_metadata_manager: TableMetadataManagerRef,
        meta_peer_client: MetaPeerClientRef,
        mailbox: MailboxRef,
        server_addr: String,
        config: GcSchedulerOptions,
    ) -> (Self, GcTicker) {
        let (tx, rx) = Self::channel();
        let gc_ticker = GcTicker::new(TICKER_INTERVAL, tx);
        let gc_trigger = Self {
            table_metadata_manager,
            meta_peer_client,
            mailbox,
            server_addr,
            receiver: rx,
            config,
            region_gc_tracker: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        };
        (gc_trigger, gc_ticker)
    }

    fn channel() -> (Sender<Event>, Receiver<Event>) {
        tokio::sync::mpsc::channel(8)
    }

    /// Starts the gc trigger.
    pub fn try_start(mut self) -> Result<()> {
        common_runtime::spawn_global(async move { self.run().await });
        info!("GC trigger started");
        Ok(())
    }

    async fn run(&mut self) {
        while let Some(event) = self.receiver.recv().await {
            match event {
                Event::Tick => {
                    info!("Received gc tick");
                    self.handle_tick().await
                }
            }
        }
    }

    async fn handle_tick(&self) {
        info!("Start to trigger gc");
        if let Err(e) = self.trigger_gc().await {
            error!(e; "Failed to trigger gc");
        }
        info!("Finished gc trigger");
    }

    async fn get_table_to_region_stats(&self) -> Result<HashMap<TableId, Vec<RegionStat>>> {
        let dn_stats = self.meta_peer_client.get_all_dn_stat_kvs().await?;
        let mut table_to_region_stats: HashMap<TableId, Vec<RegionStat>> = HashMap::new();
        for (_dn_id, stats) in dn_stats {
            for stat in stats.stats {
                for region_stat in stat.region_stats {
                    table_to_region_stats
                        .entry(region_stat.id.table_id())
                        .or_default()
                        .push(region_stat);
                }
            }
        }
        Ok(table_to_region_stats)
    }

    /// Get datanode GC statistics mapping from peer to GcStat
    async fn get_datanode_gc_stats(&self) -> Result<HashMap<Peer, GcStat>> {
        let dn_stats = self.meta_peer_client.get_all_dn_stat_kvs().await?;
        let mut datanode_gc_stats: HashMap<Peer, GcStat> = HashMap::new();

        for (_dn_key, stats) in dn_stats {
            // Get the latest stat (most recent)
            if let Some(latest_stat) = stats.stats.last() {
                let peer = Peer {
                    id: latest_stat.id,
                    addr: latest_stat.addr.clone(),
                };
                datanode_gc_stats.insert(peer, latest_stat.gc_stat.clone());
            }
        }

        Ok(datanode_gc_stats)
    }

    /// Select the best peer for GC based on load from GcStat
    fn select_best_peer_for_gc(&self, datanode_gc_stats: &HashMap<Peer, GcStat>) -> Option<Peer> {
        let mut best_peer = None;
        let mut best_load_score = f64::MAX;

        for (peer, gc_stat) in datanode_gc_stats {
            // Calculate load score: ratio of running tasks to concurrency
            // Lower score means lower load (better candidate)
            let load_score = if gc_stat.gc_concurrency > 0 {
                gc_stat.running_gc_tasks as f64 / gc_stat.gc_concurrency as f64
            } else {
                // If concurrency is 0, use a high score to avoid this peer
                f64::MAX
            };

            if load_score < best_load_score {
                best_load_score = load_score;
                best_peer = Some(peer.clone());
            }
        }

        best_peer
    }

    /// Calculate GC priority score for a region based on various metrics.
    fn calculate_gc_score(&self, region_stat: &RegionStat) -> f64 {
        let sst_count_score = region_stat.sst_num as f64 * self.config.sst_count_weight;

        let file_removal_rate_score = match &region_stat.region_manifest {
            RegionManifestInfo::Mito {
                file_removal_rate, ..
            } => *file_removal_rate as f64 * self.config.file_removal_rate_weight,
            // Metric engine doesn't have file_removal_rate, also this should be unreachable since metrics engine doesn't support gc
            RegionManifestInfo::Metric { .. } => 0.0,
        };

        sst_count_score + file_removal_rate_score
    }

    /// Filter and score regions that are candidates for GC, grouped by table.
    async fn select_gc_candidates(
        &self,
        table_to_region_stats: &HashMap<TableId, Vec<RegionStat>>,
    ) -> Result<HashMap<TableId, HashSet<GcCandidate>>> {
        let mut table_candidates: HashMap<TableId, HashSet<GcCandidate>> = HashMap::new();
        let now = Instant::now();
        let gc_tracker = self.region_gc_tracker.lock().await;

        for (table_id, region_stats) in table_to_region_stats {
            let mut candidates = Vec::new();

            for region_stat in region_stats {
                // Skip regions that are too small
                if region_stat.approximate_bytes < self.config.min_region_size_threshold {
                    continue;
                }

                // Skip regions that are in cooldown period
                if let Some(last_gc_time) = gc_tracker.get(&region_stat.id)
                    && now.duration_since(*last_gc_time) < self.config.gc_cooldown_period
                {
                    debug!("Skipping region {} due to cooldown", region_stat.id);
                    continue;
                }

                let score = self.calculate_gc_score(region_stat);

                // Only consider regions with a meaningful score
                if score > 0.0 {
                    candidates.push(GcCandidate::new(region_stat.id, score, region_stat.clone()));
                }
            }

            // Sort candidates by score in descending order and take top N
            candidates.sort_by(|a, b| b.score.cmp(&a.score));
            let top_candidates: HashSet<GcCandidate> = candidates
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

    /// Iterate through all region stats, find region that might need gc, and send gc instruction to
    /// the corresponding datanode with improved parallel processing and retry logic.
    async fn trigger_gc(&self) -> Result<()> {
        let start_time = Instant::now();
        info!("Starting GC cycle");

        // Step 1: Get all region statistics
        let table_to_region_stats = self.get_table_to_region_stats().await?;
        info!(
            "Fetched region stats for {} tables",
            table_to_region_stats.len()
        );

        // Step 2: Select GC candidates based on our scoring algorithm
        let per_table_candidates = self.select_gc_candidates(&table_to_region_stats).await?;

        if per_table_candidates.is_empty() {
            info!("No GC candidates found, skipping GC cycle");
            return Ok(());
        }

        // Step 3: Process tables concurrently with limited parallelism
        let mut table_tasks = FuturesUnordered::new();
        let mut processed_tables = 0;
        let mut successful_tables = 0;
        let mut total_regions_processed = 0;

        for (table_id, candidates) in per_table_candidates {
            let task = self.process_table_gc(table_id, candidates);
            table_tasks.push(task);

            // Limit concurrent table processing
            if table_tasks.len() >= self.config.max_concurrent_tables
                && let Some(result) = table_tasks.next().await
            {
                processed_tables += 1;
                match result {
                    Ok(regions_count) => {
                        successful_tables += 1;
                        total_regions_processed += regions_count;
                    }
                    Err(e) => {
                        error!("Failed to process table GC: {}", e);
                    }
                }
            }
        }

        // Process remaining tasks
        while let Some(result) = table_tasks.next().await {
            processed_tables += 1;
            match result {
                Ok(regions_count) => {
                    successful_tables += 1;
                    total_regions_processed += regions_count;
                }
                Err(e) => {
                    error!("Failed to process table GC: {}", e);
                }
            }
        }

        let duration = start_time.elapsed();
        info!(
            "Finished GC cycle. Processed {} tables ({} successful), {} regions total. Duration: {:?}",
            processed_tables, successful_tables, total_regions_processed, duration
        );

        Ok(())
    }

    /// Process GC for a single table with all its candidate regions.
    async fn process_table_gc(
        &self,
        table_id: TableId,
        candidates: HashSet<GcCandidate>,
    ) -> Result<usize> {
        info!(
            "Starting GC for table {} with {} candidate regions",
            table_id,
            candidates.len()
        );

        // Step 1: Get table route information
        let (_, table_peer) = self
            .table_metadata_manager
            .table_route_manager()
            .get_physical_table_route(table_id)
            .await
            .context(TableMetadataManagerSnafu)?;

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
            .get_file_references(&related_region_ids, &region_to_peer)
            .await?;

        // Step 4: Filter out candidates that don't have file references available
        let total_candidates = candidates.len();
        let mut valid_candidates = Vec::new();
        for candidate in candidates {
            // Check if we have file references for this region
            if let Some(peer) = region_to_peer.get(&candidate.region_id) {
                // Check if this peer's file references were successfully obtained
                if file_refs_manifest
                    .manifest_version
                    .contains_key(&candidate.region_id)
                {
                    valid_candidates.push(candidate);
                } else {
                    warn!(
                        "Skipping region {} due to missing file references (datanode {} may be unavailable)",
                        candidate.region_id, peer
                    );
                }
            }
        }

        // Step 5: Process each valid candidate region with retry logic
        let valid_candidates_count = valid_candidates.len();
        let mut successful_regions = 0;

        for candidate in valid_candidates {
            let region_id = candidate.region_id;
            match self
                .process_region_gc_with_retry(candidate, &file_refs_manifest, &region_to_peer)
                .await
            {
                Ok(_report) => {
                    successful_regions += 1;
                    // Update GC tracker
                    let mut gc_tracker = self.region_gc_tracker.lock().await;
                    gc_tracker.insert(region_id, Instant::now());
                }
                Err(e) => {
                    error!("Failed to GC region {} after all retries: {}", region_id, e);
                }
            }
        }

        info!(
            "Completed GC for table {}: {}/{} regions successful ({} skipped due to missing file references)",
            table_id,
            successful_regions,
            valid_candidates_count,
            total_candidates - valid_candidates_count
        );

        Ok(successful_regions)
    }

    /// Find related regions that might share files with the candidate regions.
    /// Currently returns the same regions since repartition is not implemented yet.
    /// TODO(discord9): When repartition is implemented, this should also find parent/child regions
    /// that might share files with the candidate regions.
    async fn find_related_regions(
        &self,
        candidate_region_ids: &[RegionId],
    ) -> Result<Vec<RegionId>> {
        Ok(candidate_region_ids.to_vec())
    }

    /// Get file references for the specified regions.
    ///
    /// If certain datanodes are unreachable, it logs a warning and skips those regions instead of failing the entire operation.
    async fn get_file_references(
        &self,
        region_ids: &[RegionId],
        region_to_peer: &HashMap<RegionId, Peer>,
    ) -> Result<FileRefsManifest> {
        info!("Getting file references for {} regions", region_ids.len());

        // Group regions by datanode to minimize RPC calls
        let mut datanode_regions: HashMap<Peer, Vec<RegionId>> = HashMap::new();

        for region_id in region_ids {
            if let Some(peer) = region_to_peer.get(region_id) {
                datanode_regions
                    .entry(peer.clone())
                    .or_default()
                    .push(*region_id);
            }
        }

        // Send GetFileRefs instructions to each datanode
        let mut all_file_refs = HashSet::new();
        let mut all_manifest_versions = HashMap::new();

        for (peer, regions) in datanode_regions {
            match self.send_get_file_refs_instruction(&peer, &regions).await {
                Ok(manifest) => {
                    all_file_refs.extend(manifest.file_refs);
                    all_manifest_versions.extend(manifest.manifest_version);
                }
                Err(e) => {
                    warn!(
                        "Failed to get file refs from datanode {}: {}. Skipping regions on this datanode.",
                        peer, e
                    );
                    // Continue processing other datanodes instead of failing the entire operation
                    continue;
                }
            }
        }

        Ok(FileRefsManifest {
            file_refs: all_file_refs,
            manifest_version: all_manifest_versions,
        })
    }

    /// Refresh file references for related regions, typically used before retrying GC.
    async fn refresh_file_refs_for(
        &self,
        regions: &[RegionId],
        region_to_peer: &HashMap<RegionId, Peer>,
    ) -> Result<FileRefsManifest> {
        let related_regions = self.find_related_regions(regions).await?;
        self.get_file_references(&related_regions, region_to_peer)
            .await
            .inspect_err(|e| {
                error!(
                    "Failed to refresh file references for regions {:?}: {}",
                    related_regions, e
                );
            })
    }

    /// Process GC for a single region with retry logic.
    async fn process_region_gc_with_retry(
        &self,
        candidate: GcCandidate,
        file_refs_manifest: &FileRefsManifest,
        // TODO(discord9): maybe also refresh region_to_peer mapping if needed?
        region_to_peer: &HashMap<RegionId, Peer>,
    ) -> Result<GcReport> {
        let region_id = candidate.region_id;

        // Get GC stats for all datanodes to choose the best peer based on load
        let datanode_gc_stats = self.get_datanode_gc_stats().await?;

        // Select the best peer based on GC load, or fall back to region-specific peer
        let mut peer = if let Some(best_peer) = self.select_best_peer_for_gc(&datanode_gc_stats) {
            best_peer
        } else {
            // Fall back to the original region-specific peer if no suitable peer found
            region_to_peer
                .get(&region_id)
                .with_context(|| RegionRouteNotFoundSnafu { region_id })?
                .clone()
        };

        let mut retries = 0;
        let mut current_manifest = file_refs_manifest.clone();
        // Final report for recording all deleted files
        let mut final_report = GcReport::default();

        loop {
            match self
                .send_gc_region_instruction(peer.clone(), region_id, &current_manifest)
                .await
            {
                Ok(report) => {
                    if report.need_retry_regions.is_empty() {
                        info!("Successfully completed GC for region {}", region_id);
                        final_report.merge(report);
                        // note that need_retry_regions should be empty here
                        // since no more outdated regions
                        final_report.need_retry_regions.clear();
                        return Ok(final_report);
                    } else {
                        // retry outdated regions if needed?
                        current_manifest = self
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
                    }
                }

                // Retryable errors: refresh file references and retry with backoff
                Err(e) if e.is_retryable() => {
                    retries += 1;
                    if retries >= self.config.max_retries_per_region {
                        error!(
                            "Failed to GC region {} after {} retries: {}",
                            region_id, retries, e
                        );
                        return Err(e);
                    }

                    warn!(
                        "GC failed for region {} (attempt {}/{}): {}. Retrying after backoff...",
                        region_id, retries, self.config.max_retries_per_region, e
                    );

                    // Wait for backoff period
                    sleep(self.config.retry_backoff_duration).await;

                    current_manifest = self
                        .refresh_file_refs_for(&[region_id], region_to_peer)
                        .await?;

                    // use a possibly different peer based on current load

                    // Get GC stats for all datanodes to choose the best peer based on load
                    let datanode_gc_stats = self.get_datanode_gc_stats().await?;

                    // Select the best peer based on GC load, or fall back to region-specific peer
                    peer = if let Some(best_peer) = self.select_best_peer_for_gc(&datanode_gc_stats)
                    {
                        best_peer
                    } else {
                        // Fall back to the original region-specific peer if no suitable peer found
                        region_to_peer
                            .get(&region_id)
                            .with_context(|| RegionRouteNotFoundSnafu { region_id })?
                            .clone()
                    };
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

    /// Send GetFileRefs instruction to a datanode for specified regions.
    async fn send_get_file_refs_instruction(
        &self,
        peer: &Peer,
        region_ids: &[RegionId],
    ) -> Result<FileRefsManifest> {
        info!(
            "Sending GetFileRefs instruction to datanode {} for {} regions",
            peer,
            region_ids.len()
        );

        let instruction = Instruction::GetFileRefs(GetFileRefs {
            region_ids: region_ids.to_vec(),
        });

        let msg = MailboxMessage::json_message(
            &format!("Get file references: {}", instruction),
            &format!("Metasrv@{}", self.server_addr),
            &format!("Datanode-{}@{}", peer.id, peer.addr),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        let mailbox_rx = self
            .mailbox
            .send(&Channel::Datanode(peer.id), msg, Duration::from_secs(60))
            .await?;

        match mailbox_rx.await {
            Ok(reply_msg) => {
                let reply = HeartbeatMailbox::json_reply(&reply_msg)?;
                let InstructionReply::GetFileRefs(GetFileRefsReply {
                    file_refs_manifest,
                    success,
                    error,
                }) = reply
                else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: reply_msg.to_string(),
                        reason: "Unexpected reply of the GetFileRefs instruction",
                    }
                    .fail();
                };

                if !success {
                    return error::UnexpectedSnafu {
                        violated: format!(
                            "Failed to get file references from datanode {}: {:?}",
                            peer, error
                        ),
                    }
                    .fail();
                }

                Ok(file_refs_manifest)
            }
            Err(e) => {
                error!(
                    "Failed to receive GetFileRefs reply from datanode {}: {}",
                    peer, e
                );
                Err(e)
            }
        }
    }

    /// Send GC instruction to a datanode for a specific region.
    /// TODO(discord9): return outdated regions if needed
    async fn send_gc_region_instruction(
        &self,
        peer: Peer,
        region_id: RegionId,
        file_refs_manifest: &FileRefsManifest,
    ) -> Result<GcReport> {
        info!(
            "Sending GC instruction to datanode {} for region {}",
            peer, region_id
        );

        let instruction = Instruction::GcRegions(GcRegions {
            regions: vec![region_id],
            file_refs_manifest: file_refs_manifest.clone(),
        });

        let msg = MailboxMessage::json_message(
            &format!("GC region: {}", instruction),
            &format!("Metasrv@{}", self.server_addr),
            &format!("Datanode-{}@{}", peer.id, peer.addr),
            common_time::util::current_time_millis(),
            &instruction,
        )
        .with_context(|_| error::SerializeToJsonSnafu {
            input: instruction.to_string(),
        })?;

        let receiver = self
            .mailbox
            .send(&Channel::Datanode(peer.id), msg, Duration::from_secs(60))
            .await?;

        match receiver.await {
            Ok(reply_msg) => {
                let reply = HeartbeatMailbox::json_reply(&reply_msg)?;
                let InstructionReply::GcRegions(reply) = reply else {
                    return error::UnexpectedInstructionReplySnafu {
                        mailbox_message: reply_msg.to_string(),
                        reason: "Unexpected reply of the GcRegions instruction",
                    }
                    .fail();
                };

                let res = reply.result;
                match res {
                    Ok(report) => Ok(report),
                    Err(e) => {
                        error!(
                            "Datanode {} reported error during GC for region {}: {}",
                            peer, region_id, e
                        );
                        Err(error::UnexpectedSnafu {
                            violated: format!(
                                "Datanode {} reported error during GC for region {}: {}",
                                peer, region_id, e
                            ),
                        }
                        .fail()?)
                    }
                }
            }
            Err(e) => {
                error!(
                    "Failed to receive GC reply from datanode {} for region {}: {}",
                    peer, region_id, e
                );
                Err(e)
            }
        }
    }
}
