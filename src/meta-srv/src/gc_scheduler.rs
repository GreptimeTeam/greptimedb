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
use common_meta::datanode::{RegionManifestInfo, RegionStat};
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
    /// Timeout duration for mailbox communication with datanodes.
    pub mailbox_timeout: Duration,
    /// Interval for performing full file listing during GC to find orphan files.
    /// Full file listing is expensive but necessary to clean up orphan files.
    /// Set to a larger value (e.g., 24 hours) to balance performance and cleanup.
    /// Every Nth GC cycle will use full file listing, where N = full_file_listing_interval / TICKER_INTERVAL.
    pub full_file_listing_interval: Duration,
    /// Interval for cleaning up stale region entries from the GC tracker.
    /// This removes entries for regions that no longer exist (e.g., after table drops).
    /// Set to a larger value (e.g., 6 hours) since this is just for memory cleanup.
    pub tracker_cleanup_interval: Duration,
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
            mailbox_timeout: Duration::from_secs(60),         // 60 seconds
            // Perform full file listing every 24 hours to find orphan files
            full_file_listing_interval: Duration::from_secs(60 * 60 * 24),
            // Clean up stale tracker entries every 6 hours
            tracker_cleanup_interval: Duration::from_secs(60 * 60 * 6),
        }
    }
}

impl GcSchedulerOptions {
    /// Validates the configuration options.
    pub fn validate(&self) -> Result<()> {
        if self.max_concurrent_tables == 0 {
            return error::InvalidArgumentsSnafu {
                err_msg: "max_concurrent_tables must be greater than 0",
            }
            .fail();
        }

        if self.max_retries_per_region == 0 {
            return error::InvalidArgumentsSnafu {
                err_msg: "max_retries_per_region must be greater than 0",
            }
            .fail();
        }

        if self.retry_backoff_duration.is_zero() {
            return error::InvalidArgumentsSnafu {
                err_msg: "retry_backoff_duration must be greater than 0",
            }
            .fail();
        }

        if self.sst_count_weight < 0.0 {
            return error::InvalidArgumentsSnafu {
                err_msg: "sst_count_weight must be non-negative",
            }
            .fail();
        }

        if self.file_removal_rate_weight < 0.0 {
            return error::InvalidArgumentsSnafu {
                err_msg: "file_removal_rate_weight must be non-negative",
            }
            .fail();
        }

        if self.gc_cooldown_period.is_zero() {
            return error::InvalidArgumentsSnafu {
                err_msg: "gc_cooldown_period must be greater than 0",
            }
            .fail();
        }

        if self.regions_per_table_threshold == 0 {
            return error::InvalidArgumentsSnafu {
                err_msg: "regions_per_table_threshold must be greater than 0",
            }
            .fail();
        }

        if self.mailbox_timeout.is_zero() {
            return error::InvalidArgumentsSnafu {
                err_msg: "mailbox_timeout must be greater than 0",
            }
            .fail();
        }

        if self.full_file_listing_interval.is_zero() {
            return error::InvalidArgumentsSnafu {
                err_msg: "full_file_listing_interval must be greater than 0",
            }
            .fail();
        }

        if self.tracker_cleanup_interval.is_zero() {
            return error::InvalidArgumentsSnafu {
                err_msg: "tracker_cleanup_interval must be greater than 0",
            }
            .fail();
        }

        Ok(())
    }
}

/// Represents a region candidate for GC with its priority score.
#[derive(Debug, Clone, PartialEq, Eq)]
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

/// Tracks GC timing information for a region.
#[derive(Debug, Clone)]
struct RegionGcInfo {
    /// Last time a regular GC was performed on this region.
    last_gc_time: Instant,
    /// Last time a full file listing GC was performed on this region.
    last_full_listing_time: Option<Instant>,
}

impl RegionGcInfo {
    fn new(last_gc_time: Instant) -> Self {
        Self {
            last_gc_time,
            last_full_listing_time: None,
        }
    }
}

/// Tracks the last GC time for regions to implement cooldown.
type RegionGcTracker = HashMap<RegionId, RegionGcInfo>;

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
    /// Last time the tracker was cleaned up.
    last_tracker_cleanup: Arc<tokio::sync::Mutex<Instant>>,
}

impl GcScheduler {
    /// Creates a new [`GcScheduler`].
    #[allow(unused)]
    pub(crate) fn new(
        table_metadata_manager: TableMetadataManagerRef,
        meta_peer_client: MetaPeerClientRef,
        mailbox: MailboxRef,
        server_addr: String,
    ) -> Result<(Self, GcTicker)> {
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
    ) -> Result<(Self, GcTicker)> {
        // Validate configuration before creating the scheduler
        config.validate()?;

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
            last_tracker_cleanup: Arc::new(tokio::sync::Mutex::new(Instant::now())),
        };
        Ok((gc_trigger, gc_ticker))
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

        // Periodically clean up stale tracker entries
        if let Err(e) = self.cleanup_tracker_if_needed().await {
            error!(e; "Failed to cleanup tracker");
        }

        info!("Finished gc trigger");
    }

    async fn get_table_to_region_stats(&self) -> Result<HashMap<TableId, Vec<RegionStat>>> {
        let dn_stats = self.meta_peer_client.get_all_dn_stat_kvs().await?;
        let mut table_to_region_stats: HashMap<TableId, Vec<RegionStat>> = HashMap::new();
        for (_dn_id, stats) in dn_stats {
            let mut stats = stats.stats;
            stats.sort_by_key(|s| s.timestamp_millis);

            let Some(latest_stat) = stats.last().cloned() else {
                continue;
            };

            for region_stat in latest_stat.region_stats {
                table_to_region_stats
                    .entry(region_stat.id.table_id())
                    .or_default()
                    .push(region_stat);
            }
        }
        Ok(table_to_region_stats)
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
    ) -> Result<HashMap<TableId, Vec<GcCandidate>>> {
        let mut table_candidates: HashMap<TableId, Vec<GcCandidate>> = HashMap::new();
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
                if let Some(gc_info) = gc_tracker.get(&region_stat.id)
                    && now.duration_since(gc_info.last_gc_time) < self.config.gc_cooldown_period
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

    /// Process multiple tables concurrently with limited parallelism.
    async fn process_tables_concurrently(
        &self,
        per_table_candidates: HashMap<TableId, Vec<GcCandidate>>,
    ) -> (usize, usize, usize) {
        let mut table_tasks = FuturesUnordered::new();
        let mut processed_tables = 0;
        let mut successful_tables = 0;
        let mut total_regions_processed = 0;

        for (table_id, candidates) in per_table_candidates {
            let task = self.process_table_gc(table_id, candidates);
            table_tasks.push(task);

            // Limit concurrent table processing
            if table_tasks.len() >= self.config.max_concurrent_tables {
                if let Some(result) = table_tasks.next().await {
                    let (success, regions) = Self::handle_table_result(result);
                    processed_tables += 1;
                    successful_tables += success;
                    total_regions_processed += regions;
                }
            }
        }

        // Process remaining tasks
        while let Some(result) = table_tasks.next().await {
            let (success, regions) = Self::handle_table_result(result);
            processed_tables += 1;
            successful_tables += success;
            total_regions_processed += regions;
        }

        (processed_tables, successful_tables, total_regions_processed)
    }

    /// Handle the result of processing a single table.
    fn handle_table_result(result: Result<usize>) -> (usize, usize) {
        match result {
            Ok(regions_count) => (1, regions_count),
            Err(e) => {
                error!("Failed to process table GC: {}", e);
                (0, 0)
            }
        }
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
        let (processed_tables, successful_tables, total_regions_processed) =
            self.process_tables_concurrently(per_table_candidates).await;

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
        candidates: Vec<GcCandidate>,
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
                Ok((_report, used_full_listing)) => {
                    successful_regions += 1;
                    // Update GC tracker
                    let mut gc_tracker = self.region_gc_tracker.lock().await;
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
            info!(
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

    /// Process GC for a single region with retry logic.
    /// Returns the GC report and a boolean indicating whether full file listing was used.
    async fn process_region_gc_with_retry(
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
                .send_gc_region_instruction(
                    peer.clone(),
                    region_id,
                    &current_manifest,
                    should_use_full_listing,
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

    /// Send an instruction to a datanode and wait for the reply.
    ///
    async fn send_instruction(
        &self,
        peer: &Peer,
        instruction: Instruction,
        description: &str,
        timeout: Duration,
    ) -> Result<InstructionReply> {
        let msg = MailboxMessage::json_message(
            &format!("{}: {}", description, instruction),
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
            .send(&Channel::Datanode(peer.id), msg, timeout)
            .await?;

        match mailbox_rx.await {
            Ok(reply_msg) => {
                let reply = HeartbeatMailbox::json_reply(&reply_msg)?;
                Ok(reply)
            }
            Err(e) => {
                error!(
                    "Failed to receive reply from datanode {} for {}: {}",
                    peer, description, e
                );
                Err(e)
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

        let reply = self
            .send_instruction(
                peer,
                instruction,
                "Get file references",
                self.config.mailbox_timeout,
            )
            .await?;

        let InstructionReply::GetFileRefs(GetFileRefsReply {
            file_refs_manifest,
            success,
            error,
        }) = reply
        else {
            return error::UnexpectedInstructionReplySnafu {
                mailbox_message: format!("{:?}", reply),
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

    /// Determine if full file listing should be used for a region based on the last full listing time.
    async fn should_use_full_listing(&self, region_id: RegionId) -> bool {
        let gc_tracker = self.region_gc_tracker.lock().await;
        let now = Instant::now();

        if let Some(gc_info) = gc_tracker.get(&region_id) {
            if let Some(last_full_listing) = gc_info.last_full_listing_time {
                let elapsed = now.duration_since(last_full_listing);
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

    /// Send GC instruction to a datanode for a specific region.
    async fn send_gc_region_instruction(
        &self,
        peer: Peer,
        region_id: RegionId,
        file_refs_manifest: &FileRefsManifest,
        full_file_listing: bool,
    ) -> Result<GcReport> {
        info!(
            "Sending GC instruction to datanode {} for region {} (full_file_listing: {})",
            peer, region_id, full_file_listing
        );

        let instruction = Instruction::GcRegions(GcRegions {
            regions: vec![region_id],
            file_refs_manifest: file_refs_manifest.clone(),
            full_file_listing,
        });

        let reply = self
            .send_instruction(&peer, instruction, "GC region", self.config.mailbox_timeout)
            .await?;

        let InstructionReply::GcRegions(reply) = reply else {
            return error::UnexpectedInstructionReplySnafu {
                mailbox_message: format!("{:?}", reply),
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

    /// Clean up stale entries from the region GC tracker if enough time has passed.
    /// This removes entries for regions that no longer exist in the current table routes.
    async fn cleanup_tracker_if_needed(&self) -> Result<()> {
        let mut last_cleanup = self.last_tracker_cleanup.lock().await;
        let now = Instant::now();

        // Check if enough time has passed since last cleanup
        if now.duration_since(*last_cleanup) < self.config.tracker_cleanup_interval {
            return Ok(());
        }

        info!("Starting region GC tracker cleanup");
        let cleanup_start = Instant::now();

        // Get all current region IDs from table routes
        let table_to_region_stats = self.get_table_to_region_stats().await?;
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

        *last_cleanup = now;

        info!(
            "Completed region GC tracker cleanup: removed {} stale entries out of {} total (retained {}). Duration: {:?}",
            removed_count,
            initial_count,
            tracker.len(),
            cleanup_start.elapsed()
        );

        Ok(())
    }
}
