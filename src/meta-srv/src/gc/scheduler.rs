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
use std::time::Instant;

use common_meta::datanode::RegionStat;
use common_meta::key::TableMetadataManagerRef;
use common_telemetry::{error, info};
use futures::stream::{FuturesUnordered, StreamExt};
use table::metadata::TableId;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::cluster::MetaPeerClientRef;
use crate::define_ticker;
use crate::error::Result;
use crate::gc::candidate::GcCandidate;
use crate::gc::options::{GcSchedulerOptions, TICKER_INTERVAL};
use crate::gc::tracker::RegionGcTracker;
use crate::service::mailbox::MailboxRef;

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
    pub(crate) table_metadata_manager: TableMetadataManagerRef,
    /// For getting `RegionStats`.
    pub(crate) meta_peer_client: MetaPeerClientRef,
    /// The mailbox to send messages.
    pub(crate) mailbox: MailboxRef,
    /// The server address.
    pub(crate) server_addr: String,
    /// The receiver of events.
    pub(crate) receiver: Receiver<Event>,
    /// GC configuration.
    pub(crate) config: GcSchedulerOptions,
    /// Tracks the last GC time for regions.
    pub(crate) region_gc_tracker: Arc<tokio::sync::Mutex<RegionGcTracker>>,
    /// Last time the tracker was cleaned up.
    pub(crate) last_tracker_cleanup: Arc<tokio::sync::Mutex<Instant>>,
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

    pub(crate) async fn get_table_to_region_stats(
        &self,
    ) -> Result<HashMap<TableId, Vec<RegionStat>>> {
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

    /// Process multiple tables concurrently with limited parallelism.
    pub(crate) async fn process_tables_concurrently(
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
            if table_tasks.len() >= self.config.max_concurrent_tables
                && let Some(result) = table_tasks.next().await
            {
                let (success, regions) = Self::handle_table_result(result);
                processed_tables += 1;
                successful_tables += success;
                total_regions_processed += regions;
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
}
