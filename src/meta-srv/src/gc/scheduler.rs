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
use std::time::{Duration, Instant};

use common_meta::DatanodeId;
use common_meta::key::TableMetadataManagerRef;
use common_procedure::ProcedureManagerRef;
use common_telemetry::tracing::Instrument as _;
use common_telemetry::{error, info};
use store_api::storage::{GcReport, RegionId};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, oneshot};

use crate::cluster::MetaPeerClientRef;
use crate::define_ticker;
use crate::error::{Error, Result};
use crate::gc::Region2Peers;
use crate::gc::ctx::{DefaultGcSchedulerCtx, SchedulerCtx};
use crate::gc::options::{GcSchedulerOptions, TICKER_INTERVAL};
use crate::gc::tracker::RegionGcTracker;
use crate::metrics::{
    METRIC_META_GC_SCHEDULER_CYCLES_TOTAL, METRIC_META_GC_SCHEDULER_DURATION_SECONDS,
};
use crate::service::mailbox::MailboxRef;

/// Report for a GC job.
#[derive(Debug, Default)]
pub struct GcJobReport {
    pub per_datanode_reports: HashMap<DatanodeId, GcReport>,
    pub failed_datanodes: HashMap<DatanodeId, Vec<Error>>,
}

/// [`Event`] represents various types of events that can be processed by the gc ticker.
///
/// Variants:
/// - `Tick`: This event is used to trigger gc periodically.
/// - `Manually`: This event is used to trigger a manual gc run and provides a channel
///   to send back the [`GcJobReport`] for that run.
///   Optional parameters allow specifying target regions and GC behavior.
pub enum Event {
    Tick,
    Manually {
        /// Channel sender to return the GC job report
        sender: oneshot::Sender<GcJobReport>,
        /// Optional specific region IDs to GC. If None, scheduler will select candidates automatically.
        region_ids: Option<Vec<RegionId>>,
        /// Optional override for full file listing. If None, uses scheduler config.
        full_file_listing: Option<bool>,
        /// Optional override for timeout. If None, uses scheduler config.
        timeout: Option<Duration>,
    },
}

#[allow(unused)]
pub type GcTickerRef = Arc<GcTicker>;

define_ticker!(
    /// [GcTicker] is used to trigger gc periodically.
    GcTicker,
    event_type = Event,
    event_value = Event::Tick
);

/// [`GcScheduler`] is used to periodically trigger garbage collection on datanodes.
pub struct GcScheduler {
    pub(crate) ctx: Arc<dyn SchedulerCtx>,
    /// The receiver of events.
    pub(crate) receiver: Receiver<Event>,
    /// GC configuration.
    pub(crate) config: GcSchedulerOptions,
    /// Tracks the last GC time for regions.
    pub(crate) region_gc_tracker: Arc<Mutex<RegionGcTracker>>,
    /// Last time the tracker was cleaned up.
    pub(crate) last_tracker_cleanup: Arc<Mutex<Instant>>,
}

impl GcScheduler {
    /// Creates a new [`GcScheduler`] with custom configuration.
    pub(crate) fn new_with_config(
        table_metadata_manager: TableMetadataManagerRef,
        procedure_manager: ProcedureManagerRef,
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
            ctx: Arc::new(DefaultGcSchedulerCtx::try_new(
                table_metadata_manager,
                procedure_manager,
                meta_peer_client,
                mailbox,
                server_addr,
            )?),
            receiver: rx,
            config,
            region_gc_tracker: Arc::new(Mutex::new(HashMap::new())),
            last_tracker_cleanup: Arc::new(Mutex::new(Instant::now())),
        };
        Ok((gc_trigger, gc_ticker))
    }

    pub(crate) fn channel() -> (Sender<Event>, Receiver<Event>) {
        tokio::sync::mpsc::channel(8)
    }

    /// Starts the gc trigger.
    pub fn try_start(mut self) -> Result<()> {
        common_runtime::spawn_global(async move { self.run().await });
        info!("GC trigger started");
        Ok(())
    }

    pub(crate) async fn run(&mut self) {
        while let Some(event) = self.receiver.recv().await {
            match event {
                Event::Tick => {
                    info!("Received gc tick");
                    let span =
                        common_telemetry::tracing::info_span!("meta_gc_tick", trigger = "ticker");
                    if let Err(e) = self.handle_tick().instrument(span).await {
                        error!(e; "Failed to handle gc tick");
                    }
                }
                Event::Manually {
                    sender,
                    region_ids,
                    full_file_listing,
                    timeout,
                } => {
                    info!("Received manually gc request");
                    let span =
                        common_telemetry::tracing::info_span!("meta_gc_tick", trigger = "manual");
                    match self
                        .handle_manual_gc(region_ids, full_file_listing, timeout)
                        .await
                    {
                        Ok(report) => {
                            // ignore error
                            let _ = sender.send(report);
                        }
                        Err(e) => {
                            error!(e; "Failed to handle manual gc");
                            // Send empty report on error to avoid blocking caller
                            let _ = sender.send(GcJobReport::default());
                        }
                    };
                }
            }
        }
    }

    pub(crate) async fn handle_tick(&self) -> Result<GcJobReport> {
        METRIC_META_GC_SCHEDULER_CYCLES_TOTAL.inc();
        let _timer = METRIC_META_GC_SCHEDULER_DURATION_SECONDS.start_timer();
        info!("Start to trigger gc");
        let span = common_telemetry::tracing::info_span!("meta_gc_handle_tick");
        let report = self.trigger_gc().instrument(span).await?;

        // Periodically clean up stale tracker entries
        self.cleanup_tracker_if_needed().await?;

        info!("Finished gc trigger");

        Ok(report)
    }

    /// Handles a manual GC request with optional specific parameters.
    ///
    /// If `region_ids` is specified, GC will be performed only on those regions.
    /// Otherwise, falls back to automatic candidate selection.
    pub(crate) async fn handle_manual_gc(
        &self,
        region_ids: Option<Vec<RegionId>>,
        full_file_listing: Option<bool>,
        timeout: Option<Duration>,
    ) -> Result<GcJobReport> {
        info!("Start to handle manual gc request");

        let report = if let Some(regions) = region_ids {
            let full_listing = full_file_listing.unwrap_or(false);
            let gc_timeout = timeout.unwrap_or(self.config.mailbox_timeout);

            let gc_report = self
                .ctx
                .gc_regions(&regions, full_listing, gc_timeout, Region2Peers::new())
                .await?;

            let mut per_datanode_reports = HashMap::new();
            per_datanode_reports.insert(0, gc_report);
            GcJobReport {
                per_datanode_reports,
                failed_datanodes: HashMap::new(),
            }
        } else {
            // No specific regions, use default tick behavior
            self.trigger_gc().await?
        };

        info!("Finished manual gc request");
        Ok(report)
    }
}
