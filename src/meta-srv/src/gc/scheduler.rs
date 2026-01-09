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

use common_meta::DatanodeId;
use common_meta::key::TableMetadataManagerRef;
use common_procedure::ProcedureManagerRef;
use common_telemetry::{error, info};
use store_api::storage::GcReport;
use tokio::sync::Mutex;
use tokio::sync::mpsc::{Receiver, Sender};

use crate::cluster::MetaPeerClientRef;
use crate::define_ticker;
use crate::error::{Error, Result};
use crate::gc::ctx::{DefaultGcSchedulerCtx, SchedulerCtx};
use crate::gc::options::{GcSchedulerOptions, TICKER_INTERVAL};
use crate::gc::tracker::RegionGcTracker;
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
                    if let Err(e) = self.handle_tick().await {
                        error!("Failed to handle gc tick: {}", e);
                    }
                }
            }
        }
    }

    pub(crate) async fn handle_tick(&self) -> Result<GcJobReport> {
        info!("Start to trigger gc");
        let report = self.trigger_gc().await?;

        // Periodically clean up stale tracker entries
        self.cleanup_tracker_if_needed().await?;

        info!("Finished gc trigger");

        Ok(report)
    }
}
