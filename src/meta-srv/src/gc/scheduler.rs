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

use common_meta::DatanodeId;
use common_meta::key::runtime_switch::RuntimeSwitchManagerRef;
use common_telemetry::tracing::Instrument as _;
use common_telemetry::{error, info};
use snafu::ResultExt;
use store_api::storage::{GcReport, RegionId};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{Mutex, oneshot};

use crate::define_ticker;
use crate::error::{self, Error, Result};
#[cfg(test)]
use crate::gc::ctx::PurgeReservation;
use crate::gc::ctx::{PurgeOutcome, SchedulerCtx};
use crate::gc::dropped::DroppedRegionCollector;
use crate::gc::options::{GcSchedulerOptions, TICKER_INTERVAL};
use crate::gc::tracker::RegionGcTracker;
use crate::gc::{EXPERIMENTAL_SOFT_DROP_ENABLED, Region2Peers};
use crate::metrics::{
    METRIC_META_GC_SCHEDULER_CYCLES_TOTAL, METRIC_META_GC_SCHEDULER_DURATION_SECONDS,
    METRIC_META_GC_SOFT_DROP_PURGES_TOTAL,
};

/// Report for a GC job.
#[derive(Debug)]
pub enum GcJobReport {
    PerDatanode {
        per_datanode_reports: HashMap<DatanodeId, GcReport>,
        failed_datanodes: HashMap<DatanodeId, Vec<Error>>,
    },
    Combined {
        report: GcReport,
    },
}

impl Default for GcJobReport {
    fn default() -> Self {
        Self::PerDatanode {
            per_datanode_reports: HashMap::new(),
            failed_datanodes: HashMap::new(),
        }
    }
}

impl GcJobReport {
    pub fn combined(report: GcReport) -> Self {
        Self::Combined { report }
    }

    pub fn merge_to_report(self) -> GcReport {
        match self {
            GcJobReport::Combined { report } => report,
            GcJobReport::PerDatanode {
                per_datanode_reports,
                ..
            } => {
                let mut combined = GcReport::default();
                for (_datanode_id, report) in per_datanode_reports {
                    combined.merge(report);
                }
                combined
            }
        }
    }
}

/// [`Event`] represents various types of events that can be processed by the gc ticker.
///
/// Variants:
/// - `Tick`: This event is used to trigger gc periodically.
/// - `Manually`: This event is used to trigger a manual gc run and provides a channel
///   to send back the result for that run.
///   Optional parameters allow specifying target regions and GC behavior.
pub enum Event {
    Tick,
    Manually {
        /// Channel sender to return the GC job report or error
        sender: oneshot::Sender<Result<GcJobReport>>,
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
    /// Runtime switch manager to check maintenance mode.
    pub(crate) runtime_switch_manager: RuntimeSwitchManagerRef,
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
        ctx: impl SchedulerCtx + 'static,
        runtime_switch_manager: RuntimeSwitchManagerRef,
        config: GcSchedulerOptions,
    ) -> Result<(Self, GcTicker)> {
        // Validate configuration before creating the scheduler
        config.validate()?;

        let (tx, rx) = Self::channel();
        let gc_ticker = GcTicker::new(TICKER_INTERVAL, tx);
        let gc_trigger = Self {
            ctx: Arc::new(ctx),
            runtime_switch_manager,
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
                    let result = self
                        .handle_manual_gc(region_ids, full_file_listing, timeout)
                        .instrument(span)
                        .await;
                    if let Err(e) = &result {
                        if matches!(e, Error::ManualGcRejectedByMaintenanceMode { .. }) {
                            info!("Rejected manual gc request: {}", e);
                        } else {
                            error!(e; "Failed to handle manual gc");
                        }
                    }
                    let _ = sender.send(result);
                }
            }
        }
    }

    pub(crate) async fn handle_tick(&self) -> Result<GcJobReport> {
        METRIC_META_GC_SCHEDULER_CYCLES_TOTAL.inc();
        let _timer = METRIC_META_GC_SCHEDULER_DURATION_SECONDS.start_timer();
        info!("Start to trigger gc");
        if self.is_maintenance_mode_enabled().await? {
            info!("Skip gc trigger because maintenance mode is enabled");
            return Ok(GcJobReport::default());
        }
        if EXPERIMENTAL_SOFT_DROP_ENABLED && self.config.experimental_soft_drop.enable {
            self.purge_expired_soft_dropped_tables(common_time::util::current_time_millis())
                .await;
        }
        let span = common_telemetry::tracing::info_span!("meta_gc_handle_tick");
        let report = self.trigger_gc().instrument(span).await?;

        // Periodically clean up stale tracker entries
        self.cleanup_tracker_if_needed().await?;

        info!("Finished gc trigger");

        Ok(report)
    }

    async fn purge_expired_soft_dropped_tables(&self, now_millis: i64) {
        // The scheduler is only constructed after GcSchedulerOptions::validate().
        let retention_millis =
            i64::try_from(self.config.experimental_soft_drop.retention.as_millis())
                .unwrap_or(i64::MAX);
        let dropped_tables = match self.ctx.list_dropped_tables().await {
            Ok(dropped_tables) => dropped_tables,
            Err(error) => {
                error!(error; "Failed to list soft-dropped tables for GC");
                return;
            }
        };
        let table_count = dropped_tables.len();
        let scan_start = self.ctx.next_purge_scan_start(table_count);

        for table in dropped_tables
            .iter()
            .cycle()
            .skip(scan_start)
            .take(table_count)
            .filter(|table| {
                table
                    .retention_expires_at
                    .or_else(|| {
                        table
                            .dropped_at
                            .and_then(|dropped_at| dropped_at.checked_add(retention_millis))
                    })
                    .is_some_and(|expires_at| expires_at <= now_millis)
            })
        {
            let table_id = table.table_id;
            let Some(reservation) = self
                .ctx
                .try_reserve_purge(table_id, self.config.max_concurrent_tables)
            else {
                continue;
            };
            METRIC_META_GC_SOFT_DROP_PURGES_TOTAL
                .with_label_values(&["submitted"])
                .inc();
            let ctx = self.ctx.clone();
            common_runtime::spawn_global(async move {
                match ctx.purge_dropped_table(table_id).await {
                    Ok(()) => reservation.record_outcome(PurgeOutcome::Succeeded),
                    Err(error) => {
                        reservation.record_outcome(PurgeOutcome::Failed);
                        error!(error; "Failed to purge expired soft-dropped table {}", table_id);
                    }
                }
            });
        }
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

        if self.is_maintenance_mode_enabled().await? {
            info!("Skip manual gc request because maintenance mode is enabled");
            return error::ManualGcRejectedByMaintenanceModeSnafu {}.fail();
        }

        // No specific regions, use default tick behavior
        let Some(regions) = region_ids else {
            let report = self.trigger_gc().await?;
            info!("Finished manual gc request");
            return Ok(report);
        };

        // Empty regions list, return empty report
        if regions.is_empty() {
            info!("Finished manual gc request");
            return Ok(GcJobReport::combined(GcReport::default()));
        }

        let full_listing = full_file_listing.unwrap_or(false);
        let gc_timeout = timeout.unwrap_or(self.config.mailbox_timeout);

        let region_set: HashSet<RegionId> = regions.iter().copied().collect();
        let table_reparts = self.ctx.get_table_reparts().await?;
        let dropped_collector =
            DroppedRegionCollector::new(self.ctx.as_ref(), &self.config, &self.region_gc_tracker);
        let dropped_assignment = dropped_collector
            .collect_and_assign_with_cooldown(&table_reparts, false)
            .await?;

        let mut dropped_region_set = HashSet::new();
        let mut dropped_routes_override = Region2Peers::new();
        for overrides in dropped_assignment.region_routes_override.into_values() {
            for (region_id, route) in overrides {
                if region_set.contains(&region_id) {
                    dropped_region_set.insert(region_id);
                    dropped_routes_override.insert(region_id, route);
                }
            }
        }

        let (dropped_regions, active_regions): (Vec<_>, Vec<_>) = regions
            .into_iter()
            .partition(|region_id| dropped_region_set.contains(region_id));

        let mut combined_report = GcReport::default();

        if !active_regions.is_empty() {
            let report = self
                .ctx
                .gc_regions(
                    &active_regions,
                    full_listing,
                    gc_timeout,
                    Region2Peers::new(),
                )
                .await?;
            combined_report.merge(report);
        }

        if !dropped_regions.is_empty() {
            let report = self
                .ctx
                .gc_regions(&dropped_regions, true, gc_timeout, dropped_routes_override)
                .await?;
            combined_report.merge(report);
        }

        let report = GcJobReport::combined(combined_report);

        info!("Finished manual gc request");
        Ok(report)
    }

    pub(crate) async fn is_maintenance_mode_enabled(&self) -> Result<bool> {
        self.runtime_switch_manager
            .maintenance_mode()
            .await
            .context(error::RuntimeSwitchManagerSnafu)
    }
}

#[cfg(test)]
pub(crate) fn new_test_runtime_switch_manager() -> RuntimeSwitchManagerRef {
    Arc::new(common_meta::key::runtime_switch::RuntimeSwitchManager::new(
        Arc::new(common_meta::kv_backend::memory::MemoryKvBackend::new()),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use common_meta::datanode::RegionStat;
    use common_meta::key::DroppedTableName;
    use common_meta::key::table_repart::TableRepartValue;
    use common_meta::key::table_route::PhysicalTableRouteValue;
    use store_api::storage::RegionId;
    use table::metadata::TableId;
    use table::table_name::TableName;

    use super::*;

    #[derive(Default)]
    struct CountingSchedulerCtx {
        get_table_to_region_stats_calls: AtomicUsize,
        get_table_reparts_calls: AtomicUsize,
        gc_regions_calls: AtomicUsize,
        list_dropped_tables_calls: AtomicUsize,
        purge_dropped_table_calls: AtomicUsize,
    }

    impl CountingSchedulerCtx {
        fn assert_no_scheduler_work(&self) {
            assert_eq!(
                0,
                self.get_table_to_region_stats_calls.load(Ordering::Relaxed),
                "get_table_to_region_stats should not be called"
            );
            assert_eq!(
                0,
                self.get_table_reparts_calls.load(Ordering::Relaxed),
                "get_table_reparts should not be called"
            );
            assert_eq!(
                0,
                self.gc_regions_calls.load(Ordering::Relaxed),
                "gc_regions should not be called"
            );
            assert_eq!(
                0,
                self.list_dropped_tables_calls.load(Ordering::Relaxed),
                "list_dropped_tables should not be called"
            );
            assert_eq!(
                0,
                self.purge_dropped_table_calls.load(Ordering::Relaxed),
                "purge_dropped_table should not be called"
            );
        }
    }

    #[async_trait::async_trait]
    impl SchedulerCtx for CountingSchedulerCtx {
        async fn get_table_to_region_stats(&self) -> Result<HashMap<TableId, Vec<RegionStat>>> {
            self.get_table_to_region_stats_calls
                .fetch_add(1, Ordering::Relaxed);
            panic!("get_table_to_region_stats should not be called in maintenance mode")
        }

        async fn get_table_reparts(&self) -> Result<Vec<(TableId, TableRepartValue)>> {
            self.get_table_reparts_calls.fetch_add(1, Ordering::Relaxed);
            panic!("get_table_reparts should not be called in maintenance mode")
        }

        async fn get_table_route(
            &self,
            _table_id: TableId,
        ) -> Result<(TableId, PhysicalTableRouteValue)> {
            unreachable!("get_table_route should not be called in this test")
        }

        async fn batch_get_table_route(
            &self,
            _table_ids: &[TableId],
        ) -> Result<HashMap<TableId, PhysicalTableRouteValue>> {
            unreachable!("batch_get_table_route should not be called in this test")
        }

        async fn gc_regions(
            &self,
            _region_ids: &[RegionId],
            _full_file_listing: bool,
            _timeout: Duration,
            _region_routes_override: Region2Peers,
        ) -> Result<GcReport> {
            self.gc_regions_calls.fetch_add(1, Ordering::Relaxed);
            panic!("gc_regions should not be called in maintenance mode")
        }

        async fn list_dropped_tables(&self) -> Result<Vec<DroppedTableName>> {
            self.list_dropped_tables_calls
                .fetch_add(1, Ordering::Relaxed);
            panic!("list_dropped_tables should not be called in maintenance mode")
        }

        async fn purge_dropped_table(&self, _table_id: TableId) -> Result<()> {
            self.purge_dropped_table_calls
                .fetch_add(1, Ordering::Relaxed);
            panic!("purge_dropped_table should not be called in maintenance mode")
        }

        fn try_reserve_purge(
            &self,
            _table_id: TableId,
            _max_in_flight: usize,
        ) -> Option<PurgeReservation> {
            panic!("try_reserve_purge should not be called in maintenance mode")
        }
    }

    #[derive(Default)]
    struct SoftDropSchedulerCtx {
        dropped_tables: StdMutex<Vec<DroppedTableName>>,
        purge_attempts: StdMutex<Vec<TableId>>,
        failed_table: StdMutex<Option<TableId>>,
        never_complete_tables: StdMutex<HashSet<TableId>>,
        in_flight_purges: Arc<StdMutex<HashSet<TableId>>>,
        purge_scan_cursor: AtomicUsize,
        region_gc_calls: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl SchedulerCtx for SoftDropSchedulerCtx {
        async fn get_table_to_region_stats(&self) -> Result<HashMap<TableId, Vec<RegionStat>>> {
            self.region_gc_calls.fetch_add(1, Ordering::Relaxed);
            Ok(HashMap::new())
        }

        async fn get_table_reparts(&self) -> Result<Vec<(TableId, TableRepartValue)>> {
            Ok(vec![])
        }

        async fn get_table_route(
            &self,
            _table_id: TableId,
        ) -> Result<(TableId, PhysicalTableRouteValue)> {
            unreachable!()
        }

        async fn batch_get_table_route(
            &self,
            _table_ids: &[TableId],
        ) -> Result<HashMap<TableId, PhysicalTableRouteValue>> {
            Ok(HashMap::new())
        }

        async fn gc_regions(
            &self,
            _region_ids: &[RegionId],
            _full_file_listing: bool,
            _timeout: Duration,
            _region_routes_override: Region2Peers,
        ) -> Result<GcReport> {
            Ok(GcReport::default())
        }

        async fn list_dropped_tables(&self) -> Result<Vec<DroppedTableName>> {
            Ok(self.dropped_tables.lock().unwrap().clone())
        }

        async fn purge_dropped_table(&self, table_id: TableId) -> Result<()> {
            self.purge_attempts.lock().unwrap().push(table_id);
            if self
                .never_complete_tables
                .lock()
                .unwrap()
                .contains(&table_id)
            {
                std::future::pending().await
            }
            if *self.failed_table.lock().unwrap() == Some(table_id) {
                return crate::error::UnexpectedSnafu {
                    violated: format!("mock purge failure for table {table_id}"),
                }
                .fail();
            }
            Ok(())
        }

        fn try_reserve_purge(
            &self,
            table_id: TableId,
            max_in_flight: usize,
        ) -> Option<PurgeReservation> {
            PurgeReservation::try_new(self.in_flight_purges.clone(), table_id, max_in_flight)
        }

        fn next_purge_scan_start(&self, table_count: usize) -> usize {
            if table_count == 0 {
                return 0;
            }
            self.purge_scan_cursor.fetch_add(1, Ordering::Relaxed) % table_count
        }
    }

    fn dropped_table(table_id: TableId, dropped_at: Option<i64>) -> DroppedTableName {
        DroppedTableName {
            table_id,
            table_name: TableName::new("greptime", "public", format!("table_{table_id}")),
            dropped_at,
            retention_expires_at: None,
        }
    }

    fn dropped_table_with_deadline(table_id: TableId, deadline: i64) -> DroppedTableName {
        DroppedTableName {
            retention_expires_at: Some(deadline),
            ..dropped_table(table_id, Some(i64::MAX))
        }
    }

    fn soft_drop_scheduler(ctx: Arc<dyn SchedulerCtx>) -> GcScheduler {
        let (tx, rx) = GcScheduler::channel();
        drop(tx);
        GcScheduler {
            ctx,
            runtime_switch_manager: new_test_runtime_switch_manager(),
            receiver: rx,
            config: GcSchedulerOptions {
                enable: true,
                experimental_soft_drop: crate::gc::options::ExperimentalSoftDropGcOptions {
                    enable: true,
                    retention: Duration::from_millis(100),
                },
                ..Default::default()
            },
            region_gc_tracker: Arc::new(Mutex::new(HashMap::new())),
            last_tracker_cleanup: Arc::new(Mutex::new(Instant::now())),
        }
    }

    async fn wait_for_purge_attempts(ctx: &SoftDropSchedulerCtx, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            while ctx.purge_attempts.lock().unwrap().len() < expected {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("purge tasks should be scheduled");
    }

    #[tokio::test]
    async fn test_purge_expired_soft_dropped_tables_filters_by_retention_and_timestamp() {
        let ctx = Arc::new(SoftDropSchedulerCtx::default());
        *ctx.dropped_tables.lock().unwrap() = vec![
            dropped_table(1, Some(899)),
            dropped_table(2, Some(900)),
            dropped_table(3, Some(901)),
            dropped_table(4, None),
            dropped_table(5, Some(i64::MAX)),
        ];
        let scheduler = soft_drop_scheduler(ctx.clone());

        scheduler.purge_expired_soft_dropped_tables(1_000).await;
        wait_for_purge_attempts(&ctx, 2).await;

        let mut attempts = ctx.purge_attempts.lock().unwrap().clone();
        attempts.sort_unstable();
        assert_eq!(vec![1, 2], attempts);
    }

    #[tokio::test]
    async fn test_purge_uses_fixed_deadline_before_legacy_retention_fallback() {
        let ctx = Arc::new(SoftDropSchedulerCtx::default());
        *ctx.dropped_tables.lock().unwrap() = vec![
            dropped_table_with_deadline(1, 1_000),
            dropped_table_with_deadline(2, 1_001),
            dropped_table(3, Some(900)),
        ];
        let scheduler = soft_drop_scheduler(ctx.clone());

        scheduler.purge_expired_soft_dropped_tables(1_000).await;
        wait_for_purge_attempts(&ctx, 2).await;

        let mut attempts = ctx.purge_attempts.lock().unwrap().clone();
        attempts.sort_unstable();
        assert_eq!(vec![1, 3], attempts);
    }

    #[tokio::test]
    async fn test_purge_saturates_legacy_retention_that_exceeds_i64() {
        let ctx = Arc::new(SoftDropSchedulerCtx::default());
        *ctx.dropped_tables.lock().unwrap() = vec![dropped_table(1, Some(0))];
        ctx.never_complete_tables.lock().unwrap().insert(1);
        let mut scheduler = soft_drop_scheduler(ctx.clone());
        scheduler.config.experimental_soft_drop.retention =
            Duration::from_millis(i64::MAX as u64 + 1);

        scheduler.purge_expired_soft_dropped_tables(0).await;

        assert!(!ctx.in_flight_purges.lock().unwrap().contains(&1));
    }

    #[tokio::test]
    async fn test_purge_failure_does_not_prevent_other_purges() {
        let ctx = Arc::new(SoftDropSchedulerCtx::default());
        *ctx.dropped_tables.lock().unwrap() = vec![
            dropped_table(1, Some(0)),
            dropped_table(2, Some(0)),
            dropped_table(3, Some(0)),
        ];
        *ctx.failed_table.lock().unwrap() = Some(2);
        let scheduler = soft_drop_scheduler(ctx.clone());

        scheduler.purge_expired_soft_dropped_tables(1_000).await;
        wait_for_purge_attempts(&ctx, 3).await;

        let mut attempts = ctx.purge_attempts.lock().unwrap().clone();
        attempts.sort_unstable();
        assert_eq!(vec![1, 2, 3], attempts);
    }

    #[tokio::test]
    async fn test_purge_scans_rotate_after_failure() {
        let ctx = Arc::new(SoftDropSchedulerCtx::default());
        *ctx.dropped_tables.lock().unwrap() =
            vec![dropped_table(1, Some(0)), dropped_table(2, Some(0))];
        *ctx.failed_table.lock().unwrap() = Some(1);
        let mut scheduler = soft_drop_scheduler(ctx.clone());
        scheduler.config.max_concurrent_tables = 1;

        scheduler.purge_expired_soft_dropped_tables(1_000).await;
        wait_for_purge_attempts(&ctx, 1).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while !ctx.in_flight_purges.lock().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed purge should release its in-flight slot");

        scheduler.purge_expired_soft_dropped_tables(1_000).await;
        wait_for_purge_attempts(&ctx, 2).await;

        assert_eq!(vec![1, 2], *ctx.purge_attempts.lock().unwrap());
    }

    #[tokio::test]
    async fn test_tick_ignores_experimental_soft_drop_config_and_still_runs_region_gc() {
        let ctx = Arc::new(SoftDropSchedulerCtx::default());
        *ctx.dropped_tables.lock().unwrap() = vec![dropped_table(1, Some(i64::MIN))];
        let scheduler = soft_drop_scheduler(ctx.clone());

        scheduler.handle_tick().await.unwrap();

        assert!(ctx.purge_attempts.lock().unwrap().is_empty());
        assert_eq!(1, ctx.region_gc_calls.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_tick_with_no_tombstones_makes_no_purge_submissions() {
        let ctx = Arc::new(SoftDropSchedulerCtx::default());
        let scheduler = soft_drop_scheduler(ctx.clone());

        scheduler.handle_tick().await.unwrap();

        assert!(ctx.purge_attempts.lock().unwrap().is_empty());
        assert_eq!(1, ctx.region_gc_calls.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_multiple_purge_scans_do_not_duplicate_in_flight_purges() {
        let ctx = Arc::new(SoftDropSchedulerCtx::default());
        *ctx.dropped_tables.lock().unwrap() = vec![
            dropped_table(1, Some(i64::MIN)),
            dropped_table(2, Some(i64::MIN)),
        ];
        ctx.never_complete_tables.lock().unwrap().insert(1);
        *ctx.failed_table.lock().unwrap() = Some(2);
        let mut scheduler = soft_drop_scheduler(ctx.clone());
        scheduler.config.max_concurrent_tables = 2;

        tokio::time::timeout(
            Duration::from_millis(100),
            scheduler.purge_expired_soft_dropped_tables(1_000),
        )
        .await
        .expect("first scan should not wait for purge completion");
        wait_for_purge_attempts(&ctx, 2).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while ctx.in_flight_purges.lock().unwrap().contains(&2) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("terminal failure should release its in-flight slot");
        tokio::time::timeout(
            Duration::from_millis(100),
            scheduler.purge_expired_soft_dropped_tables(1_000),
        )
        .await
        .expect("second scan should not wait for purge completion");
        wait_for_purge_attempts(&ctx, 3).await;

        let attempts = ctx.purge_attempts.lock().unwrap().clone();
        assert_eq!(
            1,
            attempts.iter().filter(|&&table_id| table_id == 1).count()
        );
        assert_eq!(
            2,
            attempts.iter().filter(|&&table_id| table_id == 2).count()
        );
        assert_eq!(0, ctx.region_gc_calls.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_purge_scan_caps_submissions() {
        let ctx = Arc::new(SoftDropSchedulerCtx::default());
        *ctx.dropped_tables.lock().unwrap() = vec![
            dropped_table(1, Some(i64::MIN)),
            dropped_table(2, Some(i64::MIN)),
            dropped_table(3, Some(i64::MIN)),
        ];
        let mut scheduler = soft_drop_scheduler(ctx.clone());
        scheduler.config.max_concurrent_tables = 2;
        ctx.never_complete_tables.lock().unwrap().extend([1, 2]);

        scheduler.purge_expired_soft_dropped_tables(1_000).await;
        wait_for_purge_attempts(&ctx, 2).await;

        assert_eq!(2, ctx.purge_attempts.lock().unwrap().len());
        assert_eq!(0, ctx.region_gc_calls.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_manual_gc_does_not_purge_soft_dropped_tables() {
        let ctx = Arc::new(SoftDropSchedulerCtx::default());
        *ctx.dropped_tables.lock().unwrap() = vec![dropped_table(1, Some(i64::MIN))];
        let scheduler = soft_drop_scheduler(ctx.clone());

        scheduler.handle_manual_gc(None, None, None).await.unwrap();

        assert!(ctx.purge_attempts.lock().unwrap().is_empty());
    }

    struct ErrorMockSchedulerCtx;

    #[async_trait::async_trait]
    impl SchedulerCtx for ErrorMockSchedulerCtx {
        async fn get_table_to_region_stats(&self) -> Result<HashMap<TableId, Vec<RegionStat>>> {
            Ok(HashMap::new())
        }

        async fn get_table_reparts(&self) -> Result<Vec<(TableId, TableRepartValue)>> {
            Ok(vec![])
        }

        async fn get_table_route(
            &self,
            _table_id: TableId,
        ) -> Result<(TableId, PhysicalTableRouteValue)> {
            unreachable!("get_table_route should not be called in this test")
        }

        async fn batch_get_table_route(
            &self,
            _table_ids: &[TableId],
        ) -> Result<HashMap<TableId, PhysicalTableRouteValue>> {
            Ok(HashMap::new())
        }

        async fn gc_regions(
            &self,
            _region_ids: &[RegionId],
            _full_file_listing: bool,
            _timeout: Duration,
            _region_routes_override: Region2Peers,
        ) -> Result<GcReport> {
            crate::error::UnexpectedSnafu {
                violated: "mock gc failure".to_string(),
            }
            .fail()
        }

        async fn list_dropped_tables(&self) -> Result<Vec<DroppedTableName>> {
            Ok(vec![])
        }

        async fn purge_dropped_table(&self, _table_id: TableId) -> Result<()> {
            Ok(())
        }

        fn try_reserve_purge(
            &self,
            table_id: TableId,
            max_in_flight: usize,
        ) -> Option<PurgeReservation> {
            PurgeReservation::try_new(
                Arc::new(StdMutex::new(HashSet::new())),
                table_id,
                max_in_flight,
            )
        }
    }

    #[tokio::test]
    async fn test_handle_manual_gc_propagates_error() {
        let (tx, rx) = GcScheduler::channel();
        drop(tx);

        let scheduler = GcScheduler {
            ctx: Arc::new(ErrorMockSchedulerCtx),
            runtime_switch_manager: new_test_runtime_switch_manager(),
            receiver: rx,
            config: GcSchedulerOptions::default(),
            region_gc_tracker: Arc::new(Mutex::new(HashMap::new())),
            last_tracker_cleanup: Arc::new(Mutex::new(Instant::now())),
        };

        let result = scheduler
            .handle_manual_gc(
                Some(vec![RegionId::new(1, 0)]),
                Some(false),
                Some(Duration::from_secs(1)),
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_maintenance_mode_skips_manual_gc() {
        let (tx, rx) = GcScheduler::channel();
        drop(tx);
        let runtime_switch_manager = new_test_runtime_switch_manager();
        runtime_switch_manager.set_maintenance_mode().await.unwrap();

        let ctx = Arc::new(CountingSchedulerCtx::default());
        let scheduler = GcScheduler {
            ctx: ctx.clone(),
            runtime_switch_manager,
            receiver: rx,
            config: GcSchedulerOptions::default(),
            region_gc_tracker: Arc::new(Mutex::new(HashMap::new())),
            last_tracker_cleanup: Arc::new(Mutex::new(Instant::now())),
        };

        let result = scheduler
            .handle_manual_gc(
                Some(vec![RegionId::new(1, 0)]),
                Some(false),
                Some(Duration::from_secs(1)),
            )
            .await;

        let err = result.unwrap_err();
        assert!(matches!(
            err,
            error::Error::ManualGcRejectedByMaintenanceMode { .. }
        ));
        assert!(err.to_string().contains("maintenance mode is enabled"));
        ctx.assert_no_scheduler_work();
    }

    #[tokio::test]
    async fn test_maintenance_mode_skips_tick_gc() {
        let (tx, rx) = GcScheduler::channel();
        drop(tx);
        let runtime_switch_manager = new_test_runtime_switch_manager();
        runtime_switch_manager.set_maintenance_mode().await.unwrap();

        let ctx = Arc::new(CountingSchedulerCtx::default());
        let scheduler = GcScheduler {
            ctx: ctx.clone(),
            runtime_switch_manager,
            receiver: rx,
            config: GcSchedulerOptions::default(),
            region_gc_tracker: Arc::new(Mutex::new(HashMap::new())),
            last_tracker_cleanup: Arc::new(Mutex::new(Instant::now())),
        };

        let result = scheduler.handle_tick().await;

        assert!(result.is_ok());
        ctx.assert_no_scheduler_work();
    }
}
