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

mod buckets;
mod picker;
mod task;
#[cfg(test)]
mod test_util;
mod twcs;
mod window;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::region::compact_request;
use common_telemetry::{debug, error};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
pub use picker::CompactionPickerRef;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;
use table::predicate::Predicate;
use tokio::sync::mpsc::{self, Sender};

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::compaction::twcs::TwcsPicker;
use crate::compaction::window::WindowedCompactionPicker;
use crate::config::MitoConfig;
use crate::error::{
    CompactRegionSnafu, Error, RegionClosedSnafu, RegionDroppedSnafu, RegionTruncatedSnafu, Result,
    TimeRangePredicateOverflowSnafu,
};
use crate::metrics::COMPACTION_STAGE_ELAPSED;
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::ScanInput;
use crate::read::seq_scan::SeqScan;
use crate::read::BoxedBatchReader;
use crate::region::options::CompactionOptions;
use crate::region::version::{VersionControlRef, VersionRef};
use crate::region::ManifestContextRef;
use crate::request::{OptionOutputTx, OutputTx, WorkerRequest};
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::{FileHandle, FileId, Level};
use crate::sst::file_purger::FilePurgerRef;
use crate::sst::version::LevelMeta;
use crate::worker::WorkerListener;

/// Region compaction request.
pub struct CompactionRequest {
    pub(crate) engine_config: Arc<MitoConfig>,
    pub(crate) current_version: VersionRef,
    pub(crate) access_layer: AccessLayerRef,
    /// Sender to send notification to the region worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,
    /// Waiters of the compaction request.
    pub(crate) waiters: Vec<OutputTx>,
    pub(crate) file_purger: FilePurgerRef,
    /// Start time of compaction task.
    pub(crate) start_time: Instant,
    pub(crate) cache_manager: CacheManagerRef,
    pub(crate) manifest_ctx: ManifestContextRef,
    pub(crate) version_control: VersionControlRef,
    pub(crate) listener: WorkerListener,
}

impl CompactionRequest {
    pub(crate) fn region_id(&self) -> RegionId {
        self.current_version.metadata.region_id
    }

    /// Push waiter to the request.
    pub(crate) fn push_waiter(&mut self, mut waiter: OptionOutputTx) {
        if let Some(waiter) = waiter.take_inner() {
            self.waiters.push(waiter);
        }
    }
}

/// Builds compaction picker according to [CompactionOptions].
pub fn compaction_options_to_picker(strategy: &CompactionOptions) -> CompactionPickerRef {
    match strategy {
        CompactionOptions::Twcs(twcs_opts) => Arc::new(TwcsPicker::new(
            twcs_opts.max_active_window_files,
            twcs_opts.max_inactive_window_files,
            twcs_opts.time_window_seconds(),
        )) as Arc<_>,
    }
}

/// Compaction scheduler tracks and manages compaction tasks.
pub(crate) struct CompactionScheduler {
    scheduler: SchedulerRef,
    /// Compacting regions.
    region_status: HashMap<RegionId, CompactionStatus>,
    /// Request sender of the worker that this scheduler belongs to.
    request_sender: Sender<WorkerRequest>,
    cache_manager: CacheManagerRef,
    engine_config: Arc<MitoConfig>,
    listener: WorkerListener,
}

impl CompactionScheduler {
    pub(crate) fn new(
        scheduler: SchedulerRef,
        request_sender: Sender<WorkerRequest>,
        cache_manager: CacheManagerRef,
        engine_config: Arc<MitoConfig>,
        listener: WorkerListener,
    ) -> Self {
        Self {
            scheduler,
            region_status: HashMap::new(),
            request_sender,
            cache_manager,
            engine_config,
            listener,
        }
    }

    /// Schedules a compaction for the region.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn schedule_compaction(
        &mut self,
        region_id: RegionId,
        compact_options: compact_request::Options,
        version_control: &VersionControlRef,
        access_layer: &AccessLayerRef,
        file_purger: &FilePurgerRef,
        waiter: OptionOutputTx,
        manifest_ctx: &ManifestContextRef,
    ) -> Result<()> {
        if let Some(status) = self.region_status.get_mut(&region_id) {
            // Region is compacting. Add the waiter to pending list.
            status.merge_waiter(waiter);
            return Ok(());
        }

        // The region can compact directly.
        let mut status = CompactionStatus::new(
            region_id,
            version_control.clone(),
            access_layer.clone(),
            file_purger.clone(),
        );
        let request = status.new_compaction_request(
            self.request_sender.clone(),
            waiter,
            self.engine_config.clone(),
            self.cache_manager.clone(),
            manifest_ctx,
            self.listener.clone(),
        );
        self.region_status.insert(region_id, status);
        self.schedule_compaction_request(request, compact_options)
    }

    /// Notifies the scheduler that the compaction job is finished successfully.
    pub(crate) fn on_compaction_finished(
        &mut self,
        region_id: RegionId,
        manifest_ctx: &ManifestContextRef,
    ) {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return;
        };

        // We should always try to compact the region until picker returns None.
        let request = status.new_compaction_request(
            self.request_sender.clone(),
            OptionOutputTx::none(),
            self.engine_config.clone(),
            self.cache_manager.clone(),
            manifest_ctx,
            self.listener.clone(),
        );
        // Try to schedule next compaction task for this region.
        if let Err(e) = self.schedule_compaction_request(
            request,
            compact_request::Options::Regular(Default::default()),
        ) {
            error!(e; "Failed to schedule next compaction for region {}", region_id);
        }
    }

    /// Notifies the scheduler that the compaction job is failed.
    pub(crate) fn on_compaction_failed(&mut self, region_id: RegionId, err: Arc<Error>) {
        error!(err; "Region {} failed to compact, cancel all pending tasks", region_id);
        // Remove this region.
        let Some(status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Fast fail: cancels all pending tasks and sends error to their waiters.
        status.on_failure(err);
    }

    /// Notifies the scheduler that the region is dropped.
    pub(crate) fn on_region_dropped(&mut self, region_id: RegionId) {
        self.remove_region_on_failure(
            region_id,
            Arc::new(RegionDroppedSnafu { region_id }.build()),
        );
    }

    /// Notifies the scheduler that the region is closed.
    pub(crate) fn on_region_closed(&mut self, region_id: RegionId) {
        self.remove_region_on_failure(region_id, Arc::new(RegionClosedSnafu { region_id }.build()));
    }

    /// Notifies the scheduler that the region is truncated.
    pub(crate) fn on_region_truncated(&mut self, region_id: RegionId) {
        self.remove_region_on_failure(
            region_id,
            Arc::new(RegionTruncatedSnafu { region_id }.build()),
        );
    }

    /// Schedules a compaction request.
    ///
    /// If the region has nothing to compact, it removes the region from the status map.
    fn schedule_compaction_request(
        &mut self,
        request: CompactionRequest,
        options: compact_request::Options,
    ) -> Result<()> {
        let picker = if let compact_request::Options::StrictWindow(window) = &options {
            let window = if window.window_seconds == 0 {
                None
            } else {
                Some(window.window_seconds)
            };
            Arc::new(WindowedCompactionPicker::new(window)) as Arc<_>
        } else {
            compaction_options_to_picker(&request.current_version.options.compaction)
        };

        let region_id = request.region_id();
        debug!(
            "Pick compaction strategy {:?} for region: {}",
            picker, region_id
        );

        let pick_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["pick"])
            .start_timer();
        let Some(mut task) = picker.pick(request) else {
            // Nothing to compact, remove it from the region status map.
            self.region_status.remove(&region_id);
            return Ok(());
        };

        drop(pick_timer);

        // Submit the compaction task.
        self.scheduler
            .schedule(Box::pin(async move {
                task.run().await;
            }))
            .map_err(|e| {
                error!(e; "Failed to submit compaction request for region {}", region_id);
                // If failed to submit the job, we need to remove the region from the scheduler.
                self.region_status.remove(&region_id);
                e
            })
    }

    fn remove_region_on_failure(&mut self, region_id: RegionId, err: Arc<Error>) {
        // Remove this region.
        let Some(status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Notifies all pending tasks.
        status.on_failure(err);
    }
}

impl Drop for CompactionScheduler {
    fn drop(&mut self) {
        for (region_id, status) in self.region_status.drain() {
            // We are shutting down so notify all pending tasks.
            status.on_failure(Arc::new(RegionClosedSnafu { region_id }.build()));
        }
    }
}

/// Pending compaction tasks.
struct PendingCompaction {
    waiters: Vec<OutputTx>,
}

impl PendingCompaction {
    /// Push waiter to the request.
    fn push_waiter(&mut self, mut waiter: OptionOutputTx) {
        if let Some(waiter) = waiter.take_inner() {
            self.waiters.push(waiter);
        }
    }

    /// Send compaction error to waiter.
    fn on_failure(&mut self, region_id: RegionId, err: Arc<Error>) {
        for waiter in self.waiters.drain(..) {
            waiter.send(Err(err.clone()).context(CompactRegionSnafu { region_id }));
        }
    }
}

/// Status of running and pending region compaction tasks.
struct CompactionStatus {
    /// Id of the region.
    region_id: RegionId,
    /// Version control of the region.
    version_control: VersionControlRef,
    /// Access layer of the region.
    access_layer: AccessLayerRef,
    /// File purger of the region.
    file_purger: FilePurgerRef,
    /// Compaction pending to schedule.
    ///
    /// For simplicity, we merge all pending compaction requests into one.
    pending_compaction: Option<PendingCompaction>,
}

impl CompactionStatus {
    /// Creates a new [CompactionStatus]
    fn new(
        region_id: RegionId,
        version_control: VersionControlRef,
        access_layer: AccessLayerRef,
        file_purger: FilePurgerRef,
    ) -> CompactionStatus {
        CompactionStatus {
            region_id,
            version_control,
            access_layer,
            file_purger,
            pending_compaction: None,
        }
    }

    /// Merge the watier to the pending compaction.
    fn merge_waiter(&mut self, waiter: OptionOutputTx) {
        let pending = self
            .pending_compaction
            .get_or_insert_with(|| PendingCompaction {
                waiters: Vec::new(),
            });
        pending.push_waiter(waiter);
    }

    fn on_failure(self, err: Arc<Error>) {
        if let Some(mut pending) = self.pending_compaction {
            pending.on_failure(self.region_id, err.clone());
        }
    }

    /// Creates a new compaction request for compaction picker.
    ///
    /// It consumes all pending compaction waiters.
    #[allow(clippy::too_many_arguments)]
    fn new_compaction_request(
        &mut self,
        request_sender: Sender<WorkerRequest>,
        waiter: OptionOutputTx,
        engine_config: Arc<MitoConfig>,
        cache_manager: CacheManagerRef,
        manifest_ctx: &ManifestContextRef,
        listener: WorkerListener,
    ) -> CompactionRequest {
        let current_version = self.version_control.current().version;
        let start_time = Instant::now();
        let mut req = CompactionRequest {
            engine_config,
            current_version,
            access_layer: self.access_layer.clone(),
            request_sender: request_sender.clone(),
            waiters: Vec::new(),
            file_purger: self.file_purger.clone(),
            start_time,
            cache_manager,
            manifest_ctx: manifest_ctx.clone(),
            version_control: self.version_control.clone(),
            listener,
        };

        if let Some(pending) = self.pending_compaction.take() {
            req.waiters = pending.waiters;
        }
        req.push_waiter(waiter);

        req
    }
}

#[derive(Debug)]
pub(crate) struct CompactionOutput {
    pub output_file_id: FileId,
    /// Compaction output file level.
    pub output_level: Level,
    /// Compaction input files.
    pub inputs: Vec<FileHandle>,
    /// Whether to remove deletion markers.
    pub filter_deleted: bool,
    /// Compaction output time range.
    pub output_time_range: Option<TimestampRange>,
}

/// Builds [BoxedBatchReader] that reads all SST files and yields batches in primary key order.
async fn build_sst_reader(
    metadata: RegionMetadataRef,
    sst_layer: AccessLayerRef,
    cache: Option<CacheManagerRef>,
    inputs: &[FileHandle],
    append_mode: bool,
    filter_deleted: bool,
    time_range: Option<TimestampRange>,
) -> Result<BoxedBatchReader> {
    let mut scan_input = ScanInput::new(sst_layer, ProjectionMapper::all(&metadata)?)
        .with_files(inputs.to_vec())
        .with_append_mode(append_mode)
        .with_cache(cache)
        .with_filter_deleted(filter_deleted)
        // We ignore file not found error during compaction.
        .with_ignore_file_not_found(true);

    // This serves as a workaround of https://github.com/GreptimeTeam/greptimedb/issues/3944
    // by converting time ranges into predicate.
    if let Some(time_range) = time_range {
        scan_input = scan_input.with_predicate(time_range_to_predicate(time_range, &metadata)?);
    }

    SeqScan::new(scan_input).build_reader().await
}

/// Converts time range to predicates so that rows outside the range will be filtered.
fn time_range_to_predicate(
    range: TimestampRange,
    metadata: &RegionMetadataRef,
) -> Result<Option<Predicate>> {
    let ts_col = metadata.time_index_column();

    // safety: time index column's type must be a valid timestamp type.
    let ts_col_unit = ts_col
        .column_schema
        .data_type
        .as_timestamp()
        .unwrap()
        .unit();

    let exprs = match (range.start(), range.end()) {
        (Some(start), Some(end)) => {
            vec![
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .gt_eq(ts_to_lit(*start, ts_col_unit)?),
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .lt(ts_to_lit(*end, ts_col_unit)?),
            ]
        }
        (Some(start), None) => {
            vec![datafusion_expr::col(ts_col.column_schema.name.clone())
                .gt_eq(ts_to_lit(*start, ts_col_unit)?)]
        }

        (None, Some(end)) => {
            vec![datafusion_expr::col(ts_col.column_schema.name.clone())
                .lt(ts_to_lit(*end, ts_col_unit)?)]
        }
        (None, None) => {
            return Ok(None);
        }
    };
    Ok(Some(Predicate::new(exprs)))
}

fn ts_to_lit(ts: Timestamp, ts_col_unit: TimeUnit) -> Result<Expr> {
    let ts = ts
        .convert_to(ts_col_unit)
        .context(TimeRangePredicateOverflowSnafu {
            timestamp: ts,
            unit: ts_col_unit,
        })?;
    let val = ts.value();
    let scalar_value = match ts_col_unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(val), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(val), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(val), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(val), None),
    };
    Ok(datafusion_expr::lit(scalar_value))
}

/// Finds all expired SSTs across levels.
fn get_expired_ssts(
    levels: &[LevelMeta],
    ttl: Option<Duration>,
    now: Timestamp,
) -> Vec<FileHandle> {
    let Some(ttl) = ttl else {
        return vec![];
    };

    let expire_time = match now.sub_duration(ttl) {
        Ok(expire_time) => expire_time,
        Err(e) => {
            error!(e; "Failed to calculate region TTL expire time");
            return vec![];
        }
    };

    levels
        .iter()
        .flat_map(|l| l.get_expired_files(&expire_time).into_iter())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::oneshot;

    use super::*;
    use crate::schedule::scheduler::{Job, Scheduler};
    use crate::test_util::scheduler_util::SchedulerEnv;
    use crate::test_util::version_util::{apply_edit, VersionControlBuilder};

    #[tokio::test]
    async fn test_schedule_empty() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
        let purger = builder.file_purger();

        // Nothing to compact.
        let version_control = Arc::new(builder.build());
        let (output_tx, output_rx) = oneshot::channel();
        let waiter = OptionOutputTx::from(output_tx);
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        scheduler
            .schedule_compaction(
                builder.region_id(),
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                &purger,
                waiter,
                &manifest_ctx,
            )
            .unwrap();
        let output = output_rx.await.unwrap().unwrap();
        assert_eq!(output, 0);
        assert!(scheduler.region_status.is_empty());

        // Only one file, picker won't compact it.
        let version_control = Arc::new(builder.push_l0_file(0, 1000).build());
        let (output_tx, output_rx) = oneshot::channel();
        let waiter = OptionOutputTx::from(output_tx);
        scheduler
            .schedule_compaction(
                builder.region_id(),
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                &purger,
                waiter,
                &manifest_ctx,
            )
            .unwrap();
        let output = output_rx.await.unwrap().unwrap();
        assert_eq!(output, 0);
        assert!(scheduler.region_status.is_empty());
    }

    #[derive(Default)]
    struct VecScheduler {
        jobs: Mutex<Vec<Job>>,
    }

    impl VecScheduler {
        fn num_jobs(&self) -> usize {
            self.jobs.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl Scheduler for VecScheduler {
        fn schedule(&self, job: Job) -> Result<()> {
            self.jobs.lock().unwrap().push(job);
            Ok(())
        }

        async fn stop(&self, _await_termination: bool) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_schedule_on_finished() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
        let purger = builder.file_purger();
        let region_id = builder.region_id();

        // 5 files to compact.
        let end = 1000 * 1000;
        let version_control = Arc::new(
            builder
                .push_l0_file(0, end)
                .push_l0_file(10, end)
                .push_l0_file(50, end)
                .push_l0_file(80, end)
                .push_l0_file(90, end)
                .build(),
        );
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                &purger,
                OptionOutputTx::none(),
                &manifest_ctx,
            )
            .unwrap();
        // Should schedule 1 compaction.
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(1, job_scheduler.num_jobs());
        let data = version_control.current();
        let file_metas: Vec<_> = data.version.ssts.levels()[0]
            .files
            .values()
            .map(|file| file.meta_ref().clone())
            .collect();

        // 5 files for next compaction and removes old files.
        apply_edit(
            &version_control,
            &[(0, end), (20, end), (40, end), (60, end), (80, end)],
            &file_metas,
            purger.clone(),
        );
        // The task is pending.
        scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                &purger,
                OptionOutputTx::none(),
                &manifest_ctx,
            )
            .unwrap();
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(1, job_scheduler.num_jobs());
        assert!(scheduler
            .region_status
            .get(&builder.region_id())
            .unwrap()
            .pending_compaction
            .is_some());

        // On compaction finished and schedule next compaction.
        scheduler.on_compaction_finished(region_id, &manifest_ctx);
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(2, job_scheduler.num_jobs());
        // 5 files for next compaction.
        apply_edit(
            &version_control,
            &[(0, end), (20, end), (40, end), (60, end), (80, end)],
            &[],
            purger.clone(),
        );
        // The task is pending.
        scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                &purger,
                OptionOutputTx::none(),
                &manifest_ctx,
            )
            .unwrap();
        assert_eq!(2, job_scheduler.num_jobs());
        assert!(scheduler
            .region_status
            .get(&builder.region_id())
            .unwrap()
            .pending_compaction
            .is_some());
    }
}
