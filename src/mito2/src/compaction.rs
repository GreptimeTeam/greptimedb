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
pub mod compactor;
pub mod picker;
mod run;
mod task;
#[cfg(test)]
mod test_util;
mod twcs;
mod window;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use api::v1::region::compact_request;
use common_base::{Plugins, TimeToLive};
use common_meta::key::SchemaMetadataManagerRef;
use common_telemetry::{debug, error, info, warn};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{RegionId, TableId};
use table::predicate::Predicate;
use tokio::sync::mpsc::{self, Sender};

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::compaction::compactor::{CompactionRegion, DefaultCompactor};
use crate::compaction::picker::{new_picker, CompactionTask};
use crate::compaction::task::CompactionTaskImpl;
use crate::config::MitoConfig;
use crate::error::{
    CompactRegionSnafu, Error, GetSchemaMetadataSnafu, RegionClosedSnafu, RegionDroppedSnafu,
    RegionTruncatedSnafu, RemoteCompactionSnafu, Result, TimeRangePredicateOverflowSnafu,
};
use crate::metrics::COMPACTION_STAGE_ELAPSED;
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::ScanInput;
use crate::read::seq_scan::SeqScan;
use crate::read::BoxedBatchReader;
use crate::region::options::MergeMode;
use crate::region::version::{VersionControlRef, VersionRef};
use crate::region::ManifestContextRef;
use crate::request::{OptionOutputTx, OutputTx, WorkerRequest};
use crate::schedule::remote_job_scheduler::{
    CompactionJob, DefaultNotifier, RemoteJob, RemoteJobSchedulerRef,
};
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::{FileHandle, FileId, FileMeta, Level};
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
    /// Start time of compaction task.
    pub(crate) start_time: Instant,
    pub(crate) cache_manager: CacheManagerRef,
    pub(crate) manifest_ctx: ManifestContextRef,
    pub(crate) listener: WorkerListener,
    pub(crate) schema_metadata_manager: SchemaMetadataManagerRef,
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
    /// Plugins for the compaction scheduler.
    plugins: Plugins,
}

impl CompactionScheduler {
    pub(crate) fn new(
        scheduler: SchedulerRef,
        request_sender: Sender<WorkerRequest>,
        cache_manager: CacheManagerRef,
        engine_config: Arc<MitoConfig>,
        listener: WorkerListener,
        plugins: Plugins,
    ) -> Self {
        Self {
            scheduler,
            region_status: HashMap::new(),
            request_sender,
            cache_manager,
            engine_config,
            listener,
            plugins,
        }
    }

    /// Schedules a compaction for the region.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn schedule_compaction(
        &mut self,
        region_id: RegionId,
        compact_options: compact_request::Options,
        version_control: &VersionControlRef,
        access_layer: &AccessLayerRef,
        waiter: OptionOutputTx,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> Result<()> {
        if let Some(status) = self.region_status.get_mut(&region_id) {
            // Region is compacting. Add the waiter to pending list.
            status.merge_waiter(waiter);
            return Ok(());
        }

        // The region can compact directly.
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), access_layer.clone());
        let request = status.new_compaction_request(
            self.request_sender.clone(),
            waiter,
            self.engine_config.clone(),
            self.cache_manager.clone(),
            manifest_ctx,
            self.listener.clone(),
            schema_metadata_manager,
        );
        self.region_status.insert(region_id, status);
        let result = self
            .schedule_compaction_request(request, compact_options)
            .await;

        self.listener.on_compaction_scheduled(region_id);
        result
    }

    /// Notifies the scheduler that the compaction job is finished successfully.
    pub(crate) async fn on_compaction_finished(
        &mut self,
        region_id: RegionId,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
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
            schema_metadata_manager,
        );
        // Try to schedule next compaction task for this region.
        if let Err(e) = self
            .schedule_compaction_request(
                request,
                compact_request::Options::Regular(Default::default()),
            )
            .await
        {
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
    async fn schedule_compaction_request(
        &mut self,
        request: CompactionRequest,
        options: compact_request::Options,
    ) -> Result<()> {
        let picker = new_picker(
            &options,
            &request.current_version.options.compaction,
            request.current_version.options.append_mode,
        );
        let region_id = request.region_id();
        let CompactionRequest {
            engine_config,
            current_version,
            access_layer,
            request_sender,
            waiters,
            start_time,
            cache_manager,
            manifest_ctx,
            listener,
            schema_metadata_manager,
        } = request;

        let ttl = find_ttl(
            region_id.table_id(),
            current_version.options.ttl,
            &schema_metadata_manager,
        )
        .await
        .unwrap_or_else(|e| {
            warn!(e; "Failed to get ttl for region: {}", region_id);
            TimeToLive::default()
        });

        debug!(
            "Pick compaction strategy {:?} for region: {}, ttl: {:?}",
            picker, region_id, ttl
        );

        let compaction_region = CompactionRegion {
            region_id,
            region_dir: access_layer.region_dir().to_string(),
            current_version: current_version.clone(),
            region_options: current_version.options.clone(),
            engine_config: engine_config.clone(),
            region_metadata: current_version.metadata.clone(),
            cache_manager: cache_manager.clone(),
            access_layer: access_layer.clone(),
            manifest_ctx: manifest_ctx.clone(),
            file_purger: None,
            ttl: Some(ttl),
        };

        let picker_output = {
            let _pick_timer = COMPACTION_STAGE_ELAPSED
                .with_label_values(&["pick"])
                .start_timer();
            picker.pick(&compaction_region)
        };

        let picker_output = if let Some(picker_output) = picker_output {
            picker_output
        } else {
            // Nothing to compact, we are done. Notifies all waiters as we consume the compaction request.
            for waiter in waiters {
                waiter.send(Ok(0));
            }
            self.region_status.remove(&region_id);
            return Ok(());
        };

        // If specified to run compaction remotely, we schedule the compaction job remotely.
        // It will fall back to local compaction if there is no remote job scheduler.
        let waiters = if current_version.options.compaction.remote_compaction() {
            if let Some(remote_job_scheduler) = &self.plugins.get::<RemoteJobSchedulerRef>() {
                let remote_compaction_job = CompactionJob {
                    compaction_region: compaction_region.clone(),
                    picker_output: picker_output.clone(),
                    start_time,
                    waiters,
                };

                let result = remote_job_scheduler
                    .schedule(
                        RemoteJob::CompactionJob(remote_compaction_job),
                        Box::new(DefaultNotifier {
                            request_sender: request_sender.clone(),
                        }),
                    )
                    .await;

                match result {
                    Ok(job_id) => {
                        info!(
                            "Scheduled remote compaction job {} for region {}",
                            job_id, region_id
                        );
                        return Ok(());
                    }
                    Err(e) => {
                        if !current_version.options.compaction.fallback_to_local() {
                            error!(e; "Failed to schedule remote compaction job for region {}", region_id);
                            return RemoteCompactionSnafu {
                                region_id,
                                job_id: None,
                                reason: e.reason,
                            }
                            .fail();
                        }

                        error!(e; "Failed to schedule remote compaction job for region {}, fallback to local compaction", region_id);

                        // Return the waiters back to the caller for local compaction.
                        e.waiters
                    }
                }
            } else {
                debug!(
                    "Remote compaction is not enabled, fallback to local compaction for region {}",
                    region_id
                );
                waiters
            }
        } else {
            waiters
        };

        // Create a local compaction task.
        let mut local_compaction_task = Box::new(CompactionTaskImpl {
            request_sender,
            waiters,
            start_time,
            listener,
            picker_output,
            compaction_region,
            compactor: Arc::new(DefaultCompactor {}),
        });

        // Submit the compaction task.
        self.scheduler
            .schedule(Box::pin(async move {
                local_compaction_task.run().await;
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

/// Finds TTL of table by first examine table options then database options.
async fn find_ttl(
    table_id: TableId,
    table_ttl: Option<TimeToLive>,
    schema_metadata_manager: &SchemaMetadataManagerRef,
) -> Result<TimeToLive> {
    // If table TTL is set, we use it.
    if let Some(table_ttl) = table_ttl {
        return Ok(table_ttl);
    }

    let ttl = schema_metadata_manager
        .get_schema_options_by_table_id(table_id)
        .await
        .context(GetSchemaMetadataSnafu)?
        .and_then(|options| options.ttl)
        .unwrap_or_default();
    Ok(ttl)
}

/// Status of running and pending region compaction tasks.
struct CompactionStatus {
    /// Id of the region.
    region_id: RegionId,
    /// Version control of the region.
    version_control: VersionControlRef,
    /// Access layer of the region.
    access_layer: AccessLayerRef,
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
    ) -> CompactionStatus {
        CompactionStatus {
            region_id,
            version_control,
            access_layer,
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
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> CompactionRequest {
        let current_version = self.version_control.current().version;
        let start_time = Instant::now();
        let mut req = CompactionRequest {
            engine_config,
            current_version,
            access_layer: self.access_layer.clone(),
            request_sender: request_sender.clone(),
            waiters: Vec::new(),
            start_time,
            cache_manager,
            manifest_ctx: manifest_ctx.clone(),
            listener,
            schema_metadata_manager,
        };

        if let Some(pending) = self.pending_compaction.take() {
            req.waiters = pending.waiters;
        }
        req.push_waiter(waiter);

        req
    }
}

#[derive(Debug, Clone)]
pub struct CompactionOutput {
    pub output_file_id: FileId,
    /// Compaction output file level.
    pub output_level: Level,
    /// Compaction input files.
    pub inputs: Vec<FileHandle>,
    /// Whether to remove deletion markers.
    pub filter_deleted: bool,
    /// Compaction output time range. Only windowed compaction specifies output time range.
    pub output_time_range: Option<TimestampRange>,
}

/// SerializedCompactionOutput is a serialized version of [CompactionOutput] by replacing [FileHandle] with [FileMeta].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedCompactionOutput {
    output_file_id: FileId,
    output_level: Level,
    inputs: Vec<FileMeta>,
    filter_deleted: bool,
    output_time_range: Option<TimestampRange>,
}

/// Builders to create [BoxedBatchReader] for compaction.
struct CompactionSstReaderBuilder<'a> {
    metadata: RegionMetadataRef,
    sst_layer: AccessLayerRef,
    cache: CacheManagerRef,
    inputs: &'a [FileHandle],
    append_mode: bool,
    filter_deleted: bool,
    time_range: Option<TimestampRange>,
    merge_mode: MergeMode,
}

impl<'a> CompactionSstReaderBuilder<'a> {
    /// Builds [BoxedBatchReader] that reads all SST files and yields batches in primary key order.
    async fn build_sst_reader(self) -> Result<BoxedBatchReader> {
        let mut scan_input = ScanInput::new(self.sst_layer, ProjectionMapper::all(&self.metadata)?)
            .with_files(self.inputs.to_vec())
            .with_append_mode(self.append_mode)
            .with_cache(self.cache)
            .with_filter_deleted(self.filter_deleted)
            // We ignore file not found error during compaction.
            .with_ignore_file_not_found(true)
            .with_merge_mode(self.merge_mode);

        // This serves as a workaround of https://github.com/GreptimeTeam/greptimedb/issues/3944
        // by converting time ranges into predicate.
        if let Some(time_range) = self.time_range {
            scan_input =
                scan_input.with_predicate(time_range_to_predicate(time_range, &self.metadata)?);
        }

        SeqScan::new(scan_input)
            .with_compaction()
            .build_reader()
            .await
    }
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
    ttl: Option<TimeToLive>,
    now: Timestamp,
) -> Vec<FileHandle> {
    let Some(ttl) = ttl.and_then(|t| t.get_duration()) else {
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
    use tokio::sync::oneshot;

    use super::*;
    use crate::test_util::mock_schema_metadata_manager;
    use crate::test_util::scheduler_util::{SchedulerEnv, VecScheduler};
    use crate::test_util::version_util::{apply_edit, VersionControlBuilder};

    #[tokio::test]
    async fn test_schedule_empty() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
        let schema_metadata_manager = mock_schema_metadata_manager();
        schema_metadata_manager
            .register_region_table_info(
                builder.region_id().table_id(),
                "test_table",
                "test_catalog",
                "test_schema",
                None,
            )
            .await;
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
                waiter,
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await
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
                waiter,
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await
            .unwrap();
        let output = output_rx.await.unwrap().unwrap();
        assert_eq!(output, 0);
        assert!(scheduler.region_status.is_empty());
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

        let schema_metadata_manager = mock_schema_metadata_manager();
        schema_metadata_manager
            .register_region_table_info(
                builder.region_id().table_id(),
                "test_table",
                "test_catalog",
                "test_schema",
                None,
            )
            .await;

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
                OptionOutputTx::none(),
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await
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
                OptionOutputTx::none(),
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await
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
        scheduler
            .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager.clone())
            .await;
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
                OptionOutputTx::none(),
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await
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
