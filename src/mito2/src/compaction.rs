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
pub mod memory_manager;
pub mod picker;
pub mod run;
mod task;
#[cfg(test)]
mod test_util;
mod twcs;
mod window;

use std::collections::HashMap;
use std::mem;
use std::sync::Arc;
use std::time::Instant;

use api::v1::region::compact_request;
use api::v1::region::compact_request::Options;
use common_base::Plugins;
use common_meta::key::SchemaMetadataManagerRef;
use common_telemetry::{debug, error, info, warn};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::{TimeToLive, Timestamp};
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{RegionId, TableId};
use task::MAX_PARALLEL_COMPACTION;
use tokio::sync::mpsc::{self, Sender};

use crate::access_layer::AccessLayerRef;
use crate::cache::{CacheManagerRef, CacheStrategy};
use crate::compaction::compactor::{CompactionRegion, CompactionVersion, DefaultCompactor};
use crate::compaction::memory_manager::{CompactionMemoryManager, OnExhaustedPolicy};
use crate::compaction::picker::{CompactionTask, PickerOutput, new_picker};
use crate::compaction::task::CompactionTaskImpl;
use crate::config::MitoConfig;
use crate::error::{
    CompactRegionSnafu, CompactionMemoryExhaustedSnafu, Error, GetSchemaMetadataSnafu,
    ManualCompactionOverrideSnafu, RegionClosedSnafu, RegionDroppedSnafu, RegionNotFoundSnafu,
    RegionTruncatedSnafu, RemoteCompactionSnafu, Result, TimeRangePredicateOverflowSnafu,
    TimeoutSnafu,
};
use crate::metrics::{COMPACTION_MEMORY_WAIT, COMPACTION_STAGE_ELAPSED, INFLIGHT_COMPACTION_COUNT};
use crate::read::projection::ProjectionMapper;
use crate::read::scan_region::{PredicateGroup, ScanInput};
use crate::read::seq_scan::SeqScan;
use crate::read::{BoxedBatchReader, BoxedRecordBatchStream};
use crate::region::options::MergeMode;
use crate::region::version::VersionControlRef;
use crate::region::{ManifestContextRef, RegionLeaderState, RegionRoleState};
use crate::request::{
    BackgroundNotify, OptionOutputTx, OutputTx, WorkerRequest, WorkerRequestWithTime,
};
use crate::schedule::remote_job_scheduler::{
    CompactionJob, DefaultNotifier, RemoteJob, RemoteJobSchedulerRef,
};
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::{FileHandle, FileMeta, Level};
use crate::sst::version::LevelMeta;
use crate::worker::WorkerListener;

/// Region compaction request.
pub struct CompactionRequest {
    pub(crate) engine_config: Arc<MitoConfig>,
    pub(crate) current_version: CompactionVersion,
    pub(crate) access_layer: AccessLayerRef,
    /// Sender to send notification to the region worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequestWithTime>,
    /// Waiters of the compaction request.
    pub(crate) waiters: Vec<OutputTx>,
    /// Start time of compaction task.
    pub(crate) start_time: Instant,
    pub(crate) cache_manager: CacheManagerRef,
    pub(crate) manifest_ctx: ManifestContextRef,
    pub(crate) listener: WorkerListener,
    pub(crate) schema_metadata_manager: SchemaMetadataManagerRef,
    pub(crate) max_parallelism: usize,
}

impl CompactionRequest {
    pub(crate) fn region_id(&self) -> RegionId {
        self.current_version.metadata.region_id
    }
}

/// Compaction scheduler tracks and manages compaction tasks.
pub(crate) struct CompactionScheduler {
    scheduler: SchedulerRef,
    /// Compacting regions.
    region_status: HashMap<RegionId, CompactionStatus>,
    /// Request sender of the worker that this scheduler belongs to.
    request_sender: Sender<WorkerRequestWithTime>,
    cache_manager: CacheManagerRef,
    engine_config: Arc<MitoConfig>,
    memory_manager: Arc<CompactionMemoryManager>,
    memory_policy: OnExhaustedPolicy,
    listener: WorkerListener,
    /// Plugins for the compaction scheduler.
    plugins: Plugins,
}

impl CompactionScheduler {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        scheduler: SchedulerRef,
        request_sender: Sender<WorkerRequestWithTime>,
        cache_manager: CacheManagerRef,
        engine_config: Arc<MitoConfig>,
        listener: WorkerListener,
        plugins: Plugins,
        memory_manager: Arc<CompactionMemoryManager>,
        memory_policy: OnExhaustedPolicy,
    ) -> Self {
        Self {
            scheduler,
            region_status: HashMap::new(),
            request_sender,
            cache_manager,
            engine_config,
            memory_manager,
            memory_policy,
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
        max_parallelism: usize,
    ) -> Result<()> {
        // skip compaction if region is in staging state
        let current_state = manifest_ctx.current_state();
        if current_state == RegionRoleState::Leader(RegionLeaderState::Staging) {
            info!(
                "Skipping compaction for region {} in staging mode, options: {:?}",
                region_id, compact_options
            );
            waiter.send(Ok(0));
            return Ok(());
        }

        if let Some(status) = self.region_status.get_mut(&region_id) {
            match compact_options {
                Options::Regular(_) => {
                    // Region is compacting. Add the waiter to pending list.
                    status.merge_waiter(waiter);
                }
                options @ Options::StrictWindow(_) => {
                    // Incoming compaction request is manually triggered.
                    status.set_pending_request(PendingCompaction {
                        options,
                        waiter,
                        max_parallelism,
                    });
                    info!(
                        "Region {} is compacting, manually compaction will be re-scheduled.",
                        region_id
                    );
                }
            }
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
            max_parallelism,
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

        if let Some(pending_request) = std::mem::take(&mut status.pending_request) {
            let PendingCompaction {
                options,
                waiter,
                max_parallelism,
            } = pending_request;

            let request = status.new_compaction_request(
                self.request_sender.clone(),
                waiter,
                self.engine_config.clone(),
                self.cache_manager.clone(),
                manifest_ctx,
                self.listener.clone(),
                schema_metadata_manager,
                max_parallelism,
            );

            if let Err(e) = self.schedule_compaction_request(request, options).await {
                error!(e; "Failed to continue pending manual compaction for region id: {}", region_id);
            } else {
                debug!(
                    "Successfully scheduled manual compaction for region id: {}",
                    region_id
                );
            }
            return;
        }

        // We should always try to compact the region until picker returns None.
        let request = status.new_compaction_request(
            self.request_sender.clone(),
            OptionOutputTx::none(),
            self.engine_config.clone(),
            self.cache_manager.clone(),
            manifest_ctx,
            self.listener.clone(),
            schema_metadata_manager,
            MAX_PARALLEL_COMPACTION,
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

    pub(crate) async fn retry_memory_pending(&mut self, region_id: RegionId) {
        let Some(pending) = ({
            let Some(status) = self.region_status.get_mut(&region_id) else {
                return;
            };
            status.take_memory_pending()
        }) else {
            return;
        };

        if let Err(e) = self.try_submit_with_memory(pending.task, region_id, pending.memory_bytes) {
            error!(e; "Failed to submit pending compaction for region {}", region_id);
        }
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
            Some(self.engine_config.max_background_compactions),
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
            max_parallelism,
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
            current_version: current_version.clone(),
            region_options: current_version.options.clone(),
            engine_config: engine_config.clone(),
            region_metadata: current_version.metadata.clone(),
            cache_manager: cache_manager.clone(),
            access_layer: access_layer.clone(),
            manifest_ctx: manifest_ctx.clone(),
            file_purger: None,
            ttl: Some(ttl),
            max_parallelism,
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
                    ttl,
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
                        INFLIGHT_COMPACTION_COUNT.inc();
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
        let local_compaction_task = Box::new(CompactionTaskImpl {
            request_sender,
            waiters,
            start_time,
            listener,
            picker_output,
            compaction_region,
            compactor: Arc::new(DefaultCompactor {}),
            memory_guard: None,
        });

        let estimated_bytes = estimate_compaction_bytes(&local_compaction_task.picker_output);
        self.try_submit_with_memory(local_compaction_task, region_id, estimated_bytes)
    }

    fn submit_compaction_task(
        &mut self,
        mut task: Box<CompactionTaskImpl>,
        region_id: RegionId,
    ) -> Result<()> {
        self.scheduler
            .schedule(Box::pin(async move {
                INFLIGHT_COMPACTION_COUNT.inc();
                task.run().await;
                INFLIGHT_COMPACTION_COUNT.dec();
            }))
            .map_err(|e| {
                error!(e; "Failed to submit compaction request for region {}", region_id);
                self.region_status.remove(&region_id);
                e
            })
    }

    fn try_submit_with_memory(
        &mut self,
        mut task: Box<CompactionTaskImpl>,
        region_id: RegionId,
        requested_bytes: u64,
    ) -> Result<()> {
        match self.memory_manager.try_acquire(requested_bytes) {
            Some(guard) => {
                task.memory_guard = Some(guard);
                self.submit_compaction_task(task, region_id)
            }
            None => self.handle_memory_exhausted(region_id, requested_bytes, task),
        }
    }

    fn handle_memory_exhausted(
        &mut self,
        region_id: RegionId,
        requested_bytes: u64,
        task: Box<CompactionTaskImpl>,
    ) -> Result<()> {
        let limit_bytes = self.memory_manager.limit_bytes();

        // Check if request exceeds total capacity
        if self.exceeds_memory_limit(requested_bytes) {
            warn!(
                "Region {} compaction requires {} bytes but total limit is {} bytes; \
                 request exceeds capacity, policy: {:?}",
                region_id, requested_bytes, limit_bytes, self.memory_policy
            );
            // Handle based on policy even when exceeding limit
            return match self.memory_policy {
                OnExhaustedPolicy::Skip => self.skip_compaction_due_to_memory(
                    region_id,
                    task,
                    requested_bytes,
                    limit_bytes,
                ),
                OnExhaustedPolicy::Wait | OnExhaustedPolicy::Fail => {
                    // Wait policy cannot wait for memory that exceeds total capacity
                    self.fail_compaction_due_to_memory(
                        region_id,
                        task,
                        requested_bytes,
                        limit_bytes,
                    )
                }
            };
        }

        // Handle temporary insufficiency based on policy
        match self.memory_policy {
            OnExhaustedPolicy::Wait => {
                // Store pending task first, fail fast if region is gone
                let status = self
                    .region_status
                    .get_mut(&region_id)
                    .context(RegionNotFoundSnafu { region_id })?;

                status.set_memory_pending(DeferredCompactionTask {
                    task,
                    memory_bytes: requested_bytes.max(1),
                });

                self.spawn_memory_waiter(region_id, requested_bytes.max(1));
                info!(
                    "Region {} compaction waits for {} bytes to become available",
                    region_id, requested_bytes
                );
                Ok(())
            }
            OnExhaustedPolicy::Skip => {
                self.skip_compaction_due_to_memory(region_id, task, requested_bytes, limit_bytes)
            }
            OnExhaustedPolicy::Fail => {
                self.fail_compaction_due_to_memory(region_id, task, requested_bytes, limit_bytes)
            }
        }
    }

    fn skip_compaction_due_to_memory(
        &mut self,
        region_id: RegionId,
        mut task: Box<CompactionTaskImpl>,
        required_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()> {
        warn!(
            "Skipping compaction for region {} due to memory limit (need {} bytes, limit {} bytes)",
            region_id, required_bytes, limit_bytes
        );
        let failure = Arc::new(
            CompactionMemoryExhaustedSnafu {
                region_id,
                required_bytes,
                limit_bytes,
                policy: "skip",
            }
            .build(),
        );
        let waiters = mem::take(&mut task.waiters);
        Self::notify_waiters_error(waiters, region_id, failure.clone());
        self.finish_region_status(region_id, failure);
        Ok(())
    }

    fn fail_compaction_due_to_memory(
        &mut self,
        region_id: RegionId,
        mut task: Box<CompactionTaskImpl>,
        required_bytes: u64,
        limit_bytes: u64,
    ) -> Result<()> {
        error!(
            "Failing compaction for region {} due to memory limit (need {} bytes, limit {} bytes)",
            region_id, required_bytes, limit_bytes
        );

        let make_error = || CompactionMemoryExhaustedSnafu {
            region_id,
            required_bytes,
            limit_bytes,
            policy: "fail",
        };

        let failure = Arc::new(make_error().build());
        let waiters = mem::take(&mut task.waiters);
        Self::notify_waiters_error(waiters, region_id, failure.clone());
        self.finish_region_status(region_id, failure);
        make_error().fail()
    }

    fn spawn_memory_waiter(&self, region_id: RegionId, bytes: u64) {
        if bytes == 0 {
            return;
        }

        let manager = self.memory_manager.clone();
        let sender = self.request_sender.clone();

        common_runtime::spawn_global(async move {
            debug!(
                "Region {} compaction waiting for {} bytes to become available",
                region_id, bytes
            );

            let timer = COMPACTION_MEMORY_WAIT.start_timer();
            manager.wait_for_available(bytes).await;
            timer.observe_duration();
            let _ = sender
                .send(WorkerRequestWithTime::new(WorkerRequest::Background {
                    region_id,
                    notify: BackgroundNotify::CompactionRetry,
                }))
                .await;
        });
    }

    fn notify_waiters_error(waiters: Vec<OutputTx>, region_id: RegionId, err: Arc<Error>) {
        for waiter in waiters {
            waiter.send(Err(err.clone()).context(CompactRegionSnafu { region_id }));
        }
    }

    fn finish_region_status(&mut self, region_id: RegionId, failure: Arc<Error>) {
        if let Some(status) = self.region_status.remove(&region_id) {
            status.on_failure(failure);
        }
    }

    fn exceeds_memory_limit(&self, bytes: u64) -> bool {
        let limit = self.memory_manager.limit_bytes();
        limit > 0 && bytes > limit
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

    let ttl = tokio::time::timeout(
        crate::config::FETCH_OPTION_TIMEOUT,
        schema_metadata_manager.get_schema_options_by_table_id(table_id),
    )
    .await
    .context(TimeoutSnafu)?
    .context(GetSchemaMetadataSnafu)?
    .and_then(|options| options.ttl)
    .unwrap_or_default()
    .into();

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
    /// Pending waiters for compaction.
    waiters: Vec<OutputTx>,
    /// Pending compactions that are supposed to run as soon as current compaction task finished.
    pending_request: Option<PendingCompaction>,
    /// Compaction blocked by memory budget.
    memory_pending: Option<DeferredCompactionTask>,
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
            waiters: Vec::new(),
            pending_request: None,
            memory_pending: None,
        }
    }

    /// Merge the waiter to the pending compaction.
    fn merge_waiter(&mut self, mut waiter: OptionOutputTx) {
        if let Some(waiter) = waiter.take_inner() {
            self.waiters.push(waiter);
        }
    }

    /// Set pending compaction request or replace current value if already exist.
    fn set_pending_request(&mut self, pending: PendingCompaction) {
        if let Some(prev) = self.pending_request.replace(pending) {
            debug!(
                "Replace pending compaction options with new request {:?} for region: {}",
                prev.options, self.region_id
            );
            prev.waiter.send(ManualCompactionOverrideSnafu.fail());
        }
    }

    fn on_failure(mut self, err: Arc<Error>) {
        for waiter in self.waiters.drain(..) {
            waiter.send(Err(err.clone()).context(CompactRegionSnafu {
                region_id: self.region_id,
            }));
        }

        if let Some(pending_compaction) = self.pending_request {
            pending_compaction
                .waiter
                .send(Err(err.clone()).context(CompactRegionSnafu {
                    region_id: self.region_id,
                }));
        }

        if let Some(mut pending_task) = self.memory_pending {
            pending_task.task.on_failure(err);
        }
    }

    /// Creates a new compaction request for compaction picker.
    ///
    /// It consumes all pending compaction waiters.
    #[allow(clippy::too_many_arguments)]
    fn new_compaction_request(
        &mut self,
        request_sender: Sender<WorkerRequestWithTime>,
        mut waiter: OptionOutputTx,
        engine_config: Arc<MitoConfig>,
        cache_manager: CacheManagerRef,
        manifest_ctx: &ManifestContextRef,
        listener: WorkerListener,
        schema_metadata_manager: SchemaMetadataManagerRef,
        max_parallelism: usize,
    ) -> CompactionRequest {
        let current_version = CompactionVersion::from(self.version_control.current().version);
        let start_time = Instant::now();
        let mut waiters = Vec::with_capacity(self.waiters.len() + 1);
        waiters.extend(std::mem::take(&mut self.waiters));

        if let Some(waiter) = waiter.take_inner() {
            waiters.push(waiter);
        }

        CompactionRequest {
            engine_config,
            current_version,
            access_layer: self.access_layer.clone(),
            request_sender: request_sender.clone(),
            waiters,
            start_time,
            cache_manager,
            manifest_ctx: manifest_ctx.clone(),
            listener,
            schema_metadata_manager,
            max_parallelism,
        }
    }

    fn set_memory_pending(&mut self, pending: DeferredCompactionTask) {
        self.memory_pending = Some(pending);
    }

    fn take_memory_pending(&mut self) -> Option<DeferredCompactionTask> {
        self.memory_pending.take()
    }
}

#[derive(Debug, Clone)]
pub struct CompactionOutput {
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

impl CompactionSstReaderBuilder<'_> {
    /// Builds [BoxedBatchReader] that reads all SST files and yields batches in primary key order.
    async fn build_sst_reader(self) -> Result<BoxedBatchReader> {
        let scan_input = self.build_scan_input(false)?.with_compaction(true);

        SeqScan::new(scan_input).build_reader_for_compaction().await
    }

    /// Builds [BoxedRecordBatchStream] that reads all SST files and yields batches in flat format for compaction.
    async fn build_flat_sst_reader(self) -> Result<BoxedRecordBatchStream> {
        let scan_input = self.build_scan_input(true)?.with_compaction(true);

        SeqScan::new(scan_input)
            .build_flat_reader_for_compaction()
            .await
    }

    fn build_scan_input(self, flat_format: bool) -> Result<ScanInput> {
        let mut scan_input = ScanInput::new(
            self.sst_layer,
            ProjectionMapper::all(&self.metadata, flat_format)?,
        )
        .with_files(self.inputs.to_vec())
        .with_append_mode(self.append_mode)
        // We use special cache strategy for compaction.
        .with_cache(CacheStrategy::Compaction(self.cache))
        .with_filter_deleted(self.filter_deleted)
        // We ignore file not found error during compaction.
        .with_ignore_file_not_found(true)
        .with_merge_mode(self.merge_mode)
        .with_flat_format(flat_format);

        // This serves as a workaround of https://github.com/GreptimeTeam/greptimedb/issues/3944
        // by converting time ranges into predicate.
        if let Some(time_range) = self.time_range {
            scan_input =
                scan_input.with_predicate(time_range_to_predicate(time_range, &self.metadata)?);
        }

        Ok(scan_input)
    }
}

/// Converts time range to predicates so that rows outside the range will be filtered.
fn time_range_to_predicate(
    range: TimestampRange,
    metadata: &RegionMetadataRef,
) -> Result<PredicateGroup> {
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
            vec![
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .gt_eq(ts_to_lit(*start, ts_col_unit)?),
            ]
        }

        (None, Some(end)) => {
            vec![
                datafusion_expr::col(ts_col.column_schema.name.clone())
                    .lt(ts_to_lit(*end, ts_col_unit)?),
            ]
        }
        (None, None) => {
            return Ok(PredicateGroup::default());
        }
    };

    let predicate = PredicateGroup::new(metadata, &exprs)?;
    Ok(predicate)
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
    let Some(ttl) = ttl else {
        return vec![];
    };

    levels
        .iter()
        .flat_map(|l| l.get_expired_files(&now, &ttl).into_iter())
        .collect()
}

/// Estimates compaction memory as the sum of all input files' maximum row-group
/// uncompressed sizes.
fn estimate_compaction_bytes(picker_output: &PickerOutput) -> u64 {
    picker_output
        .outputs
        .iter()
        .flat_map(|output| output.inputs.iter())
        .map(|file: &FileHandle| {
            let meta = file.meta_ref();
            meta.max_row_group_uncompressed_size
        })
        .sum()
}

/// Pending compaction request that is supposed to run after current task is finished,
/// typically used for manual compactions.
struct PendingCompaction {
    /// Compaction options. Currently, it can only be [StrictWindow].
    pub(crate) options: compact_request::Options,
    /// Waiters of pending requests.
    pub(crate) waiter: OptionOutputTx,
    /// Max parallelism for pending compaction.
    pub(crate) max_parallelism: usize,
}

struct DeferredCompactionTask {
    task: Box<CompactionTaskImpl>,
    memory_bytes: u64,
}

#[cfg(test)]
mod tests {
    use api::v1::region::StrictWindow;
    use common_base::AffectedRows;
    use common_datasource::compression::CompressionType;
    use tokio::sync::{Barrier, oneshot};

    use super::*;
    use crate::cache::CacheManager;
    use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
    use crate::region::ManifestContext;
    use crate::sst::FormatType;
    use crate::test_util::mock_schema_metadata_manager;
    use crate::test_util::scheduler_util::{SchedulerEnv, VecScheduler};
    use crate::test_util::version_util::{VersionControlBuilder, apply_edit};

    #[tokio::test]
    async fn test_schedule_empty() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
        let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
        schema_metadata_manager
            .register_region_table_info(
                builder.region_id().table_id(),
                "test_table",
                "test_catalog",
                "test_schema",
                None,
                kv_backend,
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
                1,
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
                1,
            )
            .await
            .unwrap();
        let output = output_rx.await.unwrap().unwrap();
        assert_eq!(output, 0);
        assert!(scheduler.region_status.is_empty());
    }

    #[tokio::test]
    async fn test_schedule_on_finished() {
        common_telemetry::init_default_ut_logging();
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
        let purger = builder.file_purger();
        let region_id = builder.region_id();

        let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
        schema_metadata_manager
            .register_region_table_info(
                builder.region_id().table_id(),
                "test_table",
                "test_catalog",
                "test_schema",
                None,
                kv_backend,
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
                1,
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
        let (tx, _rx) = oneshot::channel();
        scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                OptionOutputTx::new(Some(OutputTx::new(tx))),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
            )
            .await
            .unwrap();
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(1, job_scheduler.num_jobs());
        assert!(
            !scheduler
                .region_status
                .get(&builder.region_id())
                .unwrap()
                .waiters
                .is_empty()
        );

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
        let (tx, _rx) = oneshot::channel();
        // The task is pending.
        scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                OptionOutputTx::new(Some(OutputTx::new(tx))),
                &manifest_ctx,
                schema_metadata_manager,
                1,
            )
            .await
            .unwrap();
        assert_eq!(2, job_scheduler.num_jobs());
        assert!(
            !scheduler
                .region_status
                .get(&builder.region_id())
                .unwrap()
                .waiters
                .is_empty()
        );
    }

    #[tokio::test]
    async fn test_manual_compaction_when_compaction_in_progress() {
        common_telemetry::init_default_ut_logging();
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
        let purger = builder.file_purger();
        let region_id = builder.region_id();

        let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
        schema_metadata_manager
            .register_region_table_info(
                builder.region_id().table_id(),
                "test_table",
                "test_catalog",
                "test_schema",
                None,
                kv_backend,
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

        let file_metas: Vec<_> = version_control.current().version.ssts.levels()[0]
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

        scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                OptionOutputTx::none(),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
            )
            .await
            .unwrap();
        // Should schedule 1 compaction.
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(1, job_scheduler.num_jobs());
        assert!(
            scheduler
                .region_status
                .get(&region_id)
                .unwrap()
                .pending_request
                .is_none()
        );

        // Schedule another manual compaction.
        let (tx, _rx) = oneshot::channel();
        scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
                &version_control,
                &env.access_layer,
                OptionOutputTx::new(Some(OutputTx::new(tx))),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
            )
            .await
            .unwrap();
        assert_eq!(1, scheduler.region_status.len());
        // Current job num should be 1 since compaction is in progress.
        assert_eq!(1, job_scheduler.num_jobs());
        let status = scheduler.region_status.get(&builder.region_id()).unwrap();
        assert!(status.pending_request.is_some());

        // On compaction finished and schedule next compaction.
        scheduler
            .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager.clone())
            .await;
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(2, job_scheduler.num_jobs());

        let status = scheduler.region_status.get(&builder.region_id()).unwrap();
        assert!(status.pending_request.is_none());
    }

    #[tokio::test]
    async fn test_compaction_bypass_in_staging_mode() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);

        // Create version control and manifest context for staging mode
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = version_control.current().version.metadata.region_id;

        // Create staging manifest context using the same pattern as SchedulerEnv
        let staging_manifest_ctx = {
            let manager = RegionManifestManager::new(
                version_control.current().version.metadata.clone(),
                0,
                RegionManifestOptions {
                    manifest_dir: "".to_string(),
                    object_store: env.access_layer.object_store().clone(),
                    compress_type: CompressionType::Uncompressed,
                    checkpoint_distance: 10,
                    remove_file_options: Default::default(),
                },
                FormatType::PrimaryKey,
                &Default::default(),
            )
            .await
            .unwrap();
            Arc::new(ManifestContext::new(
                manager,
                RegionRoleState::Leader(RegionLeaderState::Staging),
            ))
        };

        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

        // Test regular compaction bypass in staging mode
        let (tx, rx) = oneshot::channel();
        scheduler
            .schedule_compaction(
                region_id,
                compact_request::Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                OptionOutputTx::new(Some(OutputTx::new(tx))),
                &staging_manifest_ctx,
                schema_metadata_manager,
                1,
            )
            .await
            .unwrap();

        let result = rx.await.unwrap();
        assert_eq!(result.unwrap(), 0); // is there a better way to check this?
        assert_eq!(0, scheduler.region_status.len());
    }

    struct MemorySchedulerEnv {
        env: SchedulerEnv,
        job_scheduler: Arc<VecScheduler>,
        request_sender: Sender<WorkerRequestWithTime>,
        cache_manager: CacheManagerRef,
        engine_config: Arc<MitoConfig>,
    }

    async fn build_memory_scheduler(
        limit_bytes: u64,
        policy: OnExhaustedPolicy,
    ) -> (CompactionScheduler, MemorySchedulerEnv) {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (request_sender, _request_receiver) = mpsc::channel(4);
        let cache_manager = Arc::new(CacheManager::default());
        let engine_config = Arc::new(MitoConfig::default());
        let scheduler = CompactionScheduler::new(
            job_scheduler.clone(),
            request_sender.clone(),
            cache_manager.clone(),
            engine_config.clone(),
            WorkerListener::default(),
            Plugins::new(),
            Arc::new(CompactionMemoryManager::new(limit_bytes)),
            policy,
        );
        (
            scheduler,
            MemorySchedulerEnv {
                env,
                job_scheduler,
                request_sender,
                cache_manager,
                engine_config,
            },
        )
    }

    async fn build_version_resources(
        env: &MemorySchedulerEnv,
    ) -> (RegionId, VersionControlRef, ManifestContextRef) {
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let manifest_ctx = env
            .env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        (region_id, version_control, manifest_ctx)
    }

    fn new_waiter() -> (
        OutputTx,
        oneshot::Receiver<crate::error::Result<AffectedRows>>,
    ) {
        let (tx, rx) = oneshot::channel::<crate::error::Result<AffectedRows>>();
        (OutputTx::new(tx), rx)
    }

    fn build_mock_task(
        env: &MemorySchedulerEnv,
        version_control: &VersionControlRef,
        manifest_ctx: &ManifestContextRef,
        waiter: Option<OutputTx>,
    ) -> Box<CompactionTaskImpl> {
        let region_id = version_control.current().version.metadata.region_id;
        let current_version = CompactionVersion::from(version_control.current().version.clone());
        let mut waiters = Vec::new();
        if let Some(waiter) = waiter {
            waiters.push(waiter);
        }
        Box::new(CompactionTaskImpl {
            compaction_region: CompactionRegion {
                region_id,
                region_options: current_version.options.clone(),
                engine_config: env.engine_config.clone(),
                region_metadata: current_version.metadata.clone(),
                cache_manager: env.cache_manager.clone(),
                access_layer: env.env.access_layer.clone(),
                manifest_ctx: manifest_ctx.clone(),
                current_version,
                file_purger: None,
                ttl: None,
                max_parallelism: 1,
            },
            request_sender: env.request_sender.clone(),
            waiters,
            start_time: Instant::now(),
            listener: WorkerListener::default(),
            compactor: Arc::new(DefaultCompactor {}),
            picker_output: PickerOutput {
                outputs: Vec::new(),
                expired_ssts: Vec::new(),
                time_window_size: 0,
                max_file_size: None,
            },
            memory_guard: None,
        })
    }

    fn insert_region_status(
        scheduler: &mut CompactionScheduler,
        region_id: RegionId,
        version_control: &VersionControlRef,
        access_layer: &AccessLayerRef,
    ) {
        scheduler.region_status.insert(
            region_id,
            CompactionStatus::new(region_id, version_control.clone(), access_layer.clone()),
        );
    }

    /// Typical compaction memory request size for testing (2MB).
    const TEST_REQUEST_BYTES: u64 = 2 * 1024 * 1024;
    /// Small memory limit for testing exhaustion scenarios (1MB, less than TEST_REQUEST_BYTES).
    const SMALL_LIMIT_BYTES: u64 = 1024 * 1024;
    /// Large memory limit for testing normal operation (8MB, much larger than TEST_REQUEST_BYTES).
    const LARGE_LIMIT_BYTES: u64 = 8 * 1024 * 1024;

    #[tokio::test]
    async fn test_try_submit_with_memory_schedules_job() {
        let (mut scheduler, env) =
            build_memory_scheduler(LARGE_LIMIT_BYTES, OnExhaustedPolicy::Wait).await;
        let (region_id, version_control, manifest_ctx) = build_version_resources(&env).await;
        insert_region_status(
            &mut scheduler,
            region_id,
            &version_control,
            &env.env.access_layer,
        );
        let task = build_mock_task(&env, &version_control, &manifest_ctx, None);

        scheduler
            .try_submit_with_memory(task, region_id, TEST_REQUEST_BYTES)
            .unwrap();

        assert_eq!(1, env.job_scheduler.num_jobs());
        assert!(scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_try_submit_with_memory_skip_notifies_waiter() {
        let (mut scheduler, env) =
            build_memory_scheduler(SMALL_LIMIT_BYTES, OnExhaustedPolicy::Skip).await;
        let (region_id, version_control, manifest_ctx) = build_version_resources(&env).await;
        insert_region_status(
            &mut scheduler,
            region_id,
            &version_control,
            &env.env.access_layer,
        );
        let (wait_sender, wait_receiver) = new_waiter();
        let task = build_mock_task(&env, &version_control, &manifest_ctx, Some(wait_sender));

        scheduler
            .try_submit_with_memory(task, region_id, TEST_REQUEST_BYTES)
            .unwrap();

        let result = wait_receiver.await.unwrap();
        assert!(result.is_err());
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_try_submit_with_memory_wait_exceed_limit_fails() {
        let (mut scheduler, env) =
            build_memory_scheduler(SMALL_LIMIT_BYTES, OnExhaustedPolicy::Wait).await;
        let (region_id, version_control, manifest_ctx) = build_version_resources(&env).await;
        insert_region_status(
            &mut scheduler,
            region_id,
            &version_control,
            &env.env.access_layer,
        );
        let (wait_sender, wait_receiver) = new_waiter();
        let task = build_mock_task(&env, &version_control, &manifest_ctx, Some(wait_sender));

        let err = scheduler
            .try_submit_with_memory(task, region_id, TEST_REQUEST_BYTES)
            .unwrap_err();
        assert!(
            matches!(
                &err,
                Error::CompactionMemoryExhausted {
                    region_id: r,
                    policy,
                    required_bytes,
                    limit_bytes,
                    ..
                } if *r == region_id
                    && policy.as_str() == "fail"
                    && *required_bytes == TEST_REQUEST_BYTES
                    && *limit_bytes == SMALL_LIMIT_BYTES
            ),
            "Expected CompactionMemoryExhausted error, got: {:?}",
            err
        );
        let result = wait_receiver.await.unwrap();
        assert!(result.is_err());
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_try_submit_with_memory_fail_policy() {
        let (mut scheduler, env) =
            build_memory_scheduler(SMALL_LIMIT_BYTES, OnExhaustedPolicy::Fail).await;
        let (region_id, version_control, manifest_ctx) = build_version_resources(&env).await;
        insert_region_status(
            &mut scheduler,
            region_id,
            &version_control,
            &env.env.access_layer,
        );
        let (wait_sender, wait_receiver) = new_waiter();
        let task = build_mock_task(&env, &version_control, &manifest_ctx, Some(wait_sender));

        let err = scheduler
            .try_submit_with_memory(task, region_id, TEST_REQUEST_BYTES)
            .unwrap_err();
        assert!(
            matches!(
                &err,
                Error::CompactionMemoryExhausted {
                    region_id: r,
                    policy,
                    required_bytes,
                    limit_bytes,
                    ..
                } if *r == region_id
                    && policy.as_str() == "fail"
                    && *required_bytes == TEST_REQUEST_BYTES
                    && *limit_bytes == SMALL_LIMIT_BYTES
            ),
            "Expected CompactionMemoryExhausted with policy=fail, got: {:?}",
            err
        );
        let result = wait_receiver.await.unwrap();
        assert!(result.is_err());
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_try_submit_with_memory_wait_eventually_succeeds() {
        use tokio::time::{Duration, sleep};

        let (mut scheduler, env) =
            build_memory_scheduler(LARGE_LIMIT_BYTES, OnExhaustedPolicy::Wait).await;
        let (region_id, version_control, manifest_ctx) = build_version_resources(&env).await;
        insert_region_status(
            &mut scheduler,
            region_id,
            &version_control,
            &env.env.access_layer,
        );

        // Occupy most of the memory budget (8MB - 1MB = 7MB occupied, only 1MB left)
        let guard = scheduler
            .memory_manager
            .try_acquire(LARGE_LIMIT_BYTES - SMALL_LIMIT_BYTES)
            .unwrap();

        // Try to submit a task requesting 2MB - should enter waiting state (only 1MB available)
        let task = build_mock_task(&env, &version_control, &manifest_ctx, None);
        scheduler
            .try_submit_with_memory(task, region_id, TEST_REQUEST_BYTES)
            .unwrap();

        // Task should not be scheduled yet (waiting for memory)
        assert_eq!(0, env.job_scheduler.num_jobs());

        // Verify the task is in pending state
        let status = scheduler.region_status.get(&region_id).unwrap();
        assert!(status.memory_pending.is_some());

        // Release memory
        drop(guard);

        // Give background waiter time to wake up and retry, then simulate worker retry
        sleep(Duration::from_millis(50)).await;
        scheduler.retry_memory_pending(region_id).await;

        // The retry should have scheduled the job and consumed memory again.
        assert_eq!(1, env.job_scheduler.num_jobs());
        assert_eq!(TEST_REQUEST_BYTES, scheduler.memory_manager.used_bytes());
        let status = scheduler.region_status.get(&region_id).unwrap();
        assert!(status.memory_pending.is_none());
    }

    #[tokio::test]
    async fn test_concurrent_memory_competition() {
        let manager = Arc::new(CompactionMemoryManager::new(3 * 1024 * 1024)); // 3MB
        let barrier = Arc::new(Barrier::new(3));
        let mut handles = vec![];

        // Spawn 3 tasks competing for memory, each trying to acquire 2MB
        for _i in 0..3 {
            let mgr = manager.clone();
            let bar = barrier.clone();
            let handle = tokio::spawn(async move {
                bar.wait().await; // Synchronize start
                mgr.try_acquire(2 * 1024 * 1024)
            });
            handles.push(handle);
        }

        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Only 1 should succeed (3MB limit, 2MB request, can only fit one)
        let succeeded = results.iter().filter(|r| r.is_some()).count();
        let failed = results.iter().filter(|r| r.is_none()).count();

        assert_eq!(succeeded, 1, "Expected exactly 1 task to acquire memory");
        assert_eq!(failed, 2, "Expected 2 tasks to fail");

        // Clean up
        drop(results);
        assert_eq!(manager.used_bytes(), 0);
    }
}
