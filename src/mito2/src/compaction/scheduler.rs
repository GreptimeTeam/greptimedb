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
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use api::v1::region::compact_request;
use common_base::Plugins;
use common_base::cancellation::CancellationHandle;
use common_memory_manager::OnExhaustedPolicy;
use common_meta::key::SchemaMetadataManagerRef;
use common_telemetry::{debug, error, info, warn};
use common_time::TimeToLive;
use common_time::range::TimestampRange;
use futures::FutureExt;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{self, Sender};

use super::{CompactionOutput, find_dynamic_options};
use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::compaction::compactor::{CompactionRegion, CompactionVersion, DefaultCompactor};
use crate::compaction::memory_manager::CompactionMemoryManager;
use crate::compaction::picker::{CompactionTask, PickerOutput, new_picker};
use crate::compaction::task::{CompactionTaskImpl, MAX_PARALLEL_COMPACTION};
use crate::config::MitoConfig;
use crate::error::{
    CompactRegionSnafu, CompactionCancelledSnafu, Error, JoinSnafu, ManualCompactionOverrideSnafu,
    RegionClosedSnafu, RegionDroppedSnafu, RegionTruncatedSnafu, RemoteCompactionSnafu, Result,
    UnexpectedSnafu,
};
use crate::metrics::{
    COMPACTION_MEMORY_REJECTED, COMPACTION_STAGE_ELAPSED, INFLIGHT_COMPACTION_COUNT,
};
use crate::region::options::RegionOptions;
use crate::region::version::VersionControlRef;
use crate::region::{ManifestContextRef, RegionLeaderState, RegionRoleState};
use crate::request::{
    BackgroundNotify, DdlRequest, OptionOutputTx, OutputTx, SenderDdlRequest, WorkerRequest,
    WorkerRequestWithTime,
};
use crate::schedule::remote_job_scheduler::{
    CompactionJob, DefaultNotifier, RemoteJob, RemoteJobSchedulerRef,
};
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::FileHandle;
use crate::sst::version::SstVersion;
use crate::worker::WorkerListener;

/// Region compaction request.
pub struct CompactionRequest {
    pub(crate) engine_config: Arc<MitoConfig>,
    pub(crate) current_version: CompactionVersion,
    pub(crate) access_layer: AccessLayerRef,
    /// Sender to send notification to the region worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequestWithTime>,
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

/// Result returned to the worker after background compaction planning.
pub(crate) enum CompactionPlanningResult {
    Prepared(PreparedCompaction),
    NoPlan,
    Error(Arc<Error>),
}

impl fmt::Debug for CompactionPlanningResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Prepared(prepared) => f
                .debug_tuple("Prepared")
                .field(&prepared.compaction_region.region_id)
                .finish(),
            Self::NoPlan => f.write_str("NoPlan"),
            Self::Error(err) => f.debug_tuple("Error").field(err).finish(),
        }
    }
}

/// Pure planning completion sent back to the owning region worker.
#[derive(Debug)]
pub(crate) struct CompactionPickFinished {
    pub(crate) region_id: RegionId,
    pub(crate) plan_id: u64,
    pub(crate) result: CompactionPlanningResult,
}

pub(crate) struct PreparedCompaction {
    compaction_region: CompactionRegion,
    picker_output: PickerOutput,
    start_time: Instant,
    ttl: TimeToLive,
}

/// Identifies an accepted compaction attempt and keeps its SST reservations alive.
/// The plan id fences terminal notifications from superseded attempts.
#[derive(Debug, Clone)]
pub(crate) struct CompactionExecution {
    plan_id: u64,
    _files: CompactingFiles,
}

impl CompactionExecution {
    fn new(plan_id: u64, files: CompactingFiles) -> Self {
        Self {
            plan_id,
            _files: files,
        }
    }

    pub(crate) fn matches(&self, other: &Self) -> bool {
        self.plan_id == other.plan_id
    }

    #[cfg(test)]
    pub(crate) fn for_test(plan_id: u64) -> Self {
        Self::new(plan_id, CompactingFiles::empty())
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
    /// Scheduler-wide generation counter for compaction plans and executions.
    /// It outlives region statuses so close/reopen cannot reuse an old identity.
    next_plan_id: u64,
}

fn requires_pending_compaction_slot(
    options: &compact_request::Options,
    time_range: Option<TimestampRange>,
) -> bool {
    matches!(options, compact_request::Options::StrictWindow(_)) || time_range.is_some()
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
            next_plan_id: 0,
        }
    }

    /// Returns the current plan id and advances the counter.
    ///
    /// Takes the counter instead of `&mut self` so callers can bump it while
    /// holding a mutable borrow of a region status.
    fn next_plan_id(counter: &mut u64) -> u64 {
        let plan_id = *counter;
        *counter = counter.wrapping_add(1);
        plan_id
    }

    /// Schedules a compaction for the region.
    /// Returns whether a compaction is scheduled.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn schedule_compaction(
        &mut self,
        region_id: RegionId,
        compact_options: compact_request::Options,
        version_control: &VersionControlRef,
        access_layer: &AccessLayerRef,
        waiter: OptionOutputTx,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
        max_parallelism: usize,
    ) -> Result<bool> {
        self.schedule_compaction_with_time_range(
            region_id,
            compact_options,
            version_control,
            access_layer,
            waiter,
            manifest_ctx,
            schema_metadata_manager,
            max_parallelism,
            None,
        )
    }

    /// Schedules a compaction constrained by an optional time range.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn schedule_compaction_with_time_range(
        &mut self,
        region_id: RegionId,
        compact_options: compact_request::Options,
        version_control: &VersionControlRef,
        access_layer: &AccessLayerRef,
        waiter: OptionOutputTx,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
        max_parallelism: usize,
        time_range: Option<TimestampRange>,
    ) -> Result<bool> {
        // skip compaction if region is in staging state
        let current_state = manifest_ctx.current_state();
        if current_state == RegionRoleState::Leader(RegionLeaderState::Staging) {
            info!(
                "Skipping compaction for region {} in staging mode, options: {:?}",
                region_id, compact_options
            );
            waiter.send(Ok(0));
            return Ok(false);
        }

        if let Some(status) = self.region_status.get_mut(&region_id) {
            // Pending Truncate/EnterStaging requests form a scheduling fence. Any later
            // compaction with a waiter is an explicit request and receives CompactionCancelled;
            // automatic triggers have no waiter, so sending the error is a no-op and the trigger
            // is simply ignored.
            if !status.pending_ddl_requests.is_empty() {
                waiter.send(CompactionCancelledSnafu.fail());
                info!(
                    "Region {} has pending DDL requests, ignoring compaction: {:?}",
                    region_id, compact_options
                );
                return Ok(false);
            }

            if requires_pending_compaction_slot(&compact_options, time_range) {
                // Incoming compaction request is manually triggered.
                status.set_pending_request(PendingCompaction {
                    options: compact_options,
                    waiter,
                    max_parallelism,
                    time_range,
                });
                info!(
                    "Region {} is compacting, manually compaction will be re-scheduled.",
                    region_id
                );
            } else {
                status.merge_regular_trigger(waiter);
            }
            return Ok(false);
        }

        // Publish the picking phase before dispatching background planning.
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), access_layer.clone());
        let request = status.new_compaction_request(
            self.request_sender.clone(),
            self.engine_config.clone(),
            self.cache_manager.clone(),
            manifest_ctx,
            self.listener.clone(),
            schema_metadata_manager,
            max_parallelism,
        );
        let plan_id = Self::next_plan_id(&mut self.next_plan_id);
        status.start_picking_with_time_range(plan_id, time_range);
        status.merge_waiter(waiter);
        self.region_status.insert(region_id, status);
        self.dispatch_compaction_planning(plan_id, request, compact_options, time_range);
        self.listener.on_compaction_scheduled(region_id);
        Ok(true)
    }

    // Handle pending manual compaction request for the region.
    //
    // Returns true if should early return, false otherwise.
    pub(crate) fn handle_pending_compaction_request(
        &mut self,
        region_id: RegionId,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> bool {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return true;
        };

        // If there is a pending manual compaction request, schedule it.
        // and defer returning the pending DDL requests to the caller.
        let Some(pending_request) = std::mem::take(&mut status.pending_request) else {
            return false;
        };

        let PendingCompaction {
            options,
            waiter,
            max_parallelism,
            time_range,
        } = pending_request;

        let request = status.new_compaction_request(
            self.request_sender.clone(),
            self.engine_config.clone(),
            self.cache_manager.clone(),
            manifest_ctx,
            self.listener.clone(),
            schema_metadata_manager,
            max_parallelism,
        );
        status.merge_waiter(waiter);
        // Bump the counter through a disjoint field borrow so the `status`
        // borrow stays alive; nothing could have removed the status since it
        // was fetched above.
        let plan_id = Self::next_plan_id(&mut self.next_plan_id);
        status.start_picking_with_time_range(plan_id, time_range);
        self.dispatch_compaction_planning(plan_id, request, options, time_range);
        debug!(
            "Successfully scheduled manual compaction planning for region id: {}",
            region_id
        );
        true
    }

    /// Notifies the scheduler that the compaction job is finished successfully.
    async fn on_compaction_finished(
        &mut self,
        region_id: RegionId,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> Vec<SenderDdlRequest> {
        if !self
            .region_status
            .get(&region_id)
            .is_some_and(|s| s.is_busy())
        {
            return Vec::new();
        }

        if self.handle_pending_compaction_request(
            region_id,
            manifest_ctx,
            schema_metadata_manager.clone(),
        ) {
            return Vec::new();
        }

        // The region status might be removed by the previous steps.
        // So we return empty DDL requests.
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return Vec::new();
        };
        let Some(mut active) = status.take_active() else {
            return Vec::new();
        };

        for waiter in std::mem::take(&mut active.waiters) {
            waiter.send(Ok(0));
        }

        // A queued DDL was waiting for the current task to terminate; chaining
        // another compaction ahead of it would delay the DDL by a whole extra
        // plan/execution cycle, so dispatch the DDLs first.
        let pending_ddl_requests = std::mem::take(&mut status.pending_ddl_requests);
        if !pending_ddl_requests.is_empty() {
            // The just-finished compaction satisfies any retained regular triggers.
            for waiter in active.regular_followup_waiters.take().unwrap_or_default() {
                waiter.send(Ok(0));
            }
            self.region_status.remove(&region_id);
            // If there are pending DDL requests, we should return them to the caller.
            // And skip try to schedule next compaction task.
            return pending_ddl_requests;
        }

        if active.regular_followup_waiters.is_some() {
            self.schedule_next_compaction_with_active(
                region_id,
                manifest_ctx,
                schema_metadata_manager,
                Some(active),
                None,
            );
            return Vec::new();
        }
        Vec::new()
    }

    /// Returns whether a terminal notification belongs to the installed execution.
    /// Background work may finish after its region status has been replaced, so
    /// matching the region id alone is insufficient.
    pub(crate) fn is_current_execution(
        &self,
        region_id: RegionId,
        execution: &CompactionExecution,
    ) -> bool {
        self.region_status
            .get(&region_id)
            .is_some_and(|status| status.matches_execution(execution))
    }

    pub(crate) async fn on_execution_finished(
        &mut self,
        region_id: RegionId,
        execution: &CompactionExecution,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> Vec<SenderDdlRequest> {
        // A stale finish must not clear the replacement phase or notify its waiters and DDLs.
        if !self.is_current_execution(region_id, execution) {
            return Vec::new();
        }
        self.on_compaction_finished(region_id, manifest_ctx, schema_metadata_manager)
            .await
    }

    pub(crate) fn is_compacting(&self, region_id: RegionId) -> bool {
        self.region_status
            .get(&region_id)
            .map(CompactionStatus::is_busy)
            .unwrap_or(false)
    }

    /// Removes the region status if it has no running task.
    ///
    /// A finished compaction leaves an idle status (`active = None`) behind when
    /// there is nothing more to schedule. If the caller decides not to chain
    /// the next compaction, it must remove the idle status; otherwise the
    /// status becomes a zombie that makes `schedule_compaction` swallow all
    /// future compaction triggers of the region.
    pub(crate) fn remove_idle_status(&mut self, region_id: RegionId) {
        if self
            .region_status
            .get(&region_id)
            .is_some_and(|status| !status.is_busy())
        {
            self.region_status.remove(&region_id);
        }
    }

    /// Schedules next compaction upon a finished compaction.
    /// Returns whether the compaction is scheduled.
    pub(crate) fn schedule_next_compaction(
        &mut self,
        region_id: RegionId,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> bool {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return false;
        };
        // A plan is already in flight; treat it as scheduled instead of
        // overwriting the current phase and orphaning the in-flight planning.
        if status.is_busy() {
            return true;
        }

        let time_range = status.time_range;
        self.schedule_next_compaction_with_active(
            region_id,
            manifest_ctx,
            schema_metadata_manager,
            None,
            time_range,
        )
    }

    fn schedule_next_compaction_with_active(
        &mut self,
        region_id: RegionId,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
        active: Option<ActiveCompaction>,
        time_range: Option<TimestampRange>,
    ) -> bool {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return false;
        };
        // We should always try to compact the region until picker returns None.
        let request = status.new_compaction_request(
            self.request_sender.clone(),
            self.engine_config.clone(),
            self.cache_manager.clone(),
            manifest_ctx,
            self.listener.clone(),
            schema_metadata_manager,
            MAX_PARALLEL_COMPACTION,
        );
        // Bump the counter through a disjoint field borrow so the `status`
        // borrow stays alive; nothing could have removed the status since it
        // was fetched above.
        let plan_id = Self::next_plan_id(&mut self.next_plan_id);
        status.start_regular_picking(plan_id, active, time_range);
        self.dispatch_compaction_planning(
            plan_id,
            request,
            compact_request::Options::Regular(Default::default()),
            time_range,
        );
        debug!(
            "Successfully scheduled next compaction planning for region id: {}",
            region_id
        );
        true
    }

    /// Notifies the scheduler that the compaction job is cancelled cooperatively.
    async fn on_compaction_cancelled(&mut self, region_id: RegionId) -> Vec<SenderDdlRequest> {
        self.remove_region_on_cancel(region_id)
    }

    pub(crate) async fn on_execution_cancelled(
        &mut self,
        region_id: RegionId,
        execution: &CompactionExecution,
    ) -> Vec<SenderDdlRequest> {
        // A stale cancellation must not remove a replacement execution's status.
        if !self.is_current_execution(region_id, execution) {
            return Vec::new();
        }
        self.on_compaction_cancelled(region_id).await
    }

    /// Notifies the scheduler that the compaction job is failed.
    fn on_compaction_failed(&mut self, region_id: RegionId, err: Arc<Error>) {
        error!(err; "Region {} failed to compact, cancel all pending tasks", region_id);
        self.remove_region_on_failure(region_id, err);
    }

    pub(crate) fn on_execution_failed(
        &mut self,
        region_id: RegionId,
        execution: &CompactionExecution,
        err: Arc<Error>,
    ) {
        // A stale failure must not tear down a replacement execution.
        if !self.is_current_execution(region_id, execution) {
            return;
        }
        self.on_compaction_failed(region_id, err);
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

    /// Cancels the running compaction and queues its dependent DDL atomically.
    ///
    /// Production callers currently use this only for [`DdlRequest::Truncate`] and
    /// [`DdlRequest::EnterStaging`]. If cancellation is still possible, the current picking or
    /// local execution is asked to stop; otherwise the DDL waits for its terminal notification.
    /// The worker dispatches the queued DDL only after that notification is handled, preventing
    /// truncate or enter-staging from racing with compaction planning, execution, or commit.
    /// Returns the sender and typed request unchanged if compaction is not running.
    pub(crate) fn try_cancel_and_add_ddl<T>(
        &mut self,
        region_id: RegionId,
        sender: OptionOutputTx,
        request: T,
        into_ddl_request: impl FnOnce(T) -> DdlRequest,
    ) -> std::result::Result<(), (OptionOutputTx, T)> {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return Err((sender, request));
        };
        if status.request_cancel() == RequestCancelResult::NotRunning {
            return Err((sender, request));
        }

        let request = SenderDdlRequest {
            region_id,
            sender,
            request: into_ddl_request(request),
        };
        debug!(
            "Added pending DDL request for region: {}, ddl: {:?}",
            request.region_id, request.request
        );
        // The first queued Truncate/EnterStaging also fences later regular triggers from
        // creating more follow-ups ahead of the DDL.
        status.pending_ddl_requests.push(request);
        Ok(())
    }

    #[cfg(test)]
    fn add_ddl_request_to_pending(&mut self, request: SenderDdlRequest) {
        self.region_status
            .get_mut(&request.region_id)
            .unwrap()
            .pending_ddl_requests
            .push(request);
    }

    #[cfg(test)]
    pub(crate) fn has_pending_ddls(&self, region_id: RegionId) -> bool {
        let has_pending = self
            .region_status
            .get(&region_id)
            .map(|status| !status.pending_ddl_requests.is_empty())
            .unwrap_or(false);
        debug!(
            "Checked pending DDL requests for region: {}, has_pending: {}",
            region_id, has_pending
        );
        has_pending
    }

    #[cfg(test)]
    pub(crate) fn request_cancel(&mut self, region_id: RegionId) -> RequestCancelResult {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return RequestCancelResult::NotRunning;
        };

        status.request_cancel()
    }

    fn dispatch_compaction_planning(
        &self,
        plan_id: u64,
        request: CompactionRequest,
        options: compact_request::Options,
        time_range: Option<TimestampRange>,
    ) {
        let plugins = self.plugins.clone();
        let max_background_compactions = self.engine_config.max_background_compactions;
        common_runtime::spawn_compact(async move {
            let region_id = request.region_id();
            let request_sender = request.request_sender.clone();
            let planning = Self::prepare_compaction(
                request,
                options,
                plugins,
                max_background_compactions,
                time_range,
            );
            Self::notify_planning_result(region_id, plan_id, request_sender, planning).await;
        });
    }

    /// Runs the planning future and always sends the planning result back to
    /// the worker, even if the planning panics.
    ///
    /// The worker only leaves the picking phase after it receives the
    /// `CompactionPickFinished` notification. If a panicked planning task
    /// swallowed the notification, the region would be stuck in the picking
    /// phase forever, blocking all future compactions and pending DDLs (e.g.
    /// entering staging) of the region.
    async fn notify_planning_result(
        region_id: RegionId,
        plan_id: u64,
        request_sender: Sender<WorkerRequestWithTime>,
        planning: impl Future<Output = CompactionPlanningResult> + Send,
    ) {
        // The idiomatic way to handle a panic result.
        let result = std::panic::AssertUnwindSafe(planning).catch_unwind().await.unwrap_or_else(|payload| {
            let reason = if let Some(message) = payload.as_ref().downcast_ref::<&str>() {
                message.to_string()
            } else if let Some(message) = payload.as_ref().downcast_ref::<String>() {
                message.clone()
            } else {
                "unknown panic".to_string()
            };
            CompactionPlanningResult::Error(Arc::new(
                UnexpectedSnafu {
                    reason: format!(
                        "Compaction planning panicked for region {region_id}, plan_id {plan_id}: {reason}"
                    ),
                }
                    .build(),
            ))
        });
        if let CompactionPlanningResult::Error(err) = &result {
            error!(err; "Compaction planning failed for region {}, plan_id: {}", region_id, plan_id);
        }
        let request = WorkerRequestWithTime::new(WorkerRequest::Background {
            region_id,
            notify: BackgroundNotify::CompactionPickFinished(CompactionPickFinished {
                region_id,
                plan_id,
                result,
            }),
        });
        if request_sender.send(request).await.is_err() {
            warn!("Failed to send compaction planning result for region {region_id}");
        }
    }

    async fn prepare_compaction(
        request: CompactionRequest,
        options: compact_request::Options,
        plugins: Plugins,
        max_background_compactions: usize,
        time_range: Option<TimestampRange>,
    ) -> CompactionPlanningResult {
        let region_id = request.region_id();
        let (dynamic_compaction_opts, ttl) = find_dynamic_options(
            region_id,
            &request.current_version.options,
            &request.schema_metadata_manager,
        )
        .await
        .unwrap_or_else(|e| {
            warn!(e; "Failed to find dynamic options for region: {}", region_id);
            (
                request.current_version.options.compaction.clone(),
                request.current_version.options.ttl.unwrap_or_default(),
            )
        });

        let picker = new_picker(
            &options,
            &dynamic_compaction_opts,
            request.current_version.options.append_mode,
            Some(max_background_compactions),
            time_range,
        );
        let region_id = request.region_id();
        let CompactionRequest {
            engine_config,
            current_version,
            access_layer,
            request_sender: _,
            start_time,
            cache_manager,
            manifest_ctx,
            listener,
            schema_metadata_manager: _,
            max_parallelism,
        } = request;

        debug!(
            "Pick compaction strategy {:?} for region: {}, ttl: {:?}",
            picker, region_id, ttl
        );

        let compaction_region = CompactionRegion {
            region_id,
            current_version: current_version.clone(),
            region_options: RegionOptions {
                compaction: dynamic_compaction_opts.clone(),
                ..current_version.options.clone()
            },
            engine_config: engine_config.clone(),
            region_metadata: current_version.metadata.clone(),
            cache_manager: cache_manager.clone(),
            access_layer: access_layer.clone(),
            manifest_ctx: manifest_ctx.clone(),
            file_purger: None,
            ttl: Some(ttl),
            max_parallelism,
            plugins,
        };

        listener.on_compaction_pick_begin(region_id).await;
        let picker_region = compaction_region.clone();
        let picker_output = match common_runtime::spawn_blocking_compact(move || {
            let _pick_timer = COMPACTION_STAGE_ELAPSED
                .with_label_values(&["pick"])
                .start_timer();
            picker.pick(&picker_region)
        })
        .await
        .context(JoinSnafu)
        {
            Ok(output) => output,
            Err(err) => return CompactionPlanningResult::Error(Arc::new(err)),
        };

        let Some(picker_output) = picker_output else {
            return CompactionPlanningResult::NoPlan;
        };

        CompactionPlanningResult::Prepared(PreparedCompaction {
            compaction_region,
            picker_output,
            start_time,
            ttl,
        })
    }

    pub(crate) async fn handle_compaction_pick_finished(
        &mut self,
        finished: CompactionPickFinished,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> Vec<SenderDdlRequest> {
        let region_id = finished.region_id;
        let plan_id = finished.plan_id;
        let Some(status) = self.region_status.get(&region_id) else {
            return Vec::new();
        };
        // Picking runs detached from the worker. Its result may arrive after
        // close/reopen or replanning installed another Picking phase for this region.
        if !status.is_picking(finished.plan_id) {
            return Vec::new();
        }
        if !status.accept_plan(finished.plan_id) {
            return self.remove_region_on_cancel(region_id);
        }

        match finished.result {
            CompactionPlanningResult::Prepared(mut prepared) => {
                let current = status.version_control.current().version;
                let Some(picker_output) =
                    refresh_picker_output(prepared.picker_output, &current.ssts)
                else {
                    return self
                        .finish_compaction_planning(
                            region_id,
                            None,
                            manifest_ctx,
                            schema_metadata_manager,
                        )
                        .await;
                };
                let Some(files) = CompactingFiles::try_new(&picker_output) else {
                    return self
                        .finish_compaction_planning(
                            region_id,
                            None,
                            manifest_ctx,
                            schema_metadata_manager,
                        )
                        .await;
                };
                prepared.picker_output = picker_output;
                let Some(status) = self.region_status.get_mut(&region_id) else {
                    return Vec::new();
                };
                let waiters = status.take_waiters();
                match self
                    .submit_prepared_compaction(prepared, files, waiters, plan_id)
                    .await
                {
                    Ok(Some(phase)) => {
                        if let Some(status) = self.region_status.get_mut(&region_id) {
                            status.set_phase(phase);
                        }
                        Vec::new()
                    }
                    Ok(None) => {
                        self.finish_compaction_planning(
                            region_id,
                            None,
                            manifest_ctx,
                            schema_metadata_manager,
                        )
                        .await
                    }
                    Err(err) => {
                        self.remove_region_on_failure(region_id, Arc::new(err));
                        Vec::new()
                    }
                }
            }
            CompactionPlanningResult::NoPlan => {
                self.finish_compaction_planning(
                    region_id,
                    None,
                    manifest_ctx,
                    schema_metadata_manager,
                )
                .await
            }
            CompactionPlanningResult::Error(err) => {
                self.finish_compaction_planning(
                    region_id,
                    Some(err),
                    manifest_ctx,
                    schema_metadata_manager,
                )
                .await
            }
        }
    }

    async fn finish_compaction_planning(
        &mut self,
        region_id: RegionId,
        err: Option<Arc<Error>>,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> Vec<SenderDdlRequest> {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return Vec::new();
        };
        let Some(mut active) = status.take_active() else {
            return Vec::new();
        };
        for waiter in std::mem::take(&mut active.waiters) {
            if let Some(err) = &err {
                waiter.send(Err(err.clone()).context(CompactRegionSnafu { region_id }));
            } else {
                waiter.send(Ok(0));
            }
        }

        status.active = Some(active);
        if self.handle_pending_compaction_request(
            region_id,
            manifest_ctx,
            schema_metadata_manager.clone(),
        ) {
            return Vec::new();
        }

        let Some(active) = self
            .region_status
            .get_mut(&region_id)
            .and_then(CompactionStatus::take_active)
        else {
            return Vec::new();
        };
        if active.regular_followup_waiters.is_some() {
            self.schedule_next_compaction_with_active(
                region_id,
                manifest_ctx,
                schema_metadata_manager,
                Some(active),
                None,
            );
            return Vec::new();
        }

        self.region_status
            .remove(&region_id)
            .map(|mut status| std::mem::take(&mut status.pending_ddl_requests))
            .unwrap_or_default()
    }

    async fn submit_prepared_compaction(
        &mut self,
        prepared: PreparedCompaction,
        files: CompactingFiles,
        waiters: Vec<OutputTx>,
        mut plan_id: u64,
    ) -> Result<Option<CompactionPhase>> {
        let PreparedCompaction {
            compaction_region,
            picker_output,
            start_time,
            ttl,
        } = prepared;
        let region_id = compaction_region.region_id;
        let dynamic_compaction_opts = &compaction_region.region_options.compaction;

        // If specified to run compaction remotely, we schedule the compaction job remotely.
        // It will fall back to local compaction if there is no remote job scheduler.
        let waiters = if dynamic_compaction_opts.remote_compaction() {
            if let Some(remote_job_scheduler) = &self.plugins.get::<RemoteJobSchedulerRef>() {
                let execution = CompactionExecution::new(plan_id, files.clone());
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
                        Box::new(DefaultNotifier::new(
                            self.request_sender.clone(),
                            execution.clone(),
                        )),
                    )
                    .await;

                match result {
                    Ok(job_id) => {
                        info!(
                            "Scheduled remote compaction job {} for region {}",
                            job_id, region_id
                        );
                        INFLIGHT_COMPACTION_COUNT.inc();
                        return Ok(Some(CompactionPhase::Remote { execution }));
                    }
                    Err(e) => {
                        if !dynamic_compaction_opts.fallback_to_local() {
                            error!(e; "Failed to schedule remote compaction job for region {}", region_id);
                            if let Some(status) = self.region_status.get_mut(&region_id) {
                                status.extend_waiters(e.waiters);
                            }
                            return RemoteCompactionSnafu {
                                region_id,
                                job_id: None,
                                reason: e.reason,
                            }
                            .fail();
                        }

                        error!(e; "Failed to schedule remote compaction job for region {}, fallback to local compaction", region_id);
                        // An error may be ambiguous after the remote scheduler consumed
                        // the notifier. Fence a delayed remote callback from the local fallback.
                        plan_id = Self::next_plan_id(&mut self.next_plan_id);
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

        // Check whether this local compaction can ever fit before submitting it.
        let estimated_bytes = estimate_compaction_bytes(&picker_output);
        if let Some(limit_bytes) = self.exceeds_compaction_memory_limit(estimated_bytes) {
            COMPACTION_MEMORY_REJECTED
                .with_label_values(&["oversized"])
                .inc();
            warn!(
                "Skip compaction for region {} because estimated memory {} bytes exceeds compaction memory limit {} bytes",
                region_id, estimated_bytes, limit_bytes,
            );
            for waiter in waiters {
                waiter.send(Ok(0));
            }
            return Ok(None);
        }

        let cancel_handle = Arc::new(CancellationHandle::default());
        let state = LocalCompactionState::new(cancel_handle.clone());
        let execution = CompactionExecution::new(plan_id, files);
        let local_compaction_task = Box::new(CompactionTaskImpl {
            state: state.clone(),
            execution: execution.clone(),
            request_sender: self.request_sender.clone(),
            waiters,
            start_time,
            listener: self.listener.clone(),
            picker_output,
            compaction_region,
            compactor: Arc::new(DefaultCompactor::with_cancel_handle(cancel_handle.clone())),
            memory_manager: self.memory_manager.clone(),
            memory_policy: self.memory_policy,
            estimated_memory_bytes: estimated_bytes,
        });

        match self.submit_compaction_task(local_compaction_task, region_id) {
            Ok(()) => Ok(Some(CompactionPhase::Local { state, execution })),
            Err((err, task)) => {
                if let (Some(status), Some(mut task)) =
                    (self.region_status.get_mut(&region_id), task)
                {
                    status.append_waiters(&mut task.waiters);
                }
                Err(err)
            }
        }
    }

    fn submit_compaction_task(
        &mut self,
        task: Box<CompactionTaskImpl>,
        region_id: RegionId,
    ) -> std::result::Result<(), (Error, Option<Box<CompactionTaskImpl>>)> {
        let task = Arc::new(Mutex::new(Some(task)));
        let task_to_run = task.clone();
        match self.scheduler.schedule(Box::pin(async move {
            let task = task_to_run
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take();
            if let Some(mut task) = task {
                INFLIGHT_COMPACTION_COUNT.inc();
                task.run().await;
                INFLIGHT_COMPACTION_COUNT.dec();
            } else {
                error!("Compaction task was missing when the scheduled job started");
            }
        })) {
            Ok(()) => Ok(()),
            Err(err) => {
                error!(err; "Failed to submit compaction request for region {}", region_id);
                let task = task
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .take();
                Err((err, task))
            }
        }
    }

    fn exceeds_compaction_memory_limit(&self, estimated_bytes: u64) -> Option<u64> {
        let limit_bytes = self.memory_manager.limit_bytes();
        if limit_bytes > 0 && estimated_bytes > limit_bytes {
            Some(limit_bytes)
        } else {
            None
        }
    }

    fn remove_region_on_failure(&mut self, region_id: RegionId, err: Arc<Error>) {
        // Remove this region.
        let Some(status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Notifies all pending tasks.
        status.on_failure(err);
    }

    fn remove_region_on_cancel(&mut self, region_id: RegionId) -> Vec<SenderDdlRequest> {
        let Some(status) = self.region_status.remove(&region_id) else {
            return Vec::new();
        };

        status.on_cancel()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct LocalCompactionState {
    cancel_handle: Arc<CancellationHandle>,
    commit_started: Arc<Mutex<bool>>,
}

#[derive(Debug)]
enum CompactionPhase {
    Picking {
        plan_id: u64,
        cancelled: bool,
    },
    Local {
        state: LocalCompactionState,
        execution: CompactionExecution,
    },
    Remote {
        execution: CompactionExecution,
    },
}

#[derive(Debug)]
struct ActiveCompaction {
    phase: CompactionPhase,
    /// Waiters satisfied by the current planning or execution cycle. Picking waiters move into
    /// the submitted task; regular triggers coalesced during execution accumulate here.
    waiters: Vec<OutputTx>,
    /// Requests one fresh regular picking cycle after the current cycle finishes. It is kept
    /// separate because the current picker snapshot may predate the trigger; `Some(empty)` records
    /// an automatic trigger without an explicit waiter.
    regular_followup_waiters: Option<Vec<OutputTx>>,
}

impl ActiveCompaction {
    fn picking(plan_id: u64, waiters: Vec<OutputTx>) -> Self {
        Self {
            phase: CompactionPhase::Picking {
                plan_id,
                cancelled: false,
            },
            waiters,
            regular_followup_waiters: None,
        }
    }

    fn start_picking(&mut self, plan_id: u64) {
        self.phase = CompactionPhase::Picking {
            plan_id,
            cancelled: false,
        };
    }

    fn start_regular_picking(&mut self, plan_id: u64) {
        self.waiters
            .extend(self.regular_followup_waiters.take().unwrap_or_default());
        self.start_picking(plan_id);
    }

    fn is_picking(&self, expected_plan_id: u64) -> bool {
        matches!(
            self.phase,
            CompactionPhase::Picking { plan_id, .. } if plan_id == expected_plan_id
        )
    }

    fn accept_plan(&self, expected_plan_id: u64) -> bool {
        matches!(
            self.phase,
            CompactionPhase::Picking {
                plan_id,
                cancelled: false,
            } if plan_id == expected_plan_id
        )
    }

    fn matches_execution(&self, execution: &CompactionExecution) -> bool {
        match &self.phase {
            CompactionPhase::Picking { .. } => None,
            CompactionPhase::Local { execution, .. } | CompactionPhase::Remote { execution } => {
                Some(execution)
            }
        }
        .is_some_and(|current| current.matches(execution))
    }

    fn request_cancel(&mut self) -> RequestCancelResult {
        match &mut self.phase {
            CompactionPhase::Picking { cancelled, .. } => {
                if *cancelled {
                    RequestCancelResult::AlreadyCancelling
                } else {
                    *cancelled = true;
                    RequestCancelResult::CancelIssued
                }
            }
            CompactionPhase::Local { state, .. } => state.request_cancel(),
            CompactionPhase::Remote { .. } => RequestCancelResult::TooLateToCancel,
        }
    }

    fn merge_regular_trigger(&mut self, mut waiter: OptionOutputTx) {
        if matches!(self.phase, CompactionPhase::Picking { .. }) {
            let regular_followup_waiters = self.regular_followup_waiters.get_or_insert_default();
            if let Some(waiter) = waiter.take_inner() {
                regular_followup_waiters.push(waiter);
            }
        } else {
            self.merge_waiter(waiter);
        }
    }

    fn merge_waiter(&mut self, mut waiter: OptionOutputTx) {
        if let Some(waiter) = waiter.take_inner() {
            self.waiters.push(waiter);
        }
    }
}

/// Owns atomic reservations for every SST selected by a compaction plan.
#[derive(Debug, Clone)]
struct CompactingFiles {
    _inner: Arc<CompactingFilesInner>,
}

#[derive(Debug)]
struct CompactingFilesInner {
    files: Vec<FileHandle>,
}

impl CompactingFiles {
    fn try_new(output: &PickerOutput) -> Option<Self> {
        let mut seen = HashSet::new();
        let mut files: Vec<FileHandle> = Vec::new();
        let selected_files = output
            .outputs
            .iter()
            .flat_map(|output| output.inputs.iter())
            .chain(output.expired_ssts.iter());

        for file in selected_files {
            if !seen.insert(file.file_id()) {
                continue;
            }
            if !file.try_set_compacting() {
                for reserved in &files {
                    reserved.set_compacting(false);
                }
                return None;
            }
            files.push(file.clone());
        }

        Some(Self {
            _inner: Arc::new(CompactingFilesInner { files }),
        })
    }

    #[cfg(test)]
    fn empty() -> Self {
        Self {
            _inner: Arc::new(CompactingFilesInner { files: Vec::new() }),
        }
    }
}

impl Drop for CompactingFilesInner {
    fn drop(&mut self) {
        for file in &self.files {
            file.set_compacting(false);
        }
    }
}

impl LocalCompactionState {
    fn new(cancel_handle: Arc<CancellationHandle>) -> Self {
        Self {
            cancel_handle,
            commit_started: Arc::new(Mutex::new(false)),
        }
    }

    /// Returns the cancellation handle for this compaction task.
    pub(crate) fn cancel_handle(&self) -> Arc<CancellationHandle> {
        self.cancel_handle.clone()
    }

    /// Marks the compaction task as started to commit,
    /// which means the compaction task is in the final stage and is about to update region version and manifest.
    /// It will reject cancellation request after this method is called.
    ///
    /// Returns true if this is the first time to mark commit started, false otherwise.
    pub(crate) fn mark_commit_started(&self) -> bool {
        let mut commit_started = self.commit_started.lock().unwrap();
        if self.cancel_handle.is_cancelled() {
            return false;
        }
        *commit_started = true;
        true
    }

    /// Request cancellation for this compaction task.
    pub(crate) fn request_cancel(&self) -> RequestCancelResult {
        // The cancel handle must under the lock of `commit_started` to avoid racing between cancellation and commit.
        let commit_started = self.commit_started.lock().unwrap();
        if *commit_started {
            return RequestCancelResult::TooLateToCancel;
        }
        if self.cancel_handle.is_cancelled() {
            return RequestCancelResult::AlreadyCancelling;
        }

        self.cancel_handle.cancel();
        RequestCancelResult::CancelIssued
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RequestCancelResult {
    CancelIssued,
    AlreadyCancelling,
    TooLateToCancel,
    NotRunning,
}

impl Drop for CompactionScheduler {
    fn drop(&mut self) {
        for (region_id, status) in self.region_status.drain() {
            // We are shutting down so notify all pending tasks.
            status.on_failure(Arc::new(RegionClosedSnafu { region_id }.build()));
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
    /// Current compaction lifecycle. `None` is the existing transient idle state.
    // TODO: Remove idle statuses and make ActiveCompaction non-optional once chained
    // scheduling can recreate the status from region context.
    active: Option<ActiveCompaction>,
    /// Optional range retained by automatic continuations of the current compaction.
    time_range: Option<TimestampRange>,
    /// Pending compactions that are supposed to run as soon as current compaction task finished.
    ///
    /// This holds strict-window requests and ranged regular requests. An unrestricted regular
    /// request is instead merged into `ActiveCompaction::regular_followup_waiters` or `waiters`.
    pending_request: Option<PendingCompaction>,
    /// Pending DDL requests that should run when compaction is done.
    ///
    /// Although [`SenderDdlRequest`] can wrap any DDL variant, production code only queues
    /// [`DdlRequest::Truncate`] and [`DdlRequest::EnterStaging`] here. Both must serialize with
    /// compaction so they observe the version after compaction terminates.
    pending_ddl_requests: Vec<SenderDdlRequest>,
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
            active: None,
            time_range: None,
            pending_request: None,
            pending_ddl_requests: Vec::new(),
        }
    }

    #[cfg(test)]
    fn start_picking(&mut self, plan_id: u64) {
        self.start_picking_with_time_range(plan_id, None);
    }

    fn start_picking_with_time_range(&mut self, plan_id: u64, time_range: Option<TimestampRange>) {
        self.time_range = time_range;
        if let Some(active) = &mut self.active {
            active.start_picking(plan_id);
        } else {
            self.active = Some(ActiveCompaction::picking(plan_id, Vec::new()));
        }
    }

    fn start_regular_picking(
        &mut self,
        plan_id: u64,
        active: Option<ActiveCompaction>,
        time_range: Option<TimestampRange>,
    ) {
        self.time_range = time_range;
        self.active = Some(if let Some(mut active) = active {
            active.start_regular_picking(plan_id);
            active
        } else {
            ActiveCompaction::picking(plan_id, Vec::new())
        });
    }

    fn is_picking(&self, expected_plan_id: u64) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| active.is_picking(expected_plan_id))
    }

    fn accept_plan(&self, expected_plan_id: u64) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| active.accept_plan(expected_plan_id))
    }

    fn is_busy(&self) -> bool {
        self.active.is_some()
    }

    fn matches_execution(&self, execution: &CompactionExecution) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| active.matches_execution(execution))
    }

    #[cfg(test)]
    fn start_local_task(&mut self) -> LocalCompactionState {
        let state = LocalCompactionState::new(Arc::new(CancellationHandle::default()));
        let execution = CompactionExecution::new(0, CompactingFiles::empty());
        let phase = CompactionPhase::Local {
            state: state.clone(),
            execution,
        };
        if let Some(active) = &mut self.active {
            active.phase = phase;
        } else {
            self.active = Some(ActiveCompaction {
                phase,
                waiters: Vec::new(),
                regular_followup_waiters: None,
            });
        }
        state
    }

    #[cfg(test)]
    fn start_remote_task(&mut self) {
        let execution = CompactionExecution::new(0, CompactingFiles::empty());
        let phase = CompactionPhase::Remote { execution };
        if let Some(active) = &mut self.active {
            active.phase = phase;
        } else {
            self.active = Some(ActiveCompaction {
                phase,
                waiters: Vec::new(),
                regular_followup_waiters: None,
            });
        }
    }

    fn request_cancel(&mut self) -> RequestCancelResult {
        let Some(active) = &mut self.active else {
            return RequestCancelResult::NotRunning;
        };
        active.request_cancel()
    }

    #[cfg(test)]
    fn clear_running_task(&mut self) -> bool {
        self.active.take().is_some()
    }

    fn merge_regular_trigger(&mut self, waiter: OptionOutputTx) {
        if let Some(active) = &mut self.active {
            active.merge_regular_trigger(waiter);
        }
    }

    /// Merge the waiter to the pending compaction.
    fn merge_waiter(&mut self, waiter: OptionOutputTx) {
        if let Some(active) = &mut self.active {
            active.merge_waiter(waiter);
        }
    }

    fn take_active(&mut self) -> Option<ActiveCompaction> {
        self.active.take()
    }

    fn take_waiters(&mut self) -> Vec<OutputTx> {
        self.active
            .as_mut()
            .map(|active| std::mem::take(&mut active.waiters))
            .unwrap_or_default()
    }

    fn extend_waiters(&mut self, waiters: Vec<OutputTx>) {
        if let Some(active) = &mut self.active {
            active.waiters.extend(waiters);
        }
    }

    fn append_waiters(&mut self, waiters: &mut Vec<OutputTx>) {
        if let Some(active) = &mut self.active {
            active.waiters.append(waiters);
        }
    }

    fn set_phase(&mut self, phase: CompactionPhase) {
        if let Some(active) = &mut self.active {
            active.phase = phase;
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
        if let Some(mut active) = self.active.take() {
            for waiter in active
                .waiters
                .drain(..)
                .chain(active.regular_followup_waiters.take().unwrap_or_default())
            {
                waiter.send(Err(err.clone()).context(CompactRegionSnafu {
                    region_id: self.region_id,
                }));
            }
        }

        if let Some(pending_compaction) = self.pending_request {
            pending_compaction
                .waiter
                .send(Err(err.clone()).context(CompactRegionSnafu {
                    region_id: self.region_id,
                }));
        }

        for pending_ddl in self.pending_ddl_requests {
            pending_ddl
                .sender
                .send(Err(err.clone()).context(CompactRegionSnafu {
                    region_id: self.region_id,
                }));
        }
    }

    #[must_use]
    fn on_cancel(mut self) -> Vec<SenderDdlRequest> {
        if let Some(mut active) = self.active.take() {
            for waiter in active
                .waiters
                .drain(..)
                .chain(active.regular_followup_waiters.take().unwrap_or_default())
            {
                waiter.send(CompactionCancelledSnafu.fail());
            }
        }

        if let Some(pending_compaction) = self.pending_request {
            pending_compaction.waiter.send(
                Err(Arc::new(CompactionCancelledSnafu.build())).context(CompactRegionSnafu {
                    region_id: self.region_id,
                }),
            );
        }

        std::mem::take(&mut self.pending_ddl_requests)
    }

    /// Creates an immutable request for background compaction planning.
    #[allow(clippy::too_many_arguments)]
    fn new_compaction_request(
        &self,
        request_sender: Sender<WorkerRequestWithTime>,
        engine_config: Arc<MitoConfig>,
        cache_manager: CacheManagerRef,
        manifest_ctx: &ManifestContextRef,
        listener: WorkerListener,
        schema_metadata_manager: SchemaMetadataManagerRef,
        max_parallelism: usize,
    ) -> CompactionRequest {
        let current_version = CompactionVersion::from(self.version_control.current().version);
        let start_time = Instant::now();

        CompactionRequest {
            engine_config,
            current_version,
            access_layer: self.access_layer.clone(),
            request_sender: request_sender.clone(),
            start_time,
            cache_manager,
            manifest_ctx: manifest_ctx.clone(),
            listener,
            schema_metadata_manager,
            max_parallelism,
        }
    }
}

/// Pending compaction request that is supposed to run after current task is finished,
/// typically used for manual compactions.
struct PendingCompaction {
    /// Compaction options.
    pub(crate) options: compact_request::Options,
    /// Waiters of pending requests.
    pub(crate) waiter: OptionOutputTx,
    /// Max parallelism for pending compaction.
    pub(crate) max_parallelism: usize,
    /// Optional time range that constrains candidate compaction windows.
    pub(crate) time_range: Option<TimestampRange>,
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

/// Rebuilds picker output with current SST handles while preserving the picker's grouping.
///
/// Picking runs in background on a version snapshot that may be stale by the
/// time the plan is accepted: a concurrent flush, compaction or index rebuild
/// can replace a selected file with a new handle carrying updated metadata
/// (e.g. `index_version`), or remove the file entirely. The handles in the
/// picker output therefore cannot be used as-is; re-resolving them against the
/// current version both detects gone files (aborting the plan) and ensures the
/// execution reads and reserves the up-to-date handle.
fn refresh_picker_output(output: PickerOutput, current: &SstVersion) -> Option<PickerOutput> {
    let refresh = |file: FileHandle| {
        current
            .file_for_compaction(&file)
            .filter(|current| !current.is_deleted() && !current.compacting())
            .cloned()
    };
    let outputs = output
        .outputs
        .into_iter()
        .map(|output| {
            let inputs = output
                .inputs
                .into_iter()
                .map(&refresh)
                .collect::<Option<Vec<_>>>()?;
            Some(CompactionOutput { inputs, ..output })
        })
        .collect::<Option<Vec<_>>>()?;
    let expired_ssts = output
        .expired_ssts
        .into_iter()
        .map(refresh)
        .collect::<Option<Vec<_>>>()?;

    Some(PickerOutput {
        outputs,
        expired_ssts,
        time_window_size: output.time_window_size,
        max_file_size: output.max_file_size,
    })
}

#[cfg(test)]
#[path = "scheduler_test.rs"]
mod tests;
