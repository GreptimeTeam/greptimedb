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

mod planning;
mod state;

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::region::compact_request;
use common_base::Plugins;
use common_memory_manager::OnExhaustedPolicy;
use common_meta::key::SchemaMetadataManagerRef;
use common_telemetry::{debug, error, info};
use common_time::range::TimestampRange;
pub(crate) use planning::CompactionPickFinished;
pub use planning::CompactionRequest;
pub(crate) use state::CompactionExecution;
use state::{ActiveCompaction, CompactionStatus, PendingCompaction};
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::compaction::memory_manager::CompactionMemoryManager;
use crate::compaction::task::MAX_PARALLEL_COMPACTION;
use crate::config::MitoConfig;
use crate::error::{
    CompactionCancelledSnafu, Error, RegionClosedSnafu, RegionDroppedSnafu, RegionTruncatedSnafu,
    Result,
};
use crate::region::version::VersionControlRef;
use crate::region::{ManifestContextRef, RegionLeaderState, RegionRoleState};
use crate::request::{DdlRequest, OptionOutputTx, SenderDdlRequest, WorkerRequestWithTime};
use crate::schedule::RequestCancelResult;
use crate::schedule::scheduler::SchedulerRef;
use crate::worker::WorkerListener;

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

impl Drop for CompactionScheduler {
    fn drop(&mut self) {
        for (region_id, status) in self.region_status.drain() {
            // We are shutting down so notify all pending tasks.
            status.on_failure(Arc::new(RegionClosedSnafu { region_id }.build()));
        }
    }
}

#[cfg(test)]
#[path = "scheduler_test.rs"]
mod tests;
