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
use state::{CompactionStatus, CompactionTrigger, PendingCompaction};
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::compaction::memory_manager::CompactionMemoryManager;
use crate::compaction::task::MAX_PARALLEL_COMPACTION;
use crate::config::MitoConfig;
use crate::error::{
    CompactionCancelledSnafu, Error, ManualCompactionAlreadyRunningSnafu, RegionClosedSnafu,
    RegionDroppedSnafu, RegionTruncatedSnafu, Result,
};
use crate::region::version::VersionControlRef;
use crate::region::{ManifestContextRef, RegionLeaderState, RegionRoleState};
use crate::request::{DdlRequest, OptionOutputTx, SenderDdlRequest, WorkerRequestWithTime};
#[cfg(test)]
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

/// Describes the immediate action produced by a compaction terminal transition.
#[derive(Debug)]
pub(crate) enum CompactionTransition {
    /// No follow-up work was dispatched and no DDL became ready.
    NoAction,
    /// An automatic follow-up planning cycle was dispatched.
    AutomaticFollowupScheduled,
    /// DDL requests became ready after the compaction lifecycle terminated.
    DdlReady(Vec<SenderDdlRequest>),
}

impl CompactionTransition {
    fn from_pending_ddls(pending_ddls: Vec<SenderDdlRequest>) -> Self {
        if pending_ddls.is_empty() {
            Self::NoAction
        } else {
            Self::DdlReady(pending_ddls)
        }
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        !matches!(self, Self::DdlReady(pending_ddls) if !pending_ddls.is_empty())
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        match self {
            Self::DdlReady(pending_ddls) => pending_ddls.len(),
            Self::NoAction | Self::AutomaticFollowupScheduled => 0,
        }
    }
}

// API used by callers outside the compaction scheduler module.
impl CompactionScheduler {
    /// Creates an empty scheduler bound to one region worker.
    ///
    /// # Effects
    ///
    /// Stores the worker request sender and shared scheduling resources. No
    /// compaction is dispatched until a scheduling method is called.
    ///
    /// # Constraints
    ///
    /// All lifecycle methods on the returned scheduler must be driven by the
    /// same worker so region state transitions remain serialized.
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

    /// Accepts an automatic compaction trigger for a region.
    ///
    /// # Effects
    ///
    /// Starts planning when no scheduler status exists for the region.
    /// Otherwise, an active status coalesces the trigger into one unrestricted
    /// follow-up cycle. Returns `true` only when this call dispatches planning.
    ///
    /// # Constraints
    ///
    /// The owning region worker must call this method serially. Automatic
    /// triggers have no waiter and may be ignored while a DDL fence is active.
    /// The region identity comes from `version_control`; `access_layer` and
    /// `manifest_ctx` must belong to the same region.
    pub(crate) fn schedule_automatic_compaction(
        &mut self,
        compact_options: compact_request::Options,
        version_control: &VersionControlRef,
        access_layer: &AccessLayerRef,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> Result<bool> {
        self.schedule_compaction(
            CompactionTrigger::Automatic,
            compact_options,
            version_control,
            access_layer,
            OptionOutputTx::none(),
            manifest_ctx,
            schema_metadata_manager,
            1, // Default for automatic compaction
            None,
        )
    }

    /// Accepts a manual compaction request for a region.
    ///
    /// # Effects
    ///
    /// Starts planning when no scheduler status exists for the region. During
    /// an automatic compaction, queues the request and replaces any older
    /// pending manual request; during a manual compaction, rejects the new
    /// request through `waiter`. Returns `true` only when this call dispatches
    /// planning.
    ///
    /// # Constraints
    ///
    /// The owning region worker must call this method serially. The supplied
    /// waiter is completed exactly once by cycle completion, replacement,
    /// cancellation, failure, or region teardown. The region identity comes
    /// from `version_control`; `access_layer` and `manifest_ctx` must belong to
    /// the same region.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn schedule_manual_compaction(
        &mut self,
        compact_options: compact_request::Options,
        version_control: &VersionControlRef,
        access_layer: &AccessLayerRef,
        waiter: OptionOutputTx,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
        max_parallelism: usize,
        time_range: Option<TimestampRange>,
    ) -> Result<bool> {
        self.schedule_compaction(
            CompactionTrigger::Manual,
            compact_options,
            version_control,
            access_layer,
            waiter,
            manifest_ctx,
            schema_metadata_manager,
            max_parallelism,
            time_range,
        )
    }

    /// Applies a background planning result to the current region state.
    ///
    /// # Effects
    ///
    /// Rejects stale plans, submits a prepared local or remote execution, or
    /// completes the cycle when planning produced no executable plan. The
    /// returned transition reports a dispatched automatic follow-up or DDL
    /// requests released by the resulting terminal transition.
    ///
    /// # Constraints
    ///
    /// The owning worker must call this method for planning notifications and
    /// must execute every returned DDL request. The notification may be stale;
    /// stale notifications intentionally have no effect.
    pub(crate) async fn handle_compaction_pick_finished(
        &mut self,
        finished: CompactionPickFinished,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> CompactionTransition {
        self.handle_compaction_pick_finished_inner(finished, manifest_ctx, schema_metadata_manager)
            .await
    }

    /// Returns whether a terminal notification belongs to the installed execution.
    ///
    /// # Effects
    ///
    /// Performs a read-only identity check against the region's current plan.
    ///
    /// # Constraints
    ///
    /// Callers must check this before applying a compaction edit. Matching only
    /// the region id is insufficient because close/reopen can replace a status.
    pub(crate) fn is_current_execution(
        &self,
        region_id: RegionId,
        execution: &CompactionExecution,
    ) -> bool {
        self.region_status
            .get(&region_id)
            .is_some_and(|status| status.matches_execution(execution))
    }

    /// Completes the installed execution after its edit has been applied.
    ///
    /// # Effects
    ///
    /// Notifies waiters, schedules a pending manual or explicit automatic
    /// follow-up, or removes the region status. The returned transition reports
    /// a dispatched automatic follow-up or DDLs that are now safe to execute.
    ///
    /// # Constraints
    ///
    /// The owning worker must call this only after accepting and applying the
    /// execution's result, and must execute every returned DDL request. Stale
    /// executions are ignored.
    pub(crate) async fn on_execution_finished(
        &mut self,
        region_id: RegionId,
        execution: &CompactionExecution,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> CompactionTransition {
        if !self.is_current_execution(region_id, execution) {
            return CompactionTransition::NoAction;
        }
        self.on_compaction_finished(region_id, manifest_ctx, schema_metadata_manager)
            .await
    }

    /// Completes a cooperatively canceled execution.
    ///
    /// # Effects
    ///
    /// Removes the matching region status, notifies compaction waiters, and
    /// returns DDLs that were waiting for cancellation.
    ///
    /// # Constraints
    ///
    /// The owning worker must execute every returned DDL request. A stale
    /// cancellation is ignored.
    pub(crate) async fn on_execution_cancelled(
        &mut self,
        region_id: RegionId,
        execution: &CompactionExecution,
    ) -> Vec<SenderDdlRequest> {
        if !self.is_current_execution(region_id, execution) {
            return Vec::new();
        }
        self.on_compaction_cancelled(region_id).await
    }

    /// Records failure of the installed execution.
    ///
    /// # Effects
    ///
    /// Removes the matching region status and fails all compaction waiters and
    /// dependent DDL requests with the supplied error.
    ///
    /// # Constraints
    ///
    /// A stale execution failure is ignored so it cannot tear down replacement
    /// state.
    pub(crate) fn on_execution_failed(
        &mut self,
        region_id: RegionId,
        execution: &CompactionExecution,
        err: Arc<Error>,
    ) {
        if !self.is_current_execution(region_id, execution) {
            return;
        }
        self.on_compaction_failed(region_id, err);
    }

    /// Removes compaction state because the region was dropped.
    ///
    /// # Effects
    ///
    /// Fails all compaction and DDL waiters owned by the status.
    ///
    /// # Constraints
    ///
    /// The owning worker must invoke this as part of serialized region teardown.
    pub(crate) fn on_region_dropped(&mut self, region_id: RegionId) {
        self.remove_region_on_failure(
            region_id,
            Arc::new(RegionDroppedSnafu { region_id }.build()),
        );
    }

    /// Removes compaction state because the region was closed.
    ///
    /// # Effects
    ///
    /// Fails all compaction and DDL waiters owned by the status.
    ///
    /// # Constraints
    ///
    /// The owning worker must invoke this as part of serialized region teardown.
    pub(crate) fn on_region_closed(&mut self, region_id: RegionId) {
        self.remove_region_on_failure(region_id, Arc::new(RegionClosedSnafu { region_id }.build()));
    }

    /// Removes compaction state because the region was truncated.
    ///
    /// # Effects
    ///
    /// Fails all compaction and DDL waiters owned by the status.
    ///
    /// # Constraints
    ///
    /// The owning worker must invoke this after truncate completes.
    pub(crate) fn on_region_truncated(&mut self, region_id: RegionId) {
        self.remove_region_on_failure(
            region_id,
            Arc::new(RegionTruncatedSnafu { region_id }.build()),
        );
    }

    /// Cancels a running compaction and queues its dependent DDL atomically.
    ///
    /// # Effects
    ///
    /// Requests cancellation when the active phase can still stop, then queues
    /// the DDL. The queued DDL forms a scheduling fence for subsequent
    /// compaction triggers. If no compaction is running, returns the sender and
    /// typed request unchanged.
    ///
    /// # Constraints
    ///
    /// Production callers use this only for [`DdlRequest::Truncate`] and
    /// [`DdlRequest::EnterStaging`]. `Ok(())` means the caller must wait for a
    /// terminal callback to receive and execute the queued DDL; `Err` means the
    /// caller must execute the returned request directly.
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
        status.request_cancel();

        let request = SenderDdlRequest {
            region_id,
            sender,
            request: into_ddl_request(request),
        };
        debug!(
            "Added pending DDL request for region: {}, ddl: {:?}",
            request.region_id, request.request
        );
        status.pending_ddl_requests.push(request);
        Ok(())
    }
}

// Internal state-machine helpers.
impl CompactionScheduler {
    /// Returns the current plan id and advances the counter.
    ///
    /// Takes the counter instead of `&mut self` so callers can bump it while
    /// holding a mutable borrow of a region status.
    fn next_plan_id(counter: &mut u64) -> u64 {
        let plan_id = *counter;
        *counter = counter.wrapping_add(1);
        plan_id
    }

    #[allow(clippy::too_many_arguments)]
    fn schedule_compaction(
        &mut self,
        trigger: CompactionTrigger,
        compact_options: compact_request::Options,
        version_control: &VersionControlRef,
        access_layer: &AccessLayerRef,
        waiter: OptionOutputTx,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
        max_parallelism: usize,
        time_range: Option<TimestampRange>,
    ) -> Result<bool> {
        let region_id = version_control.region_id();
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
            // manual request receives CompactionCancelled; automatic triggers are ignored.
            if !status.pending_ddl_requests.is_empty() {
                waiter.send(CompactionCancelledSnafu.fail());
                info!(
                    "Region {} has pending DDL requests, ignoring compaction: {:?}",
                    region_id, compact_options
                );
                return Ok(false);
            }

            match trigger {
                CompactionTrigger::Automatic => status.mark_automatic_trigger(),
                CompactionTrigger::Manual if status.is_manual_compaction() => {
                    waiter.send(ManualCompactionAlreadyRunningSnafu { region_id }.fail());
                    info!(
                        "Region {} already has a manually triggered compaction running",
                        region_id
                    );
                }
                CompactionTrigger::Manual => {
                    status.set_pending_request(PendingCompaction {
                        options: compact_options,
                        waiter,
                        max_parallelism,
                        time_range,
                    });
                    info!(
                        "Region {} is running an automatic compaction; manual compaction will be re-scheduled",
                        region_id
                    );
                }
            }
            return Ok(false);
        }

        // Publish the picking phase before dispatching background planning.
        let plan_id = Self::next_plan_id(&mut self.next_plan_id);
        let mut status = CompactionStatus::new(
            region_id,
            version_control.clone(),
            access_layer.clone(),
            plan_id,
            trigger,
        );
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
        self.region_status.insert(region_id, status);
        self.dispatch_compaction_planning(plan_id, request, compact_options, time_range);
        self.listener.on_compaction_scheduled(region_id);
        Ok(true)
    }

    // Handle pending manual compaction request for the region.
    //
    // Returns true if should early return, false otherwise.
    fn handle_pending_compaction_request(
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
        status.start_picking_with_trigger(plan_id, CompactionTrigger::Manual);
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
    ) -> CompactionTransition {
        if !self.region_status.contains_key(&region_id) {
            return CompactionTransition::NoAction;
        }

        if self.handle_pending_compaction_request(
            region_id,
            manifest_ctx,
            schema_metadata_manager.clone(),
        ) {
            return CompactionTransition::NoAction;
        }

        // The region status might be removed by the previous steps.
        // So we return empty DDL requests.
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return CompactionTransition::NoAction;
        };
        for waiter in status.take_waiters() {
            waiter.send(Ok(0));
        }

        // A queued DDL was waiting for the current task to terminate; chaining
        // another compaction ahead of it would delay the DDL by a whole extra
        // plan/execution cycle, so dispatch the DDLs first.
        let pending_ddl_requests = std::mem::take(&mut status.pending_ddl_requests);
        if !pending_ddl_requests.is_empty() {
            // The DDL supersedes any retained automatic follow-up.
            self.region_status.remove(&region_id);
            // If there are pending DDL requests, we should return them to the caller.
            // And skip try to schedule next compaction task.
            return CompactionTransition::DdlReady(pending_ddl_requests);
        }

        if status.active.reset_automatic_followup()
            && self.schedule_automatic_followup(region_id, manifest_ctx, schema_metadata_manager)
        {
            return CompactionTransition::AutomaticFollowupScheduled;
        }
        self.region_status.remove(&region_id);
        CompactionTransition::NoAction
    }

    fn schedule_automatic_followup(
        &mut self,
        region_id: RegionId,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> bool {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return false;
        };
        // An external automatic trigger requires one unrestricted follow-up pick.
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
        status.start_regular_picking(plan_id);
        self.dispatch_compaction_planning(
            plan_id,
            request,
            compact_request::Options::Regular(Default::default()),
            None,
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

    /// Notifies the scheduler that the compaction job is failed.
    fn on_compaction_failed(&mut self, region_id: RegionId, err: Arc<Error>) {
        error!(err; "Region {} failed to compact, cancel all pending tasks", region_id);
        self.remove_region_on_failure(region_id, err);
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
    fn has_pending_ddls(&self, region_id: RegionId) -> bool {
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
    fn request_cancel(&mut self, region_id: RegionId) -> RequestCancelResult {
        self.region_status
            .get_mut(&region_id)
            .unwrap()
            .request_cancel()
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
