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
#[cfg(test)]
mod tests {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/compaction_test.rs"
    ));
}
mod twcs;
mod window;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use api::v1::region::compact_request;
use api::v1::region::compact_request::Options;
use common_base::Plugins;
use common_base::cancellation::CancellationHandle;
use common_memory_manager::OnExhaustedPolicy;
use common_meta::key::SchemaMetadataManagerRef;
use common_telemetry::{debug, error, info, warn};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::{TimeToLive, Timestamp};
use datafusion_common::ScalarValue;
use datafusion_expr::Expr;
use datatypes::extension::json::is_structured_json_field;
use datatypes::types::json_type::JsonNativeType;
use futures::FutureExt;
use parquet::arrow::parquet_to_arrow_schema;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;
use task::MAX_PARALLEL_COMPACTION;
use tokio::sync::mpsc::{self, Sender};

use crate::access_layer::AccessLayerRef;
use crate::cache::{CacheManagerRef, CacheStrategy};
use crate::compaction::compactor::{CompactionRegion, CompactionVersion, DefaultCompactor};
use crate::compaction::memory_manager::CompactionMemoryManager;
use crate::compaction::picker::{CompactionTask, PickerOutput, new_picker};
use crate::compaction::task::CompactionTaskImpl;
use crate::config::MitoConfig;
use crate::error::{
    CompactRegionSnafu, CompactionCancelledSnafu, DataTypeMismatchSnafu, Error,
    GetSchemaMetadataSnafu, JoinSnafu, ManualCompactionOverrideSnafu, ParquetToArrowSchemaSnafu,
    RegionClosedSnafu, RegionDroppedSnafu, RegionTruncatedSnafu, RemoteCompactionSnafu, Result,
    TimeRangePredicateOverflowSnafu, TimeoutSnafu, UnexpectedSnafu,
};
use crate::metrics::{
    COMPACTION_MEMORY_REJECTED, COMPACTION_STAGE_ELAPSED, INFLIGHT_COMPACTION_COUNT,
};
use crate::read::FlatSource;
use crate::read::flat_projection::FlatProjectionMapper;
use crate::read::read_columns::ReadColumns;
use crate::read::scan_region::{PredicateGroup, ScanInput};
use crate::read::seq_scan::SeqScan;
use crate::region::options::{MergeMode, RegionOptions};
use crate::region::version::VersionControlRef;
use crate::region::{ManifestContextRef, RegionLeaderState, RegionRoleState};
use crate::request::{
    BackgroundNotify, OptionOutputTx, OutputTx, SenderDdlRequest, WorkerRequest,
    WorkerRequestWithTime,
};
use crate::schedule::remote_job_scheduler::{
    CompactionJob, DefaultNotifier, RemoteJob, RemoteJobSchedulerRef,
};
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::{FileHandle, FileMeta, Level};
use crate::sst::parquet::reader::MetadataCacheMetrics;
use crate::sst::version::{LevelMeta, SstVersion};
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
    pub(crate) version_control: VersionControlRef,
    pub(crate) result: CompactionPlanningResult,
}

pub(crate) struct PreparedCompaction {
    compaction_region: CompactionRegion,
    picker_output: PickerOutput,
    start_time: Instant,
    ttl: TimeToLive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionExecutionKind {
    Local,
    Remote,
}

#[derive(Debug, Clone)]
pub(crate) struct CompactionExecution {
    plan_id: u64,
    version_control: VersionControlRef,
    kind: CompactionExecutionKind,
    _files: CompactingFiles,
}

impl CompactionExecution {
    fn new(
        plan_id: u64,
        version_control: VersionControlRef,
        kind: CompactionExecutionKind,
        files: CompactingFiles,
    ) -> Self {
        Self {
            plan_id,
            version_control,
            kind,
            _files: files,
        }
    }

    pub(crate) fn matches(&self, other: &Self) -> bool {
        self.plan_id == other.plan_id
            && self.kind == other.kind
            && Arc::ptr_eq(&self.version_control, &other.version_control)
    }

    pub(crate) fn version_control(&self) -> &VersionControlRef {
        &self.version_control
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        version_control: VersionControlRef,
        kind: CompactionExecutionKind,
    ) -> Self {
        Self::new(0, version_control, kind, CompactingFiles::empty())
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
    /// Monotonically increasing token for compaction plans.
    next_plan_id: u64,
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

    fn next_plan_id(&mut self) -> u64 {
        let plan_id = self.next_plan_id;
        self.next_plan_id = self.next_plan_id.wrapping_add(1);
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
            match compact_options {
                Options::Regular(_) => {
                    status.merge_regular_trigger(waiter);
                }
                options @ Options::StrictWindow(_) => {
                    // Incoming compaction request is manually triggered.
                    if status.pending_ddl_requests.is_empty() {
                        status.set_pending_request(PendingCompaction {
                            options,
                            waiter,
                            max_parallelism,
                        });
                        info!(
                            "Region {} is compacting, manually compaction will be re-scheduled.",
                            region_id
                        );
                    } else {
                        waiter.send(CompactionCancelledSnafu.fail());
                        info!(
                            "Region {} has pending DDL requests, cancelling manual compaction.",
                            region_id
                        );
                    }
                }
            }
            return Ok(false);
        }

        // Publish the picking phase before dispatching background planning.
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), access_layer.clone());
        status.merge_waiter(waiter);
        let request = status.new_compaction_request(
            self.request_sender.clone(),
            self.engine_config.clone(),
            self.cache_manager.clone(),
            manifest_ctx,
            self.listener.clone(),
            schema_metadata_manager,
            max_parallelism,
        );
        let plan_id = self.next_plan_id();
        status.start_picking(plan_id);
        self.region_status.insert(region_id, status);
        self.dispatch_compaction_planning(
            plan_id,
            version_control.clone(),
            request,
            compact_options,
        );
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
        let version_control = status.version_control.clone();
        let plan_id = self.next_plan_id();
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return true;
        };
        status.start_picking(plan_id);
        self.dispatch_compaction_planning(plan_id, version_control, request, options);
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
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return Vec::new();
        };
        status.clear_running_task();

        // If there a pending compaction request, handle it first
        // and defer returning the pending DDL requests to the caller.
        if self.handle_pending_compaction_request(
            region_id,
            manifest_ctx,
            schema_metadata_manager.clone(),
        ) {
            return Vec::new();
        }

        let Some(status) = self.region_status.get_mut(&region_id) else {
            // The region status might be removed by the previous steps.
            // So we return empty DDL requests.
            return Vec::new();
        };

        for waiter in std::mem::take(&mut status.waiters) {
            waiter.send(Ok(0));
        }

        if status.regular_replan_pending {
            self.schedule_next_compaction(region_id, manifest_ctx, schema_metadata_manager);
            return Vec::new();
        }

        // If there are pending DDL requests, run them.
        let pending_ddl_requests = std::mem::take(&mut status.pending_ddl_requests);
        if !pending_ddl_requests.is_empty() {
            self.region_status.remove(&region_id);
            // If there are pending DDL requests, we should return them to the caller.
            // And skip try to schedule next compaction task.
            return pending_ddl_requests;
        }
        Vec::new()
    }

    pub(crate) fn is_current_execution(
        &self,
        region_id: RegionId,
        execution: &CompactionExecution,
    ) -> bool {
        self.region_status
            .get(&region_id)
            .is_some_and(|status| status.matches_execution(execution))
    }

    pub(crate) fn is_current_region_execution(
        &self,
        region_id: RegionId,
        current_version_control: &VersionControlRef,
        execution: &CompactionExecution,
    ) -> bool {
        Arc::ptr_eq(current_version_control, execution.version_control())
            && self.is_current_execution(region_id, execution)
    }

    pub(crate) async fn on_execution_finished(
        &mut self,
        region_id: RegionId,
        execution: &CompactionExecution,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> Vec<SenderDdlRequest> {
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
    /// A finished compaction leaves an idle status (phase = None) behind when
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
        let version_control = status.version_control.clone();
        let plan_id = self.next_plan_id();
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return false;
        };
        status.start_regular_picking(plan_id);
        self.dispatch_compaction_planning(
            plan_id,
            version_control,
            request,
            compact_request::Options::Regular(Default::default()),
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

    /// Add ddl request to pending queue.
    ///
    /// # Panics
    /// Panics if region didn't request compaction.
    pub(crate) fn add_ddl_request_to_pending(&mut self, request: SenderDdlRequest) {
        debug!(
            "Added pending DDL request for region: {}, ddl: {:?}",
            request.region_id, request.request
        );
        let status = self.region_status.get_mut(&request.region_id).unwrap();
        status.queue_ddl(request);
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

    pub(crate) fn request_cancel(&mut self, region_id: RegionId) -> RequestCancelResult {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return RequestCancelResult::NotRunning;
        };

        status.request_cancel()
    }

    fn dispatch_compaction_planning(
        &self,
        plan_id: u64,
        version_control: VersionControlRef,
        request: CompactionRequest,
        options: compact_request::Options,
    ) {
        let plugins = self.plugins.clone();
        let max_background_compactions = self.engine_config.max_background_compactions;
        common_runtime::spawn_compact(async move {
            let region_id = request.region_id();
            let request_sender = request.request_sender.clone();
            let planning =
                Self::prepare_compaction(request, options, plugins, max_background_compactions);
            Self::notify_planning_result(
                region_id,
                plan_id,
                version_control,
                request_sender,
                planning,
            )
            .await;
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
        version_control: VersionControlRef,
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
                version_control,
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

    pub(crate) async fn accept_compaction_pick_finished(
        &mut self,
        finished: CompactionPickFinished,
        current_version_control: &VersionControlRef,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> Vec<SenderDdlRequest> {
        let region_id = finished.region_id;
        let plan_id = finished.plan_id;
        let version_control = finished.version_control.clone();
        if !Arc::ptr_eq(current_version_control, &finished.version_control) {
            return Vec::new();
        }
        let Some(status) = self.region_status.get(&region_id) else {
            return Vec::new();
        };
        if !status.is_picking(finished.plan_id)
            || !Arc::ptr_eq(&status.version_control, &finished.version_control)
        {
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
                let waiters = std::mem::take(&mut status.waiters);
                match self
                    .submit_prepared_compaction(prepared, files, waiters, plan_id, version_control)
                    .await
                {
                    Ok(Some(phase)) => {
                        if let Some(status) = self.region_status.get_mut(&region_id) {
                            status.phase = Some(phase);
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

    #[cfg(test)]
    async fn handle_compaction_pick_finished(
        &mut self,
        finished: CompactionPickFinished,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> Vec<SenderDdlRequest> {
        let current_version_control = finished.version_control.clone();
        self.accept_compaction_pick_finished(
            finished,
            &current_version_control,
            manifest_ctx,
            schema_metadata_manager,
        )
        .await
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
        status.clear_running_task();
        for waiter in std::mem::take(&mut status.waiters) {
            if let Some(err) = &err {
                waiter.send(Err(err.clone()).context(CompactRegionSnafu { region_id }));
            } else {
                waiter.send(Ok(0));
            }
        }

        if self.handle_pending_compaction_request(
            region_id,
            manifest_ctx,
            schema_metadata_manager.clone(),
        ) {
            return Vec::new();
        }

        if self
            .region_status
            .get(&region_id)
            .is_some_and(|status| status.regular_replan_pending)
        {
            self.schedule_next_compaction(region_id, manifest_ctx, schema_metadata_manager);
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
        plan_id: u64,
        version_control: VersionControlRef,
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
                let execution = CompactionExecution::new(
                    plan_id,
                    version_control.clone(),
                    CompactionExecutionKind::Remote,
                    files.clone(),
                );
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
                                status.waiters.extend(e.waiters);
                            }
                            return RemoteCompactionSnafu {
                                region_id,
                                job_id: None,
                                reason: e.reason,
                            }
                            .fail();
                        }

                        error!(e; "Failed to schedule remote compaction job for region {}, fallback to local compaction", region_id);

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
        let execution = CompactionExecution::new(
            plan_id,
            version_control,
            CompactionExecutionKind::Local,
            files,
        );
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
                    status.waiters.append(&mut task.waiters);
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

impl CompactionPhase {
    fn execution(&self) -> Option<&CompactionExecution> {
        match self {
            Self::Picking { .. } => None,
            Self::Local { execution, .. } | Self::Remote { execution } => Some(execution),
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

/// Finds compaction options and TTL together with a single metadata fetch to reduce RTT.
async fn find_dynamic_options(
    region_id: RegionId,
    region_options: &crate::region::options::RegionOptions,
    schema_metadata_manager: &SchemaMetadataManagerRef,
) -> Result<(crate::region::options::CompactionOptions, TimeToLive)> {
    let table_id = region_id.table_id();
    if let (true, Some(ttl)) = (region_options.compaction_override, region_options.ttl) {
        debug!(
            "Use region options directly for table {}: compaction={:?}, ttl={:?}",
            table_id, region_options.compaction, region_options.ttl
        );
        return Ok((region_options.compaction.clone(), ttl));
    }

    let db_options = tokio::time::timeout(
        crate::config::FETCH_OPTION_TIMEOUT,
        schema_metadata_manager.get_schema_options_by_table_id(table_id),
    )
    .await
    .context(TimeoutSnafu)?
    .context(GetSchemaMetadataSnafu)?;

    let ttl = if let Some(ttl) = region_options.ttl {
        debug!(
            "Use region TTL directly for table {}: ttl={:?}",
            table_id, region_options.ttl
        );
        ttl
    } else {
        db_options
            .as_ref()
            .and_then(|options| options.ttl)
            .unwrap_or_default()
            .into()
    };

    let compaction = if !region_options.compaction_override {
        if let Some(schema_opts) = db_options {
            let map: HashMap<String, String> = schema_opts
                .extra_options
                .iter()
                .filter_map(|(k, v)| {
                    if k.starts_with("compaction.") {
                        Some((k.clone(), v.clone()))
                    } else {
                        None
                    }
                })
                .collect();
            if map.is_empty() {
                region_options.compaction.clone()
            } else {
                crate::region::options::RegionOptions::try_from_options(region_id, &map)
                    .map(|o| o.compaction)
                    .unwrap_or_else(|e| {
                        error!(e; "Failed to create RegionOptions from map");
                        region_options.compaction.clone()
                    })
            }
        } else {
            debug!(
                "DB options is None for table {}, use region compaction: compaction={:?}",
                table_id, region_options.compaction
            );
            region_options.compaction.clone()
        }
    } else {
        debug!(
            "No schema options for table {}, use region compaction: compaction={:?}",
            table_id, region_options.compaction
        );
        region_options.compaction.clone()
    };

    debug!(
        "Resolved dynamic options for table {}: compaction={:?}, ttl={:?}",
        table_id, compaction, ttl
    );
    Ok((compaction, ttl))
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
    /// Pending DDL requests that should run when compaction is done.
    pending_ddl_requests: Vec<SenderDdlRequest>,
    /// Current compaction phase.
    phase: Option<CompactionPhase>,
    /// Whether a regular trigger arrived while the current plan was being picked.
    regular_replan_pending: bool,
    /// Waiters owned by the pending regular follow-up.
    pending_regular_waiters: Vec<OutputTx>,
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
            pending_ddl_requests: Vec::new(),
            phase: None,
            regular_replan_pending: false,
            pending_regular_waiters: Vec::new(),
        }
    }

    fn start_picking(&mut self, plan_id: u64) {
        self.phase = Some(CompactionPhase::Picking {
            plan_id,
            cancelled: false,
        });
    }

    fn start_regular_picking(&mut self, plan_id: u64) {
        self.regular_replan_pending = false;
        self.waiters.append(&mut self.pending_regular_waiters);
        self.start_picking(plan_id);
    }

    fn is_picking(&self, expected_plan_id: u64) -> bool {
        matches!(
            self.phase,
            Some(CompactionPhase::Picking { plan_id, .. }) if plan_id == expected_plan_id
        )
    }

    fn accept_plan(&self, expected_plan_id: u64) -> bool {
        matches!(
            self.phase,
            Some(CompactionPhase::Picking {
                plan_id,
                cancelled: false,
            }) if plan_id == expected_plan_id
        )
    }

    fn is_busy(&self) -> bool {
        self.phase.is_some()
    }

    fn matches_execution(&self, execution: &CompactionExecution) -> bool {
        Arc::ptr_eq(&self.version_control, execution.version_control())
            && self
                .phase
                .as_ref()
                .and_then(CompactionPhase::execution)
                .is_some_and(|current| current.matches(execution))
    }

    #[cfg(test)]
    fn start_local_task(&mut self) -> LocalCompactionState {
        let state = LocalCompactionState::new(Arc::new(CancellationHandle::default()));
        let execution = CompactionExecution::new(
            0,
            self.version_control.clone(),
            CompactionExecutionKind::Local,
            CompactingFiles::empty(),
        );
        self.phase = Some(CompactionPhase::Local {
            state: state.clone(),
            execution,
        });
        state
    }

    #[cfg(test)]
    fn start_remote_task(&mut self) {
        let execution = CompactionExecution::new(
            0,
            self.version_control.clone(),
            CompactionExecutionKind::Remote,
            CompactingFiles::empty(),
        );
        self.phase = Some(CompactionPhase::Remote { execution });
    }

    fn request_cancel(&mut self) -> RequestCancelResult {
        let Some(phase) = &mut self.phase else {
            return RequestCancelResult::NotRunning;
        };

        match phase {
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

    fn clear_running_task(&mut self) -> bool {
        self.phase.take().is_some()
    }

    fn merge_regular_trigger(&mut self, mut waiter: OptionOutputTx) {
        if self.can_retain_regular_followup() {
            self.regular_replan_pending = true;
            if let Some(waiter) = waiter.take_inner() {
                self.pending_regular_waiters.push(waiter);
            }
        } else {
            self.merge_waiter(waiter);
        }
    }

    fn can_retain_regular_followup(&self) -> bool {
        matches!(self.phase, Some(CompactionPhase::Picking { .. }))
            && self.pending_ddl_requests.is_empty()
    }

    fn queue_ddl(&mut self, request: SenderDdlRequest) {
        // The first queued DDL fences later regular triggers from creating more follow-ups.
        self.pending_ddl_requests.push(request);
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
        for waiter in self
            .waiters
            .drain(..)
            .chain(self.pending_regular_waiters.drain(..))
        {
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
        for waiter in self
            .waiters
            .drain(..)
            .chain(self.pending_regular_waiters.drain(..))
        {
            waiter.send(CompactionCancelledSnafu.fail());
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

/// Builders to create [BoxedRecordBatchStream] for compaction.
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
    /// Build a [FlatSource] that yields Arrow `RecordBatch`s from reading all the input SST files,
    /// for compaction. The schema of the [FlatSource] is unified.
    async fn build_flat_sst_reader(self) -> Result<FlatSource> {
        let scan_input = self.build_scan_input().await?;

        let schema = scan_input.mapper.output_schema();
        let schema = schema.arrow_schema();

        let stream = SeqScan::new(scan_input)
            .build_flat_reader_for_compaction()
            .await?;
        Ok(FlatSource::new_stream(schema.clone(), stream))
    }

    async fn build_scan_input(self) -> Result<ScanInput> {
        let schema = self.metadata.schema.arrow_schema();
        let parquet_metadata = self.collect_parquet_metadata().await?;
        let batch_size = crate::batch_size::estimate_batch_size(
            parquet_metadata
                .iter()
                .flat_map(|metadata| metadata.row_groups())
                .map(|row_group| {
                    let uncompressed_bytes = row_group
                        .columns()
                        .iter()
                        .map(|column| column.uncompressed_size() as u64)
                        .sum();
                    (row_group.num_rows() as u64, uncompressed_bytes)
                }),
        );
        let json_type_hint = if schema.fields().iter().any(is_structured_json_field) {
            let mut json_type_hint = schema
                .fields()
                .iter()
                .filter(|&field| is_structured_json_field(field))
                .map(|field| (field.name().clone(), JsonNativeType::Null))
                .collect::<HashMap<_, _>>();

            for metadata in &parquet_metadata {
                let file_metadata = metadata.file_metadata();
                let schema = parquet_to_arrow_schema(
                    file_metadata.schema_descr(),
                    file_metadata.key_value_metadata(),
                )
                .context(ParquetToArrowSchemaSnafu {
                    file: "compaction input",
                })?;
                for field in schema.fields() {
                    let Some(merged) = json_type_hint.get_mut(field.name()) else {
                        continue;
                    };

                    let json_type = JsonNativeType::try_from(field.data_type())
                        .context(DataTypeMismatchSnafu)?;
                    merged.merge(&json_type);
                }
            }

            Some(json_type_hint)
        } else {
            None
        };

        let projection = (0..self.metadata.column_metadatas.len()).collect();
        let read_columns = ReadColumns::from_deduped_column_ids(
            self.metadata.column_metadatas.iter().map(|x| x.column_id),
        );
        let mapper = FlatProjectionMapper::new_with_read_columns(
            &self.metadata,
            projection,
            read_columns,
            json_type_hint.as_ref(),
        )?;

        let mut scan_input = ScanInput::new(self.sst_layer, mapper)
            .with_files(self.inputs.to_vec())
            .with_compaction(true)
            .with_batch_size(batch_size)
            .with_append_mode(self.append_mode)
            // We use special cache strategy for compaction.
            .with_cache(CacheStrategy::Compaction(self.cache))
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

        Ok(scan_input)
    }

    async fn collect_parquet_metadata(&self) -> Result<Vec<Arc<ParquetMetaData>>> {
        let mut metadata = Vec::with_capacity(self.inputs.len());

        for file_handle in self.inputs {
            let file_path =
                file_handle.file_path(self.sst_layer.table_dir(), self.sst_layer.path_type());
            let file_size = file_handle.meta_ref().file_size;
            let parquet_metadata = match self
                .sst_layer
                .read_sst(file_handle.clone())
                .cache(CacheStrategy::Compaction(self.cache.clone()))
                .read_parquet_metadata(
                    &file_path,
                    file_size,
                    &mut MetadataCacheMetrics::default(),
                    PageIndexPolicy::default(),
                )
                .await
                .map(|x| x.0.parquet_metadata())
            {
                Ok(x) => x,
                Err(e) if e.is_object_not_found() => continue,
                Err(e) => return Err(e),
            };
            metadata.push(parquet_metadata);
        }
        Ok(metadata)
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

/// Rebuilds picker output with current SST handles while preserving the picker's grouping.
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
