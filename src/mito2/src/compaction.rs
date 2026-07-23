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

use std::any::Any;
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
    pub(crate) async fn handle_pending_compaction_request(
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

        let request = {
            status.new_compaction_request(
                self.request_sender.clone(),
                self.engine_config.clone(),
                self.cache_manager.clone(),
                manifest_ctx,
                self.listener.clone(),
                schema_metadata_manager,
                max_parallelism,
            )
        };
        self.region_status
            .get_mut(&region_id)
            .unwrap()
            .merge_waiter(waiter);
        let version_control = self.region_status[&region_id].version_control.clone();
        let plan_id = self.next_plan_id();
        self.region_status
            .get_mut(&region_id)
            .unwrap()
            .start_picking(plan_id);
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
        if self
            .handle_pending_compaction_request(
                region_id,
                manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await
        {
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
            self.schedule_next_compaction(region_id, manifest_ctx, schema_metadata_manager)
                .await;
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

    /// Schedules next compaction upon a finished compaction.
    /// Returns whether the compaction is scheduled.
    pub(crate) async fn schedule_next_compaction(
        &mut self,
        region_id: RegionId,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
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
        let version_control = self.region_status[&region_id].version_control.clone();
        let plan_id = self.next_plan_id();
        self.region_status
            .get_mut(&region_id)
            .unwrap()
            .start_regular_picking(plan_id);
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
                let current = self.region_status[&region_id]
                    .version_control
                    .current()
                    .version;
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

        if self
            .handle_pending_compaction_request(
                region_id,
                manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await
        {
            return Vec::new();
        }

        if self.region_status[&region_id].regular_replan_pending {
            self.schedule_next_compaction(region_id, manifest_ctx, schema_metadata_manager)
                .await;
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

#[cfg(test)]
mod tests {
    use std::assert_matches;
    use std::time::Duration;

    use api::v1::region::StrictWindow;
    use common_datasource::compression::CompressionType;
    use common_meta::key::schema_name::SchemaNameValue;
    use common_time::DatabaseTimeToLive;
    use store_api::storage::FileId;
    use tokio::sync::{Barrier, oneshot};

    use super::*;
    use crate::compaction::memory_manager::{CompactionMemoryGuard, new_compaction_memory_manager};
    use crate::compaction::test_util::new_file_handle;
    use crate::engine::listener::CompactionPlanningGate;
    use crate::error::InvalidSchedulerStateSnafu;
    use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
    use crate::region::ManifestContext;
    use crate::schedule::scheduler::{Job, Scheduler};
    use crate::sst::FormatType;
    use crate::test_util::mock_schema_metadata_manager;
    use crate::test_util::scheduler_util::{SchedulerEnv, VecScheduler};
    use crate::test_util::version_util::{VersionControlBuilder, apply_edit};

    struct FailingScheduler;

    struct SuccessfulRemoteScheduler;

    struct FailingRemoteScheduler;

    #[derive(Default)]
    struct HoldingRemoteScheduler {
        notifier: Mutex<Option<Box<dyn crate::schedule::remote_job_scheduler::Notifier>>>,
    }

    impl HoldingRemoteScheduler {
        fn drop_notifier(&self) {
            self.notifier.lock().unwrap().take();
        }
    }

    #[async_trait::async_trait]
    impl crate::schedule::remote_job_scheduler::RemoteJobScheduler for SuccessfulRemoteScheduler {
        async fn schedule(
            &self,
            _job: RemoteJob,
            _notifier: Box<dyn crate::schedule::remote_job_scheduler::Notifier>,
        ) -> std::result::Result<
            crate::schedule::remote_job_scheduler::JobId,
            crate::schedule::remote_job_scheduler::RemoteJobSchedulerError,
        > {
            Ok(crate::schedule::remote_job_scheduler::JobId::parse_str(
                "00000000-0000-0000-0000-000000000001",
            )
            .unwrap())
        }
    }

    #[async_trait::async_trait]
    impl crate::schedule::remote_job_scheduler::RemoteJobScheduler for FailingRemoteScheduler {
        async fn schedule(
            &self,
            job: RemoteJob,
            _notifier: Box<dyn crate::schedule::remote_job_scheduler::Notifier>,
        ) -> std::result::Result<
            crate::schedule::remote_job_scheduler::JobId,
            crate::schedule::remote_job_scheduler::RemoteJobSchedulerError,
        > {
            let RemoteJob::CompactionJob(job) = job;
            Err(
                crate::schedule::remote_job_scheduler::RemoteJobSchedulerError {
                    location: snafu::location!(),
                    reason: "remote scheduler rejected job".to_string(),
                    waiters: job.waiters,
                },
            )
        }
    }

    #[async_trait::async_trait]
    impl crate::schedule::remote_job_scheduler::RemoteJobScheduler for HoldingRemoteScheduler {
        async fn schedule(
            &self,
            _job: RemoteJob,
            notifier: Box<dyn crate::schedule::remote_job_scheduler::Notifier>,
        ) -> std::result::Result<
            crate::schedule::remote_job_scheduler::JobId,
            crate::schedule::remote_job_scheduler::RemoteJobSchedulerError,
        > {
            self.notifier.lock().unwrap().replace(notifier);
            Ok(crate::schedule::remote_job_scheduler::JobId::parse_str(
                "00000000-0000-0000-0000-000000000002",
            )
            .unwrap())
        }
    }

    fn compactable_version() -> VersionControlRef {
        let mut builder = VersionControlBuilder::new();
        let end = 1000 * 1000;
        Arc::new(
            builder
                .push_l0_file(0, end)
                .push_l0_file(10, end)
                .push_l0_file(50, end)
                .push_l0_file(80, end)
                .push_l0_file(90, end)
                .build(),
        )
    }

    async fn begin_pick_result(
        env: &SchedulerEnv,
        scheduler: &mut CompactionScheduler,
        rx: &mut mpsc::Receiver<WorkerRequestWithTime>,
        version_control: &VersionControlRef,
    ) -> (
        CompactionPickFinished,
        ManifestContextRef,
        SchemaMetadataManagerRef,
    ) {
        let region_id = version_control.current().version.metadata.region_id;
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        assert!(
            scheduler
                .schedule_compaction(
                    region_id,
                    Options::Regular(Default::default()),
                    version_control,
                    &env.access_layer,
                    OptionOutputTx::none(),
                    &manifest_ctx,
                    schema_metadata_manager.clone(),
                    1,
                )
                .await
                .unwrap()
        );
        let finished = recv_compaction_pick_finished(rx).await;
        assert!(matches!(
            &finished.result,
            CompactionPlanningResult::Prepared(_)
        ));
        (finished, manifest_ctx, schema_metadata_manager)
    }

    fn selected_files(finished: &CompactionPickFinished) -> Vec<FileHandle> {
        let CompactionPlanningResult::Prepared(prepared) = &finished.result else {
            panic!("expected prepared compaction");
        };
        prepared
            .picker_output
            .outputs
            .iter()
            .flat_map(|output| output.inputs.iter().cloned())
            .chain(prepared.picker_output.expired_ssts.iter().cloned())
            .collect()
    }

    fn use_remote_compaction(finished: &mut CompactionPickFinished) {
        let CompactionPlanningResult::Prepared(prepared) = &mut finished.result else {
            panic!("expected prepared compaction");
        };
        let crate::region::options::CompactionOptions::Twcs(options) =
            &mut prepared.compaction_region.region_options.compaction;
        options.remote_compaction = true;
        options.fallback_to_local = false;
    }

    fn picker_output_with_files(
        output_files: Vec<FileHandle>,
        expired_ssts: Vec<FileHandle>,
    ) -> PickerOutput {
        PickerOutput {
            outputs: vec![CompactionOutput {
                output_level: 1,
                inputs: output_files,
                filter_deleted: false,
                output_time_range: None,
            }],
            expired_ssts,
            ..Default::default()
        }
    }

    #[async_trait::async_trait]
    impl Scheduler for FailingScheduler {
        fn schedule(&self, _job: Job) -> Result<()> {
            InvalidSchedulerStateSnafu.fail()
        }

        async fn stop(&self, _await_termination: bool) -> Result<()> {
            Ok(())
        }
    }

    async fn recv_compaction_pick_finished(
        rx: &mut mpsc::Receiver<WorkerRequestWithTime>,
    ) -> CompactionPickFinished {
        let request = rx.recv().await.expect("worker request channel closed");
        match request.request {
            WorkerRequest::Background {
                notify: BackgroundNotify::CompactionPickFinished(finished),
                ..
            } => finished,
            other => panic!("unexpected worker request: {other:?}"),
        }
    }

    fn assert_compaction_lifecycle_error(err: Error, lifecycle: &str) {
        let Error::CompactRegion { source, .. } = err else {
            panic!("expected compact-region error, got {err:?}");
        };
        let matches_lifecycle = matches!(
            (lifecycle, source.as_ref()),
            ("close", Error::RegionClosed { .. })
                | ("drop", Error::RegionDropped { .. })
                | ("truncate", Error::RegionTruncated { .. })
        );
        assert!(
            matches_lifecycle,
            "unexpected {lifecycle} error source: {source:?}"
        );
    }

    #[tokio::test]
    async fn test_picking_phase_tracks_plan_and_cancellation() {
        let env = SchedulerEnv::new().await;
        let builder = VersionControlBuilder::new();
        let mut status = CompactionStatus::new(
            builder.region_id(),
            Arc::new(builder.build()),
            env.access_layer.clone(),
        );

        status.start_picking(7);

        assert!(status.is_picking(7));
        assert!(!status.is_picking(8));
        assert!(status.is_busy());
        assert!(status.accept_plan(7));
        assert!(!status.accept_plan(8));
        assert_eq!(status.request_cancel(), RequestCancelResult::CancelIssued);
        assert_eq!(
            status.request_cancel(),
            RequestCancelResult::AlreadyCancelling
        );
        assert!(status.is_picking(7));
        assert!(status.is_busy());
        assert!(!status.accept_plan(7));
    }

    #[tokio::test]
    async fn test_picking_coalesces_regular_waiter_and_queues_manual_request() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
        status.start_picking(1);
        scheduler.region_status.insert(region_id, status);

        let (regular_tx, _regular_rx) = oneshot::channel();
        assert!(
            !scheduler
                .schedule_compaction(
                    region_id,
                    compact_request::Options::Regular(Default::default()),
                    &version_control,
                    &env.access_layer,
                    OptionOutputTx::from(regular_tx),
                    &manifest_ctx,
                    schema_metadata_manager.clone(),
                    1,
                )
                .await
                .unwrap()
        );
        let (manual_tx, _manual_rx) = oneshot::channel();
        assert!(
            !scheduler
                .schedule_compaction(
                    region_id,
                    compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
                    &version_control,
                    &env.access_layer,
                    OptionOutputTx::from(manual_tx),
                    &manifest_ctx,
                    schema_metadata_manager,
                    1,
                )
                .await
                .unwrap()
        );

        let status = scheduler.region_status.get(&region_id).unwrap();
        assert!(status.is_busy());
        assert!(status.waiters.is_empty());
        assert!(status.regular_replan_pending);
        assert_eq!(status.pending_regular_waiters.len(), 1);
        assert!(status.pending_request.is_some());
    }

    #[tokio::test]
    async fn test_picking_plan_ids_are_monotonic() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);

        let first = scheduler.next_plan_id();
        let second = scheduler.next_plan_id();

        assert_eq!(second, first + 1);
    }

    #[test]
    fn test_picking_compacting_files_deduplicates_handles() {
        let file = new_file_handle(FileId::random(), 0, 10, 0);
        let output = picker_output_with_files(vec![file.clone(), file.clone()], vec![file.clone()]);

        let files = CompactingFiles::try_new(&output).unwrap();

        assert!(file.compacting());
        drop(files);
        assert!(!file.compacting());
    }

    #[test]
    fn test_picking_compacting_files_rolls_back_on_conflict() {
        let first = new_file_handle(FileId::random(), 0, 10, 0);
        let conflicting = new_file_handle(FileId::random(), 0, 10, 0);
        conflicting.set_compacting(true);
        let output = picker_output_with_files(vec![first.clone(), conflicting.clone()], vec![]);

        assert!(CompactingFiles::try_new(&output).is_none());
        assert!(!first.compacting());
        assert!(conflicting.compacting());
    }

    #[test]
    fn test_picking_compacting_files_drop_clears_all_reservations() {
        let output_file = new_file_handle(FileId::random(), 0, 10, 0);
        let expired_file = new_file_handle(FileId::random(), 0, 10, 0);
        let output =
            picker_output_with_files(vec![output_file.clone()], vec![expired_file.clone()]);

        let files = CompactingFiles::try_new(&output).unwrap();
        assert!(output_file.compacting());
        assert!(expired_file.compacting());

        drop(files);
        assert!(!output_file.compacting());
        assert!(!expired_file.compacting());
    }

    #[test]
    fn test_pick_result_refreshes_handles_and_preserves_output_options() {
        let purger = crate::test_util::new_noop_file_purger();
        let stale = new_file_handle(FileId::random(), 0, 10, 0);
        let expired = new_file_handle(FileId::random(), 20, 30, 0);
        let mut current_meta = stale.meta_ref().clone();
        current_meta.index_version = 1;
        current_meta.index_file_size = 128;
        let mut current = crate::sst::version::SstVersion::new();
        current.add_files(
            purger.clone(),
            [current_meta.clone(), expired.meta_ref().clone()].into_iter(),
        );
        let output_time_range = TimestampRange::new(
            common_time::Timestamp::new_millisecond(0),
            common_time::Timestamp::new_millisecond(10),
        );
        let output = PickerOutput {
            outputs: vec![CompactionOutput {
                output_level: 2,
                inputs: vec![stale.clone()],
                filter_deleted: true,
                output_time_range,
            }],
            expired_ssts: vec![expired],
            time_window_size: 3600,
            max_file_size: Some(4096),
        };

        let refreshed = refresh_picker_output(output, &current).unwrap();

        assert_eq!(refreshed.outputs.len(), 1);
        assert_eq!(refreshed.outputs[0].output_level, 2);
        assert!(refreshed.outputs[0].filter_deleted);
        assert_eq!(refreshed.outputs[0].output_time_range, output_time_range);
        assert_eq!(refreshed.time_window_size, 3600);
        assert_eq!(refreshed.max_file_size, Some(4096));
        assert_eq!(refreshed.outputs[0].inputs[0].meta_ref(), &current_meta);
        assert_eq!(refreshed.expired_ssts.len(), 1);
        stale.set_compacting(true);
        assert!(!refreshed.outputs[0].inputs[0].compacting());
    }

    #[test]
    fn test_pick_result_rejects_ambiguous_file_id_across_levels() {
        let purger = crate::test_util::new_noop_file_purger();
        let selected = new_file_handle(FileId::random(), 0, 10, 1);
        let mut wrong_level = selected.meta_ref().clone();
        wrong_level.level = 0;
        let current_level = selected.meta_ref().clone();
        let mut current = crate::sst::version::SstVersion::new();
        current.add_files(purger.clone(), std::iter::once(wrong_level));
        let output = picker_output_with_files(vec![selected], Vec::new());

        assert!(refresh_picker_output(output.clone(), &current).is_none());
        current.add_files(purger, std::iter::once(current_level));
        assert!(refresh_picker_output(output, &current).is_none());
    }

    #[tokio::test]
    async fn test_find_compaction_options_db_level() {
        let builder = VersionControlBuilder::new();
        let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
        let region_id = builder.region_id();
        let table_id = region_id.table_id();
        // Register table without ttl but with db-level compaction options
        let mut schema_value = SchemaNameValue {
            ttl: Some(DatabaseTimeToLive::default()),
            ..Default::default()
        };
        schema_value
            .extra_options
            .insert("compaction.type".to_string(), "twcs".to_string());
        schema_value
            .extra_options
            .insert("compaction.twcs.time_window".to_string(), "2h".to_string());
        schema_metadata_manager
            .register_region_table_info(
                table_id,
                "t",
                "c",
                "s",
                Some(schema_value),
                kv_backend.clone(),
            )
            .await;

        let version_control = Arc::new(builder.build());
        let region_opts = version_control.current().version.options.clone();
        let (opts, _) = find_dynamic_options(region_id, &region_opts, &schema_metadata_manager)
            .await
            .unwrap();
        match opts {
            crate::region::options::CompactionOptions::Twcs(t) => {
                assert_eq!(t.time_window_seconds(), Some(2 * 3600));
            }
        }
    }

    #[tokio::test]
    async fn test_find_compaction_options_priority() {
        fn schema_value_with_twcs(time_window: &str) -> SchemaNameValue {
            let mut schema_value = SchemaNameValue {
                ttl: Some(DatabaseTimeToLive::default()),
                ..Default::default()
            };
            schema_value
                .extra_options
                .insert("compaction.type".to_string(), "twcs".to_string());
            schema_value.extra_options.insert(
                "compaction.twcs.time_window".to_string(),
                time_window.to_string(),
            );
            schema_value
        }

        let cases = [
            (
                "db options set and table override set",
                Some(schema_value_with_twcs("2h")),
                true,
                Some(Duration::from_secs(5 * 3600)),
                Some(5 * 3600),
            ),
            (
                "db options set and table override not set",
                Some(schema_value_with_twcs("2h")),
                false,
                None,
                Some(2 * 3600),
            ),
            (
                "db options not set and table override set",
                None,
                true,
                Some(Duration::from_secs(4 * 3600)),
                Some(4 * 3600),
            ),
            (
                "db options not set and table override not set",
                None,
                false,
                None,
                None,
            ),
        ];

        for (case_name, schema_value, override_set, table_window, expected_window) in cases {
            let builder = VersionControlBuilder::new();
            let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
            let region_id = builder.region_id();
            let table_id = region_id.table_id();
            schema_metadata_manager
                .register_region_table_info(
                    table_id,
                    "t",
                    "c",
                    "s",
                    schema_value,
                    kv_backend.clone(),
                )
                .await;

            let version_control = Arc::new(builder.build());
            let mut region_opts = version_control.current().version.options.clone();
            region_opts.compaction_override = override_set;
            if let Some(window) = table_window {
                let crate::region::options::CompactionOptions::Twcs(twcs) =
                    &mut region_opts.compaction;
                twcs.time_window = Some(window);
            }

            let (opts, _) = find_dynamic_options(region_id, &region_opts, &schema_metadata_manager)
                .await
                .unwrap();
            match opts {
                crate::region::options::CompactionOptions::Twcs(t) => {
                    assert_eq!(t.time_window_seconds(), expected_window, "{case_name}");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_schedule_empty() {
        let env = SchedulerEnv::new().await;
        let (tx, mut rx) = mpsc::channel(4);
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
        let scheduled = scheduler
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
        assert!(scheduled);
        let finished = recv_compaction_pick_finished(&mut rx).await;
        assert!(matches!(&finished.result, CompactionPlanningResult::NoPlan));
        scheduler
            .handle_compaction_pick_finished(
                finished,
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await;
        let output = output_rx.await.unwrap().unwrap();
        assert_eq!(output, 0);
        assert!(scheduler.region_status.is_empty());

        // Only one file, picker won't compact it.
        let version_control = Arc::new(builder.push_l0_file(0, 1000).build());
        let (output_tx, output_rx) = oneshot::channel();
        let waiter = OptionOutputTx::from(output_tx);
        let scheduled = scheduler
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
        assert!(scheduled);
        let finished = recv_compaction_pick_finished(&mut rx).await;
        assert!(matches!(&finished.result, CompactionPlanningResult::NoPlan));
        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;
        let output = output_rx.await.unwrap().unwrap();
        assert_eq!(output, 0);
        assert!(scheduler.region_status.is_empty());
    }

    #[tokio::test]
    async fn test_schedule_compaction_returns_true_when_task_scheduled() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let end = 1000 * 1000;
        // Five overlapping L0 files are enough for the regular picker to create a task.
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
        let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
        schema_metadata_manager
            .register_region_table_info(
                region_id.table_id(),
                "test_table",
                "test_catalog",
                "test_schema",
                None,
                kv_backend,
            )
            .await;

        let scheduled = scheduler
            .schedule_compaction(
                region_id,
                Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                OptionOutputTx::none(),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
            )
            .await
            .unwrap();

        // The boolean result is what the worker uses to decide whether to update
        // last_schedule_compaction_millis.
        assert!(scheduled);
        assert_eq!(0, job_scheduler.num_jobs());
        let finished = recv_compaction_pick_finished(&mut rx).await;
        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;
        assert_eq!(1, job_scheduler.num_jobs());
        assert!(scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_picking_coalesces_duplicate_same_region_triggers() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
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
        let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
        schema_metadata_manager
            .register_region_table_info(
                region_id.table_id(),
                "test_table",
                "test_catalog",
                "test_schema",
                None,
                kv_backend,
            )
            .await;
        let gate = Arc::new(CompactionPlanningGate::new(region_id));
        let gate_guard = gate.arm();
        scheduler.listener = WorkerListener::new(Some(gate.clone()));
        let (first_tx, _first_rx) = oneshot::channel();

        let mut schedule = tokio::spawn({
            let version_control = version_control.clone();
            let access_layer = env.access_layer.clone();
            let manifest_ctx = manifest_ctx.clone();
            let schema_metadata_manager = schema_metadata_manager.clone();
            async move {
                let scheduled = scheduler
                    .schedule_compaction(
                        region_id,
                        Options::Regular(Default::default()),
                        &version_control,
                        &access_layer,
                        OptionOutputTx::from(first_tx),
                        &manifest_ctx,
                        schema_metadata_manager,
                        1,
                    )
                    .await
                    .unwrap();
                (scheduled, scheduler)
            }
        });

        tokio::time::timeout(Duration::from_secs(5), gate.wait_until_entered())
            .await
            .expect("planning did not reach the picker gate");
        let (scheduled, mut scheduler) =
            match tokio::time::timeout(Duration::from_secs(5), &mut schedule).await {
                Ok(result) => result.unwrap(),
                Err(_) => {
                    panic!("schedule_compaction awaited picker planning")
                }
            };

        assert!(scheduled);
        let status = scheduler.region_status.get(&region_id).unwrap();
        assert!(status.is_busy());
        assert_eq!(status.waiters.len(), 1);
        assert_eq!(0, job_scheduler.num_jobs());

        let (second_tx, _second_rx) = oneshot::channel();
        assert!(
            !scheduler
                .schedule_compaction(
                    region_id,
                    Options::Regular(Default::default()),
                    &version_control,
                    &env.access_layer,
                    OptionOutputTx::from(second_tx),
                    &manifest_ctx,
                    schema_metadata_manager.clone(),
                    1,
                )
                .await
                .unwrap()
        );
        let (manual_tx, _manual_rx) = oneshot::channel();
        assert!(
            !scheduler
                .schedule_compaction(
                    region_id,
                    Options::StrictWindow(StrictWindow { window_seconds: 60 }),
                    &version_control,
                    &env.access_layer,
                    OptionOutputTx::from(manual_tx),
                    &manifest_ctx,
                    schema_metadata_manager,
                    1,
                )
                .await
                .unwrap()
        );
        let status = scheduler.region_status.get(&region_id).unwrap();
        assert_eq!(status.waiters.len(), 1);
        assert!(status.regular_replan_pending);
        assert_eq!(status.pending_regular_waiters.len(), 1);
        assert!(status.pending_request.is_some());
        assert_eq!(1, gate.invocation_count());

        gate_guard.release();
        let finished = tokio::time::timeout(
            Duration::from_secs(5),
            recv_compaction_pick_finished(&mut rx),
        )
        .await
        .expect("planning did not send a terminal notification");
        assert_eq!(finished.region_id, region_id);
        assert!(matches!(
            finished.result,
            CompactionPlanningResult::Prepared(_)
        ));
    }

    #[tokio::test]
    async fn test_planning_no_plan_completion_clears_picking_and_notifies_waiter() {
        let env = SchedulerEnv::new().await;
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        let (waiter_tx, waiter_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
        status.merge_waiter(OptionOutputTx::from(waiter_tx));
        status.start_picking(7);
        scheduler.region_status.insert(region_id, status);

        let pending_ddls = scheduler
            .handle_compaction_pick_finished(
                CompactionPickFinished {
                    region_id,
                    plan_id: 7,
                    version_control,
                    result: CompactionPlanningResult::NoPlan,
                },
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await;

        assert!(pending_ddls.is_empty());
        assert_eq!(0, waiter_rx.await.unwrap().unwrap());
        assert!(!scheduler.region_status.contains_key(&region_id));
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_planning_error_completion_clears_picking_and_notifies_waiter_once() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        let (waiter_tx, waiter_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
        status.merge_waiter(OptionOutputTx::from(waiter_tx));
        status.start_picking(9);
        scheduler.region_status.insert(region_id, status);

        let pending_ddls = scheduler
            .handle_compaction_pick_finished(
                CompactionPickFinished {
                    region_id,
                    plan_id: 9,
                    version_control,
                    result: CompactionPlanningResult::Error(Arc::new(
                        InvalidSchedulerStateSnafu.build(),
                    )),
                },
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await;

        assert!(pending_ddls.is_empty());
        assert!(waiter_rx.await.unwrap().is_err());
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_planning_panic_still_sends_pick_finished() {
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let (tx, mut rx) = mpsc::channel(4);

        CompactionScheduler::notify_planning_result(region_id, 7, version_control, tx, async {
            panic!("planning boom")
        })
        .await;

        let finished = recv_compaction_pick_finished(&mut rx).await;
        assert_eq!(region_id, finished.region_id);
        assert_eq!(7, finished.plan_id);
        let CompactionPlanningResult::Error(err) = finished.result else {
            panic!("expected planning error, got {:?}", finished.result);
        };
        assert!(
            err.to_string().contains("planning boom"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_picking_lifecycle_close_drop_truncate_fail_waiters_and_ignore_completion() {
        for lifecycle in ["close", "drop", "truncate"] {
            let env = SchedulerEnv::new().await;
            let (tx, _rx) = mpsc::channel(4);
            let mut scheduler = env.mock_compaction_scheduler(tx);
            let builder = VersionControlBuilder::new();
            let region_id = builder.region_id();
            let version_control = Arc::new(builder.build());
            let manifest_ctx = env
                .mock_manifest_context(version_control.current().version.metadata.clone())
                .await;
            let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
            let (first_tx, first_rx) = oneshot::channel();
            let (second_tx, second_rx) = oneshot::channel();
            let (manual_tx, manual_rx) = oneshot::channel();
            let mut status =
                CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
            status.merge_waiter(OptionOutputTx::from(first_tx));
            status.set_pending_request(PendingCompaction {
                options: compact_request::Options::StrictWindow(StrictWindow {
                    window_seconds: 60,
                }),
                waiter: OptionOutputTx::from(manual_tx),
                max_parallelism: 1,
            });
            status.start_picking(7);
            status.merge_regular_trigger(OptionOutputTx::from(second_tx));
            scheduler.region_status.insert(region_id, status);

            match lifecycle {
                "close" => scheduler.on_region_closed(region_id),
                "drop" => scheduler.on_region_dropped(region_id),
                "truncate" => scheduler.on_region_truncated(region_id),
                _ => unreachable!(),
            }

            assert!(!scheduler.region_status.contains_key(&region_id));
            for result in [
                first_rx.await.unwrap(),
                second_rx.await.unwrap(),
                manual_rx.await.unwrap(),
            ] {
                assert_compaction_lifecycle_error(result.unwrap_err(), lifecycle);
            }
            let pending_ddls = scheduler
                .accept_compaction_pick_finished(
                    CompactionPickFinished {
                        region_id,
                        plan_id: 7,
                        version_control: version_control.clone(),
                        result: CompactionPlanningResult::NoPlan,
                    },
                    &version_control,
                    &manifest_ctx,
                    schema_metadata_manager,
                )
                .await;
            assert!(pending_ddls.is_empty());
            assert!(!scheduler.region_status.contains_key(&region_id));
        }
    }

    #[tokio::test]
    async fn test_picking_lifecycle_replacement_ignores_old_completion() {
        let env = SchedulerEnv::new().await;
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let stale_version_control = compactable_version();
        let region_id = stale_version_control.current().version.metadata.region_id;
        let (finished, _stale_manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &stale_version_control).await;
        let selected = selected_files(&finished);
        let replacement_version_control = compactable_version();
        let manifest_ctx = env
            .mock_manifest_context(
                replacement_version_control
                    .current()
                    .version
                    .metadata
                    .clone(),
            )
            .await;
        let (stale_tx, stale_rx) = oneshot::channel();
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .merge_waiter(OptionOutputTx::from(stale_tx));
        scheduler.on_region_closed(region_id);
        assert!(stale_rx.await.unwrap().is_err());

        let (replacement_tx, mut replacement_rx) = oneshot::channel();
        let replacement_plan_id = finished.plan_id + 1;
        let mut replacement_status = CompactionStatus::new(
            region_id,
            replacement_version_control.clone(),
            env.access_layer.clone(),
        );
        replacement_status.merge_waiter(OptionOutputTx::from(replacement_tx));
        replacement_status.start_picking(replacement_plan_id);
        scheduler
            .region_status
            .insert(region_id, replacement_status);

        let pending_ddls = scheduler
            .accept_compaction_pick_finished(
                finished,
                &replacement_version_control,
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await;

        assert!(pending_ddls.is_empty());
        let status = &scheduler.region_status[&region_id];
        assert!(status.is_picking(replacement_plan_id));
        assert_eq!(status.waiters.len(), 1);
        assert!(selected.iter().all(|file| !file.compacting()));
        assert_matches!(
            replacement_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
    }

    #[tokio::test]
    async fn test_picking_lifecycle_staging_waits_for_cancellation_ack() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        let (waiter_tx, waiter_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
        status.merge_waiter(OptionOutputTx::from(waiter_tx));
        status.start_picking(7);
        scheduler.region_status.insert(region_id, status);
        let (ddl_tx, mut ddl_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(ddl_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        assert_eq!(
            scheduler.request_cancel(region_id),
            RequestCancelResult::CancelIssued
        );
        assert!(scheduler.region_status[&region_id].is_picking(7));
        assert!(scheduler.has_pending_ddls(region_id));
        assert_matches!(ddl_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));

        let pending_ddls = scheduler
            .accept_compaction_pick_finished(
                CompactionPickFinished {
                    region_id,
                    plan_id: 7,
                    version_control: version_control.clone(),
                    result: CompactionPlanningResult::NoPlan,
                },
                &version_control,
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await;

        assert_eq!(pending_ddls.len(), 1);
        assert!(!scheduler.region_status.contains_key(&region_id));
        assert!(waiter_rx.await.unwrap().is_err());
        assert_matches!(ddl_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_picking_lifecycle_manual_noop_precedes_pending_ddl() {
        let env = SchedulerEnv::new().await;
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        let (regular_tx, regular_rx) = oneshot::channel();
        let (followup_tx, mut followup_rx) = oneshot::channel();
        let (manual_tx, mut manual_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
        status.merge_waiter(OptionOutputTx::from(regular_tx));
        status.start_picking(7);
        scheduler.region_status.insert(region_id, status);
        assert!(
            !scheduler
                .schedule_compaction(
                    region_id,
                    compact_request::Options::Regular(Default::default()),
                    &version_control,
                    &env.access_layer,
                    OptionOutputTx::from(followup_tx),
                    &manifest_ctx,
                    schema_metadata_manager.clone(),
                    1,
                )
                .await
                .unwrap()
        );
        assert!(
            !scheduler
                .schedule_compaction(
                    region_id,
                    compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
                    &version_control,
                    &env.access_layer,
                    OptionOutputTx::from(manual_tx),
                    &manifest_ctx,
                    schema_metadata_manager.clone(),
                    1,
                )
                .await
                .unwrap()
        );
        let (ddl_tx, mut ddl_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(ddl_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        let pending_ddls = scheduler
            .accept_compaction_pick_finished(
                CompactionPickFinished {
                    region_id,
                    plan_id: 7,
                    version_control: version_control.clone(),
                    result: CompactionPlanningResult::NoPlan,
                },
                &version_control,
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await;

        assert!(pending_ddls.is_empty());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), regular_rx)
                .await
                .expect("current regular waiter was not notified before manual planning")
                .unwrap()
                .unwrap(),
            0
        );
        assert!(scheduler.has_pending_ddls(region_id));
        assert_matches!(
            followup_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
        assert_matches!(
            manual_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
        assert_matches!(ddl_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));

        let mut manual_finished = recv_compaction_pick_finished(&mut rx).await;
        manual_finished.result = CompactionPlanningResult::NoPlan;
        let pending_ddls = scheduler
            .accept_compaction_pick_finished(
                manual_finished,
                &version_control,
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await;
        assert!(pending_ddls.is_empty());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), manual_rx)
                .await
                .expect("manual waiter was not notified before regular follow-up planning")
                .unwrap()
                .unwrap(),
            0
        );
        assert_matches!(
            followup_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
        assert_matches!(ddl_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));

        let mut regular_finished = tokio::time::timeout(
            Duration::from_secs(5),
            recv_compaction_pick_finished(&mut rx),
        )
        .await
        .expect("regular follow-up was not planned after manual completion");
        regular_finished.result = CompactionPlanningResult::NoPlan;
        let pending_ddls = scheduler
            .accept_compaction_pick_finished(
                regular_finished,
                &version_control,
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await;
        assert_eq!(pending_ddls.len(), 1);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), followup_rx)
                .await
                .expect("regular follow-up waiter was not notified")
                .unwrap()
                .unwrap(),
            0
        );
        assert_matches!(ddl_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_ddl_fence_prevents_repeated_regular_followups() {
        let env = SchedulerEnv::new().await;
        let (tx, mut rx) = mpsc::channel(8);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        let (current_tx, current_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
        status.merge_waiter(OptionOutputTx::from(current_tx));
        status.start_picking(7);
        scheduler.region_status.insert(region_id, status);

        let (prefence_tx, prefence_rx) = oneshot::channel();
        assert!(
            !scheduler
                .schedule_compaction(
                    region_id,
                    compact_request::Options::Regular(Default::default()),
                    &version_control,
                    &env.access_layer,
                    OptionOutputTx::from(prefence_tx),
                    &manifest_ctx,
                    schema_metadata_manager.clone(),
                    1,
                )
                .await
                .unwrap()
        );
        let (ddl_tx, mut ddl_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(ddl_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        let pending_ddls = scheduler
            .accept_compaction_pick_finished(
                CompactionPickFinished {
                    region_id,
                    plan_id: 7,
                    version_control: version_control.clone(),
                    result: CompactionPlanningResult::NoPlan,
                },
                &version_control,
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await;
        assert!(pending_ddls.is_empty());
        assert_eq!(current_rx.await.unwrap().unwrap(), 0);
        assert_matches!(ddl_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));

        let mut followup_finished = tokio::time::timeout(
            Duration::from_secs(5),
            recv_compaction_pick_finished(&mut rx),
        )
        .await
        .expect("pre-fence regular follow-up was not planned");
        let mut postfence_waiters = Vec::new();
        for _ in 0..3 {
            let (waiter_tx, waiter_rx) = oneshot::channel();
            assert!(
                !scheduler
                    .schedule_compaction(
                        region_id,
                        compact_request::Options::Regular(Default::default()),
                        &version_control,
                        &env.access_layer,
                        OptionOutputTx::from(waiter_tx),
                        &manifest_ctx,
                        schema_metadata_manager.clone(),
                        1,
                    )
                    .await
                    .unwrap()
            );
            postfence_waiters.push(waiter_rx);
        }
        assert!(
            !scheduler
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
                .unwrap()
        );

        followup_finished.result = CompactionPlanningResult::NoPlan;
        let pending_ddls = scheduler
            .accept_compaction_pick_finished(
                followup_finished,
                &version_control,
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await;
        assert_eq!(pending_ddls.len(), 1);
        assert_eq!(prefence_rx.await.unwrap().unwrap(), 0);
        for waiter in postfence_waiters {
            assert_eq!(waiter.await.unwrap().unwrap(), 0);
        }
        assert!(!scheduler.region_status.contains_key(&region_id));
        assert!(rx.try_recv().is_err());
        assert_matches!(ddl_rx.try_recv(), Err(oneshot::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn test_picking_lifecycle_scheduler_drop_notifies_pending_requests() {
        let env = SchedulerEnv::new().await;
        let (waiter_rx, manual_rx, ddl_rx) = {
            let (tx, _rx) = mpsc::channel(4);
            let mut scheduler = env.mock_compaction_scheduler(tx);
            let builder = VersionControlBuilder::new();
            let region_id = builder.region_id();
            let version_control = Arc::new(builder.build());
            let (waiter_tx, waiter_rx) = oneshot::channel();
            let (manual_tx, manual_rx) = oneshot::channel();
            let (ddl_tx, ddl_rx) = oneshot::channel();
            let mut status =
                CompactionStatus::new(region_id, version_control, env.access_layer.clone());
            status.merge_waiter(OptionOutputTx::from(waiter_tx));
            status.set_pending_request(PendingCompaction {
                options: compact_request::Options::StrictWindow(StrictWindow {
                    window_seconds: 60,
                }),
                waiter: OptionOutputTx::from(manual_tx),
                max_parallelism: 1,
            });
            status.pending_ddl_requests.push(SenderDdlRequest {
                region_id,
                sender: OptionOutputTx::from(ddl_tx),
                request: crate::request::DdlRequest::EnterStaging(
                    store_api::region_request::EnterStagingRequest {
                        partition_directive:
                            store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                    },
                ),
            });
            status.start_picking(7);
            scheduler.region_status.insert(region_id, status);
            (waiter_rx, manual_rx, ddl_rx)
        };

        for result in [
            waiter_rx.await.unwrap(),
            manual_rx.await.unwrap(),
            ddl_rx.await.unwrap(),
        ] {
            assert_compaction_lifecycle_error(result.unwrap_err(), "close");
        }
    }

    #[tokio::test]
    async fn test_pick_result_matching_plan_submits_once_and_owns_reservations() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version_control = compactable_version();
        let region_id = version_control.current().version.metadata.region_id;
        let (finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        let selected = selected_files(&finished);

        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;

        assert_eq!(job_scheduler.num_jobs(), 1);
        assert!(selected.iter().all(FileHandle::compacting));
        scheduler.on_compaction_failed(region_id, Arc::new(InvalidSchedulerStateSnafu.build()));
        assert!(selected.iter().all(FileHandle::compacting));
        drop(scheduler);
        drop(env);
        drop(job_scheduler);
        assert!(selected.iter().all(|file| !file.compacting()));
    }

    #[tokio::test]
    async fn test_pick_result_mismatched_token_keeps_status_and_waiter_untouched() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version_control = compactable_version();
        let region_id = version_control.current().version.metadata.region_id;
        let (mut finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        let (waiter_tx, mut waiter_rx) = oneshot::channel();
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .merge_waiter(OptionOutputTx::from(waiter_tx));
        finished.plan_id += 1;

        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;

        assert_eq!(job_scheduler.num_jobs(), 0);
        assert!(scheduler.region_status[&region_id].is_busy());
        assert_eq!(scheduler.region_status[&region_id].waiters.len(), 1);
        assert_matches!(
            waiter_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
    }

    #[tokio::test]
    async fn test_pick_result_different_version_control_keeps_status_untouched() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version_control = compactable_version();
        let region_id = version_control.current().version.metadata.region_id;
        let (mut finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        let (waiter_tx, mut waiter_rx) = oneshot::channel();
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .merge_waiter(OptionOutputTx::from(waiter_tx));
        finished.version_control = compactable_version();

        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;

        assert_eq!(job_scheduler.num_jobs(), 0);
        assert!(scheduler.region_status[&region_id].is_busy());
        assert_eq!(scheduler.region_status[&region_id].waiters.len(), 1);
        assert_matches!(
            waiter_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
    }

    #[tokio::test]
    async fn test_pick_result_rejects_replaced_current_region_instance() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let stale_version_control = compactable_version();
        let region_id = stale_version_control.current().version.metadata.region_id;
        let (finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &stale_version_control).await;
        let replacement_version_control = compactable_version();
        assert_eq!(
            replacement_version_control
                .current()
                .version
                .metadata
                .region_id,
            region_id
        );
        assert!(!Arc::ptr_eq(
            &stale_version_control,
            &replacement_version_control
        ));
        assert!(Arc::ptr_eq(
            &scheduler.region_status[&region_id].version_control,
            &finished.version_control
        ));
        let replacement_files: Vec<_> = replacement_version_control
            .current()
            .version
            .ssts
            .levels()
            .iter()
            .flat_map(LevelMeta::files)
            .cloned()
            .collect();
        let (waiter_tx, mut waiter_rx) = oneshot::channel();
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .merge_waiter(OptionOutputTx::from(waiter_tx));

        scheduler
            .accept_compaction_pick_finished(
                finished,
                &replacement_version_control,
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await;

        assert_eq!(job_scheduler.num_jobs(), 0);
        assert!(scheduler.region_status[&region_id].is_busy());
        assert_eq!(scheduler.region_status[&region_id].waiters.len(), 1);
        assert_matches!(
            waiter_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
        assert!(replacement_files.iter().all(|file| !file.compacting()));
    }

    #[tokio::test]
    async fn test_pick_result_accepts_unrelated_concurrent_flush() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version_control = compactable_version();
        let (finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        let selected = selected_files(&finished);
        apply_edit(
            &version_control,
            &[(2_000_000, 3_000_000)],
            &[],
            selected[0].file_purger(),
        );

        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;

        assert_eq!(job_scheduler.num_jobs(), 1);
        assert!(selected.iter().all(FileHandle::compacting));
    }

    #[tokio::test]
    async fn test_pick_result_rejects_removed_selected_file_cleanly() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version_control = compactable_version();
        let (finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        let selected = selected_files(&finished);
        let (waiter_tx, waiter_rx) = oneshot::channel();
        let region_id = finished.region_id;
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .merge_waiter(OptionOutputTx::from(waiter_tx));
        apply_edit(
            &version_control,
            &[],
            &[selected[0].meta_ref().clone()],
            selected[0].file_purger(),
        );

        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;

        assert_eq!(job_scheduler.num_jobs(), 0);
        assert_eq!(waiter_rx.await.unwrap().unwrap(), 0);
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_pick_result_refreshes_replaced_selected_file() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version_control = compactable_version();
        let (finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        let selected = selected_files(&finished);
        let stale = selected[0].clone();
        let mut replacement = stale.meta_ref().clone();
        replacement.index_version = 1;
        replacement.index_file_size = 128;
        version_control.apply_edit(
            Some(crate::manifest::action::RegionEdit {
                files_to_add: vec![replacement],
                files_to_remove: Vec::new(),
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            }),
            &[],
            stale.file_purger(),
        );
        let current = version_control
            .current()
            .version
            .ssts
            .file_for_compaction(&stale)
            .unwrap()
            .clone();

        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;

        assert_eq!(job_scheduler.num_jobs(), 1);
        assert!(!stale.compacting());
        assert!(current.compacting());
        assert_eq!(current.meta_ref().index_version, 1);
    }

    #[tokio::test]
    async fn test_pick_result_rejects_deleted_or_compacting_current_file() {
        for deleted in [true, false] {
            let job_scheduler = Arc::new(VecScheduler::default());
            let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
            let (tx, mut rx) = mpsc::channel(4);
            let mut scheduler = env.mock_compaction_scheduler(tx);
            let version_control = compactable_version();
            let (finished, manifest_ctx, schema_metadata_manager) =
                begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
            let selected = selected_files(&finished);
            if deleted {
                selected[0].mark_deleted();
            } else {
                selected[0].set_compacting(true);
            }

            scheduler
                .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
                .await;

            assert_eq!(job_scheduler.num_jobs(), 0);
            assert!(scheduler.region_status.is_empty());
            if !deleted {
                assert!(selected[0].compacting());
                selected[0].set_compacting(false);
            }
        }
    }

    #[tokio::test]
    async fn test_pick_result_local_submission_failure_releases_and_notifies_once() {
        let env = SchedulerEnv::new()
            .await
            .scheduler(Arc::new(FailingScheduler));
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version_control = compactable_version();
        let region_id = version_control.current().version.metadata.region_id;
        let (finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        let selected = selected_files(&finished);
        let (waiter_tx, waiter_rx) = oneshot::channel();
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .merge_waiter(OptionOutputTx::from(waiter_tx));

        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;

        assert!(waiter_rx.await.unwrap().is_err());
        assert!(selected.iter().all(|file| !file.compacting()));
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_pick_result_rejects_removed_expired_file_cleanly() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let version_control = compactable_version();
        let (mut finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        let CompactionPlanningResult::Prepared(prepared) = &mut finished.result else {
            unreachable!();
        };
        let expired = prepared.picker_output.outputs[0].inputs.pop().unwrap();
        prepared.picker_output.expired_ssts.push(expired.clone());
        apply_edit(
            &version_control,
            &[],
            &[expired.meta_ref().clone()],
            expired.file_purger(),
        );

        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;

        assert_eq!(job_scheduler.num_jobs(), 0);
        assert!(scheduler.region_status.is_empty());
    }

    #[tokio::test]
    async fn test_pick_result_local_completion_and_cancel_release_reservations() {
        for cancel in [false, true] {
            let job_scheduler = Arc::new(VecScheduler::default());
            let env = SchedulerEnv::new().await.scheduler(job_scheduler);
            let (tx, mut rx) = mpsc::channel(4);
            let mut scheduler = env.mock_compaction_scheduler(tx);
            let version_control = compactable_version();
            let region_id = version_control.current().version.metadata.region_id;
            let (finished, manifest_ctx, schema_metadata_manager) =
                begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
            let selected = selected_files(&finished);
            scheduler
                .handle_compaction_pick_finished(
                    finished,
                    &manifest_ctx,
                    schema_metadata_manager.clone(),
                )
                .await;
            assert!(selected.iter().all(FileHandle::compacting));

            if cancel {
                assert_eq!(
                    scheduler.request_cancel(region_id),
                    RequestCancelResult::CancelIssued
                );
                scheduler.on_compaction_cancelled(region_id).await;
            } else {
                scheduler
                    .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager)
                    .await;
            }

            assert!(selected.iter().all(FileHandle::compacting));
            drop(scheduler);
            drop(env);
            assert!(selected.iter().all(|file| !file.compacting()));
        }
    }

    #[tokio::test]
    async fn test_pick_result_remote_completion_and_failure_release_reservations() {
        for failed in [false, true] {
            let env = SchedulerEnv::new().await;
            let (tx, mut rx) = mpsc::channel(4);
            let mut scheduler = env.mock_compaction_scheduler(tx);
            scheduler
                .plugins
                .insert::<RemoteJobSchedulerRef>(Arc::new(SuccessfulRemoteScheduler));
            let version_control = compactable_version();
            let region_id = version_control.current().version.metadata.region_id;
            let (mut finished, manifest_ctx, schema_metadata_manager) =
                begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
            use_remote_compaction(&mut finished);
            let selected = selected_files(&finished);
            scheduler
                .handle_compaction_pick_finished(
                    finished,
                    &manifest_ctx,
                    schema_metadata_manager.clone(),
                )
                .await;
            assert!(selected.iter().all(FileHandle::compacting));

            if failed {
                scheduler
                    .on_compaction_failed(region_id, Arc::new(InvalidSchedulerStateSnafu.build()));
            } else {
                scheduler
                    .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager)
                    .await;
            }

            assert!(selected.iter().all(|file| !file.compacting()));
        }
    }

    #[tokio::test]
    async fn test_pick_result_remote_submission_failure_releases_and_notifies_once() {
        let env = SchedulerEnv::new().await;
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        scheduler
            .plugins
            .insert::<RemoteJobSchedulerRef>(Arc::new(FailingRemoteScheduler));
        let version_control = compactable_version();
        let region_id = version_control.current().version.metadata.region_id;
        let (mut finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        use_remote_compaction(&mut finished);
        let selected = selected_files(&finished);
        let (waiter_tx, waiter_rx) = oneshot::channel();
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .merge_waiter(OptionOutputTx::from(waiter_tx));

        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;

        assert!(waiter_rx.await.unwrap().is_err());
        assert!(selected.iter().all(|file| !file.compacting()));
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_local_reservations_survive_status_removal_until_execution_drops() {
        let selected;
        {
            let job_scheduler = Arc::new(VecScheduler::default());
            let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
            let (tx, mut rx) = mpsc::channel(4);
            let mut scheduler = env.mock_compaction_scheduler(tx);
            let version_control = compactable_version();
            let region_id = version_control.current().version.metadata.region_id;
            let (finished, manifest_ctx, schema_metadata_manager) =
                begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
            selected = selected_files(&finished);
            scheduler
                .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
                .await;
            assert!(selected.iter().all(FileHandle::compacting));

            scheduler.on_region_closed(region_id);

            assert!(selected.iter().all(FileHandle::compacting));
        }
        assert!(selected.iter().all(|file| !file.compacting()));
    }

    #[tokio::test]
    async fn test_remote_reservations_survive_status_removal_until_notifier_drops() {
        let env = SchedulerEnv::new().await;
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let remote_scheduler = Arc::new(HoldingRemoteScheduler::default());
        scheduler
            .plugins
            .insert::<RemoteJobSchedulerRef>(remote_scheduler.clone());
        let version_control = compactable_version();
        let region_id = version_control.current().version.metadata.region_id;
        let (mut finished, manifest_ctx, schema_metadata_manager) =
            begin_pick_result(&env, &mut scheduler, &mut rx, &version_control).await;
        use_remote_compaction(&mut finished);
        let selected = selected_files(&finished);
        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;
        assert!(selected.iter().all(FileHandle::compacting));

        scheduler.on_region_closed(region_id);

        assert!(selected.iter().all(FileHandle::compacting));
        remote_scheduler.drop_notifier();
        assert!(selected.iter().all(|file| !file.compacting()));
    }

    #[tokio::test]
    async fn test_stale_local_success_does_not_clear_replacement_phase() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let stale_version_control = compactable_version();
        let replacement_version_control = compactable_version();
        let region_id = replacement_version_control
            .current()
            .version
            .metadata
            .region_id;
        assert!(!Arc::ptr_eq(
            &stale_version_control,
            &replacement_version_control
        ));
        let manifest_ctx = env
            .mock_manifest_context(
                replacement_version_control
                    .current()
                    .version
                    .metadata
                    .clone(),
            )
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        let stale_execution =
            CompactionExecution::for_test(stale_version_control, CompactionExecutionKind::Local);
        let mut status = CompactionStatus::new(
            region_id,
            replacement_version_control.clone(),
            env.access_layer.clone(),
        );
        status.start_local_task();
        scheduler.region_status.insert(region_id, status);
        let files_before = replacement_version_control
            .current()
            .version
            .ssts
            .owned_num_files(region_id);
        if scheduler.is_current_region_execution(
            region_id,
            &replacement_version_control,
            &stale_execution,
        ) {
            apply_edit(
                &replacement_version_control,
                &[(2_000_000, 3_000_000)],
                &[],
                crate::test_util::new_noop_file_purger(),
            );
        }

        scheduler
            .on_execution_finished(
                region_id,
                &stale_execution,
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await;

        assert!(scheduler.region_status[&region_id].is_busy());
        assert_eq!(
            replacement_version_control
                .current()
                .version
                .ssts
                .owned_num_files(region_id),
            files_before
        );
    }

    #[tokio::test]
    async fn test_stale_local_cancel_does_not_remove_replacement_status() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let stale_version_control = compactable_version();
        let replacement_version_control = compactable_version();
        let region_id = replacement_version_control
            .current()
            .version
            .metadata
            .region_id;
        let stale_execution =
            CompactionExecution::for_test(stale_version_control, CompactionExecutionKind::Local);
        let mut status = CompactionStatus::new(
            region_id,
            replacement_version_control,
            env.access_layer.clone(),
        );
        status.start_local_task();
        scheduler.region_status.insert(region_id, status);

        scheduler
            .on_execution_cancelled(region_id, &stale_execution)
            .await;

        assert!(scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_stale_local_failure_does_not_remove_replacement_status_or_waiter() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let stale_version_control = compactable_version();
        let replacement_version_control = compactable_version();
        let region_id = replacement_version_control
            .current()
            .version
            .metadata
            .region_id;
        let stale_execution =
            CompactionExecution::for_test(stale_version_control, CompactionExecutionKind::Local);
        let (waiter_tx, mut waiter_rx) = oneshot::channel();
        let mut status = CompactionStatus::new(
            region_id,
            replacement_version_control,
            env.access_layer.clone(),
        );
        status.start_local_task();
        status.merge_waiter(OptionOutputTx::from(waiter_tx));
        scheduler.region_status.insert(region_id, status);

        scheduler.on_execution_failed(
            region_id,
            &stale_execution,
            Arc::new(InvalidSchedulerStateSnafu.build()),
        );

        assert!(scheduler.region_status.contains_key(&region_id));
        assert_matches!(
            waiter_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        );
    }

    #[tokio::test]
    async fn test_stale_remote_success_does_not_clear_replacement_phase() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let stale_version_control = compactable_version();
        let replacement_version_control = compactable_version();
        let region_id = replacement_version_control
            .current()
            .version
            .metadata
            .region_id;
        let stale_execution =
            CompactionExecution::for_test(stale_version_control, CompactionExecutionKind::Remote);
        let manifest_ctx = env
            .mock_manifest_context(
                replacement_version_control
                    .current()
                    .version
                    .metadata
                    .clone(),
            )
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        let mut status = CompactionStatus::new(
            region_id,
            replacement_version_control.clone(),
            env.access_layer.clone(),
        );
        status.start_remote_task();
        scheduler.region_status.insert(region_id, status);
        let files_before = replacement_version_control
            .current()
            .version
            .ssts
            .owned_num_files(region_id);
        if scheduler.is_current_region_execution(
            region_id,
            &replacement_version_control,
            &stale_execution,
        ) {
            apply_edit(
                &replacement_version_control,
                &[(2_000_000, 3_000_000)],
                &[],
                crate::test_util::new_noop_file_purger(),
            );
        }

        scheduler
            .on_execution_finished(
                region_id,
                &stale_execution,
                &manifest_ctx,
                schema_metadata_manager,
            )
            .await;

        assert!(scheduler.region_status[&region_id].is_busy());
        assert_eq!(
            replacement_version_control
                .current()
                .version
                .ssts
                .owned_num_files(region_id),
            files_before
        );
    }

    #[tokio::test]
    async fn test_stale_remote_failure_does_not_remove_replacement_status() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let stale_version_control = compactable_version();
        let replacement_version_control = compactable_version();
        let region_id = replacement_version_control
            .current()
            .version
            .metadata
            .region_id;
        let stale_execution =
            CompactionExecution::for_test(stale_version_control, CompactionExecutionKind::Remote);
        let mut status = CompactionStatus::new(
            region_id,
            replacement_version_control,
            env.access_layer.clone(),
        );
        status.start_remote_task();
        scheduler.region_status.insert(region_id, status);

        scheduler.on_execution_failed(
            region_id,
            &stale_execution,
            Arc::new(InvalidSchedulerStateSnafu.build()),
        );

        assert!(scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_schedule_compaction_skips_task_exceeding_memory_limit() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        scheduler.memory_manager = Arc::new(new_compaction_memory_manager(1024 * 1024));

        let mut builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let end = 1000 * 1000;
        let version_control = Arc::new(
            builder
                .push_l0_file_with_max_row_group_size(0, end, 1024 * 1024)
                .push_l0_file_with_max_row_group_size(10, end, 1024 * 1024)
                .push_l0_file_with_max_row_group_size(50, end, 1024 * 1024)
                .push_l0_file_with_max_row_group_size(80, end, 1024 * 1024)
                .push_l0_file_with_max_row_group_size(90, end, 1024 * 1024)
                .build(),
        );
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, kv_backend) = mock_schema_metadata_manager();
        schema_metadata_manager
            .register_region_table_info(
                region_id.table_id(),
                "test_table",
                "test_catalog",
                "test_schema",
                None,
                kv_backend,
            )
            .await;
        let (output_tx, output_rx) = oneshot::channel();
        let rejected = COMPACTION_MEMORY_REJECTED.with_label_values(&["oversized"]);
        let rejected_before = rejected.get();

        let scheduled = scheduler
            .schedule_compaction(
                region_id,
                Options::Regular(Default::default()),
                &version_control,
                &env.access_layer,
                OptionOutputTx::from(output_tx),
                &manifest_ctx,
                schema_metadata_manager.clone(),
                1,
            )
            .await
            .unwrap();

        assert!(scheduled);
        let finished = recv_compaction_pick_finished(&mut rx).await;
        let selected = selected_files(&finished);
        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;
        assert_eq!(output_rx.await.unwrap().unwrap(), 0);
        assert_eq!(rejected_before + 1, rejected.get());
        assert_eq!(0, job_scheduler.num_jobs());
        assert!(!scheduler.region_status.contains_key(&region_id));
        assert!(selected.iter().all(|file| !file.compacting()));
    }

    #[tokio::test]
    async fn test_schedule_on_finished() {
        common_telemetry::init_default_ut_logging();
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
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
        let scheduled = scheduler
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
        assert!(scheduled);
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(0, job_scheduler.num_jobs());
        let finished = recv_compaction_pick_finished(&mut rx).await;
        scheduler
            .handle_compaction_pick_finished(
                finished,
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await;
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
        let scheduled = scheduler
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
        assert!(!scheduled);
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
        let scheduled = scheduler
            .schedule_next_compaction(region_id, &manifest_ctx, schema_metadata_manager.clone())
            .await;
        assert!(scheduled);
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(1, job_scheduler.num_jobs());
        let finished = recv_compaction_pick_finished(&mut rx).await;
        scheduler
            .handle_compaction_pick_finished(
                finished,
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await;
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
        let scheduled = scheduler
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
        assert!(!scheduled);
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
    async fn test_schedule_compaction_clears_status_when_submission_fails() {
        common_telemetry::init_default_ut_logging();
        let env = SchedulerEnv::new()
            .await
            .scheduler(Arc::new(FailingScheduler));
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
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
        let region_id = builder.region_id();
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
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

        let result = scheduler
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
            .await;

        assert!(result.unwrap());
        assert!(scheduler.region_status.contains_key(&region_id));
        let finished = recv_compaction_pick_finished(&mut rx).await;
        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_manual_compaction_when_compaction_in_progress() {
        common_telemetry::init_default_ut_logging();
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, mut rx) = mpsc::channel(4);
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
        assert_eq!(0, job_scheduler.num_jobs());
        let finished = recv_compaction_pick_finished(&mut rx).await;
        scheduler
            .handle_compaction_pick_finished(
                finished,
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await;
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
        assert_eq!(1, job_scheduler.num_jobs());
        let finished = recv_compaction_pick_finished(&mut rx).await;
        scheduler
            .handle_compaction_pick_finished(
                finished,
                &manifest_ctx,
                schema_metadata_manager.clone(),
            )
            .await;
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
                    manifest_cache: None,
                },
                FormatType::PrimaryKey,
                &Default::default(),
            )
            .await
            .unwrap();
            Arc::new(ManifestContext::new(
                manager,
                RegionRoleState::Leader(RegionLeaderState::Staging),
                None,
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

    #[tokio::test]
    async fn test_add_ddl_request_to_pending() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = builder.region_id();

        scheduler.region_status.insert(
            region_id,
            CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
        );
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .start_local_task();

        let (output_tx, _output_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(output_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        assert!(scheduler.has_pending_ddls(region_id));
    }

    #[tokio::test]
    async fn test_request_cancel_state_transitions() {
        let env = SchedulerEnv::new().await;
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let mut status =
            CompactionStatus::new(region_id, version_control, env.access_layer.clone());
        let state = status.start_local_task();

        assert_eq!(status.request_cancel(), RequestCancelResult::CancelIssued);
        assert!(state.cancel_handle().is_cancelled());
        assert_eq!(
            status.request_cancel(),
            RequestCancelResult::AlreadyCancelling
        );

        assert!(!state.mark_commit_started());
        assert_eq!(
            status.request_cancel(),
            RequestCancelResult::AlreadyCancelling
        );

        assert!(status.clear_running_task());
        assert_eq!(status.request_cancel(), RequestCancelResult::NotRunning);
    }

    #[tokio::test]
    async fn test_request_cancel_remote_compaction_is_too_late() {
        let env = SchedulerEnv::new().await;
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let mut status =
            CompactionStatus::new(region_id, version_control, env.access_layer.clone());

        status.start_remote_task();

        assert_eq!(
            status.request_cancel(),
            RequestCancelResult::TooLateToCancel
        );
        assert!(status.is_busy());
    }

    #[tokio::test]
    async fn test_on_compaction_cancelled_returns_pending_ddl_requests() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = builder.region_id();
        let _manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (_schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

        let (regular_tx, regular_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control, env.access_layer.clone());
        status.start_picking(7);
        status.merge_regular_trigger(OptionOutputTx::from(regular_tx));
        status.start_local_task();
        scheduler.region_status.insert(region_id, status);

        let (output_tx, _output_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(output_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        let pending_ddls = scheduler.on_compaction_cancelled(region_id).await;

        assert_eq!(pending_ddls.len(), 1);
        assert!(!scheduler.has_pending_ddls(region_id));
        assert!(!scheduler.region_status.contains_key(&region_id));
        assert_eq!(job_scheduler.num_jobs(), 0);
        assert!(regular_rx.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_on_compaction_cancelled_prioritizes_pending_ddls_over_pending_compaction() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = builder.region_id();
        let _manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (_schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

        scheduler.region_status.insert(
            region_id,
            CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
        );
        let status = scheduler.region_status.get_mut(&region_id).unwrap();
        status.start_local_task();
        let (manual_tx, manual_rx) = oneshot::channel();
        status.set_pending_request(PendingCompaction {
            options: compact_request::Options::StrictWindow(StrictWindow { window_seconds: 60 }),
            waiter: OptionOutputTx::from(manual_tx),
            max_parallelism: 1,
        });

        let (output_tx, _output_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(output_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        let pending_ddls = scheduler.on_compaction_cancelled(region_id).await;

        assert_eq!(pending_ddls.len(), 1);
        assert!(!scheduler.region_status.contains_key(&region_id));
        assert_eq!(job_scheduler.num_jobs(), 0);
        assert_matches!(manual_rx.await.unwrap(), Err(_));
    }

    #[tokio::test]
    async fn test_pending_ddl_request_failed_on_compaction_failed() {
        let env = SchedulerEnv::new().await;
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = builder.region_id();

        let (regular_tx, regular_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control, env.access_layer.clone());
        status.start_picking(7);
        status.merge_regular_trigger(OptionOutputTx::from(regular_tx));
        status.start_local_task();
        scheduler.region_status.insert(region_id, status);

        let (output_tx, output_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(output_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        assert!(scheduler.has_pending_ddls(region_id));
        scheduler
            .on_compaction_failed(region_id, Arc::new(RegionClosedSnafu { region_id }.build()));

        assert!(!scheduler.has_pending_ddls(region_id));
        let result = output_rx.await.unwrap();
        assert_matches!(result, Err(_));
        assert!(regular_rx.await.unwrap().is_err());
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_pending_ddl_request_failed_on_region_closed() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = builder.region_id();

        scheduler.region_status.insert(
            region_id,
            CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
        );

        let (output_tx, output_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(output_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        assert!(scheduler.has_pending_ddls(region_id));
        scheduler.on_region_closed(region_id);

        assert!(!scheduler.has_pending_ddls(region_id));
        let result = output_rx.await.unwrap();
        assert_matches!(result, Err(_));
    }

    #[tokio::test]
    async fn test_pending_ddl_request_failed_on_region_dropped() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = builder.region_id();

        scheduler.region_status.insert(
            region_id,
            CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
        );

        let (output_tx, output_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(output_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        assert!(scheduler.has_pending_ddls(region_id));
        scheduler.on_region_dropped(region_id);

        assert!(!scheduler.has_pending_ddls(region_id));
        let result = output_rx.await.unwrap();
        assert_matches!(result, Err(_));
    }

    #[tokio::test]
    async fn test_pending_ddl_request_failed_on_region_truncated() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = builder.region_id();

        scheduler.region_status.insert(
            region_id,
            CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
        );

        let (output_tx, output_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(output_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        assert!(scheduler.has_pending_ddls(region_id));
        scheduler.on_region_truncated(region_id);

        assert!(!scheduler.has_pending_ddls(region_id));
        let result = output_rx.await.unwrap();
        assert_matches!(result, Err(_));
    }

    #[tokio::test]
    async fn test_on_compaction_finished_returns_pending_ddl_requests() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = builder.region_id();
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

        scheduler.region_status.insert(
            region_id,
            CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
        );
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .start_local_task();

        let (output_tx, _output_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(output_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        let pending_ddls = scheduler
            .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager)
            .await;

        assert_eq!(pending_ddls.len(), 1);
        assert!(!scheduler.has_pending_ddls(region_id));
        assert!(!scheduler.region_status.contains_key(&region_id));
        assert_eq!(job_scheduler.num_jobs(), 0);
    }

    #[tokio::test]
    async fn test_on_compaction_finished_replays_pending_ddl_after_manual_noop() {
        let env = SchedulerEnv::new().await;
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = builder.region_id();
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

        let (manual_tx, manual_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
        status.start_local_task();
        status.set_pending_request(PendingCompaction {
            options: compact_request::Options::Regular(Default::default()),
            waiter: OptionOutputTx::from(manual_tx),
            max_parallelism: 1,
        });
        scheduler.region_status.insert(region_id, status);

        let (ddl_tx, _ddl_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(ddl_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        let pending_ddls = scheduler
            .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager.clone())
            .await;

        assert!(pending_ddls.is_empty());
        let finished = recv_compaction_pick_finished(&mut rx).await;
        let pending_ddls = scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;
        assert_eq!(pending_ddls.len(), 1);
        assert!(!scheduler.region_status.contains_key(&region_id));
        assert_eq!(manual_rx.await.unwrap().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_on_compaction_finished_returns_empty_when_region_absent() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let region_id = builder.region_id();
        let version_control = Arc::new(builder.build());
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

        let pending_ddls = scheduler
            .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager)
            .await;

        assert!(pending_ddls.is_empty());
    }

    #[tokio::test]
    async fn test_on_compaction_finished_manual_schedule_error_cleans_status() {
        let env = SchedulerEnv::new()
            .await
            .scheduler(Arc::new(FailingScheduler));
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
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
        let region_id = builder.region_id();
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

        let (manual_tx, manual_rx) = oneshot::channel();
        let mut status =
            CompactionStatus::new(region_id, version_control.clone(), env.access_layer.clone());
        status.start_local_task();
        status.set_pending_request(PendingCompaction {
            options: compact_request::Options::Regular(Default::default()),
            waiter: OptionOutputTx::from(manual_tx),
            max_parallelism: 1,
        });
        scheduler.region_status.insert(region_id, status);

        let (ddl_tx, ddl_rx) = oneshot::channel();
        scheduler.add_ddl_request_to_pending(SenderDdlRequest {
            region_id,
            sender: OptionOutputTx::from(ddl_tx),
            request: crate::request::DdlRequest::EnterStaging(
                store_api::region_request::EnterStagingRequest {
                    partition_directive:
                        store_api::region_request::StagingPartitionDirective::RejectAllWrites,
                },
            ),
        });

        let pending_ddls = scheduler
            .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager.clone())
            .await;

        assert!(pending_ddls.is_empty());
        let finished = recv_compaction_pick_finished(&mut rx).await;
        let pending_ddls = scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;
        assert!(pending_ddls.is_empty());
        assert!(!scheduler.region_status.contains_key(&region_id));
        assert_matches!(manual_rx.await.unwrap(), Err(_));
        assert_matches!(ddl_rx.await.unwrap(), Err(_));
    }

    #[tokio::test]
    async fn test_on_compaction_finished_next_schedule_noop_removes_status() {
        let env = SchedulerEnv::new().await;
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let region_id = builder.region_id();
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

        scheduler.region_status.insert(
            region_id,
            CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
        );
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .start_local_task();

        let pending_ddls = scheduler
            .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager)
            .await;

        assert!(pending_ddls.is_empty());
        assert!(scheduler.region_status.contains_key(&region_id));

        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        // With no compactable files, next scheduling returns false and removes
        // the status without creating a background task.
        let scheduled = scheduler
            .schedule_next_compaction(region_id, &manifest_ctx, schema_metadata_manager.clone())
            .await;
        assert!(scheduled);
        let finished = recv_compaction_pick_finished(&mut rx).await;
        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_on_compaction_finished_next_schedule_error_cleans_status() {
        let env = SchedulerEnv::new()
            .await
            .scheduler(Arc::new(FailingScheduler));
        let (tx, mut rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
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
        let region_id = builder.region_id();
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();

        scheduler.region_status.insert(
            region_id,
            CompactionStatus::new(region_id, version_control, env.access_layer.clone()),
        );
        scheduler
            .region_status
            .get_mut(&region_id)
            .unwrap()
            .start_local_task();

        let pending_ddls = scheduler
            .on_compaction_finished(region_id, &manifest_ctx, schema_metadata_manager)
            .await;

        assert!(pending_ddls.is_empty());
        assert!(scheduler.region_status.contains_key(&region_id));

        let (schema_metadata_manager, _kv_backend) = mock_schema_metadata_manager();
        // The failing scheduler simulates a submit error; callers must see false.
        let scheduled = scheduler
            .schedule_next_compaction(region_id, &manifest_ctx, schema_metadata_manager.clone())
            .await;
        assert!(scheduled);
        let finished = recv_compaction_pick_finished(&mut rx).await;
        scheduler
            .handle_compaction_pick_finished(finished, &manifest_ctx, schema_metadata_manager)
            .await;
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_concurrent_memory_competition() {
        let manager = Arc::new(new_compaction_memory_manager(3 * 1024 * 1024)); // 3MB
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

        let results: Vec<Option<CompactionMemoryGuard>> = futures::future::join_all(handles)
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
