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

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use api::v1::region::compact_request;
use common_base::Plugins;
use common_meta::key::SchemaMetadataManagerRef;
use common_telemetry::{debug, error, info, warn};
use common_time::TimeToLive;
use common_time::range::TimestampRange;
use futures::FutureExt;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{self, Sender};

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::compaction::compactor::{CompactionRegion, CompactionVersion, DefaultCompactor};
use crate::compaction::picker::{CompactionTask, PickerOutput, new_picker};
use crate::compaction::scheduler::state::{CompactingFiles, CompactionExecution, CompactionPhase};
use crate::compaction::scheduler::{CompactionScheduler, CompactionTransition};
use crate::compaction::task::CompactionTaskImpl;
use crate::compaction::{CompactionOutput, find_dynamic_options};
use crate::config::MitoConfig;
use crate::error::{CompactRegionSnafu, Error, RemoteCompactionSnafu, Result, UnexpectedSnafu};
use crate::metrics::{
    COMPACTION_MEMORY_REJECTED, COMPACTION_STAGE_ELAPSED, INFLIGHT_COMPACTION_COUNT,
};
use crate::region::ManifestContextRef;
use crate::region::options::RegionOptions;
use crate::request::{BackgroundNotify, OutputTx, WorkerRequest, WorkerRequestWithTime};
use crate::schedule::CancellableTaskState;
use crate::schedule::remote_job_scheduler::{
    CompactionJob, DefaultNotifier, RemoteJob, RemoteJobSchedulerRef,
};
use crate::sst::file::{FileHandle, UncommittedSsts};
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
    pub(super) compaction_region: CompactionRegion,
    pub(super) picker_output: PickerOutput,
    start_time: Instant,
    ttl: TimeToLive,
}

impl CompactionScheduler {
    pub(super) fn dispatch_compaction_planning(
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
    pub(super) async fn notify_planning_result(
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

        // Avoid queuing serial outputs from a stale snapshot. Repicking after each
        // batch lets newly flushed L0 files outrank L1 work that has not started.
        let max_picker_outputs = max_background_compactions.min(request.max_parallelism.max(1));
        let picker = new_picker(
            &options,
            &dynamic_compaction_opts,
            request.current_version.options.append_mode,
            Some(max_picker_outputs),
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
        let _pick_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["pick"])
            .start_timer();
        let picker_output = match picker.pick(&compaction_region).await {
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

    /// Applies a background planning result to the current compaction lifecycle.
    ///
    /// # Returns
    ///
    /// Reports an automatic follow-up dispatched after Picking, or DDL requests
    /// released when Picking terminates without creating an execution. The
    /// owning worker must dispatch returned DDLs immediately because no
    /// execution callback will follow.
    pub(super) async fn handle_compaction_pick_finished_inner(
        &mut self,
        finished: CompactionPickFinished,
        manifest_ctx: &ManifestContextRef,
        schema_metadata_manager: SchemaMetadataManagerRef,
    ) -> CompactionTransition {
        let region_id = finished.region_id;
        let plan_id = finished.plan_id;
        let Some(status) = self.region_status.get(&region_id) else {
            return CompactionTransition::NoAction;
        };

        if !status.is_picking(finished.plan_id) {
            return CompactionTransition::NoAction;
        }
        // Cancellation during Picking is completed by this notification. No
        // execution callback will follow, so return DDLs released by the fence.
        if !status.accept_plan(finished.plan_id) {
            return CompactionTransition::from_pending_ddls(
                self.remove_region_on_cancel(region_id),
            );
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
                    return CompactionTransition::NoAction;
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
                        // The execution now owns the terminal transition. Any
                        // fenced DDLs remain queued until its callback arrives.
                        CompactionTransition::NoAction
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
                        CompactionTransition::NoAction
                    }
                }
            }
            // These paths never create an execution. Finish the Picking
            // lifecycle here and release DDLs if no follow-up was scheduled.
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
    ) -> CompactionTransition {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return CompactionTransition::NoAction;
        };
        for waiter in status.take_waiters() {
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
            return CompactionTransition::NoAction;
        }

        let Some(status) = self.region_status.get_mut(&region_id) else {
            return CompactionTransition::NoAction;
        };

        // A queued DDL supersedes a retained automatic follow-up, matching the
        // execution terminal path in `on_compaction_finished`.
        let pending_ddls = std::mem::take(&mut status.pending_ddl_requests);
        if !pending_ddls.is_empty() {
            self.region_status.remove(&region_id);
            return CompactionTransition::DdlReady(pending_ddls);
        }

        if status.active.reset_automatic_followup()
            && self.schedule_automatic_followup(region_id, manifest_ctx, schema_metadata_manager)
        {
            return CompactionTransition::AutomaticFollowupScheduled;
        }

        self.region_status.remove(&region_id);
        CompactionTransition::NoAction
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

        let state = CancellableTaskState::new();
        let cancel_handle = state.cancel_handle();
        let execution = CompactionExecution::new(plan_id, files);
        let uncommitted = UncommittedSsts::new(
            region_id,
            compaction_region.access_layer.clone(),
            Some(compaction_region.cache_manager.clone()),
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
            compactor: Arc::new(DefaultCompactor::with_cancel_handle(
                cancel_handle.clone(),
                uncommitted.clone(),
            )),
            memory_manager: self.memory_manager.clone(),
            memory_policy: self.memory_policy,
            estimated_memory_bytes: estimated_bytes,
            uncommitted,
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
