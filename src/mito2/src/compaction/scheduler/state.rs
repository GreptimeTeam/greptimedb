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

use std::collections::{HashMap, HashSet, hash_map};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use api::v1::region::compact_request;
use common_meta::key::SchemaMetadataManagerRef;
use common_telemetry::debug;
use common_time::range::TimestampRange;
use snafu::ResultExt;
use store_api::compaction_task::{self, CompactionTaskEntry};
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::compaction::CompactionOutput;
use crate::compaction::compactor::{CompactionRegion, CompactionVersion};
use crate::compaction::picker::PickerOutput;
use crate::compaction::scheduler::CancelCompactionResult;
use crate::compaction::scheduler::history::PickHistory;
use crate::compaction::scheduler::local::{LocalCompaction, LocalCompactionState};
use crate::compaction::scheduler::planning::CompactionRequest;
use crate::config::MitoConfig;
use crate::error::{
    CompactRegionSnafu, CompactionCancelledSnafu, Error, ManualCompactionOverrideSnafu,
};
use crate::region::ManifestContextRef;
use crate::region::version::VersionControlRef;
use crate::request::{OptionOutputTx, OutputTx, SenderDdlRequest, WorkerRequestWithTime};
#[cfg(test)]
use crate::schedule::CancellableTaskState;
use crate::schedule::RequestCancelResult;
use crate::sst::file::{FileHandle, RegionFileId};
use crate::worker::WorkerListener;

/// Identifies an accepted compaction attempt and keeps its SST reservations alive.
/// The plan id fences terminal notifications from superseded attempts.
#[derive(Debug, Clone)]
pub(crate) struct CompactionExecution {
    plan_id: u64,
    _files: CompactingFiles,
}

impl CompactionExecution {
    /// Returns the attempt ID used to reject stale execution notifications.
    pub(crate) fn plan_id(&self) -> u64 {
        self.plan_id
    }
    pub(super) fn new(plan_id: u64, files: CompactingFiles) -> Self {
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

#[derive(Debug)]
pub(crate) enum CompactionPhase {
    /// The region is registered but has no compaction in progress.
    Idle,
    Picking {
        plan_id: u64,
        cancelled: bool,
    },
    #[cfg(test)]
    Local {
        state: CancellableTaskState,
        execution: CompactionExecution,
    },
    Remote {
        execution: CompactionExecution,
    },
    Units(LocalCompaction),
}

/// Describes how the current compaction cycle was triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompactionTrigger {
    Automatic,
    Manual,
}

#[derive(Debug)]
pub(super) struct ActiveCompaction {
    /// Query metadata owned by this live cycle, dropped when it becomes idle.
    pub(crate) task_state: Option<CompactionTaskState>,
    pub(super) phase: CompactionPhase,
    trigger: CompactionTrigger,
    /// Waiters satisfied by the current planning or execution cycle. Picking waiters move into
    /// the submitted task; regular triggers coalesced during execution accumulate here.
    pub(super) waiters: Vec<OutputTx>,
    /// An automatic trigger arrived during this cycle and requires one unrestricted regular
    /// picking cycle after the current cycle finishes. Recorded in every phase so an external
    /// trigger always resets the continuation scope, even during local/remote execution.
    pub(super) automatic_followup_required: bool,
    /// Records an accepted explicit cancellation of this task.
    task_cancel_requested: bool,
}

impl ActiveCompaction {
    /// Creates an empty cycle while retaining the region's scheduling record.
    fn idle() -> Self {
        Self {
            task_state: None,
            phase: CompactionPhase::Idle,
            trigger: CompactionTrigger::Automatic,
            waiters: Vec::new(),
            automatic_followup_required: false,
            task_cancel_requested: false,
        }
    }

    #[cfg(test)]
    pub(super) fn picking(
        plan_id: u64,
        waiters: Vec<OutputTx>,
        trigger: CompactionTrigger,
    ) -> Self {
        Self {
            task_state: None,
            phase: CompactionPhase::Picking {
                plan_id,
                cancelled: false,
            },
            trigger,
            waiters,
            automatic_followup_required: false,
            task_cancel_requested: false,
        }
    }

    pub(super) fn start_picking(&mut self, plan_id: u64, trigger: CompactionTrigger) {
        self.task_state = None;
        self.task_cancel_requested = false;
        self.phase = CompactionPhase::Picking {
            plan_id,
            cancelled: false,
        };
        self.trigger = trigger;
    }

    pub(super) fn start_regular_picking(&mut self, plan_id: u64) {
        self.start_picking(plan_id, CompactionTrigger::Automatic);
    }

    /// Marks an automatic trigger. The trigger is coalesced into a single unrestricted
    /// follow-up cycle regardless of the current phase.
    pub(super) fn mark_automatic_trigger(&mut self) {
        self.automatic_followup_required = true;
    }

    /// Resets whether an unrestricted follow-up cycle is required and
    /// return the previous value.
    pub(super) fn reset_automatic_followup(&mut self) -> bool {
        std::mem::take(&mut self.automatic_followup_required)
    }

    pub(super) fn is_manual(&self) -> bool {
        self.trigger == CompactionTrigger::Manual
    }

    pub(super) fn is_picking(&self, expected_plan_id: u64) -> bool {
        matches!(
            self.phase,
            CompactionPhase::Picking { plan_id, .. } if plan_id == expected_plan_id
        )
    }

    pub(super) fn accept_plan(&self, expected_plan_id: u64) -> bool {
        matches!(
            self.phase,
            CompactionPhase::Picking {
                plan_id,
                cancelled: false,
            } if plan_id == expected_plan_id
        )
    }

    pub(super) fn matches_execution(&self, execution: &CompactionExecution) -> bool {
        match &self.phase {
            CompactionPhase::Idle | CompactionPhase::Picking { .. } => None,
            #[cfg(test)]
            CompactionPhase::Local { execution, .. } => Some(execution),
            CompactionPhase::Units(_) => None,
            CompactionPhase::Remote { execution } => Some(execution),
        }
        .is_some_and(|current| current.matches(execution))
    }

    pub(super) fn request_cancel(&mut self) -> RequestCancelResult {
        match &mut self.phase {
            CompactionPhase::Idle => RequestCancelResult::TooLateToCancel,
            CompactionPhase::Picking { cancelled, .. } => {
                if *cancelled {
                    RequestCancelResult::AlreadyCancelling
                } else {
                    *cancelled = true;
                    RequestCancelResult::CancelIssued
                }
            }
            #[cfg(test)]
            CompactionPhase::Local { state, .. } => state.request_cancel(),
            CompactionPhase::Units(units) => units.request_cancel(),
            CompactionPhase::Remote { .. } => RequestCancelResult::TooLateToCancel,
        }
    }

    pub(super) fn merge_waiter(&mut self, mut waiter: OptionOutputTx) {
        if let Some(waiter) = waiter.take_inner() {
            self.waiters.push(waiter);
        }
    }
    /// Captures worker-owned metadata while retaining live background task handles.
    fn current_task_state(&self) -> Option<CompactionTaskState> {
        let mut task = self.task_state.clone()?;
        let entry = &mut task.entry;
        match &self.phase {
            CompactionPhase::Idle => return None,
            CompactionPhase::Picking { cancelled, .. } => {
                entry.state = if *cancelled {
                    compaction_task::STATE_CANCELLING
                } else {
                    compaction_task::STATE_RUNNING
                }
                .into();
                entry.phase = compaction_task::PHASE_PLANNING.into();
            }
            CompactionPhase::Remote { .. } => {
                entry.state = compaction_task::STATE_RUNNING.into();
                entry.phase = compaction_task::PHASE_REMOTE.into();
            }
            CompactionPhase::Units(local) => task.local_state = Some(local.current_state()),
            #[cfg(test)]
            CompactionPhase::Local { .. } => {
                entry.state = compaction_task::STATE_RUNNING.into();
                entry.phase = compaction_task::PHASE_EXECUTING.into();
            }
        }
        Some(task)
    }
}

/// Owns atomic reservations for every SST selected by a compaction plan.
#[derive(Debug, Clone)]
pub(crate) struct CompactingFiles {
    files: HashMap<RegionFileId, Arc<CompactingFile>>,
}

/// Keeps an SST reserved until the last shared lease is dropped.
#[derive(Debug)]
struct CompactingFile {
    file: FileHandle,
}

impl CompactingFiles {
    /// Reserves both merge inputs and expired files from an accepted picker result.
    pub(super) fn try_new(output: &PickerOutput) -> Option<Self> {
        Self::try_reserve(
            output
                .outputs
                .iter()
                .flat_map(|o| &o.inputs)
                .chain(&output.expired_ssts),
        )
    }

    /// Reserves distinct inputs, releasing acquired leases if any input is already busy.
    fn try_reserve<'a>(selected_files: impl IntoIterator<Item = &'a FileHandle>) -> Option<Self> {
        let mut files = HashMap::new();

        for file in selected_files {
            let hash_map::Entry::Vacant(entry) = files.entry(file.file_id()) else {
                continue;
            };
            if !file.try_set_compacting() {
                return None;
            }
            entry.insert(Arc::new(CompactingFile { file: file.clone() }));
        }

        Some(Self { files })
    }

    /// Clones the shared leases of a dependency group's inputs without briefly releasing any input.
    /// `self` retains its leases, so an input is released only after every owner, including
    /// the original collection, drops its lease.
    /// A delayed remote notifier may retain the same leases across local fallback.
    /// Indexing keeps splitting proportional to the group's input count.
    pub(crate) fn for_inputs(&self, inputs: &[FileHandle]) -> Self {
        Self {
            files: inputs
                .iter()
                .filter_map(|file| {
                    let id = file.file_id();
                    self.files.get(&id).map(|lease| (id, lease.clone()))
                })
                .collect(),
        }
    }

    #[cfg(test)]
    pub(super) fn empty() -> Self {
        Self {
            files: HashMap::new(),
        }
    }
}

impl Drop for CompactingFile {
    fn drop(&mut self) {
        self.file.set_compacting(false);
    }
}

/// A handed-off DDL batch, retained until each reply has been observed once.
#[derive(Debug)]
pub(crate) struct DdlExecution {
    generation: u64,
    pending_replies: HashSet<usize>,
}

impl DdlExecution {
    /// Tracks a batch's individual replies so duplicate notifications are harmless.
    pub(crate) fn new(generation: u64, count: usize) -> Self {
        Self {
            generation,
            pending_replies: (0..count).collect(),
        }
    }

    /// Returns true only for the last distinct reply of this batch.
    pub(crate) fn complete(&mut self, generation: u64, request_index: usize) -> bool {
        self.generation == generation
            && self.pending_replies.remove(&request_index)
            && self.pending_replies.is_empty()
    }
}

/// Scheduling state retained for the lifetime of a loaded region.
pub(super) struct CompactionStatus {
    /// Current cycles shared with queries without entering the region worker.
    pub(crate) shared_state: CompactionStateRef,
    /// Resident automatic-pick history, retained across batches until region unload.
    pub(crate) pick_history: PickHistory,
    /// Id of the region.
    pub(super) region_id: RegionId,
    /// Version control of the region.
    pub(super) version_control: VersionControlRef,
    /// Access layer of the region.
    pub(super) access_layer: AccessLayerRef,
    /// Current compaction lifecycle.
    pub(super) active: ActiveCompaction,
    /// A manual compaction waiting for the current automatic compaction to finish.
    ///
    /// A manual request is rejected if the current compaction is also manual. Automatic requests
    /// are merged into the active compaction instead of using this slot.
    pub(super) pending_request: Option<PendingCompaction>,
    /// Metadata for the pending manual request, moved to the active cycle on promotion.
    pub(crate) pending_task_state: Option<CompactionTaskState>,
    /// Pending DDL requests that should run when compaction is done.
    ///
    /// Although [`SenderDdlRequest`] can wrap any DDL variant, production code only queues
    /// [`crate::request::DdlRequest::Truncate`] and [`crate::request::DdlRequest::EnterStaging`] here. Both must serialize with
    /// compaction so they observe the version after compaction terminates.
    pub(super) pending_ddl_requests: Vec<SenderDdlRequest>,
    /// DDLs already handed to the worker keep new compaction paused until their replies.
    pub(crate) executing_ddl: Option<DdlExecution>,
}

impl CompactionStatus {
    /// Cancels an exact identity, checking pending work before the active cycle.
    pub(crate) fn cancel_task(&mut self, task_id: &str) -> CancelCompactionResult {
        if self
            .pending_task_state
            .as_ref()
            .is_some_and(|state| state.entry.task_id.as_ref() == task_id)
        {
            self.pending_task_state = None;
            if let Some(pending) = self.pending_request.take() {
                pending.waiter.send(CompactionCancelledSnafu.fail());
            }
            self.publish_state();
            return CancelCompactionResult::CancelIssued;
        }
        if !self
            .active
            .task_state
            .as_ref()
            .is_some_and(|state| state.entry.task_id.as_ref() == task_id)
        {
            return CancelCompactionResult::NotFound;
        }
        if matches!(self.active.phase, CompactionPhase::Remote { .. }) {
            return CancelCompactionResult::Unsupported;
        }
        if self.active.task_cancel_requested {
            return CancelCompactionResult::AlreadyCancelling;
        }
        let result = self.request_cancel();
        if result != RequestCancelResult::TooLateToCancel {
            self.active.task_cancel_requested = true;
        }
        result.into()
    }

    /// Publishes both slots together without holding the lock while building their metadata.
    pub(crate) fn publish_state(&self) {
        let active = self.active.current_task_state().map(Arc::new);
        let pending = self
            .pending_request
            .as_ref()
            .and(self.pending_task_state.as_ref())
            .cloned()
            .map(Arc::new);
        let previous = {
            let mut shared = self.shared_state.write().unwrap();
            std::mem::replace(&mut *shared, CompactionState { active, pending })
        };
        drop(previous);
    }

    /// Registers an idle region without starting a compaction cycle.
    pub(super) fn new(
        region_id: RegionId,
        version_control: VersionControlRef,
        access_layer: AccessLayerRef,
    ) -> CompactionStatus {
        CompactionStatus {
            shared_state: Arc::new(RwLock::new(CompactionState::default())),
            pick_history: PickHistory::default(),
            region_id,
            version_control,
            access_layer,
            active: ActiveCompaction::idle(),
            pending_request: None,
            pending_task_state: None,
            pending_ddl_requests: Vec::new(),
            executing_ddl: None,
        }
    }

    #[cfg(test)]
    pub(super) fn for_test(
        region_id: RegionId,
        version_control: VersionControlRef,
        access_layer: AccessLayerRef,
    ) -> Self {
        let mut status = Self::new(region_id, version_control, access_layer);
        status.active = ActiveCompaction::picking(0, Vec::new(), CompactionTrigger::Automatic);
        status
    }

    /// Registration alone does not indicate active work.
    pub(crate) fn is_compacting(&self) -> bool {
        !matches!(self.active.phase, CompactionPhase::Idle)
    }

    /// Both queued and handed-off DDLs prevent new compaction admission.
    pub(crate) fn has_ddl(&self) -> bool {
        !self.pending_ddl_requests.is_empty() || self.executing_ddl.is_some()
    }

    /// Ends the current cycle without losing region or DDL execution state.
    pub(crate) fn become_idle(&mut self) {
        self.active = ActiveCompaction::idle();
        self.publish_state();
    }

    #[cfg(test)]
    pub(super) fn start_picking(&mut self, plan_id: u64) {
        self.start_picking_with_trigger(plan_id, CompactionTrigger::Automatic);
    }

    pub(super) fn start_picking_with_trigger(&mut self, plan_id: u64, trigger: CompactionTrigger) {
        self.active.start_picking(plan_id, trigger);
    }

    pub(super) fn start_regular_picking(&mut self, plan_id: u64) {
        self.active.start_regular_picking(plan_id);
    }

    pub(super) fn is_picking(&self, expected_plan_id: u64) -> bool {
        self.active.is_picking(expected_plan_id)
    }

    pub(super) fn accept_plan(&self, expected_plan_id: u64) -> bool {
        self.active.accept_plan(expected_plan_id)
    }

    pub(super) fn is_manual_compaction(&self) -> bool {
        self.active.is_manual()
    }

    pub(super) fn matches_execution(&self, execution: &CompactionExecution) -> bool {
        self.active.matches_execution(execution)
    }

    #[cfg(test)]
    pub(super) fn start_local_task(&mut self) -> CancellableTaskState {
        let state = CancellableTaskState::new();
        let execution = CompactionExecution::new(0, CompactingFiles::empty());
        let phase = CompactionPhase::Local {
            state: state.clone(),
            execution,
        };
        self.active.phase = phase;
        state
    }

    #[cfg(test)]
    pub(super) fn start_remote_task(&mut self) {
        let execution = CompactionExecution::new(0, CompactingFiles::empty());
        let phase = CompactionPhase::Remote { execution };
        self.active.phase = phase;
    }

    pub(super) fn request_cancel(&mut self) -> RequestCancelResult {
        let result = self.active.request_cancel();
        self.publish_state();
        result
    }

    pub(super) fn mark_automatic_trigger(&mut self) {
        self.active.mark_automatic_trigger();
    }

    /// Merge the waiter to the pending compaction.
    pub(super) fn merge_waiter(&mut self, waiter: OptionOutputTx) {
        self.active.merge_waiter(waiter);
    }

    pub(super) fn take_waiters(&mut self) -> Vec<OutputTx> {
        std::mem::take(&mut self.active.waiters)
    }

    pub(super) fn extend_waiters(&mut self, waiters: Vec<OutputTx>) {
        self.active.waiters.extend(waiters);
    }

    pub(super) fn set_phase(&mut self, phase: CompactionPhase) {
        self.active.phase = phase;
        self.publish_state();
    }

    /// Sets a pending manual compaction request, replacing an older pending request.
    pub(super) fn set_pending_request(&mut self, pending: PendingCompaction) {
        self.pending_task_state = Some(CompactionTaskState::new(
            self.region_id,
            &pending.options,
            pending.max_parallelism,
            pending.time_range,
            CompactionTrigger::Manual,
        ));
        if let Some(prev) = self.pending_request.replace(pending) {
            debug!(
                "Replace pending compaction options with new request {:?} for region: {}",
                prev.options, self.region_id
            );
            prev.waiter.send(ManualCompactionOverrideSnafu.fail());
        }
        self.publish_state();
    }

    pub(super) fn on_failure(&mut self, err: Arc<Error>) {
        let mut active = std::mem::replace(&mut self.active, ActiveCompaction::idle());
        for waiter in active.waiters.drain(..) {
            waiter.send(Err(err.clone()).context(CompactRegionSnafu {
                region_id: self.region_id,
            }));
        }

        self.pending_task_state = None;
        if let Some(pending_compaction) = self.pending_request.take() {
            pending_compaction
                .waiter
                .send(Err(err.clone()).context(CompactRegionSnafu {
                    region_id: self.region_id,
                }));
        }

        for pending_ddl in self.pending_ddl_requests.drain(..) {
            pending_ddl
                .sender
                .send(Err(err.clone()).context(CompactRegionSnafu {
                    region_id: self.region_id,
                }));
        }
        self.publish_state();
    }

    /// Ends task-specific cancellation only when no pending DDL takes precedence.
    /// Returns whether the caller may promote a different pending compaction.
    pub(crate) fn finish_task_cancellation(&mut self) -> bool {
        if !self.active.task_cancel_requested || !self.pending_ddl_requests.is_empty() {
            return false;
        }
        let mut active = std::mem::replace(&mut self.active, ActiveCompaction::idle());
        for waiter in active.waiters.drain(..) {
            waiter.send(CompactionCancelledSnafu.fail());
        }
        self.publish_state();
        true
    }

    #[must_use]
    pub(super) fn on_cancel(&mut self) -> Vec<SenderDdlRequest> {
        let mut active = std::mem::replace(&mut self.active, ActiveCompaction::idle());
        for waiter in active.waiters.drain(..) {
            waiter.send(CompactionCancelledSnafu.fail());
        }

        self.pending_task_state = None;
        if let Some(pending_compaction) = self.pending_request.take() {
            pending_compaction.waiter.send(
                Err(Arc::new(CompactionCancelledSnafu.build())).context(CompactRegionSnafu {
                    region_id: self.region_id,
                }),
            );
        }

        self.publish_state();
        std::mem::take(&mut self.pending_ddl_requests)
    }

    /// Creates an immutable request for background compaction planning.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_compaction_request(
        &self,
        request_sender: Sender<WorkerRequestWithTime>,
        engine_config: Arc<MitoConfig>,
        cache_manager: CacheManagerRef,
        manifest_ctx: &ManifestContextRef,
        listener: WorkerListener,
        schema_metadata_manager: SchemaMetadataManagerRef,
        max_parallelism: usize,
        pick_history: Option<PickHistory>,
    ) -> CompactionRequest {
        let current_version = CompactionVersion::from(self.version_control.current().version);
        let start_time = Instant::now();

        CompactionRequest {
            pick_history,
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

/// A manual compaction request waiting for an automatic compaction to finish.
pub(super) struct PendingCompaction {
    /// Compaction options.
    pub(crate) options: compact_request::Options,
    /// Waiters of pending requests.
    pub(crate) waiter: OptionOutputTx,
    /// Max parallelism for pending compaction.
    pub(crate) max_parallelism: usize,
    /// Optional time range that constrains candidate compaction windows.
    pub(crate) time_range: Option<TimestampRange>,
}

/// Current cycles of one loaded region, without completed-task history.
#[derive(Debug, Default)]
pub(crate) struct CompactionState {
    active: Option<Arc<CompactionTaskState>>,
    pending: Option<Arc<CompactionTaskState>>,
}

/// Shared current cycles of a loaded region.
pub(crate) type CompactionStateRef = Arc<RwLock<CompactionState>>;

impl CompactionState {
    /// Clones current cycle handles under a short read lock.
    pub(crate) fn current(
        state: &CompactionStateRef,
    ) -> (
        Option<Arc<CompactionTaskState>>,
        Option<Arc<CompactionTaskState>>,
    ) {
        let state = state.read().unwrap();
        (state.active.clone(), state.pending.clone())
    }
}

/// Per-cycle request metadata and live execution handles, without retaining SSTs.
#[derive(Debug, Clone)]
pub(crate) struct CompactionTaskState {
    pub(crate) entry: CompactionTaskEntry,
    pub(crate) submitted: Instant,
    local_state: Option<LocalCompactionState>,
}

impl CompactionTaskState {
    /// Captures request parameters and assigns the identity of a new compaction cycle.
    pub(crate) fn new(
        region_id: RegionId,
        options: &compact_request::Options,
        max_parallelism: usize,
        time_range: Option<TimestampRange>,
        trigger: CompactionTrigger,
    ) -> Self {
        let (kind, window) = match options {
            compact_request::Options::Regular(_) => ("regular", None),
            compact_request::Options::StrictWindow(window) => {
                ("strict_window", Some(window.window_seconds))
            }
        };
        let request_options = serde_json::json!({
            "type": kind,
            "window_seconds": window,
            "max_parallelism": max_parallelism,
            "time_range": time_range,
        })
        .to_string();
        Self {
            submitted: Instant::now(),
            local_state: None,
            entry: CompactionTaskEntry {
                task_id: uuid::Uuid::new_v4().to_string().into(),
                region_id,
                table_id: region_id.table_id(),
                node_id: None,
                trigger_type: match trigger {
                    CompactionTrigger::Manual => compaction_task::TRIGGER_TYPE_MANUAL,
                    CompactionTrigger::Automatic => compaction_task::TRIGGER_TYPE_AUTOMATIC,
                }
                .into(),
                state: compaction_task::STATE_QUEUED.into(),
                phase: compaction_task::PHASE_WAITING.into(),
                submitted_at: common_time::util::current_time_millis(),
                elapsed_millis: 0,
                input_files: None,
                input_bytes: None,
                input_time_start: None,
                input_time_end: None,
                output_time_start: None,
                output_time_end: None,
                total_units: None,
                completed_units: None,
                request_options: request_options.into(),
                effective_options: None,
            },
        }
    }

    /// Captures the resolved parameters and work selected by an accepted plan.
    pub(crate) fn accept_plan(&mut self, region: &CompactionRegion, output: &PickerOutput) {
        let max_parallelism = region.max_parallelism.max(1);
        let max_picker_outputs = region
            .engine_config
            .max_background_compactions
            .min(max_parallelism);
        self.entry.effective_options = Some(
            serde_json::json!({
                "compaction": region.region_options.compaction,
                "ttl": region.ttl,
                "merge_mode": region.region_options.merge_mode(),
                "max_picker_outputs": max_picker_outputs,
                "max_parallelism": max_parallelism,
                "time_window_seconds": output.time_window_size,
                "max_output_file_size": output.max_file_size,
            })
            .to_string()
            .into(),
        );
        self.set_plan_stats(
            output
                .outputs
                .iter()
                .flat_map(|output| &output.inputs)
                .chain(&output.expired_ssts),
            output.outputs.iter(),
        );
    }

    /// Records the cycle's accepted inputs and planned output envelope, without retaining SSTs.
    /// Called again after local memory admission to exclude rejected units. These aggregates
    /// remain stable as individual units finish; inputs include expired SSTs and output ends
    /// retain half-open semantics. If any output lacks a planned range, both output bounds
    /// are None (SQL NULL). An unbounded side propagates independently across outputs.
    pub(crate) fn set_plan_stats<'a>(
        &mut self,
        inputs: impl Iterator<Item = &'a FileHandle>,
        outputs: impl Iterator<Item = &'a CompactionOutput>,
    ) {
        let mut seen = HashSet::new();
        let mut bytes = 0;
        let mut input_start = None;
        let mut input_end = None;
        for file in inputs {
            if !seen.insert(file.file_id()) {
                continue;
            }
            bytes += file.meta_ref().file_size;
            let (start, end) = file.time_range();
            input_start =
                Some(input_start.map_or(start, |previous| std::cmp::min(previous, start)));
            input_end = Some(input_end.map_or(end, |previous| std::cmp::max(previous, end)));
        }
        self.entry.input_files = Some(seen.len() as u64);
        self.entry.input_bytes = Some(bytes);
        self.entry.input_time_start = input_start;
        self.entry.input_time_end = input_end;

        let envelope = outputs
            .map(|output| {
                output
                    .output_time_range
                    .map(|range| (*range.start(), *range.end()))
                    .unwrap_or((None, None))
            })
            .reduce(|(start, end), (next_start, next_end)| {
                (
                    start.zip(next_start).map(|(a, b)| a.min(b)),
                    end.zip(next_end).map(|(a, b)| a.max(b)),
                )
            });
        let (start, end) = envelope.unwrap_or((None, None));
        self.entry.output_time_start = start;
        self.entry.output_time_end = end;
    }

    /// Copies cycle metadata with the reporting node and current monotonic elapsed time.
    pub(crate) fn snapshot(&self, node_id: Option<u64>) -> CompactionTaskEntry {
        let mut entry = self.entry.clone();
        if let Some(local) = &self.local_state {
            local.inspect(&mut entry);
        }
        entry.node_id = node_id;
        entry.elapsed_millis = self.submitted.elapsed().as_millis().min(u64::MAX as u128) as u64;
        entry
    }
}

#[cfg(test)]
mod tests {
    use common_time::Timestamp;
    use store_api::storage::FileId;

    use super::*;
    use crate::compaction::test_util::new_file_handle;
    use crate::test_util::new_noop_file_purger;

    fn plan_time_range(start: Option<Timestamp>, end: Option<Timestamp>) -> TimestampRange {
        TimestampRange::from((
            start.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Included),
            end.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
        ))
    }

    fn plan_stats_fixture() -> (CompactionTaskState, [FileHandle; 3], [CompactionOutput; 2]) {
        let file = |start, end, size| {
            let original = new_file_handle(FileId::random(), 0, 0, 0);
            let mut meta = original.meta_ref().clone();
            meta.time_range = (start, end);
            meta.file_size = size;
            FileHandle::new(meta, new_noop_file_purger())
        };
        // Compare timestamps in their original units: numeric values alone have the wrong order.
        let early = file(
            Timestamp::new_second(1),
            Timestamp::new_millisecond(1500),
            10,
        );
        let late = file(
            Timestamp::new_microsecond(3_000_001),
            Timestamp::new_second(4),
            20,
        );
        let expired = file(Timestamp::new_second(-2), Timestamp::new_second(-1), 30);
        let output = |inputs, start, end| CompactionOutput {
            output_level: 1,
            inputs,
            filter_deleted: false,
            output_time_range: Some(plan_time_range(start, end)),
        };
        let outputs = [
            output(
                vec![early.clone()],
                Some(Timestamp::new_second(0)),
                Some(Timestamp::new_second(2)),
            ),
            output(
                vec![late.clone()],
                Some(Timestamp::new_second(3)),
                Some(Timestamp::new_second(5)),
            ),
        ];
        let info = CompactionTaskState::new(
            RegionId::new(1, 1),
            &compact_request::Options::Regular(Default::default()),
            1,
            None,
            CompactionTrigger::Manual,
        );
        (info, [early, late, expired], outputs)
    }

    type PlanStats = (
        Option<u64>,
        Option<u64>,
        Option<Timestamp>,
        Option<Timestamp>,
        Option<Timestamp>,
        Option<Timestamp>,
    );

    fn plan_stats(info: &CompactionTaskState) -> PlanStats {
        let entry = &info.entry;
        (
            entry.input_files,
            entry.input_bytes,
            entry.input_time_start,
            entry.input_time_end,
            entry.output_time_start,
            entry.output_time_end,
        )
    }

    #[test]
    fn test_compaction_plan_stats_deduplication_and_admission() {
        let (mut info, [early, late, expired], outputs) = plan_stats_fixture();
        // Repeated inputs across logical windows count once; expired SSTs extend only inputs.
        info.set_plan_stats(
            [&early, &late, &early, &expired].into_iter(),
            outputs.iter(),
        );
        assert_eq!(
            plan_stats(&info),
            (
                Some(3),
                Some(60),
                Some(Timestamp::new_second(-2)),
                Some(Timestamp::new_second(4)),
                Some(Timestamp::new_second(0)),
                Some(Timestamp::new_second(5))
            )
        );
        // Recompute after admission excludes a unit, including its output window.
        info.set_plan_stats([&early, &expired].into_iter(), outputs[..1].iter());
        assert_eq!(
            plan_stats(&info),
            (
                Some(2),
                Some(40),
                Some(Timestamp::new_second(-2)),
                Some(Timestamp::new_millisecond(1500)),
                Some(Timestamp::new_second(0)),
                Some(Timestamp::new_second(2))
            )
        );
    }

    #[test]
    fn test_compaction_plan_stats_expiration_and_empty_reset() {
        let (mut info, [early, late, expired], outputs) = plan_stats_fixture();
        info.set_plan_stats([&early, &late, &expired].into_iter(), outputs.iter());
        // Expiration-only cycles have input coverage but no output envelope.
        info.set_plan_stats([&expired].into_iter(), std::iter::empty());
        assert_eq!(
            plan_stats(&info),
            (
                Some(1),
                Some(30),
                Some(Timestamp::new_second(-2)),
                Some(Timestamp::new_second(-1)),
                None,
                None
            )
        );
        info.set_plan_stats(std::iter::empty(), std::iter::empty());
        assert_eq!(
            plan_stats(&info),
            (Some(0), Some(0), None, None, None, None)
        );
    }

    #[test]
    fn test_compaction_plan_stats_missing_and_unbounded_ranges() {
        let (mut info, [early, late, _], outputs) = plan_stats_fixture();
        // An unbounded side of any output remains unbounded in the cycle envelope.
        for (range, expected_start, expected_end) in [
            (None, None, None),
            (
                Some(plan_time_range(None, Some(Timestamp::new_second(5)))),
                None,
                Some(Timestamp::new_second(5)),
            ),
            (
                Some(plan_time_range(Some(Timestamp::new_second(3)), None)),
                Some(Timestamp::new_second(0)),
                None,
            ),
        ] {
            let mut mixed = outputs.clone();
            mixed[1].output_time_range = range;
            // Order must not affect propagation of unbounded ranges.
            for reverse in [false, true] {
                if reverse {
                    mixed.reverse();
                }
                info.set_plan_stats([&early, &late].into_iter(), mixed.iter());
                assert_eq!(info.entry.output_time_start, expected_start);
                assert_eq!(info.entry.output_time_end, expected_end);
            }
        }
    }

    #[test]
    fn test_compaction_unit_input_leases_release_independently() {
        let x = new_file_handle(FileId::random(), 0, 100, 0);
        let y = new_file_handle(FileId::random(), 100, 200, 0);
        let unused = new_file_handle(FileId::random(), 200, 300, 0);
        let reserved = CompactingFiles::try_reserve([&x, &y, &x, &unused]).unwrap();
        let first = reserved.for_inputs(&[x.clone(), x.clone()]);
        let second = reserved.for_inputs(std::slice::from_ref(&y));
        drop(reserved);
        assert!(x.compacting() && y.compacting());
        assert!(
            !unused.compacting(),
            "unit leases must not retain unrelated inputs"
        );
        drop(first);
        assert!(!x.compacting() && y.compacting());
        let notification = second.clone();
        drop(second);
        assert!(y.compacting());
        drop(notification);
        assert!(!y.compacting());
    }
}
