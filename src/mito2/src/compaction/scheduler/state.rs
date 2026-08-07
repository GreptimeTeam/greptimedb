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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use api::v1::region::compact_request;
use common_meta::key::SchemaMetadataManagerRef;
use common_telemetry::debug;
use common_time::range::TimestampRange;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::mpsc::Sender;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::compaction::compactor::CompactionVersion;
use crate::compaction::picker::PickerOutput;
use crate::compaction::scheduler::planning::CompactionRequest;
use crate::config::MitoConfig;
use crate::error::{
    CompactRegionSnafu, CompactionCancelledSnafu, Error, ManualCompactionOverrideSnafu,
};
use crate::region::ManifestContextRef;
use crate::region::version::VersionControlRef;
use crate::request::{OptionOutputTx, OutputTx, SenderDdlRequest, WorkerRequestWithTime};
use crate::schedule::{CancellableTaskState, RequestCancelResult};
use crate::sst::file::FileHandle;
use crate::worker::WorkerListener;

/// Identifies an accepted compaction attempt and keeps its SST reservations alive.
/// The plan id fences terminal notifications from superseded attempts.
#[derive(Debug, Clone)]
pub(crate) struct CompactionExecution {
    plan_id: u64,
    _files: CompactingFiles,
}

impl CompactionExecution {
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
pub(super) enum CompactionPhase {
    Picking {
        plan_id: u64,
        cancelled: bool,
    },
    Local {
        state: CancellableTaskState,
        execution: CompactionExecution,
    },
    Remote {
        execution: CompactionExecution,
    },
}

/// Describes how the current compaction cycle was triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum CompactionTrigger {
    Automatic,
    Manual,
}

#[derive(Debug)]
pub(super) struct ActiveCompaction {
    pub(super) phase: CompactionPhase,
    trigger: CompactionTrigger,
    /// Waiters satisfied by the current planning or execution cycle. Picking waiters move into
    /// the submitted task; regular triggers coalesced during execution accumulate here.
    pub(super) waiters: Vec<OutputTx>,
    /// An automatic trigger arrived during this cycle and requires one unrestricted regular
    /// picking cycle after the current cycle finishes. Recorded in every phase so an external
    /// trigger always resets the continuation scope, even during local/remote execution.
    pub(super) automatic_followup_required: bool,
}

impl ActiveCompaction {
    pub(super) fn picking(
        plan_id: u64,
        waiters: Vec<OutputTx>,
        trigger: CompactionTrigger,
    ) -> Self {
        Self {
            phase: CompactionPhase::Picking {
                plan_id,
                cancelled: false,
            },
            trigger,
            waiters,
            automatic_followup_required: false,
        }
    }

    pub(super) fn start_picking(&mut self, plan_id: u64, trigger: CompactionTrigger) {
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
            CompactionPhase::Picking { .. } => None,
            CompactionPhase::Local { execution, .. } | CompactionPhase::Remote { execution } => {
                Some(execution)
            }
        }
        .is_some_and(|current| current.matches(execution))
    }

    pub(super) fn request_cancel(&mut self) -> RequestCancelResult {
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

    pub(super) fn merge_waiter(&mut self, mut waiter: OptionOutputTx) {
        if let Some(waiter) = waiter.take_inner() {
            self.waiters.push(waiter);
        }
    }
}

/// Owns atomic reservations for every SST selected by a compaction plan.
#[derive(Debug, Clone)]
pub(super) struct CompactingFiles {
    _inner: Arc<CompactingFilesInner>,
}

#[derive(Debug)]
struct CompactingFilesInner {
    files: Vec<FileHandle>,
}

impl CompactingFiles {
    pub(super) fn try_new(output: &PickerOutput) -> Option<Self> {
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
    pub(super) fn empty() -> Self {
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

/// Status of running and pending region compaction tasks.
pub(super) struct CompactionStatus {
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
    /// Pending DDL requests that should run when compaction is done.
    ///
    /// Although [`SenderDdlRequest`] can wrap any DDL variant, production code only queues
    /// [`crate::request::DdlRequest::Truncate`] and [`crate::request::DdlRequest::EnterStaging`] here. Both must serialize with
    /// compaction so they observe the version after compaction terminates.
    pub(super) pending_ddl_requests: Vec<SenderDdlRequest>,
}

impl CompactionStatus {
    /// Creates a new picking [CompactionStatus].
    pub(super) fn new(
        region_id: RegionId,
        version_control: VersionControlRef,
        access_layer: AccessLayerRef,
        plan_id: u64,
        trigger: CompactionTrigger,
    ) -> CompactionStatus {
        CompactionStatus {
            region_id,
            version_control,
            access_layer,
            active: ActiveCompaction::picking(plan_id, Vec::new(), trigger),
            pending_request: None,
            pending_ddl_requests: Vec::new(),
        }
    }

    #[cfg(test)]
    pub(super) fn for_test(
        region_id: RegionId,
        version_control: VersionControlRef,
        access_layer: AccessLayerRef,
    ) -> Self {
        Self::new(
            region_id,
            version_control,
            access_layer,
            0,
            CompactionTrigger::Automatic,
        )
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
        self.active.request_cancel()
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

    pub(super) fn append_waiters(&mut self, waiters: &mut Vec<OutputTx>) {
        self.active.waiters.append(waiters);
    }

    pub(super) fn set_phase(&mut self, phase: CompactionPhase) {
        self.active.phase = phase;
    }

    /// Sets a pending manual compaction request, replacing an older pending request.
    pub(super) fn set_pending_request(&mut self, pending: PendingCompaction) {
        if let Some(prev) = self.pending_request.replace(pending) {
            debug!(
                "Replace pending compaction options with new request {:?} for region: {}",
                prev.options, self.region_id
            );
            prev.waiter.send(ManualCompactionOverrideSnafu.fail());
        }
    }

    pub(super) fn on_failure(mut self, err: Arc<Error>) {
        for waiter in self.active.waiters.drain(..) {
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
    pub(super) fn on_cancel(mut self) -> Vec<SenderDdlRequest> {
        for waiter in self.active.waiters.drain(..) {
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
    pub(super) fn new_compaction_request(
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
