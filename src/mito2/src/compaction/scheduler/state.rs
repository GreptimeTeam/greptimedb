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

#[derive(Debug)]
pub(super) struct ActiveCompaction {
    pub(super) phase: CompactionPhase,
    /// Waiters satisfied by the current planning or execution cycle. Picking waiters move into
    /// the submitted task; regular triggers coalesced during execution accumulate here.
    pub(super) waiters: Vec<OutputTx>,
    /// Requests one fresh regular picking cycle after the current cycle finishes. It is kept
    /// separate because the current picker snapshot may predate the trigger; `Some(empty)` records
    /// an automatic trigger without an explicit waiter.
    pub(super) regular_followup_waiters: Option<Vec<OutputTx>>,
}

impl ActiveCompaction {
    pub(super) fn picking(plan_id: u64, waiters: Vec<OutputTx>) -> Self {
        Self {
            phase: CompactionPhase::Picking {
                plan_id,
                cancelled: false,
            },
            waiters,
            regular_followup_waiters: None,
        }
    }

    pub(super) fn start_picking(&mut self, plan_id: u64) {
        self.phase = CompactionPhase::Picking {
            plan_id,
            cancelled: false,
        };
    }

    pub(super) fn start_regular_picking(&mut self, plan_id: u64) {
        self.waiters
            .extend(self.regular_followup_waiters.take().unwrap_or_default());
        self.start_picking(plan_id);
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

    pub(super) fn merge_regular_trigger(&mut self, mut waiter: OptionOutputTx) {
        if matches!(self.phase, CompactionPhase::Picking { .. }) {
            let regular_followup_waiters = self.regular_followup_waiters.get_or_insert_default();
            if let Some(waiter) = waiter.take_inner() {
                regular_followup_waiters.push(waiter);
            }
        } else {
            self.merge_waiter(waiter);
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
    /// Current compaction lifecycle. `None` is the existing transient idle state.
    // TODO: Remove idle statuses and make ActiveCompaction non-optional once chained
    // scheduling can recreate the status from region context.
    pub(super) active: Option<ActiveCompaction>,
    /// Optional range retained by automatic continuations of the current compaction.
    pub(super) time_range: Option<TimestampRange>,
    /// Pending compactions that are supposed to run as soon as current compaction task finished.
    ///
    /// This holds strict-window requests and ranged regular requests. An unrestricted regular
    /// request is instead merged into `ActiveCompaction::regular_followup_waiters` or `waiters`.
    pub(super) pending_request: Option<PendingCompaction>,
    /// Pending DDL requests that should run when compaction is done.
    ///
    /// Although [`SenderDdlRequest`] can wrap any DDL variant, production code only queues
    /// [`crate::request::DdlRequest::Truncate`] and [`crate::request::DdlRequest::EnterStaging`] here. Both must serialize with
    /// compaction so they observe the version after compaction terminates.
    pub(super) pending_ddl_requests: Vec<SenderDdlRequest>,
}

impl CompactionStatus {
    /// Creates a new [CompactionStatus]
    pub(super) fn new(
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
    pub(super) fn start_picking(&mut self, plan_id: u64) {
        self.start_picking_with_time_range(plan_id, None);
    }

    pub(super) fn start_picking_with_time_range(
        &mut self,
        plan_id: u64,
        time_range: Option<TimestampRange>,
    ) {
        self.time_range = time_range;
        if let Some(active) = &mut self.active {
            active.start_picking(plan_id);
        } else {
            self.active = Some(ActiveCompaction::picking(plan_id, Vec::new()));
        }
    }

    pub(super) fn start_regular_picking(
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

    pub(super) fn is_picking(&self, expected_plan_id: u64) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| active.is_picking(expected_plan_id))
    }

    pub(super) fn accept_plan(&self, expected_plan_id: u64) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| active.accept_plan(expected_plan_id))
    }

    pub(super) fn is_busy(&self) -> bool {
        self.active.is_some()
    }

    pub(super) fn matches_execution(&self, execution: &CompactionExecution) -> bool {
        self.active
            .as_ref()
            .is_some_and(|active| active.matches_execution(execution))
    }

    #[cfg(test)]
    pub(super) fn start_local_task(&mut self) -> CancellableTaskState {
        let state = CancellableTaskState::new();
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
    pub(super) fn start_remote_task(&mut self) {
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

    pub(super) fn request_cancel(&mut self) -> RequestCancelResult {
        let Some(active) = &mut self.active else {
            return RequestCancelResult::NotRunning;
        };
        active.request_cancel()
    }

    #[cfg(test)]
    pub(super) fn clear_running_task(&mut self) -> bool {
        self.active.take().is_some()
    }

    pub(super) fn merge_regular_trigger(&mut self, waiter: OptionOutputTx) {
        if let Some(active) = &mut self.active {
            active.merge_regular_trigger(waiter);
        }
    }

    /// Merge the waiter to the pending compaction.
    pub(super) fn merge_waiter(&mut self, waiter: OptionOutputTx) {
        if let Some(active) = &mut self.active {
            active.merge_waiter(waiter);
        }
    }

    pub(super) fn take_active(&mut self) -> Option<ActiveCompaction> {
        self.active.take()
    }

    pub(super) fn take_waiters(&mut self) -> Vec<OutputTx> {
        self.active
            .as_mut()
            .map(|active| std::mem::take(&mut active.waiters))
            .unwrap_or_default()
    }

    pub(super) fn extend_waiters(&mut self, waiters: Vec<OutputTx>) {
        if let Some(active) = &mut self.active {
            active.waiters.extend(waiters);
        }
    }

    pub(super) fn append_waiters(&mut self, waiters: &mut Vec<OutputTx>) {
        if let Some(active) = &mut self.active {
            active.waiters.append(waiters);
        }
    }

    pub(super) fn set_phase(&mut self, phase: CompactionPhase) {
        if let Some(active) = &mut self.active {
            active.phase = phase;
        }
    }

    /// Set pending compaction request or replace current value if already exist.
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
    pub(super) fn on_cancel(mut self) -> Vec<SenderDdlRequest> {
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

/// Pending compaction request that is supposed to run after current task is finished,
/// typically used for manual compactions.
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
