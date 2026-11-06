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

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::{AtomicBool, Ordering};

use common_meta::key::SchemaMetadataManagerRef;
use store_api::compaction_task::{self, CompactionTaskEntry};
use store_api::storage::RegionId;

use crate::compaction::compactor::{CompactionContext, CompactionRegion};
use crate::compaction::scheduler::state::{CompactingFiles, CompactionExecution, CompactionPhase};
use crate::compaction::scheduler::{CompactionScheduler, CompactionTransition};
use crate::compaction::task::CompactionTask;
use crate::compaction::unit::CompactionUnit;
use crate::error::{self, Error};
use crate::metrics::INFLIGHT_COMPACTION_COUNT;
use crate::region::ManifestContextRef;
use crate::request::{
    BackgroundNotify, CompactionUnitNotification, WorkerRequest, WorkerRequestWithTime,
};
use crate::schedule::{CancellableTaskState, RequestCancelResult};
use crate::sst::file::UncommittedSsts;

/// Tracks one unit through its terminal result and subsequent task exit.
#[derive(Debug)]
struct UnitExecution {
    state: CancellableTaskState,
    /// Observation flags only; they do not publish other fields or form an atomic snapshot.
    /// The owning worker is their sole writer.
    finished: AtomicBool,
    released: AtomicBool,
}

/// A request's local units. All transitions run on the owning region worker.
#[derive(Debug)]
pub(crate) struct LocalCompaction {
    /// All pending and submitted units, retained until task exit. Tasks own their input leases.
    /// Keyed by each unit's execution plan ID, allocated from the scheduler's `next_plan_id`;
    /// it equals `CompactionExecution::plan_id()` and the `plan_id` in unit notifications.
    units: HashMap<u64, Arc<UnitExecution>>,
    /// Built once after admission. No units are added after this set is published.
    /// Shared handles retain execution flags only, never tasks or SST leases.
    unit_states: Arc<[Arc<UnitExecution>]>,
    #[cfg(test)]
    publication_count: Arc<AtomicUsize>,
    pending: VecDeque<CompactionTask>,
    parallelism: usize,
    error: Option<Arc<Error>>,
    made_progress: bool,
    /// Retains accepted cancellation while non-cancellable siblings finish committing.
    cancellation_requested: bool,
}

/// Query-visible local progress with shared background execution flags.
#[derive(Debug, Clone)]
pub(crate) struct LocalCompactionState {
    units: Arc<[Arc<UnitExecution>]>,
    cancellation_requested: bool,
}

impl LocalCompactionState {
    /// Aggregates live units: cancellation takes precedence over running and queued states;
    /// committing takes precedence over executing and waiting phases. A cancelling cycle
    /// can still be committing while non-cancellable siblings finish.
    pub(crate) fn inspect(&self, entry: &mut CompactionTaskEntry) {
        entry.total_units = Some(self.units.len() as u64);
        let mut completed = 0;
        let mut cancelling = self.cancellation_requested;
        let mut committing = false;
        let mut running = false;
        for unit in self.units.iter() {
            completed += u64::from(unit.finished.load(Ordering::Relaxed));
            if unit.released.load(Ordering::Relaxed) {
                continue;
            }
            cancelling |= unit.state.is_cancelled();
            committing |= unit.state.commit_started();
            running |= unit.state.started();
        }
        entry.completed_units = Some(completed);
        running |= committing;
        entry.state = if cancelling {
            compaction_task::STATE_CANCELLING
        } else if running {
            compaction_task::STATE_RUNNING
        } else {
            compaction_task::STATE_QUEUED
        }
        .into();
        entry.phase = if committing {
            compaction_task::PHASE_COMMITTING
        } else if running {
            compaction_task::PHASE_EXECUTING
        } else {
            compaction_task::PHASE_WAITING
        }
        .into();
    }
}

impl LocalCompaction {
    /// Clones progress and live task handles without retaining tasks or their SST leases.
    pub(crate) fn current_state(&self) -> LocalCompactionState {
        #[cfg(test)]
        self.publication_count.fetch_add(1, Ordering::Relaxed);
        LocalCompactionState {
            units: self.unit_states.clone(),
            cancellation_requested: self.cancellation_requested,
        }
    }

    #[cfg(test)]
    pub(crate) fn publication_count_for_test(&self) -> Arc<AtomicUsize> {
        self.publication_count.clone()
    }

    #[cfg(test)]
    pub(crate) fn inspection_stats_for_test(&self) -> (usize, usize, usize) {
        (
            Arc::as_ptr(&self.unit_states) as *const () as usize,
            self.unit_states.len(),
            Arc::strong_count(&self.unit_states),
        )
    }

    #[cfg(test)]
    pub(crate) fn released_units_for_test(&self) -> usize {
        self.unit_states
            .iter()
            .filter(|unit| unit.released.load(Ordering::Relaxed))
            .count()
    }

    #[cfg(test)]
    pub(crate) fn unit_ids_for_test(&self) -> Vec<u64> {
        self.units.keys().copied().collect()
    }

    #[cfg(test)]
    pub(crate) fn inspect(&self, entry: &mut CompactionTaskEntry) {
        self.current_state().inspect(entry);
    }

    /// Requests cancellation for all units that have not started committing.
    pub(crate) fn request_cancel(&mut self) -> RequestCancelResult {
        let mut result = RequestCancelResult::TooLateToCancel;
        for unit in self.units.values() {
            match unit.state.request_cancel() {
                RequestCancelResult::CancelIssued => result = RequestCancelResult::CancelIssued,
                RequestCancelResult::AlreadyCancelling
                    if result != RequestCancelResult::CancelIssued =>
                {
                    result = RequestCancelResult::AlreadyCancelling;
                }
                _ => {}
            }
        }
        self.cancellation_requested |= result != RequestCancelResult::TooLateToCancel;
        result
    }

    /// Counts submitted units that have not exited, including queued jobs.
    fn num_running(&self) -> usize {
        self.units.len() - self.pending.len()
    }

    /// Checks this attempt's phase without depending on any sibling's apply status.
    fn can_apply(&self, id: u64) -> bool {
        self.units.get(&id).is_some_and(|unit| {
            !unit.finished.load(Ordering::Relaxed) && unit.state.commit_started()
        })
    }

    /// Removes an exited or rejected unit from scheduling and shared phase aggregation.
    fn release_unit(&mut self, id: u64) {
        if let Some(unit) = self.units.remove(&id) {
            unit.released.store(true, Ordering::Relaxed);
        }
    }

    /// Records a terminal result once, but still awaits task exit.
    fn finish(&mut self, id: u64, result: std::result::Result<bool, Arc<Error>>) {
        let Some(unit) = self.units.get(&id) else {
            return;
        };
        if unit.finished.load(Ordering::Relaxed) {
            return;
        }
        unit.finished.store(true, Ordering::Relaxed);
        match result {
            Ok(progress) => self.made_progress |= progress,
            Err(err) => {
                // A real failure must not be hidden by a sibling's cooperative cancellation.
                if self.error.as_ref().is_none_or(|current| {
                    matches!(current.as_ref(), Error::CompactionCancelled { .. })
                }) {
                    self.error = Some(err);
                }
            }
        }
    }
}

impl Drop for LocalCompaction {
    /// Stops cancellable work when its region or compaction cycle is removed.
    fn drop(&mut self) {
        self.request_cancel();
    }
}

impl CompactionScheduler {
    /// Builds unit tasks with fresh execution IDs and shares their already-reserved inputs.
    pub(crate) fn prepare_local_units(
        &mut self,
        region: CompactionRegion,
        units: Vec<CompactionUnit>,
        reserved: CompactingFiles,
    ) -> CompactionPhase {
        // Memory admission may have removed oversized units from the picker output.
        // Report only the inputs that this local cycle will actually execute.
        if let Some(info) = self
            .region_status
            .get_mut(&region.region_id)
            .and_then(|status| status.active.task_state.as_mut())
        {
            info.set_plan_stats(
                units.iter().flat_map(|unit| &unit.inputs),
                units.iter().flat_map(|unit| &unit.outputs),
            );
        }
        let mut local = LocalCompaction {
            units: HashMap::new(),
            unit_states: Arc::from([]),
            #[cfg(test)]
            publication_count: Arc::new(AtomicUsize::new(0)),
            pending: VecDeque::new(),
            parallelism: region.max_parallelism.max(1),
            error: None,
            made_progress: false,
            cancellation_requested: false,
        };
        let context = CompactionContext::from(&region);
        // Planning is complete; unit tasks must not pin the other units' inputs.
        drop(region);
        for unit in units {
            let files = reserved.for_inputs(&unit.inputs);
            let id = Self::next_plan_id(&mut self.next_plan_id);
            let execution = CompactionExecution::new(id, files);
            let state = CancellableTaskState::new();
            local.units.insert(
                id,
                Arc::new(UnitExecution {
                    state: state.clone(),
                    finished: AtomicBool::new(false),
                    released: AtomicBool::new(false),
                }),
            );
            local.pending.push_back(CompactionTask {
                state,
                execution,
                compaction_context: context.clone(),
                request_sender: self.request_sender.clone(),
                listener: self.listener.clone(),
                estimated_memory_bytes: unit.estimated_memory_bytes(),
                unit,
                memory_manager: self.memory_manager.clone(),
                memory_policy: self.memory_policy,
                uncommitted: UncommittedSsts::new(
                    context.region_id,
                    context.access_layer.clone(),
                    Some(context.cache_manager.clone()),
                ),
            });
        }
        local.unit_states = local.units.values().cloned().collect();
        CompactionPhase::Units(local)
    }

    /// Returns local unit state only while the region is in a local execution phase.
    fn local_units_mut(&mut self, region_id: RegionId) -> Option<&mut LocalCompaction> {
        match &mut self.region_status.get_mut(&region_id)?.active.phase {
            CompactionPhase::Units(local) => Some(local),
            _ => None,
        }
    }

    /// Submits pending units up to the request limit and arranges task-exit notifications.
    pub(crate) fn dispatch_local_units(&mut self, region_id: RegionId) {
        self.submit_local_units(region_id);
        if let Some(status) = self.region_status.get(&region_id) {
            status.publish_state();
        }
    }

    fn submit_local_units(&mut self, region_id: RegionId) {
        loop {
            let Some(local) = self.local_units_mut(region_id) else {
                return;
            };
            if local.num_running() >= local.parallelism {
                return;
            }
            let Some(mut task) = local.pending.pop_front() else {
                return;
            };
            let id = task.execution.plan_id();
            let sender = self.request_sender.clone();
            let completion = Box::pin(async move {
                let _ = sender
                    .send(WorkerRequestWithTime::new(WorkerRequest::Background {
                        region_id,
                        notify: BackgroundNotify::CompactionUnit(
                            CompactionUnitNotification::Released { plan_id: id },
                        ),
                    }))
                    .await;
            });
            let job = Box::pin(async move {
                INFLIGHT_COMPACTION_COUNT.inc();
                task.run().await;
                drop(task);
                INFLIGHT_COMPACTION_COUNT.dec();
            });
            if let Err(err) = self.scheduler.schedule_with_completion(job, completion)
                && let Some(local) = self.local_units_mut(region_id)
            {
                local.finish(id, Err(Arc::new(err)));
                local.release_unit(id);
            }
        }
    }

    /// Accepts an edit from any current unit that has started committing and is not finished.
    pub(crate) fn can_apply_unit(&self, region_id: RegionId, id: u64) -> bool {
        self.region_status.get(&region_id).is_some_and(|status| {
            matches!(&status.active.phase, CompactionPhase::Units(local)
                if local.can_apply(id))
        })
    }

    /// Records apply progress or failure without ending sibling executions.
    pub(crate) fn on_unit_finished(
        &mut self,
        region_id: RegionId,
        id: u64,
        result: std::result::Result<bool, Arc<Error>>,
    ) {
        if let Some(local) = self.local_units_mut(region_id) {
            local.finish(id, result);
        }
        if let Some(status) = self.region_status.get(&region_id) {
            status.publish_state();
        }
    }

    /// Accounts for task exit, starts pending units, and checks request completion.
    pub(crate) async fn on_unit_released(
        &mut self,
        region_id: RegionId,
        id: u64,
        manifest: &ManifestContextRef,
        schemas: SchemaMetadataManagerRef,
    ) -> CompactionTransition {
        let Some(local) = self.local_units_mut(region_id) else {
            return CompactionTransition::NoAction;
        };
        if !local.units.contains_key(&id) {
            return CompactionTransition::NoAction;
        }
        local.finish(
            id,
            Err(Arc::new(
                error::UnexpectedSnafu {
                    reason: "Compaction unit exited without a terminal notification",
                }
                .build(),
            )),
        );
        local.release_unit(id);
        self.dispatch_local_units(region_id);
        self.finish_local_units(region_id, manifest, schemas).await
    }

    /// Completes the request only after all units exit, using the existing terminal paths.
    pub(crate) async fn finish_local_units(
        &mut self,
        region_id: RegionId,
        manifest: &ManifestContextRef,
        schemas: SchemaMetadataManagerRef,
    ) -> CompactionTransition {
        let Some(local) = self.local_units_mut(region_id) else {
            return CompactionTransition::NoAction;
        };
        if !local.units.is_empty() {
            return CompactionTransition::NoAction;
        }
        if let Some(err) = local.error.take() {
            if matches!(err.as_ref(), Error::CompactionCancelled { .. }) {
                return self.finish_cancelled_compaction(region_id, manifest, schemas);
            }
            self.on_compaction_failed(region_id, err);
            return CompactionTransition::NoAction;
        }
        let progress = local.made_progress;
        if let Some(status) = self.region_status.get_mut(&region_id) {
            for waiter in status.take_waiters() {
                waiter.send(Ok(0));
            }
        }
        self.on_compaction_finished(region_id, manifest, schemas, progress)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local_compaction() -> LocalCompaction {
        let units: HashMap<_, _> = [1, 2]
            .into_iter()
            .map(|id| {
                (
                    id,
                    Arc::new(UnitExecution {
                        state: CancellableTaskState::new(),
                        finished: AtomicBool::new(false),
                        released: AtomicBool::new(false),
                    }),
                )
            })
            .collect();
        LocalCompaction {
            unit_states: units.values().cloned().collect(),
            publication_count: Arc::new(AtomicUsize::new(0)),
            units,
            pending: VecDeque::new(),
            parallelism: 2,
            error: None,
            made_progress: false,
            cancellation_requested: false,
        }
    }

    fn task_entry() -> CompactionTaskEntry {
        use crate::compaction::scheduler::state::{CompactionTaskState, CompactionTrigger};

        CompactionTaskState::new(
            RegionId::from_u64(1),
            &api::v1::region::compact_request::Options::Regular(Default::default()),
            2,
            None,
            CompactionTrigger::Manual,
        )
        .snapshot(None)
    }

    #[test]
    fn test_shared_units_observe_finish_and_release() {
        let mut local = local_compaction();
        let published = local.current_state();
        let mut entry = task_entry();
        for unit in local.units.values() {
            assert!(
                published
                    .units
                    .iter()
                    .any(|shared| Arc::ptr_eq(shared, unit))
            );
        }
        assert!(local.units[&1].state.mark_commit_started());
        local.finish(1, Ok(true));
        local.finish(1, Ok(false));
        published.inspect(&mut entry);
        assert_eq!(entry.completed_units, Some(1));
        assert_eq!((&*entry.state, &*entry.phase), ("running", "committing"));

        local.release_unit(1);
        local.release_unit(1);
        local.release_unit(99);
        published.inspect(&mut entry);
        assert_eq!(entry.completed_units, Some(1));
        assert_eq!((&*entry.state, &*entry.phase), ("queued", "waiting"));
        assert!(Arc::ptr_eq(&published.units, &local.current_state().units));

        // Rejected scheduling finishes and releases a queued unit through the same path.
        local.finish(2, Err(Arc::new(error::CompactionCancelledSnafu.build())));
        local.release_unit(2);
        published.inspect(&mut entry);
        assert_eq!(entry.total_units, Some(2));
        assert_eq!(entry.completed_units, Some(2));
        assert!(local.units.is_empty());
        assert!(Arc::ptr_eq(&published.units, &local.current_state().units));
    }

    #[test]
    fn test_shared_units_match_live_map_aggregation() {
        #[derive(Debug, Clone, Copy)]
        enum Phase {
            Queued,
            Running,
            Committing,
            Finished,
            Released,
            Cancelled,
            CancelledReleased,
        }
        let phases = [
            Phase::Queued,
            Phase::Running,
            Phase::Committing,
            Phase::Finished,
            Phase::Released,
            Phase::Cancelled,
            Phase::CancelledReleased,
        ];
        for first in phases {
            for second in phases {
                let mut local = local_compaction();
                let published = local.current_state();
                let mut completed = 0;
                for (id, phase) in [(1, first), (2, second)] {
                    let state = &local.units[&id].state;
                    match phase {
                        Phase::Queued => {}
                        Phase::Running => state.mark_started(),
                        Phase::Committing | Phase::Finished | Phase::Released => {
                            assert!(state.mark_commit_started());
                        }
                        Phase::Cancelled | Phase::CancelledReleased => {
                            assert_eq!(state.request_cancel(), RequestCancelResult::CancelIssued);
                        }
                    }
                    if matches!(
                        phase,
                        Phase::Finished | Phase::Released | Phase::CancelledReleased
                    ) {
                        local.finish(id, Ok(false));
                        completed += 1;
                    }
                    if matches!(phase, Phase::Released | Phase::CancelledReleased) {
                        local.release_unit(id);
                    }
                }
                // The former implementation aggregated only units remaining in this map.
                let cancelling = local.units.values().any(|unit| unit.state.is_cancelled());
                let committing = local.units.values().any(|unit| unit.state.commit_started());
                let running = committing || local.units.values().any(|unit| unit.state.started());
                let expected_state = if cancelling {
                    "cancelling"
                } else if running {
                    "running"
                } else {
                    "queued"
                };
                let expected_phase = if committing {
                    "committing"
                } else if running {
                    "executing"
                } else {
                    "waiting"
                };
                let mut entry = task_entry();
                published.inspect(&mut entry);
                assert_eq!(
                    (
                        &*entry.state,
                        &*entry.phase,
                        entry.total_units,
                        entry.completed_units
                    ),
                    (expected_state, expected_phase, Some(2), Some(completed)),
                    "{first:?}, {second:?}",
                );
            }
        }
    }

    #[test]
    fn test_compaction_unit_apply_accepts_independent_executions() {
        let mut local = local_compaction();
        assert!(!local.can_apply(1));
        assert!(!local.can_apply(2));
        assert!(!local.can_apply(99));
        for unit in local.units.values() {
            assert!(unit.state.mark_commit_started());
        }
        assert!(local.can_apply(1) && local.can_apply(2));
        local.finish(2, Ok(true));
        assert!(local.can_apply(1));
        assert!(!local.can_apply(2));
        local.finish(2, Err(Arc::new(error::CompactionCancelledSnafu.build())));
        local.finish(99, Err(Arc::new(error::CompactionCancelledSnafu.build())));
        assert!(local.can_apply(1));
        assert!(local.error.is_none());
        assert!(local.made_progress);
        local.finish(1, Ok(false));
        assert!(!local.can_apply(1));
        assert_eq!(
            2,
            local.units.len(),
            "terminal units remain tracked until task exit"
        );
    }

    #[test]
    fn test_compaction_cancelling_survives_cancelled_sibling_release() {
        let mut local = local_compaction();
        let mut entry = task_entry();
        // One published view observes background phase changes without republishing.
        let published = local.current_state();
        published.inspect(&mut entry);
        assert_eq!((&*entry.state, &*entry.phase), ("queued", "waiting"));
        local.units[&1].state.mark_started();
        published.inspect(&mut entry);
        assert_eq!((&*entry.state, &*entry.phase), ("running", "executing"));
        assert!(local.units[&1].state.mark_commit_started());
        published.inspect(&mut entry);
        assert_eq!((&*entry.state, &*entry.phase), ("running", "committing"));
        assert_eq!(local.request_cancel(), RequestCancelResult::CancelIssued);
        published.inspect(&mut entry);
        assert_eq!((&*entry.state, &*entry.phase), ("cancelling", "committing"));

        // The cancelled sibling exits first; the committing sibling cannot be cancelled.
        local.finish(2, Err(Arc::new(error::CompactionCancelledSnafu.build())));
        local.release_unit(2);
        assert!(!local.units[&1].state.is_cancelled());
        local.inspect(&mut entry);
        assert_eq!((&*entry.state, &*entry.phase), ("cancelling", "committing"));
        assert_eq!(entry.completed_units, Some(1));
        assert_eq!(entry.total_units, Some(2));

        // A request arriving after every unit crossed the boundary is not accepted.
        let mut too_late = local_compaction();
        for unit in too_late.units.values() {
            assert!(unit.state.mark_commit_started());
        }
        assert_eq!(
            too_late.request_cancel(),
            RequestCancelResult::TooLateToCancel
        );
        too_late.inspect(&mut entry);
        assert_eq!((&*entry.state, &*entry.phase), ("running", "committing"));
    }
}
