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

use common_meta::key::SchemaMetadataManagerRef;
use store_api::storage::RegionId;

use crate::compaction::compactor::CompactionRegion;
use crate::compaction::picker::CompactionTask;
use crate::compaction::scheduler::state::{CompactingFiles, CompactionExecution, CompactionPhase};
use crate::compaction::scheduler::{CompactionScheduler, CompactionTransition};
use crate::compaction::task::CompactionTaskImpl;
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
    /// Scheduler-held input lease, released when the terminal result is handled.
    execution: Option<CompactionExecution>,
    state: CancellableTaskState,
    finished: bool,
}

/// A request's local units. All transitions run on the owning region worker.
#[derive(Debug)]
pub(super) struct LocalCompaction {
    units: HashMap<u64, UnitExecution>,
    pending: VecDeque<CompactionTaskImpl>,
    /// Submitted units awaiting task exit, including queued tasks.
    running: usize,
    parallelism: usize,
    error: Option<Arc<Error>>,
    made_progress: bool,
}

impl LocalCompaction {
    /// Requests cancellation for all units that have not started committing.
    pub(super) fn request_cancel(&self) -> RequestCancelResult {
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
        result
    }

    /// Checks this attempt's phase without depending on any sibling's apply status.
    fn can_apply(&self, id: u64) -> bool {
        self.units
            .get(&id)
            .is_some_and(|unit| !unit.finished && unit.state.commit_started())
    }

    /// Records a terminal result once, but still awaits task exit.
    fn finish(&mut self, id: u64, result: std::result::Result<bool, Arc<Error>>) {
        let Some(unit) = self.units.get_mut(&id) else {
            return;
        };
        if unit.finished {
            return;
        }
        unit.finished = true;
        unit.execution.take();
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
    pub(super) fn prepare_local_units(
        &mut self,
        region: CompactionRegion,
        units: Vec<CompactionUnit>,
        reserved: CompactingFiles,
    ) -> CompactionPhase {
        let mut local = LocalCompaction {
            units: HashMap::new(),
            pending: VecDeque::new(),
            running: 0,
            parallelism: region.max_parallelism.max(1),
            error: None,
            made_progress: false,
        };
        for unit in units {
            let files = reserved.for_inputs(&unit.inputs);
            let id = Self::next_plan_id(&mut self.next_plan_id);
            let execution = CompactionExecution::new(id, files);
            let state = CancellableTaskState::new();
            local.units.insert(
                id,
                UnitExecution {
                    execution: Some(execution.clone()),
                    state: state.clone(),
                    finished: false,
                },
            );
            local.pending.push_back(CompactionTaskImpl {
                state,
                execution,
                compaction_region: region.clone(),
                request_sender: self.request_sender.clone(),
                listener: self.listener.clone(),
                estimated_memory_bytes: unit.estimated_memory_bytes(),
                unit,
                memory_manager: self.memory_manager.clone(),
                memory_policy: self.memory_policy,
                uncommitted: UncommittedSsts::new(
                    region.region_id,
                    region.access_layer.clone(),
                    Some(region.cache_manager.clone()),
                ),
            });
        }
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
    pub(super) fn dispatch_local_units(&mut self, region_id: RegionId) {
        loop {
            let Some(local) = self.local_units_mut(region_id) else {
                return;
            };
            if local.running >= local.parallelism {
                return;
            }
            let Some(mut task) = local.pending.pop_front() else {
                return;
            };
            let id = task.execution.plan_id();
            local.running += 1;
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
                local.units.remove(&id);
                local.running -= 1;
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
        local.units.remove(&id);
        local.running -= 1;
        self.dispatch_local_units(region_id);
        self.finish_local_units(region_id, manifest, schemas).await
    }

    /// Completes the request only after all units exit, using the existing terminal paths.
    pub(super) async fn finish_local_units(
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
                return CompactionTransition::from_pending_ddls(
                    self.finish_compaction_on_cancel(region_id),
                );
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

    #[test]
    fn test_compaction_unit_apply_accepts_independent_executions() {
        let units = [1, 2]
            .into_iter()
            .map(|id| {
                (
                    id,
                    UnitExecution {
                        execution: Some(CompactionExecution::for_test(id)),
                        state: CancellableTaskState::new(),
                        finished: false,
                    },
                )
            })
            .collect();
        let mut local = LocalCompaction {
            units,
            pending: VecDeque::new(),
            running: 2,
            parallelism: 2,
            error: None,
            made_progress: false,
        };
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
}
