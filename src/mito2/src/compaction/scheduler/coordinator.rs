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

#[cfg(test)]
#[path = "coordinator_test.rs"]
mod tests;

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::task::{Context, Poll};
use std::time::Instant;

use common_telemetry::{debug, error};
use futures::FutureExt;
use store_api::storage::RegionId;
use tokio::sync::Notify;

use crate::error::{InvalidSchedulerStateSnafu, Result};
use crate::metrics::{
    COMPACTION_ADMISSION_WAIT, COMPACTION_ADMITTED_UNITS, COMPACTION_READY_UNITS,
};
use crate::schedule::CancellableTaskState;
use crate::schedule::scheduler::{Job, LocalScheduler, SchedulerRef};

/// Prepared work consumed on submission to the coordinator, preventing repeated
/// admission of the same unit. Its task owns the execution ID, cancellation state
/// and input leases.
pub(crate) struct ReadyUnit {
    /// Time the unit became ready; feeds `COMPACTION_ADMISSION_WAIT`.
    queued_at: Instant,
    /// Cancellation state shared with the unit's task and the region worker.
    state: CancellableTaskState,
    /// Immutable merge-work estimate; zero for metadata-only expiration.
    input_bytes: u64,
    /// Execution future that owns the reserved input leases.
    job: Job,
    /// Runs only after `job` and its resources are dropped, on every path:
    /// admission, withdrawal, executor rejection, panic and shutdown.
    completion: Job,
}

impl ReadyUnit {
    /// Creates ready work.
    ///
    /// `job` owns the reserved inputs and must be dropped before `completion`
    /// runs; callers must not poll or drop the two in the opposite order.
    pub(crate) fn new(
        state: CancellableTaskState,
        input_bytes: u64,
        job: Job,
        completion: Job,
    ) -> Self {
        Self {
            queued_at: Instant::now(),
            state,
            input_bytes,
            job,
            completion,
        }
    }

    fn release(self) {
        drop(self.job);
        common_runtime::spawn_global(self.completion);
    }
}

/// Engine-wide compaction admission, in front of the generic FIFO executor.
/// Only admitted jobs enter that executor, so its queue cannot hide contenders.
pub(crate) struct CompactionCoordinator {
    /// Executor that runs admitted units. [`Self::new_local`] derives its runner
    /// count from the same value as `capacity`.
    executor: SchedulerRef,
    /// Maximum number of units holding capacity at once, including units the
    /// executor has not polled yet. Clamped to at least one.
    capacity: usize,
    /// Admission accounting shared by every region worker. [`Self::lock`]
    /// deliberately ignores poisoning because an unwind leaves the accounting
    /// consistent.
    state: Mutex<CoordinatorState>,
    /// Signal that wakes [`Self::stop`] when `running` or the ready index changes.
    drained: Notify,
}

/// Admission accounting shared by every region worker.
#[derive(Default)]
struct CoordinatorState {
    /// Per-region service and ready units. Idle regions stay here until they are
    /// unloaded, so batch boundaries cannot erase their service debt.
    regions: HashMap<RegionId, RegionQueue>,
    /// Iteration scales with current ready regions, not a hash table's peak capacity.
    /// Idle regions retain service debt in `regions` without participating in admission.
    ready_regions: BTreeSet<RegionId>,
    /// Units holding capacity from admission through their released future,
    /// including work that was authorized but never polled.
    running: usize,
    /// Set by [`CompactionCoordinator::stop`]; later submissions are rejected.
    closed: bool,
    /// Non-recursive dispatch guard: only the current drainer admits and submits.
    dispatching: bool,
    /// Monotonic floor of observed [`RegionQueue::service`], in the same
    /// half-byte unit. It clamps a region that slept through a busy period.
    virtual_time: u128,
    /// Global admission tick. A larger value means a later admission; it breaks
    /// service ties and is unrelated to the WAL or row `sequence`.
    sequence: u64,
}

/// One region's ready units and accumulated service.
struct RegionQueue {
    /// Prepared units in fair-picker order. Admission pops the front.
    ready: VecDeque<ReadyUnit>,
    /// Units of this region currently holding capacity.
    running: usize,
    /// Weighted service consumed so far, in half-byte units: each admission adds
    /// its charge, and only work refunded before its first poll is subtracted.
    service: u128,
    /// [`CoordinatorState::sequence`] of this region's last admission. Service
    /// ties go to the region whose last admission is older.
    last_served: u64,
    /// Set by [`CompactionCoordinator::forget_region`]; the entry is pruned once
    /// it has no ready or running work.
    retired: bool,
}

/// The executor owns this future from admission until all task resources exit.
/// Drop ordering also covers rejection, panic and shutdown, not only success.
struct AdmittedUnit {
    job: Option<Job>,
    completion: Option<Job>,
    state: CancellableTaskState,
    coordinator: Weak<CompactionCoordinator>,
    region_id: RegionId,
    /// Immutable charge, including admission-time weight; refunded only before first poll.
    charged_cost: u128,
    started: bool,
}

impl std::fmt::Debug for CompactionCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.lock();
        f.debug_struct("CompactionCoordinator")
            .field("capacity", &self.capacity)
            .field("running", &state.running)
            .field("regions", &state.regions.len())
            .finish()
    }
}

impl CompactionCoordinator {
    /// Builds a coordinator over `executor` with an independent capacity.
    ///
    /// Production code should prefer [`Self::new_local`], which derives both
    /// numbers from one value; the admission premise only holds while fewer
    /// units are admitted than the executor can run at once.
    pub(crate) fn new(executor: SchedulerRef, capacity: usize) -> Self {
        Self {
            executor,
            capacity: capacity.max(1),
            state: Mutex::new(CoordinatorState::default()),
            drained: Notify::new(),
        }
    }

    /// Builds a coordinator whose executor runs at most `concurrency` units.
    ///
    /// Capacity and runner count come from the same value, so admitted work can
    /// never wait in the executor's FIFO behind already-admitted contenders.
    pub(crate) fn new_local(concurrency: usize) -> Self {
        let concurrency = concurrency.max(1);
        Self::new(Arc::new(LocalScheduler::new(concurrency)), concurrency)
    }

    fn lock(&self) -> MutexGuard<'_, CoordinatorState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Accepts a bounded batch's ready work, not an unbounded discovered backlog.
    pub(crate) fn schedule(self: &Arc<Self>, region_id: RegionId, unit: ReadyUnit) -> Result<()> {
        {
            let mut state = self.lock();
            if state.closed {
                return InvalidSchedulerStateSnafu.fail();
            }
            // A cancelled manual unit can still be in the worker's pending
            // list when cancellation withdraws its coordinator-visible siblings.
            if unit.state.is_cancelled() {
                drop(state);
                unit.release();
                return Ok(());
            }
            let service = state.virtual_time;
            let queue = state
                .regions
                .entry(region_id)
                .or_insert_with(|| RegionQueue {
                    ready: VecDeque::new(),
                    running: 0,
                    service,
                    last_served: 0,
                    retired: false,
                });
            queue.retired = false;
            // Idle periods cannot bank unlimited credit against active regions.
            if queue.ready.is_empty() {
                queue.service = queue.service.max(service);
            }
            queue.ready.push_back(unit);
            state.ready_regions.insert(region_id);
            COMPACTION_READY_UNITS.inc();
        }
        self.dispatch();
        Ok(())
    }

    /// Withdraws cancelled queued work without waiting for an execution slot.
    /// Only the owning worker can cancel a unit; a reopened region's new units
    /// have different cancellation state and are not affected by an old drop.
    pub(crate) fn cancel_pending(self: &Arc<Self>, region_id: RegionId) {
        let cancelled = {
            let mut state = self.lock();
            let Some(queue) = state.regions.get_mut(&region_id) else {
                return;
            };
            let mut cancelled = Vec::new();
            let mut ready = VecDeque::new();
            for unit in queue.ready.drain(..) {
                if unit.state.is_cancelled() {
                    cancelled.push(unit);
                } else {
                    ready.push_back(unit);
                }
            }
            queue.ready = ready;
            if queue.ready.is_empty() {
                state.ready_regions.remove(&region_id);
            }
            state.prune(region_id);
            cancelled
        };
        for unit in cancelled {
            Self::withdraw(unit);
        }
        self.drained.notify_one();
    }

    /// Keep service debt between batches, but not after the region is unloaded.
    /// In-flight old work can drain before its accounting entry is reclaimed.
    pub(crate) fn forget_region(&self, region_id: RegionId) {
        let mut state = self.lock();
        if let Some(queue) = state.regions.get_mut(&region_id) {
            queue.retired = true;
        }
        state.prune(region_id);
    }

    /// Drains the admission queue before stopping its executor. Workers remain
    /// alive to acknowledge apply while these jobs drain.
    pub(crate) async fn stop(&self) -> Result<()> {
        self.lock().closed = true;
        loop {
            let notified = self.drained.notified();
            let drained = {
                let state = self.lock();
                state.running == 0 && state.ready_regions.is_empty()
            };
            if drained {
                break;
            }
            notified.await;
        }
        self.executor.stop(true).await
    }

    fn dispatch(self: &Arc<Self>) {
        {
            let mut state = self.lock();
            if state.dispatching {
                return;
            }
            state.dispatching = true;
        }
        loop {
            let next = {
                let mut state = self.lock();
                let next = (state.running < self.capacity)
                    .then(|| state.admit())
                    .flatten();
                if next.is_none() {
                    state.dispatching = false;
                }
                next
            };
            let Some((region_id, unit, charged_cost)) = next else {
                break;
            };
            let admitted = AdmittedUnit::new(self, region_id, unit, charged_cost);
            // Rejection drops the future and returns its credit/resources. The
            // existing Released path reports a missing terminal result as failure.
            if let Err(err) = self.executor.schedule(Box::pin(admitted)) {
                error!(err; "Failed to submit admitted compaction for region {}", region_id);
            }
        }
    }

    fn release(self: &Arc<Self>, region_id: RegionId, refund: u128) {
        {
            let mut state = self.lock();
            state.running -= 1;
            if let Some(queue) = state.regions.get_mut(&region_id) {
                queue.running -= 1;
                queue.service = queue.service.saturating_sub(refund);
            }
            state.prune(region_id);
        }
        self.drained.notify_one();
        // dispatching serializes drainers without holding a lock across executor
        // submission. In particular, synchronous rejection cannot recurse here.
        self.dispatch();
    }

    fn withdraw(unit: ReadyUnit) {
        COMPACTION_READY_UNITS.dec();
        unit.release();
    }
}

impl Drop for CompactionCoordinator {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for queue in state.regions.values_mut() {
            for unit in queue.ready.drain(..) {
                Self::withdraw(unit);
            }
        }
    }
}

impl CoordinatorState {
    /// Least-served selection charges bytes at admission, including jobs that
    /// have not started. A backlog can boost weight by at most two; no region can
    /// obtain unlimited weight. Positive byte cost prevents zero-sized work from
    /// monopolizing ties. Queue order retains the region's fair picker priority.
    fn admit(&mut self) -> Option<(RegionId, ReadyUnit, u128)> {
        let region_id = self
            .ready_regions
            .iter()
            .filter_map(|id| self.regions.get(id).map(|queue| (id, queue)))
            .min_by_key(|(id, q)| (q.service, q.last_served, **id))
            .map(|(id, _)| *id)?;
        let queue = self.regions.get_mut(&region_id)?;
        let weight = if queue.ready.len() > 1 { 2 } else { 1 };
        let unit = queue.ready.pop_front()?;
        if queue.ready.is_empty() {
            self.ready_regions.remove(&region_id);
        }
        COMPACTION_READY_UNITS.dec();
        COMPACTION_ADMITTED_UNITS.inc();
        self.virtual_time = self.virtual_time.max(queue.service);
        let charged_cost = charge(unit.input_bytes, weight);
        queue.service = queue.service.saturating_add(charged_cost);
        self.sequence = self.sequence.wrapping_add(1);
        queue.last_served = self.sequence;
        queue.running += 1;
        self.running += 1;
        debug!(
            "Admit compaction unit: region={}, input_bytes={}, weight={}, service={}, ready={}, admitted={}",
            region_id,
            unit.input_bytes,
            weight,
            queue.service,
            queue.ready.len(),
            queue.running,
        );
        Some((region_id, unit, charged_cost))
    }

    fn prune(&mut self, region_id: RegionId) {
        if self
            .regions
            .get(&region_id)
            .is_some_and(|q| q.retired && q.running == 0 && q.ready.is_empty())
        {
            self.regions.remove(&region_id);
        }
    }
}

fn charge(bytes: u64, weight: u128) -> u128 {
    // Half-byte units avoid rounding a small task's cost down to zero.
    u128::from(bytes.max(1)) * 2 / weight
}

impl AdmittedUnit {
    fn new(
        coordinator: &Arc<CompactionCoordinator>,
        region_id: RegionId,
        unit: ReadyUnit,
        charged_cost: u128,
    ) -> Self {
        COMPACTION_ADMISSION_WAIT.observe(unit.queued_at.elapsed().as_secs_f64());
        let job = Box::pin(async move {
            if let Err(panic) = std::panic::AssertUnwindSafe(unit.job).catch_unwind().await {
                error!(
                    "Compaction executor panicked: {}",
                    crate::compaction::format_panic_reason(panic.as_ref())
                );
            }
        });
        Self {
            job: Some(job),
            completion: Some(unit.completion),
            state: unit.state,
            coordinator: Arc::downgrade(coordinator),
            region_id,
            charged_cost,
            started: false,
        }
    }
}

impl Future for AdmittedUnit {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if !self.started && self.state.is_cancelled() {
            return Poll::Ready(());
        }
        self.started = true;
        let Some(job) = self.job.as_mut() else {
            return Poll::Ready(());
        };
        let poll = job.as_mut().poll(cx);
        if poll.is_ready() {
            // A completed future stays completed if the executor polls it again.
            drop(self.job.take());
        }
        poll
    }
}

impl Drop for AdmittedUnit {
    fn drop(&mut self) {
        // This ordering is essential even when the executor rejects the future
        // before its first poll: Released must never precede input lease release.
        drop(self.job.take());
        COMPACTION_ADMITTED_UNITS.dec();
        if let Some(coordinator) = self.coordinator.upgrade() {
            let refund = if self.started { 0 } else { self.charged_cost };
            coordinator.release(self.region_id, refund);
        }
        if let Some(completion) = self.completion.take() {
            common_runtime::spawn_global(completion);
        }
    }
}
