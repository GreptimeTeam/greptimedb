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

mod output;
mod picker;
#[cfg(test)]
mod test_util;
mod twcs;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use common_telemetry::{debug, error};
pub use picker::CompactionPickerRef;
use snafu::ResultExt;
use store_api::storage::{CompactionStrategy, RegionId, TwcsOptions};
use tokio::sync::mpsc::{self, Sender};

use crate::access_layer::AccessLayerRef;
use crate::compaction::twcs::TwcsPicker;
use crate::error::{CompactRegionSnafu, Error, RegionClosedSnafu, RegionDroppedSnafu, Result};
use crate::region::version::VersionRef;
use crate::region::MitoRegionRef;
use crate::request::{OptionOutputTx, OutputTx, WorkerRequest};
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file_purger::FilePurgerRef;

/// Region compaction request.
pub struct CompactionRequest {
    pub(crate) current_version: VersionRef,
    pub(crate) access_layer: AccessLayerRef,
    pub(crate) ttl: Option<Duration>,
    pub(crate) compaction_time_window: Option<i64>,
    /// Sender to send notification to the region worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,
    /// Waiters of the compaction request.
    pub(crate) waiters: Vec<OutputTx>,
    pub(crate) file_purger: FilePurgerRef,
}

impl CompactionRequest {
    pub(crate) fn region_id(&self) -> RegionId {
        self.current_version.metadata.region_id
    }

    /// Push waiter to the request.
    pub(crate) fn push_waiter(&mut self, mut waiter: OptionOutputTx) {
        if let Some(waiter) = waiter.take_inner() {
            self.waiters.push(waiter);
        }
    }
}

/// Builds compaction picker according to [CompactionStrategy].
pub fn compaction_strategy_to_picker(strategy: &CompactionStrategy) -> CompactionPickerRef {
    match strategy {
        CompactionStrategy::Twcs(twcs_opts) => Arc::new(TwcsPicker::new(
            twcs_opts.max_active_window_files,
            twcs_opts.max_inactive_window_files,
            twcs_opts.time_window_seconds,
        )) as Arc<_>,
    }
}

/// Compaction scheduler tracks and manages compaction tasks.
pub(crate) struct CompactionScheduler {
    scheduler: SchedulerRef,
    /// Compacting regions.
    region_status: HashMap<RegionId, CompactionStatus>,
    /// Request sender of the worker that this scheduler belongs to.
    request_sender: Sender<WorkerRequest>,
}

impl CompactionScheduler {
    pub(crate) fn new(scheduler: SchedulerRef, request_sender: Sender<WorkerRequest>) -> Self {
        Self {
            scheduler,
            region_status: HashMap::new(),
            request_sender,
        }
    }

    /// Schedules a compaction for the region.
    pub(crate) fn schedule_compaction(
        &mut self,
        region: &MitoRegionRef,
        waiter: OptionOutputTx,
    ) -> Result<()> {
        let status = self
            .region_status
            .entry(region.region_id)
            .or_insert_with(|| CompactionStatus::new(region.clone()));
        if status.compacting {
            // Region is compacting. Add the waiter to pending list.
            status.merge_waiter(waiter);
            return Ok(());
        }

        // The region can compact directly.
        let mut req = Self::new_compaction_request(self.request_sender.clone(), region);
        // Merge with all pending requests.
        if let Some(pending) = status.pending_compaction.take() {
            req.waiters = pending.waiters;
        }
        req.push_waiter(waiter);

        // Submit compaction request.
        self.submit_request(req)
    }

    /// Notifies the scheduler that the compaction job is finished successfully.
    pub(crate) fn on_compaction_finished(&mut self, region_id: RegionId) {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return;
        };

        if status.pending_compaction.is_none() {
            // The region doesn't have pending compaction request, we can remove it.
            self.region_status.remove(&region_id);
            return;
        }

        let region = status.region.clone();
        // Try to schedule next compaction task for this region.
        if let Err(e) = self.schedule_compaction(&region, OptionOutputTx::none()) {
            error!(e; "Failed to schedule next compaction for region {}", region_id);
        }
    }

    /// Notifies the scheduler that the compaction job is failed.
    pub(crate) fn on_compaction_failed(&mut self, region_id: RegionId, err: Arc<Error>) {
        error!(err; "Region {} failed to flush, cancel all pending tasks", region_id);
        // Remove this region.
        let Some(status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Fast fail: cancels all pending tasks and sends error to their waiters.
        status.on_failure(err);
    }

    /// Notifies the scheduler that the region is dropped.
    pub(crate) fn on_region_dropped(&mut self, region_id: RegionId) {
        // Remove this region.
        let Some(status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Notifies all pending tasks.
        status.on_failure(Arc::new(RegionDroppedSnafu { region_id }.build()));
    }

    /// Notifies the scheduler that the region is closed.
    pub(crate) fn on_region_closed(&mut self, region_id: RegionId) {
        // Remove this region.
        let Some(status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Notifies all pending tasks.
        status.on_failure(Arc::new(RegionClosedSnafu { region_id }.build()));
    }

    /// Submit a compaction request.
    ///
    /// Callers must ensure that the region doesn't have compaction task running.
    fn submit_request(&mut self, req: CompactionRequest) -> Result<()> {
        let region_id = req.region_id();
        self.scheduler
            .schedule(Box::pin(async {
                // TODO(hl): build picker according to region options.
                let picker = compaction_strategy_to_picker(&CompactionStrategy::Twcs(
                    TwcsOptions::default(),
                ));
                debug!(
                    "Pick compaction strategy {:?} for region: {}",
                    picker,
                    req.region_id()
                );
                // FIXME(yingwen): We should remove the region from region status.
                let Some(mut task) = picker.pick(req) else {
                    return;
                };
                task.run().await;
            }))
            .map_err(|e| {
                error!(e; "Failed to submit compaction request for region {}", region_id);

                // If failed to submit the job, we need to remove the region from the scheduler.
                self.region_status.remove(&region_id);

                e
            })
    }

    /// Creates a new compaction request for compaction picker.
    fn new_compaction_request(
        request_sender: Sender<WorkerRequest>,
        region: &MitoRegionRef,
    ) -> CompactionRequest {
        let current_version = region.version_control.current().version;
        let access_layer = region.access_layer.clone();
        let file_purger = region.file_purger.clone();

        let req = CompactionRequest {
            current_version,
            access_layer,
            ttl: None,                    // TODO(hl): get TTL info from region metadata
            compaction_time_window: None, // TODO(hl): get persisted region compaction time window
            request_sender: request_sender.clone(),
            waiters: Vec::new(),
            file_purger,
        };

        req
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

/// Pending compaction tasks.
struct PendingCompaction {
    waiters: Vec<OutputTx>,
}

impl PendingCompaction {
    /// Push waiter to the request.
    fn push_waiter(&mut self, mut waiter: OptionOutputTx) {
        if let Some(waiter) = waiter.take_inner() {
            self.waiters.push(waiter);
        }
    }

    /// Send flush error to waiter.
    fn on_failure(&mut self, region_id: RegionId, err: Arc<Error>) {
        for waiter in self.waiters.drain(..) {
            waiter.send(Err(err.clone()).context(CompactRegionSnafu { region_id }));
        }
    }
}

/// Status of running and pending region compaction tasks.
struct CompactionStatus {
    region: MitoRegionRef,
    /// Whether a compaction task is running.
    ///
    /// It may be failed to submit a compaction task so we need this flag to track
    /// whether the region is compacting.
    compacting: bool,
    /// Compaction pending to schedule.
    ///
    /// For simplicity, we merge all pending compaction requests into one.
    pending_compaction: Option<PendingCompaction>,
}

impl CompactionStatus {
    /// Creates a new [CompactionStatus]
    fn new(region: MitoRegionRef) -> CompactionStatus {
        CompactionStatus {
            region,
            compacting: false,
            pending_compaction: None,
        }
    }

    /// Merge the watier to the pending compaction.
    fn merge_waiter(&mut self, waiter: OptionOutputTx) {
        let pending = self
            .pending_compaction
            .get_or_insert_with(|| PendingCompaction {
                waiters: Vec::new(),
            });
        pending.push_waiter(waiter);
    }

    fn on_failure(self, err: Arc<Error>) {
        if let Some(mut pending) = self.pending_compaction {
            pending.on_failure(self.region.region_id, err.clone());
        }
    }
}
