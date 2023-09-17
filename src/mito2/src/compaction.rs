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
use crate::error::{
    CompactRegionSnafu, Error, RegionClosedSnafu, RegionDroppedSnafu, RegionTruncatedSnafu, Result,
};
use crate::region::version::{VersionControlRef, VersionRef};
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
        region_id: RegionId,
        version_control: &VersionControlRef,
        access_layer: &AccessLayerRef,
        file_purger: &FilePurgerRef,
        waiter: OptionOutputTx,
    ) -> Result<()> {
        let status = self.region_status.entry(region_id).or_insert_with(|| {
            CompactionStatus::new(
                region_id,
                version_control.clone(),
                access_layer.clone(),
                file_purger.clone(),
            )
        });
        if status.compacting {
            // Region is compacting. Add the waiter to pending list.
            status.merge_waiter(waiter);
            return Ok(());
        }

        // The region can compact directly.
        let request = status.new_compaction_request(self.request_sender.clone(), waiter);
        // Mark the region as compacting.
        status.compacting = true;
        self.schedule_compaction_request(request)
    }

    /// Notifies the scheduler that the compaction job is finished successfully.
    pub(crate) fn on_compaction_finished(&mut self, region_id: RegionId) {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return;
        };
        status.compacting = false;
        // We should always try to compact the region until picker returns None.
        let request =
            status.new_compaction_request(self.request_sender.clone(), OptionOutputTx::none());
        // Try to schedule next compaction task for this region.
        if let Err(e) = self.schedule_compaction_request(request) {
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

    /// Schedules a compaction request.
    ///
    /// If the region has nothing to compact, it removes the region from the status map.
    fn schedule_compaction_request(&mut self, request: CompactionRequest) -> Result<()> {
        // TODO(hl): build picker according to region options.
        let picker =
            compaction_strategy_to_picker(&CompactionStrategy::Twcs(TwcsOptions::default()));
        let region_id = request.region_id();
        debug!(
            "Pick compaction strategy {:?} for region: {}",
            picker, region_id
        );
        let Some(mut task) = picker.pick(request) else {
            // Nothing to compact, remove it from the region status map.
            self.region_status.remove(&region_id);
            return Ok(());
        };

        // Submit the compaction task.
        self.scheduler
            .schedule(Box::pin(async move {
                task.run().await;
            }))
            .map_err(|e| {
                error!(e; "Failed to submit compaction request for region {}", region_id);

                // If failed to submit the job, we need to remove the region from the scheduler.
                self.region_status.remove(&region_id);

                e
            })
    }

    fn remove_region_on_failure(&mut self, region_id: RegionId, err: Arc<Error>) {
        // Remove this region.
        let Some(status) = self.region_status.remove(&region_id) else {
            return;
        };

        // Notifies all pending tasks.
        status.on_failure(err);
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
    /// Id of the region.
    region_id: RegionId,
    /// Version control of the region.
    version_control: VersionControlRef,
    /// Access layer of the region.
    access_layer: AccessLayerRef,
    /// File purger of the region.
    file_purger: FilePurgerRef,
    /// Whether a compaction task is running.
    compacting: bool,
    /// Compaction pending to schedule.
    ///
    /// For simplicity, we merge all pending compaction requests into one.
    pending_compaction: Option<PendingCompaction>,
}

impl CompactionStatus {
    /// Creates a new [CompactionStatus]
    fn new(
        region_id: RegionId,
        version_control: VersionControlRef,
        access_layer: AccessLayerRef,
        file_purger: FilePurgerRef,
    ) -> CompactionStatus {
        CompactionStatus {
            region_id,
            version_control,
            access_layer,
            file_purger,
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
            pending.on_failure(self.region_id, err.clone());
        }
    }

    /// Creates a new compaction request for compaction picker.
    ///
    /// It consumes all pending compaction waiters.
    fn new_compaction_request(
        &mut self,
        request_sender: Sender<WorkerRequest>,
        waiter: OptionOutputTx,
    ) -> CompactionRequest {
        let current_version = self.version_control.current().version;
        let mut req = CompactionRequest {
            current_version,
            access_layer: self.access_layer.clone(),
            // TODO(hl): get TTL info from region metadata
            ttl: None,
            // TODO(hl): get persisted region compaction time window
            compaction_time_window: None,
            request_sender: request_sender.clone(),
            waiters: Vec::new(),
            file_purger: self.file_purger.clone(),
        };

        if let Some(pending) = self.pending_compaction.take() {
            req.waiters = pending.waiters;
        }
        req.push_waiter(waiter);

        req
    }
}

#[cfg(test)]
mod tests {
    use common_query::Output;
    use tokio::sync::oneshot;

    use super::*;
    use crate::test_util::scheduler_util::SchedulerEnv;
    use crate::test_util::version_util::VersionControlBuilder;

    #[tokio::test]
    async fn test_schedule_empty() {
        let env = SchedulerEnv::new();
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
        let purger = builder.file_purger();

        // Nothing to compact.
        let version_control = Arc::new(builder.build());
        let (output_tx, output_rx) = oneshot::channel();
        let waiter = OptionOutputTx::from(output_tx);
        scheduler
            .schedule_compaction(
                builder.region_id(),
                &version_control,
                &env.access_layer,
                &purger,
                waiter,
            )
            .unwrap();
        let output = output_rx.await.unwrap().unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));
        assert!(scheduler.region_status.is_empty());

        // Only one file, picker won't compact it.
        let version_control = Arc::new(builder.push_l0_file(0, 1000).build());
        let (output_tx, output_rx) = oneshot::channel();
        let waiter = OptionOutputTx::from(output_tx);
        scheduler
            .schedule_compaction(
                builder.region_id(),
                &version_control,
                &env.access_layer,
                &purger,
                waiter,
            )
            .unwrap();
        let output = output_rx.await.unwrap().unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));
        assert!(scheduler.region_status.is_empty());
    }
}
