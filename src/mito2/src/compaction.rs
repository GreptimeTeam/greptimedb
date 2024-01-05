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

mod picker;
#[cfg(test)]
mod test_util;
mod twcs;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common_base::readable_size::ReadableSize;
use common_telemetry::{debug, error};
pub use picker::CompactionPickerRef;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{self, Sender};

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::compaction::twcs::TwcsPicker;
use crate::config::MitoConfig;
use crate::error::{
    CompactRegionSnafu, Error, RegionClosedSnafu, RegionDroppedSnafu, RegionTruncatedSnafu, Result,
};
use crate::metrics::COMPACTION_STAGE_ELAPSED;
use crate::region::options::CompactionOptions;
use crate::region::version::{VersionControlRef, VersionRef};
use crate::request::{OptionOutputTx, OutputTx, WorkerRequest};
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file_purger::FilePurgerRef;

/// Region compaction request.
pub struct CompactionRequest {
    pub(crate) current_version: VersionRef,
    pub(crate) access_layer: AccessLayerRef,
    /// Sender to send notification to the region worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,
    /// Waiters of the compaction request.
    pub(crate) waiters: Vec<OutputTx>,
    pub(crate) file_purger: FilePurgerRef,
    /// Start time of compaction task.
    pub(crate) start_time: Instant,
    /// Buffering threshold while writing SST files.
    pub(crate) sst_write_buffer_size: ReadableSize,
    pub(crate) cache_manager: CacheManagerRef,
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

/// Builds compaction picker according to [CompactionOptions].
pub fn compaction_options_to_picker(strategy: &CompactionOptions) -> CompactionPickerRef {
    match strategy {
        CompactionOptions::Twcs(twcs_opts) => Arc::new(TwcsPicker::new(
            twcs_opts.max_active_window_files,
            twcs_opts.max_inactive_window_files,
            twcs_opts.time_window_seconds(),
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
    cache_manager: CacheManagerRef,
}

impl CompactionScheduler {
    pub(crate) fn new(
        scheduler: SchedulerRef,
        request_sender: Sender<WorkerRequest>,
        cache_manager: CacheManagerRef,
    ) -> Self {
        Self {
            scheduler,
            region_status: HashMap::new(),
            request_sender,
            cache_manager,
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
        engine_config: Arc<MitoConfig>,
    ) -> Result<()> {
        if let Some(status) = self.region_status.get_mut(&region_id) {
            // Region is compacting. Add the waiter to pending list.
            status.merge_waiter(waiter);
            return Ok(());
        }

        // The region can compact directly.
        let mut status = CompactionStatus::new(
            region_id,
            version_control.clone(),
            access_layer.clone(),
            file_purger.clone(),
        );
        let request = status.new_compaction_request(
            self.request_sender.clone(),
            waiter,
            engine_config,
            self.cache_manager.clone(),
        );
        self.region_status.insert(region_id, status);
        self.schedule_compaction_request(request)
    }

    /// Notifies the scheduler that the compaction job is finished successfully.
    pub(crate) fn on_compaction_finished(
        &mut self,
        region_id: RegionId,
        engine_config: Arc<MitoConfig>,
    ) {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return;
        };
        // We should always try to compact the region until picker returns None.
        let request = status.new_compaction_request(
            self.request_sender.clone(),
            OptionOutputTx::none(),
            engine_config,
            self.cache_manager.clone(),
        );
        // Try to schedule next compaction task for this region.
        if let Err(e) = self.schedule_compaction_request(request) {
            error!(e; "Failed to schedule next compaction for region {}", region_id);
        }
    }

    /// Notifies the scheduler that the compaction job is failed.
    pub(crate) fn on_compaction_failed(&mut self, region_id: RegionId, err: Arc<Error>) {
        error!(err; "Region {} failed to compact, cancel all pending tasks", region_id);
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
        let picker = compaction_options_to_picker(&request.current_version.options.compaction);
        let region_id = request.region_id();
        debug!(
            "Pick compaction strategy {:?} for region: {}",
            picker, region_id
        );

        let pick_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["pick"])
            .start_timer();
        let Some(mut task) = picker.pick(request) else {
            // Nothing to compact, remove it from the region status map.
            self.region_status.remove(&region_id);
            return Ok(());
        };
        drop(pick_timer);

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

    /// Send compaction error to waiter.
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
        engine_config: Arc<MitoConfig>,
        cache_manager: CacheManagerRef,
    ) -> CompactionRequest {
        let current_version = self.version_control.current().version;
        let start_time = Instant::now();
        let mut req = CompactionRequest {
            current_version,
            access_layer: self.access_layer.clone(),
            request_sender: request_sender.clone(),
            waiters: Vec::new(),
            file_purger: self.file_purger.clone(),
            start_time,
            sst_write_buffer_size: engine_config.sst_write_buffer_size,
            cache_manager,
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
    use std::sync::Mutex;

    use tokio::sync::oneshot;

    use super::*;
    use crate::schedule::scheduler::{Job, Scheduler};
    use crate::test_util::scheduler_util::SchedulerEnv;
    use crate::test_util::version_util::{apply_edit, VersionControlBuilder};

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
                Arc::new(MitoConfig::default()),
            )
            .unwrap();
        let output = output_rx.await.unwrap().unwrap();
        assert_eq!(output, 0);
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
                Arc::new(MitoConfig::default()),
            )
            .unwrap();
        let output = output_rx.await.unwrap().unwrap();
        assert_eq!(output, 0);
        assert!(scheduler.region_status.is_empty());
    }

    #[derive(Default)]
    struct VecScheduler {
        jobs: Mutex<Vec<Job>>,
    }

    impl VecScheduler {
        fn num_jobs(&self) -> usize {
            self.jobs.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl Scheduler for VecScheduler {
        fn schedule(&self, job: Job) -> Result<()> {
            self.jobs.lock().unwrap().push(job);
            Ok(())
        }

        async fn stop(&self, _await_termination: bool) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_schedule_on_finished() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().scheduler(job_scheduler.clone());
        let (tx, _rx) = mpsc::channel(4);
        let mut scheduler = env.mock_compaction_scheduler(tx);
        let mut builder = VersionControlBuilder::new();
        let purger = builder.file_purger();
        let region_id = builder.region_id();

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
        scheduler
            .schedule_compaction(
                region_id,
                &version_control,
                &env.access_layer,
                &purger,
                OptionOutputTx::none(),
                Arc::new(MitoConfig::default()),
            )
            .unwrap();
        // Should schedule 1 compaction.
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(1, job_scheduler.num_jobs());
        let data = version_control.current();
        let file_metas: Vec<_> = data.version.ssts.levels()[0]
            .files
            .values()
            .map(|file| file.meta())
            .collect();

        // 5 files for next compaction and removes old files.
        apply_edit(
            &version_control,
            &[(0, end), (20, end), (40, end), (60, end), (80, end)],
            &file_metas,
            purger.clone(),
        );
        // The task is pending.
        scheduler
            .schedule_compaction(
                region_id,
                &version_control,
                &env.access_layer,
                &purger,
                OptionOutputTx::none(),
                Arc::new(MitoConfig::default()),
            )
            .unwrap();
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(1, job_scheduler.num_jobs());
        assert!(scheduler
            .region_status
            .get(&builder.region_id())
            .unwrap()
            .pending_compaction
            .is_some());

        // On compaction finished and schedule next compaction.
        scheduler.on_compaction_finished(region_id, Arc::new(MitoConfig::default()));
        assert_eq!(1, scheduler.region_status.len());
        assert_eq!(2, job_scheduler.num_jobs());
        // 5 files for next compaction.
        apply_edit(
            &version_control,
            &[(0, end), (20, end), (40, end), (60, end), (80, end)],
            &[],
            purger.clone(),
        );
        // The task is pending.
        scheduler
            .schedule_compaction(
                region_id,
                &version_control,
                &env.access_layer,
                &purger,
                OptionOutputTx::none(),
                Arc::new(MitoConfig::default()),
            )
            .unwrap();
        assert_eq!(2, job_scheduler.num_jobs());
        assert!(scheduler
            .region_status
            .get(&builder.region_id())
            .unwrap()
            .pending_compaction
            .is_some());
    }
}
