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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common_runtime::{RepeatedTask, TaskFunction};
use common_telemetry::logging;
use snafu::{ensure, ResultExt};
use store_api::logstore::LogStore;
use store_api::storage::{RegionId, SequenceNumber};
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::{oneshot, Notify};

use crate::compaction::{CompactionPickerRef, CompactionRequestImpl, CompactionSchedulerRef};
use crate::config::EngineConfig;
use crate::engine::RegionMap;
use crate::error::{
    DuplicateFlushSnafu, Error, Result, StartPickTaskSnafu, StopPickTaskSnafu, WaitFlushSnafu,
};
use crate::flush::{FlushJob, FlushPicker, PickerConfig};
use crate::manifest::region::RegionManifest;
use crate::memtable::{MemtableId, MemtableRef};
use crate::metrics::FLUSH_ERRORS_TOTAL;
use crate::region;
use crate::region::{RegionWriterRef, SharedDataRef};
use crate::scheduler::rate_limit::BoxedRateLimitToken;
use crate::scheduler::{Handler, LocalScheduler, Request, Scheduler, SchedulerConfig};
use crate::sst::AccessLayerRef;
use crate::wal::Wal;

/// Key for [FlushRequest].
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum FlushKey {
    Engine,
    Region(RegionId, SequenceNumber),
}

/// Flush request.
pub enum FlushRequest<S: LogStore> {
    /// Flush the engine.
    Engine,
    /// Flush a region.
    Region {
        /// Region flush request.
        req: FlushRegionRequest<S>,
        /// Flush result sender.
        sender: Sender<Result<()>>,
    },
}

impl<S: LogStore> Request for FlushRequest<S> {
    type Key = FlushKey;

    #[inline]
    fn key(&self) -> FlushKey {
        match &self {
            FlushRequest::Engine => FlushKey::Engine,
            FlushRequest::Region { req, .. } => {
                FlushKey::Region(req.shared.id(), req.flush_sequence)
            }
        }
    }

    fn complete(self, result: Result<()>) {
        if let FlushRequest::Region { sender, .. } = self {
            let _ = sender.send(result);
        }
    }
}

/// Region flush request.
pub struct FlushRegionRequest<S: LogStore> {
    /// Max memtable id in these memtables,
    /// used to remove immutable memtables in current version.
    pub max_memtable_id: MemtableId,
    /// Memtables to be flushed.
    pub memtables: Vec<MemtableRef>,
    /// Last sequence of data to be flushed.
    pub flush_sequence: SequenceNumber,
    /// Shared data of region to be flushed.
    pub shared: SharedDataRef,
    /// Sst access layer of the region.
    pub sst_layer: AccessLayerRef,
    /// Region writer, used to persist log entry that points to the latest manifest file.
    pub writer: RegionWriterRef<S>,
    /// Region write-ahead logging, used to write data/meta to the log file.
    pub wal: Wal<S>,
    /// Region manifest service, used to persist metadata.
    pub manifest: RegionManifest,
    /// Storage engine config
    pub engine_config: Arc<EngineConfig>,

    // Compaction related options:
    /// TTL of the region.
    pub ttl: Option<Duration>,
    /// Time window for compaction.
    pub compaction_time_window: Option<i64>,
    pub compaction_picker: CompactionPickerRef<S>,
}

impl<S: LogStore> FlushRegionRequest<S> {
    #[inline]
    fn region_id(&self) -> RegionId {
        self.shared.id()
    }
}

impl<S: LogStore> From<&FlushRegionRequest<S>> for FlushJob<S> {
    fn from(req: &FlushRegionRequest<S>) -> FlushJob<S> {
        FlushJob {
            max_memtable_id: req.max_memtable_id,
            memtables: req.memtables.clone(),
            flush_sequence: req.flush_sequence,
            shared: req.shared.clone(),
            sst_layer: req.sst_layer.clone(),
            writer: req.writer.clone(),
            wal: req.wal.clone(),
            manifest: req.manifest.clone(),
            engine_config: req.engine_config.clone(),
        }
    }
}

impl<S: LogStore> From<&FlushRegionRequest<S>> for CompactionRequestImpl<S> {
    fn from(req: &FlushRegionRequest<S>) -> CompactionRequestImpl<S> {
        CompactionRequestImpl {
            region_id: req.region_id(),
            sst_layer: req.sst_layer.clone(),
            writer: req.writer.clone(),
            shared: req.shared.clone(),
            manifest: req.manifest.clone(),
            wal: req.wal.clone(),
            ttl: req.ttl,
            compaction_time_window: req.compaction_time_window,
            sender: None,
            picker: req.compaction_picker.clone(),
            sst_write_buffer_size: req.engine_config.sst_write_buffer_size,
            // compaction triggered by flush always reschedules
            reschedule_on_finish: true,
        }
    }
}

/// A handle to get the flush result.
#[derive(Debug)]
pub struct FlushHandle {
    region_id: RegionId,
    receiver: Receiver<Result<()>>,
}

impl FlushHandle {
    /// Waits until the flush job is finished.
    pub async fn wait(self) -> Result<()> {
        self.receiver.await.context(WaitFlushSnafu {
            region_id: self.region_id,
        })?
    }
}

/// Flush scheduler.
pub struct FlushScheduler<S: LogStore> {
    /// Flush task scheduler.
    scheduler: LocalScheduler<FlushRequest<S>>,
    /// Auto flush task.
    auto_flush_task: RepeatedTask<Error>,
    #[cfg(test)]
    pending_tasks: Arc<tokio::sync::RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}

pub type FlushSchedulerRef<S> = Arc<FlushScheduler<S>>;

impl<S: LogStore> FlushScheduler<S> {
    /// Returns a new [FlushScheduler].
    pub fn new(
        config: SchedulerConfig,
        compaction_scheduler: CompactionSchedulerRef<S>,
        regions: Arc<RegionMap<S>>,
        picker_config: PickerConfig,
    ) -> Result<Self> {
        let task_interval = picker_config.schedule_interval;
        let picker = FlushPicker::new(picker_config);
        // Now we just clone the picker since we don't need to share states and
        // the clone of picker is cheap.
        let task_fn = AutoFlushFunction {
            regions: regions.clone(),
            picker: picker.clone(),
        };
        let auto_flush_task = RepeatedTask::new(task_interval, Box::new(task_fn));
        auto_flush_task
            .start(common_runtime::bg_runtime())
            .context(StartPickTaskSnafu)?;
        #[cfg(test)]
        let pending_tasks = Arc::new(tokio::sync::RwLock::new(vec![]));
        let handler = FlushHandler {
            compaction_scheduler,
            regions,
            picker,
            #[cfg(test)]
            pending_tasks: pending_tasks.clone(),
        };

        Ok(Self {
            scheduler: LocalScheduler::new(config, handler),
            auto_flush_task,
            #[cfg(test)]
            pending_tasks,
        })
    }

    /// Schedules a region flush request and return the handle to the flush task.
    pub fn schedule_region_flush(&self, req: FlushRegionRequest<S>) -> Result<FlushHandle> {
        let region_id = req.region_id();
        let sequence = req.flush_sequence;
        let (sender, receiver) = oneshot::channel();

        let scheduled = self
            .scheduler
            .schedule(FlushRequest::Region { req, sender })?;
        // Normally we should not have duplicate flush request.
        ensure!(
            scheduled,
            DuplicateFlushSnafu {
                region_id,
                sequence,
            }
        );

        Ok(FlushHandle {
            region_id,
            receiver,
        })
    }

    /// Schedules a engine flush request.
    pub fn schedule_engine_flush(&self) -> Result<()> {
        let _ = self.scheduler.schedule(FlushRequest::Engine)?;
        Ok(())
    }

    /// Stop the scheduler.
    pub async fn stop(&self) -> Result<()> {
        self.auto_flush_task
            .stop()
            .await
            .context(StopPickTaskSnafu)?;
        self.scheduler.stop(true).await?;

        #[cfg(test)]
        let _ = futures::future::join_all(self.pending_tasks.write().await.drain(..)).await;

        Ok(())
    }
}

struct FlushHandler<S: LogStore> {
    compaction_scheduler: CompactionSchedulerRef<S>,
    regions: Arc<RegionMap<S>>,
    picker: FlushPicker,
    #[cfg(test)]
    pending_tasks: Arc<tokio::sync::RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}

#[async_trait::async_trait]
impl<S: LogStore> Handler for FlushHandler<S> {
    type Request = FlushRequest<S>;

    async fn handle_request(
        &self,
        req: FlushRequest<S>,
        token: BoxedRateLimitToken,
        finish_notifier: Arc<Notify>,
    ) -> Result<()> {
        let compaction_scheduler = self.compaction_scheduler.clone();
        let region_map = self.regions.clone();
        let picker = self.picker.clone();
        let _handle = common_runtime::spawn_bg(async move {
            match req {
                FlushRequest::Engine => {
                    let regions = region_map.list_regions();
                    picker.pick_by_write_buffer_full(&regions).await;
                }
                FlushRequest::Region { req, sender } => {
                    execute_flush_region(req, sender, compaction_scheduler).await;
                }
            }

            // releases rate limit token
            token.try_release();
            // notify scheduler to schedule next task when current task finishes.
            finish_notifier.notify_one();
        });

        #[cfg(test)]
        self.pending_tasks.write().await.push(_handle);
        Ok(())
    }
}

async fn execute_flush_region<S: LogStore>(
    req: FlushRegionRequest<S>,
    sender: Sender<Result<()>>,
    compaction_scheduler: CompactionSchedulerRef<S>,
) {
    let mut flush_job = FlushJob::from(&req);

    if let Err(e) = flush_job.run().await {
        logging::error!(e; "Failed to flush region {}", req.region_id());

        FLUSH_ERRORS_TOTAL.inc();

        FlushRequest::Region { req, sender }.complete(Err(e));
    } else {
        logging::debug!("Successfully flush region: {}", req.region_id());

        // Update last flush time.
        req.shared.update_flush_millis();

        let compaction_request = CompactionRequestImpl::from(&req);
        let max_files_in_l0 = req.engine_config.max_files_in_l0;
        let shared_data = req.shared.clone();

        let level0_file_num = shared_data
            .version_control
            .current()
            .ssts()
            .level(0)
            .file_num();
        if level0_file_num <= max_files_in_l0 {
            logging::debug!(
                "No enough SST files in level 0 (threshold: {}), skip compaction",
                max_files_in_l0
            );
        } else {
            // If flush is success, schedule a compaction request for this region.
            let _ =
                region::schedule_compaction(shared_data, compaction_scheduler, compaction_request);
        }

        // Complete the request.
        FlushRequest::Region { req, sender }.complete(Ok(()));
    }
}

/// Task function to pick regions to flush.
struct AutoFlushFunction<S: LogStore> {
    /// Regions of the engine.
    regions: Arc<RegionMap<S>>,
    picker: FlushPicker,
}

#[async_trait]
impl<S: LogStore> TaskFunction<Error> for AutoFlushFunction<S> {
    async fn call(&mut self) -> Result<()> {
        // Get all regions.
        let regions = self.regions.list_regions();
        let _ = self.picker.pick_by_interval(&regions).await;

        Ok(())
    }

    fn name(&self) -> &str {
        "FlushPicker-pick-task"
    }
}
