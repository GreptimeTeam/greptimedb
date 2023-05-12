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
use metrics::increment_counter;
use snafu::{ensure, ResultExt};
use store_api::logstore::LogStore;
use store_api::storage::{RegionId, SequenceNumber};
use tokio::sync::oneshot::{Receiver, Sender};
use tokio::sync::{oneshot, Notify};

use crate::compaction::{CompactionRequestImpl, CompactionSchedulerRef};
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

/// Key for [FlushRequest], consist of a region id and the flush
/// sequence.
type FlushKey = (RegionId, SequenceNumber);

/// Region flush request.
pub struct FlushRequest<S: LogStore> {
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
    pub writer: RegionWriterRef,
    /// Region write-ahead logging, used to write data/meta to the log file.
    pub wal: Wal<S>,
    /// Region manifest service, used to persist metadata.
    pub manifest: RegionManifest,
    /// Storage engine config
    pub engine_config: Arc<EngineConfig>,
    /// Flush result sender. Callers should set the sender to None.
    pub sender: Option<Sender<Result<()>>>,

    // Compaction related options:
    /// TTL of the region.
    pub ttl: Option<Duration>,
    /// Time window for compaction.
    pub compaction_time_window: Option<i64>,
}

impl<S: LogStore> FlushRequest<S> {
    #[inline]
    fn region_id(&self) -> RegionId {
        self.shared.id()
    }
}

impl<S: LogStore> Request for FlushRequest<S> {
    type Key = FlushKey;

    #[inline]
    fn key(&self) -> FlushKey {
        (self.shared.id(), self.flush_sequence)
    }

    fn complete(self, result: Result<()>) {
        if let Some(sender) = self.sender {
            let _ = sender.send(result);
        }
    }
}

impl<S: LogStore> From<&FlushRequest<S>> for FlushJob<S> {
    fn from(req: &FlushRequest<S>) -> FlushJob<S> {
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

impl<S: LogStore> From<&FlushRequest<S>> for CompactionRequestImpl<S> {
    fn from(req: &FlushRequest<S>) -> CompactionRequestImpl<S> {
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
            sst_write_buffer_size: req.engine_config.sst_write_buffer_size,
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
        let handler = FlushHandler {
            compaction_scheduler,
        };
        let task_interval = picker_config.picker_schedule_interval();
        let picker = FlushPicker::new(picker_config);
        let task_fn = AutoFlushFunction { regions, picker };
        let auto_flush_task = RepeatedTask::new(task_interval, Box::new(task_fn));
        auto_flush_task
            .start(common_runtime::bg_runtime())
            .context(StartPickTaskSnafu)?;

        Ok(Self {
            scheduler: LocalScheduler::new(config, handler),
            auto_flush_task,
        })
    }

    /// Schedules a flush request and return the handle to the flush task.
    ///
    /// # Panics
    /// Panics if `sender` of the `req` is not `None`.
    pub fn schedule_flush(&self, mut req: FlushRequest<S>) -> Result<FlushHandle> {
        assert!(req.sender.is_none());

        let region_id = req.region_id();
        let sequence = req.flush_sequence;
        let (sender, receiver) = oneshot::channel();
        req.sender = Some(sender);

        let scheduled = self.scheduler.schedule(req)?;
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

    /// Stop the scheduler.
    pub async fn stop(&self) -> Result<()> {
        self.auto_flush_task
            .stop()
            .await
            .context(StopPickTaskSnafu)?;
        self.scheduler.stop(true).await?;

        Ok(())
    }
}

struct FlushHandler<S: LogStore> {
    compaction_scheduler: CompactionSchedulerRef<S>,
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
        common_runtime::spawn_bg(async move {
            execute_flush(req, compaction_scheduler).await;

            // releases rate limit token
            token.try_release();
            // notify scheduler to schedule next task when current task finishes.
            finish_notifier.notify_one();
        });

        Ok(())
    }
}

async fn execute_flush<S: LogStore>(
    req: FlushRequest<S>,
    compaction_scheduler: CompactionSchedulerRef<S>,
) {
    let mut flush_job = FlushJob::from(&req);

    if let Err(e) = flush_job.run().await {
        logging::error!(e; "Failed to flush regoin {}", req.region_id());

        increment_counter!(FLUSH_ERRORS_TOTAL);

        req.complete(Err(e));
    } else {
        logging::debug!("Successfully flush region: {}", req.region_id());

        // Update last flush time.
        req.shared.update_flush_millis();

        let compaction_request = CompactionRequestImpl::from(&req);
        let max_files_in_l0 = req.engine_config.max_files_in_l0;
        let shared_data = req.shared.clone();

        // If flush is success, schedule a compaction request for this region.
        region::schedule_compaction(
            shared_data,
            compaction_scheduler,
            compaction_request,
            max_files_in_l0,
        );

        // Complete the request.
        req.complete(Ok(()));
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
        self.picker.pick_by_interval(&regions).await;

        Ok(())
    }

    fn name(&self) -> &str {
        "FlushPicker-pick-task"
    }
}
