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

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use common_base::readable_size::ReadableSize;
use common_telemetry::{debug, error, info};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::oneshot::Sender;
use tokio::sync::Notify;

use crate::compaction::task::CompactionTask;
use crate::compaction::CompactionPickerRef;
use crate::error::Result;
use crate::manifest::region::RegionManifest;
use crate::region::{RegionWriterRef, SharedDataRef};
use crate::scheduler::rate_limit::BoxedRateLimitToken;
use crate::scheduler::{Handler, Request};
use crate::schema::RegionSchemaRef;
use crate::sst::AccessLayerRef;
use crate::version::LevelMetasRef;
use crate::wal::Wal;

impl<S: LogStore> Request for CompactionRequestImpl<S> {
    type Key = RegionId;

    #[inline]
    fn key(&self) -> RegionId {
        self.region_id
    }

    fn complete(self, result: Result<()>) {
        if let Some(sender) = self.sender {
            // We don't care the send result as callers might not
            // wait the result.
            let _ = sender.send(result);
        }
    }
}

/// Region compaction request.
pub struct CompactionRequestImpl<S: LogStore> {
    pub region_id: RegionId,
    pub sst_layer: AccessLayerRef,
    pub writer: RegionWriterRef<S>,
    pub shared: SharedDataRef,
    pub manifest: RegionManifest,
    pub wal: Wal<S>,
    pub ttl: Option<Duration>,
    pub compaction_time_window: Option<i64>,
    /// Compaction result sender.
    pub sender: Option<Sender<Result<()>>>,
    pub picker: CompactionPickerRef<S>,
    pub sst_write_buffer_size: ReadableSize,
    /// Whether to immediately reschedule another compaction when finished.
    pub reschedule_on_finish: bool,
}

impl<S: LogStore> CompactionRequestImpl<S> {
    #[inline]
    pub(crate) fn schema(&self) -> RegionSchemaRef {
        self.shared.version_control.current().schema().clone()
    }

    #[inline]
    pub(crate) fn levels(&self) -> LevelMetasRef {
        self.shared.version_control.current().ssts().clone()
    }
}

pub struct CompactionHandler<S: LogStore> {
    _phantom_data: PhantomData<S>,
    #[cfg(test)]
    pub pending_tasks: Arc<tokio::sync::RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}

impl<S: LogStore> Default for CompactionHandler<S> {
    fn default() -> Self {
        Self {
            _phantom_data: Default::default(),
            #[cfg(test)]
            pending_tasks: Arc::new(Default::default()),
        }
    }
}

impl<S: LogStore> CompactionHandler<S> {
    #[cfg(test)]
    pub fn new_with_pending_tasks(
        tasks: Arc<tokio::sync::RwLock<Vec<tokio::task::JoinHandle<()>>>>,
    ) -> Self {
        Self {
            _phantom_data: Default::default(),
            pending_tasks: tasks,
        }
    }
}

#[async_trait::async_trait]
impl<S> Handler for CompactionHandler<S>
where
    S: LogStore,
{
    type Request = CompactionRequestImpl<S>;

    async fn handle_request(
        &self,
        req: Self::Request,
        token: BoxedRateLimitToken,
        finish_notifier: Arc<Notify>,
    ) -> Result<()> {
        let region_id = req.key();
        let Some(task) = req.picker.pick(&req)? else {
            info!("No file needs compaction in region: {:?}", region_id);
            req.complete(Ok(()));
            return Ok(());
        };

        debug!("Compaction task, region: {:?}, task: {:?}", region_id, task);
        // TODO(hl): we need to keep a track of task handle here to allow task cancellation.
        let _handle = common_runtime::spawn_bg(async move {
            if let Err(e) = task.run().await {
                // TODO(hl): maybe resubmit compaction task on failure?
                error!(e; "Failed to compact region: {:?}", region_id);

                req.complete(Err(e));
            } else {
                info!("Successfully compacted region: {:?}", region_id);

                req.complete(Ok(()));
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
