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

use common_telemetry::{debug, error, info};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::oneshot::Sender;
use tokio::sync::Notify;

use crate::compaction::picker::{Picker, PickerContext};
use crate::compaction::task::CompactionTask;
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
            // We don't care the send result as
            let _ = sender.send(result);
        }
    }
}

/// Region compaction request.
pub struct CompactionRequestImpl<S: LogStore> {
    pub region_id: RegionId,
    pub sst_layer: AccessLayerRef,
    pub writer: RegionWriterRef,
    pub shared: SharedDataRef,
    pub manifest: RegionManifest,
    pub wal: Wal<S>,
    pub ttl: Option<Duration>,
    /// Compaction result sender.
    pub sender: Option<Sender<Result<()>>>,
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

pub struct CompactionHandler<P> {
    pub picker: P,
}

impl<P> CompactionHandler<P> {
    pub fn new(picker: P) -> Self {
        Self { picker }
    }
}

#[async_trait::async_trait]
impl<P> Handler for CompactionHandler<P>
where
    P: Picker + Send + Sync,
{
    type Request = P::Request;

    async fn handle_request(
        &self,
        req: Self::Request,
        token: BoxedRateLimitToken,
        finish_notifier: Arc<Notify>,
    ) -> Result<()> {
        let region_id = req.key();
        let Some(task) = self.picker.pick(&PickerContext {}, &req)? else {
            info!("No file needs compaction in region: {:?}", region_id);
            req.complete(Ok(()));
            return Ok(());
        };

        debug!("Compaction task, region: {:?}, task: {:?}", region_id, task);
        // TODO(hl): we need to keep a track of task handle here to allow task cancellation.
        common_runtime::spawn_bg(async move {
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

        Ok(())
    }
}
