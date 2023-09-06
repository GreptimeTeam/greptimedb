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

use std::sync::Arc;
use std::time::Duration;

use common_query::Output;
use common_telemetry::debug;
pub use picker::CompactionPickerRef;
use store_api::storage::{CompactionStrategy, RegionId, TwcsOptions};
use tokio::sync::{mpsc, oneshot};

use crate::access_layer::AccessLayerRef;
use crate::compaction::twcs::TwcsPicker;
use crate::error;
use crate::error::Result;
use crate::region::version::VersionRef;
use crate::request::WorkerRequest;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file_purger::FilePurgerRef;

/// Region compaction request.
pub struct CompactionRequest {
    pub(crate) current_version: VersionRef,
    pub(crate) access_layer: AccessLayerRef,
    pub(crate) ttl: Option<Duration>,
    pub(crate) compaction_time_window: Option<i64>,
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,
    pub(crate) waiters: Option<oneshot::Sender<error::Result<Output>>>,
    pub(crate) file_purger: FilePurgerRef,
}

impl CompactionRequest {
    pub(crate) fn region_id(&self) -> RegionId {
        self.current_version.metadata.region_id
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

pub(crate) struct CompactionScheduler {
    scheduler: SchedulerRef,
    // TODO(hl): maybe tracks region compaction status in CompactionScheduler
}

impl CompactionScheduler {
    pub(crate) fn new(scheduler: SchedulerRef) -> Self {
        Self { scheduler }
    }

    /// Schedules a region compaction task.
    pub(crate) fn schedule_compaction(&self, req: CompactionRequest) -> Result<()> {
        self.scheduler.schedule(Box::pin(async {
            // TODO(hl): build picker according to region options.
            let strategy =
                compaction_strategy_to_picker(&CompactionStrategy::Twcs(TwcsOptions::default()));
            debug!(
                "Pick compaction strategy {:?} for region: {}",
                strategy,
                req.region_id()
            );
            let Some(mut task) = strategy.pick(req).unwrap() else {
                return;
            };
            task.run().await;
        }))
    }
}
