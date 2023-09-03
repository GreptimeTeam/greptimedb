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

use common_query::Output;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{CompactionStrategy, RegionId};
use tokio::sync::{mpsc, oneshot};

use crate::access_layer::AccessLayerRef;
use crate::compaction::twcs::TwcsPicker;
use crate::error;
use crate::region::version::VersionRef;
use crate::request::WorkerRequest;
use crate::sst::file_purger::FilePurgerRef;

mod picker;
mod twcs;

mod output;
#[cfg(test)]
mod test_util;

pub use picker::CompactionPickerRef;

/// Region compaction request.
pub struct CompactionRequest {
    pub(crate) region_id: RegionId,
    pub(crate) region_metadata: RegionMetadataRef,
    pub(crate) current_version: VersionRef,
    pub(crate) access_layer: AccessLayerRef,
    pub(crate) ttl: Option<Duration>,
    pub(crate) compaction_time_window: Option<i64>,
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,
    pub(crate) waiters: Vec<oneshot::Sender<error::Result<Output>>>,
    pub(crate) file_purger: FilePurgerRef,
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
