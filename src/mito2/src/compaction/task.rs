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

use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::time::Instant;

use common_telemetry::{error, info};
use snafu::ResultExt;
use store_api::manifest::ManifestVersion;
use tokio::sync::mpsc;

use crate::compaction::compactor::{CompactionRegion, Compactor};
use crate::compaction::picker::{CompactionTask, PickerOutput};
use crate::error;
use crate::error::CompactRegionSnafu;
use crate::manifest::action::RegionEdit;
use crate::metrics::{COMPACTION_FAILURE_COUNT, COMPACTION_STAGE_ELAPSED};
use crate::request::{
    BackgroundNotify, CompactionFailed, CompactionFinished, OutputTx, WorkerRequest,
};
use crate::worker::WorkerListener;

/// Maximum number of compaction tasks in parallel.
pub const MAX_PARALLEL_COMPACTION: usize = 1;

pub(crate) struct CompactionTaskImpl {
    pub compaction_region: CompactionRegion,
    /// Request sender to notify the worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,
    /// Senders that are used to notify waiters waiting for pending compaction tasks.
    pub waiters: Vec<OutputTx>,
    /// Start time of compaction task
    pub start_time: Instant,
    /// Event listener.
    pub(crate) listener: WorkerListener,
    /// Compactor to handle compaction.
    pub(crate) compactor: Arc<dyn Compactor>,
    /// Output of the picker.
    pub(crate) picker_output: PickerOutput,
}

impl Debug for CompactionTaskImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwcsCompactionTask")
            .field("region_id", &self.compaction_region.region_id)
            .field("picker_output", &self.picker_output)
            .field(
                "append_mode",
                &self.compaction_region.region_options.append_mode,
            )
            .finish()
    }
}

impl Drop for CompactionTaskImpl {
    fn drop(&mut self) {
        self.mark_files_compacting(false)
    }
}

impl CompactionTaskImpl {
    fn mark_files_compacting(&self, compacting: bool) {
        self.picker_output
            .outputs
            .iter()
            .for_each(|o| o.inputs.iter().for_each(|f| f.set_compacting(compacting)));
    }

    async fn handle_compaction(&mut self) -> error::Result<(ManifestVersion, RegionEdit)> {
        self.mark_files_compacting(true);

        let merge_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["merge"])
            .start_timer();

        let compaction_result = match self
            .compactor
            .merge_ssts(&self.compaction_region, self.picker_output.clone())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!(e; "Failed to compact region: {}", self.compaction_region.region_id);
                merge_timer.stop_and_discard();
                return Err(e);
            }
        };
        let merge_time = merge_timer.stop_and_record();

        info!(
            "Compacted SST files, region_id: {}, input: {:?}, output: {:?}, window: {:?}, waiter_num: {}, merge_time: {}s",
            self.compaction_region.region_id,
            compaction_result.files_to_remove,
            compaction_result.files_to_add,
            compaction_result.compaction_time_window,
            self.waiters.len(),
            merge_time,
        );

        self.listener
            .on_merge_ssts_finished(self.compaction_region.region_id)
            .await;

        let _manifest_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["write_manifest"])
            .start_timer();

        self.compactor
            .update_manifest(&self.compaction_region, compaction_result)
            .await
    }

    /// Handles compaction failure, notifies all waiters.
    fn on_failure(&mut self, err: Arc<error::Error>) {
        COMPACTION_FAILURE_COUNT.inc();
        for waiter in self.waiters.drain(..) {
            waiter.send(Err(err.clone()).context(CompactRegionSnafu {
                region_id: self.compaction_region.region_id,
            }));
        }
    }

    /// Notifies region worker to handle post-compaction tasks.
    async fn send_to_worker(&self, request: WorkerRequest) {
        if let Err(e) = self.request_sender.send(request).await {
            error!(
                "Failed to notify compaction job status for region {}, request: {:?}",
                self.compaction_region.region_id, e.0
            );
        }
    }
}

#[async_trait::async_trait]
impl CompactionTask for CompactionTaskImpl {
    async fn run(&mut self) {
        let notify = match self.handle_compaction().await {
            Ok((manifest_version, edit)) => {
                BackgroundNotify::CompactionFinished(CompactionFinished {
                    region_id: self.compaction_region.region_id,
                    senders: std::mem::take(&mut self.waiters),
                    start_time: self.start_time,
                    edit,
                    manifest_version,
                })
            }
            Err(e) => {
                error!(e; "Failed to compact region, region id: {}", self.compaction_region.region_id);
                let err = Arc::new(e);
                // notify compaction waiters
                self.on_failure(err.clone());
                BackgroundNotify::CompactionFailed(CompactionFailed {
                    region_id: self.compaction_region.region_id,
                    err,
                })
            }
        };

        self.send_to_worker(WorkerRequest::Background {
            region_id: self.compaction_region.region_id,
            notify,
        })
        .await;
    }
}
