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
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;
use tokio::sync::mpsc;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::compaction::compactor::{CompactionRegion, Compactor};
use crate::compaction::picker::{CompactionTask, PickerOutput};
use crate::config::MitoConfig;
use crate::error;
use crate::error::CompactRegionSnafu;
use crate::metrics::{COMPACTION_FAILURE_COUNT, COMPACTION_STAGE_ELAPSED};
use crate::region::version::VersionControlRef;
use crate::region::ManifestContextRef;
use crate::request::{
    BackgroundNotify, CompactionFailed, CompactionFinished, OutputTx, WorkerRequest,
};
use crate::sst::file_purger::FilePurgerRef;
use crate::worker::WorkerListener;

pub const MAX_PARALLEL_COMPACTION: usize = 8;

pub(crate) struct CompactionTaskImpl {
    pub engine_config: Arc<MitoConfig>,
    pub region_id: RegionId,
    pub metadata: RegionMetadataRef,
    pub sst_layer: AccessLayerRef,
    pub file_purger: FilePurgerRef,
    /// Request sender to notify the worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequest>,
    /// Senders that are used to notify waiters waiting for pending compaction tasks.
    pub waiters: Vec<OutputTx>,
    /// Start time of compaction task
    pub start_time: Instant,
    pub(crate) cache_manager: CacheManagerRef,
    /// The region is using append mode.
    pub(crate) append_mode: bool,
    /// Manifest context.
    pub(crate) manifest_ctx: ManifestContextRef,
    /// Version control to update.
    pub(crate) version_control: VersionControlRef,
    /// Event listener.
    pub(crate) listener: WorkerListener,

    pub(crate) compactor: Arc<dyn Compactor>,
    pub(crate) picker_output: Option<PickerOutput>,
}

impl Debug for CompactionTaskImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(picker_output) = &self.picker_output {
            f.debug_struct("TwcsCompactionTask")
                .field("region_id", &self.region_id)
                .field("outputs", &picker_output.outputs)
                .field("expired_ssts", &picker_output.expired_ssts)
                .field("compaction_time_window", &picker_output.time_window_size)
                .field("append_mode", &self.append_mode)
                .finish()
        } else {
            f.debug_struct("TwcsCompactionTask")
                .field("region_id", &self.region_id)
                .field("append_mode", &self.append_mode)
                .finish()
        }
    }
}

impl Drop for CompactionTaskImpl {
    fn drop(&mut self) {
        self.mark_files_compacting(false)
    }
}

impl CompactionTaskImpl {
    fn mark_files_compacting(&self, compacting: bool) {
        if let Some(picker_output) = &self.picker_output {
            picker_output
                .outputs
                .iter()
                .for_each(|o| o.inputs.iter().for_each(|f| f.set_compacting(compacting)));
        }
    }

    async fn handle_compaction(&mut self) -> error::Result<()> {
        let pick_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["pick"])
            .start_timer();
        let picker_output = self
            .compactor
            .picker()
            .pick(self.version_control.current().version);
        drop(pick_timer);

        if picker_output.is_none() {
            // Nothing to compact, we are done. Notifies all waiters as we consume the compaction request.
            for waiter in self.waiters.drain(..) {
                waiter.send(Ok(0));
            }
            return Ok(());
        }
        self.picker_output = picker_output;

        self.mark_files_compacting(true);

        let merge_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["merge"])
            .start_timer();

        let compaction_region = CompactionRegion {
            region_id: self.region_id,
            region_options: self.version_control.current().version.options.clone(),
            engine_config: self.engine_config.clone(),
            region_metadata: self.metadata.clone(),
            manifest_ctx: self.manifest_ctx.clone(),
            version_control: self.version_control.clone(),
            access_layer: self.sst_layer.clone(),
            cache_manager: self.cache_manager.clone(),
            file_purger: self.file_purger.clone(),
        };

        let compaction_result = match self
            .compactor
            .merge_ssts(compaction_region.clone(), self.picker_output.clone())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                error!(e; "Failed to compact region: {}", self.region_id);
                merge_timer.stop_and_discard();
                return Err(e);
            }
        };

        let merge_time = merge_timer.stop_and_record();

        if compaction_result.is_empty() {
            info!(
                "No files to compact, region_id: {}, window: {:?}",
                self.region_id, compaction_result.compaction_time_window
            );
            return Ok(());
        }

        info!(
            "Compacted SST files, region_id: {}, input: {:?}, output: {:?}, window: {:?}, waiter_num: {}, merge_time: {}s",
            self.region_id,
            compaction_result.fileds_to_remove,
            compaction_result.files_to_add,
            compaction_result.compaction_time_window,
            self.waiters.len(),
            merge_time,
        );

        self.listener.on_merge_ssts_finished(self.region_id).await;

        let _manifest_timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["write_manifest"])
            .start_timer();
        self.compactor
            .update_manifest(compaction_region.clone(), compaction_result)
            .await?;
        Ok(())
    }

    /// Handles compaction failure, notifies all waiters.
    fn on_failure(&mut self, err: Arc<error::Error>) {
        COMPACTION_FAILURE_COUNT.inc();
        for waiter in self.waiters.drain(..) {
            waiter.send(Err(err.clone()).context(CompactRegionSnafu {
                region_id: self.region_id,
            }));
        }
    }

    /// Notifies region worker to handle post-compaction tasks.
    async fn send_to_worker(&self, request: WorkerRequest) {
        if let Err(e) = self.request_sender.send(request).await {
            error!(
                "Failed to notify compaction job status for region {}, request: {:?}",
                self.region_id, e.0
            );
        }
    }
}

#[async_trait::async_trait]
impl CompactionTask for CompactionTaskImpl {
    async fn run(&mut self) {
        let notify = match self.handle_compaction().await {
            Ok(()) => BackgroundNotify::CompactionFinished(CompactionFinished {
                region_id: self.region_id,
                senders: std::mem::take(&mut self.waiters),
                start_time: self.start_time,
            }),
            Err(e) => {
                error!(e; "Failed to compact region, region id: {}", self.region_id);
                let err = Arc::new(e);
                // notify compaction waiters
                self.on_failure(err.clone());
                BackgroundNotify::CompactionFailed(CompactionFailed {
                    region_id: self.region_id,
                    err,
                })
            }
        };

        self.send_to_worker(WorkerRequest::Background {
            region_id: self.region_id,
            notify,
        })
        .await;
    }
}
