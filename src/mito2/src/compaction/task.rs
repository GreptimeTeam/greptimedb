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

use common_base::cancellation::CancellableFuture;
use common_memory_manager::OnExhaustedPolicy;
use common_telemetry::error;
use snafu::ResultExt;
use tokio::sync::{mpsc, oneshot};

use crate::compaction::CompactionExecution;
use crate::compaction::compactor::{CompactionRegion, Compactor, DefaultCompactor, MergeOutput};
use crate::compaction::memory_manager::CompactionMemoryManager;
use crate::compaction::picker::CompactionTask;
use crate::compaction::unit::CompactionUnit;
use crate::error::{self, CompactionCancelledSnafu, CompactionMemoryExhaustedSnafu, Result};
use crate::manifest::action::RegionEdit;
use crate::metrics::{
    COMPACTION_FAILURE_COUNT, COMPACTION_INPUT_BYTES, COMPACTION_OUTPUT_BYTES,
    COMPACTION_STAGE_ELAPSED,
};
use crate::request::{
    BackgroundNotify, CompactionUnitNotification, WorkerRequest, WorkerRequestWithTime,
};
use crate::schedule::CancellableTaskState;
use crate::sst::file::UncommittedSsts;
use crate::worker::WorkerListener;

pub const MAX_PARALLEL_COMPACTION: usize = 1;

/// One local atomic unit. The scheduler owns request waiters and sibling units.
pub(crate) struct CompactionTaskImpl {
    pub(crate) state: CancellableTaskState,
    pub(crate) execution: CompactionExecution,
    pub(crate) compaction_region: CompactionRegion,
    pub(crate) request_sender: mpsc::Sender<WorkerRequestWithTime>,
    pub(crate) listener: WorkerListener,
    pub(crate) unit: CompactionUnit,
    pub(crate) memory_manager: Arc<CompactionMemoryManager>,
    pub(crate) memory_policy: OnExhaustedPolicy,
    /// Estimated memory bytes needed for this compaction.
    pub(crate) estimated_memory_bytes: u64,
    /// Finalized output SSTs not committed to the manifest yet.
    pub(crate) uncommitted: UncommittedSsts,
}

impl Debug for CompactionTaskImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TwcsCompactionTask")
            .field("region_id", &self.compaction_region.region_id)
            .field("unit", &self.unit)
            .field(
                "append_mode",
                &self.compaction_region.region_options.append_mode,
            )
            .finish()
    }
}

impl CompactionTaskImpl {
    /// Acquires the unit's memory budget and merges on the compaction runtime.
    async fn merge(&self) -> Result<MergeOutput> {
        let region_id = self.compaction_region.region_id;
        self.listener
            .on_compaction_unit_merge_begin(region_id, self.execution.plan_id())
            .await;
        let bytes = self.estimated_memory_bytes;
        let _memory_guard = if bytes == 0 {
            None
        } else {
            let _wait_timer = crate::metrics::COMPACTION_MEMORY_WAIT.start_timer();
            Some(
                CancellableFuture::new(
                    self.memory_manager
                        .acquire_with_policy(bytes, self.memory_policy),
                    self.state.cancel_handle(),
                )
                .await
                .map_err(|_| CompactionCancelledSnafu.build())?
                .context(CompactionMemoryExhaustedSnafu {
                    region_id,
                    policy: format!("{:?}", self.memory_policy),
                })?,
            )
        };
        let _timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["merge"])
            .start_timer();
        let compactor = DefaultCompactor::with_cancel_handle(
            self.state.cancel_handle(),
            self.uncommitted.clone(),
        );
        let region = self.compaction_region.clone();
        let unit = self.unit.clone();
        let output =
            common_runtime::spawn_compact(
                async move { compactor.merge_unit(&region, &unit).await },
            )
            .await
            .context(error::JoinSnafu)??;
        COMPACTION_INPUT_BYTES.inc_by(output.input_file_size() as f64);
        COMPACTION_OUTPUT_BYTES.inc_by(output.output_file_size() as f64);
        self.listener.on_merge_ssts_finished(region_id).await;
        Ok(output)
    }

    /// Commits this unit through the existing manifest path, independently of sibling applies.
    async fn publish(&self, output: MergeOutput) -> Result<RegionEdit> {
        if !self.state.mark_commit_started() {
            return CompactionCancelledSnafu.fail();
        }
        // Report this unit's SSTs before its manifest update; sibling units may progress independently.
        self.compaction_region.invoke_sst_hook(&output).await;
        self.listener
            .on_compaction_commit_begin(self.compaction_region.region_id)
            .await;
        let _timer = COMPACTION_STAGE_ELAPSED
            .with_label_values(&["write_manifest"])
            .start_timer();
        let compactor = DefaultCompactor::with_cancel_handle(
            self.state.cancel_handle(),
            self.uncommitted.clone(),
        );
        let (edit, _manifest_version) = compactor
            .update_manifest(&self.compaction_region, output)
            .await?;
        self.uncommitted.disarm_cleanup();
        self.listener
            .on_compaction_unit_committed(
                self.compaction_region.region_id,
                self.execution.plan_id(),
            )
            .await;
        Ok(edit)
    }

    /// Sends a unit lifecycle event to the owning region worker.
    async fn notify(&self, notify: CompactionUnitNotification) -> Result<()> {
        self.request_sender
            .send(WorkerRequestWithTime::new(WorkerRequest::Background {
                region_id: self.compaction_region.region_id,
                notify: BackgroundNotify::CompactionUnit(notify),
            }))
            .await
            .map_err(|_| error::InvalidSenderSnafu.build())
    }
}

#[async_trait::async_trait]
impl CompactionTask for CompactionTaskImpl {
    /// Executes one unit and retains its resources until worker apply or failure acknowledgement.
    async fn run(&mut self) {
        let start = std::time::Instant::now();
        let result = match self.merge().await {
            Ok(output) => self.publish(output).await,
            Err(err) => Err(err),
        };
        if let Err(err) = &result {
            if !matches!(err, error::Error::CompactionCancelled { .. }) {
                COMPACTION_FAILURE_COUNT.inc();
                error!(err; "Compaction unit failed, region: {}, plan_id: {}",
                    self.compaction_region.region_id, self.execution.plan_id());
            }
            if self.state.commit_started() && err.may_have_persisted_manifest_update() {
                self.uncommitted.disarm_cleanup();
            } else {
                self.uncommitted.cleanup().await;
            }
        }
        let succeeded = result.is_ok();
        let (applied, receiver) = oneshot::channel();
        let notify = CompactionUnitNotification::Finished {
            plan_id: self.execution.plan_id(),
            result: result.map_err(Arc::new),
            applied,
        };
        if let Err(err) = self.notify(notify).await {
            error!(err; "Failed to notify compaction unit completion, region: {}", self.compaction_region.region_id);
            return;
        }
        // This keeps the execution slot and inputs alive through worker apply.
        if receiver.await.is_ok() && succeeded {
            crate::metrics::COMPACTION_ELAPSED_TOTAL.observe(start.elapsed().as_secs_f64());
        }
    }
}
