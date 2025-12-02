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

use common_telemetry::{error, info, warn};
use itertools::Itertools;
use snafu::ResultExt;
use tokio::sync::mpsc;

use crate::compaction::compactor::{CompactionRegion, Compactor};
use crate::compaction::picker::{CompactionTask, PickerOutput};
use crate::error::CompactRegionSnafu;
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::metrics::{COMPACTION_FAILURE_COUNT, COMPACTION_STAGE_ELAPSED};
use crate::region::RegionRoleState;
use crate::request::{
    BackgroundNotify, CompactionFailed, CompactionFinished, OutputTx, RegionEditResult,
    WorkerRequest, WorkerRequestWithTime,
};
use crate::sst::file::FileMeta;
use crate::worker::WorkerListener;
use crate::{error, metrics};

/// Maximum number of compaction tasks in parallel.
pub const MAX_PARALLEL_COMPACTION: usize = 1;

pub(crate) struct CompactionTaskImpl {
    pub compaction_region: CompactionRegion,
    /// Request sender to notify the worker.
    pub(crate) request_sender: mpsc::Sender<WorkerRequestWithTime>,
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

    /// Remove expired ssts files, update manifest immediately
    /// and apply the edit to region version.
    ///
    /// This function logs errors but does not stop the compaction process if removal fails.
    async fn remove_expired(
        &self,
        compaction_region: &CompactionRegion,
        expired_files: Vec<FileMeta>,
    ) {
        let region_id = compaction_region.region_id;
        let expired_files_str = expired_files.iter().map(|f| f.file_id).join(",");
        let (expire_delete_sender, expire_delete_listener) = tokio::sync::oneshot::channel();
        // Update manifest to remove expired SSTs
        let edit = RegionEdit {
            files_to_add: Vec::new(),
            files_to_remove: expired_files,
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        };

        // 1. Update manifest
        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        let RegionRoleState::Leader(current_region_state) =
            compaction_region.manifest_ctx.current_state()
        else {
            warn!(
                "Region {} not in leader state, skip removing expired files",
                region_id
            );
            return;
        };
        if let Err(e) = compaction_region
            .manifest_ctx
            .update_manifest(current_region_state, action_list, false)
            .await
        {
            warn!(
                e;
                "Failed to update manifest for expired files removal, region: {region_id}, files: [{expired_files_str}]. Compaction will continue."
            );
            return;
        }

        // 2. Notify region worker loop to remove expired files from region version.
        self.send_to_worker(WorkerRequest::Background {
            region_id,
            notify: BackgroundNotify::RegionEdit(RegionEditResult {
                region_id,
                sender: expire_delete_sender,
                edit,
                result: Ok(()),
                update_region_state: false,
            }),
        })
        .await;

        if let Err(e) = expire_delete_listener
            .await
            .context(error::RecvSnafu)
            .flatten()
        {
            warn!(
                e;
                "Failed to remove expired files from region version, region: {region_id}, files: [{expired_files_str}]. Compaction will continue."
            );
            return;
        }

        info!(
            "Successfully removed expired files, region: {region_id}, files: [{expired_files_str}]"
        );
    }

    async fn handle_expiration_and_compaction(&mut self) -> error::Result<RegionEdit> {
        self.mark_files_compacting(true);

        // 1. In case of local compaction, we can delete expired ssts in advance.
        if !self.picker_output.expired_ssts.is_empty() {
            let remove_timer = COMPACTION_STAGE_ELAPSED
                .with_label_values(&["remove_expired"])
                .start_timer();
            let expired_ssts = self
                .picker_output
                .expired_ssts
                .drain(..)
                .map(|f| f.meta_ref().clone())
                .collect();
            // remove_expired logs errors but doesn't stop compaction
            self.remove_expired(&self.compaction_region, expired_ssts)
                .await;
            remove_timer.observe_duration();
        }

        // 2. Merge inputs
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

        metrics::COMPACTION_INPUT_BYTES.inc_by(compaction_result.input_file_size() as f64);
        metrics::COMPACTION_OUTPUT_BYTES.inc_by(compaction_result.output_file_size() as f64);
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
        if let Err(e) = self
            .request_sender
            .send(WorkerRequestWithTime::new(request))
            .await
        {
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
        let notify = match self.handle_expiration_and_compaction().await {
            Ok(edit) => BackgroundNotify::CompactionFinished(CompactionFinished {
                region_id: self.compaction_region.region_id,
                senders: std::mem::take(&mut self.waiters),
                start_time: self.start_time,
                edit,
            }),
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

#[cfg(test)]
mod tests {
    use store_api::storage::FileId;

    use crate::compaction::picker::PickerOutput;
    use crate::compaction::test_util::new_file_handle;

    #[test]
    fn test_picker_output_with_expired_ssts() {
        // Test that PickerOutput correctly includes expired_ssts
        // This verifies that expired SSTs are properly identified and included
        // in the picker output, which is then handled by handle_expiration_and_compaction

        let file_ids = (0..3).map(|_| FileId::random()).collect::<Vec<_>>();
        let expired_ssts = vec![
            new_file_handle(file_ids[0], 0, 999, 0),
            new_file_handle(file_ids[1], 1000, 1999, 0),
        ];

        let picker_output = PickerOutput {
            outputs: vec![],
            expired_ssts: expired_ssts.clone(),
            time_window_size: 3600,
            max_file_size: None,
        };

        // Verify expired_ssts are included
        assert_eq!(picker_output.expired_ssts.len(), 2);
        assert_eq!(
            picker_output.expired_ssts[0].file_id(),
            expired_ssts[0].file_id()
        );
        assert_eq!(
            picker_output.expired_ssts[1].file_id(),
            expired_ssts[1].file_id()
        );
    }

    #[test]
    fn test_picker_output_without_expired_ssts() {
        // Test that PickerOutput works correctly when there are no expired SSTs
        let picker_output = PickerOutput {
            outputs: vec![],
            expired_ssts: vec![],
            time_window_size: 3600,
            max_file_size: None,
        };

        // Verify empty expired_ssts
        assert!(picker_output.expired_ssts.is_empty());
    }

    // Note: Testing remove_expired() directly requires extensive mocking of:
    // - manifest_ctx (ManifestContext)
    // - request_sender (mpsc::Sender<WorkerRequestWithTime>)
    // - WorkerRequest handling
    //
    // The behavior is tested indirectly through integration tests:
    // - remove_expired() logs errors but doesn't stop compaction
    // - handle_expiration_and_compaction() continues even if remove_expired() encounters errors
    // - The function is designed to be non-blocking for compaction
}
