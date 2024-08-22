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

//! Handles manifest.
//!
//! It updates the manifest and applies the changes to the region in background.

use std::collections::{HashMap, VecDeque};

use common_telemetry::{info, warn};
use store_api::storage::RegionId;

use crate::error::{RegionBusySnafu, RegionNotFoundSnafu, Result};
use crate::manifest::action::{
    RegionChange, RegionEdit, RegionMetaAction, RegionMetaActionList, RegionTruncate,
};
use crate::region::{MitoRegionRef, RegionState};
use crate::request::{
    BackgroundNotify, OptionOutputTx, RegionChangeResult, RegionEditRequest, RegionEditResult,
    TruncateResult, WorkerRequest,
};
use crate::worker::RegionWorkerLoop;

pub(crate) type RegionEditQueues = HashMap<RegionId, RegionEditQueue>;

/// A queue for temporary store region edit requests, if the region is in the "Editing" state.
/// When the current region edit request is completed, the next (if there exists) request in the
/// queue will be processed.
/// Everything is done in the region worker loop.
pub(crate) struct RegionEditQueue {
    region_id: RegionId,
    requests: VecDeque<RegionEditRequest>,
}

impl RegionEditQueue {
    const QUEUE_MAX_LEN: usize = 128;

    fn new(region_id: RegionId) -> Self {
        Self {
            region_id,
            requests: VecDeque::new(),
        }
    }

    fn enqueue(&mut self, request: RegionEditRequest) {
        if self.requests.len() > Self::QUEUE_MAX_LEN {
            let _ = request.tx.send(
                RegionBusySnafu {
                    region_id: self.region_id,
                }
                .fail(),
            );
            return;
        };
        self.requests.push_back(request);
    }

    fn dequeue(&mut self) -> Option<RegionEditRequest> {
        self.requests.pop_front()
    }
}

impl<S> RegionWorkerLoop<S> {
    /// Handles region edit request.
    pub(crate) async fn handle_region_edit(&mut self, request: RegionEditRequest) {
        let region_id = request.region_id;
        let Some(region) = self.regions.get_region(region_id) else {
            let _ = request.tx.send(RegionNotFoundSnafu { region_id }.fail());
            return;
        };

        if !region.is_writable() {
            if region.state() == RegionState::Editing {
                self.region_edit_queues
                    .entry(region_id)
                    .or_insert_with(|| RegionEditQueue::new(region_id))
                    .enqueue(request);
            } else {
                let _ = request.tx.send(RegionBusySnafu { region_id }.fail());
            }
            return;
        }

        let RegionEditRequest {
            region_id: _,
            edit,
            tx: sender,
        } = request;

        // Marks the region as editing.
        if let Err(e) = region.set_editing() {
            let _ = sender.send(Err(e));
            return;
        }

        let request_sender = self.sender.clone();
        // Now the region is in editing state.
        // Updates manifest in background.
        common_runtime::spawn_global(async move {
            let result = edit_region(&region, edit.clone()).await;
            let notify = WorkerRequest::Background {
                region_id,
                notify: BackgroundNotify::RegionEdit(RegionEditResult {
                    region_id,
                    sender,
                    edit,
                    result,
                }),
            };
            // We don't set state back as the worker loop is already exited.
            if let Err(res) = request_sender.send(notify).await {
                warn!(
                    "Failed to send region edit result back to the worker, region_id: {}, res: {:?}",
                    region_id, res
                );
            }
        });
    }

    /// Handles region edit result.
    pub(crate) async fn handle_region_edit_result(&mut self, edit_result: RegionEditResult) {
        let region = match self.regions.get_region(edit_result.region_id) {
            Some(region) => region,
            None => {
                let _ = edit_result.sender.send(
                    RegionNotFoundSnafu {
                        region_id: edit_result.region_id,
                    }
                    .fail(),
                );
                return;
            }
        };

        if edit_result.result.is_ok() {
            // Applies the edit to the region.
            region
                .version_control
                .apply_edit(edit_result.edit, &[], region.file_purger.clone());
        }

        // Sets the region as writable.
        region.switch_state_to_writable(RegionState::Editing);

        let _ = edit_result.sender.send(edit_result.result);

        if let Some(edit_queue) = self.region_edit_queues.get_mut(&edit_result.region_id) {
            if let Some(request) = edit_queue.dequeue() {
                self.handle_region_edit(request).await;
            }
        }
    }

    /// Writes truncate action to the manifest and then applies it to the region in background.
    pub(crate) fn handle_manifest_truncate_action(
        &self,
        region: MitoRegionRef,
        truncate: RegionTruncate,
        sender: OptionOutputTx,
    ) {
        // Marks the region as truncating.
        // This prevents the region from being accessed by other write requests.
        if let Err(e) = region.set_truncating() {
            sender.send(Err(e));
            return;
        }
        // Now the region is in truncating state.

        let request_sender = self.sender.clone();
        let manifest_ctx = region.manifest_ctx.clone();

        // Updates manifest in background.
        common_runtime::spawn_global(async move {
            // Write region truncated to manifest.
            let action_list =
                RegionMetaActionList::with_action(RegionMetaAction::Truncate(truncate.clone()));

            let result = manifest_ctx
                .update_manifest(RegionState::Truncating, action_list)
                .await;

            // Sends the result back to the request sender.
            let truncate_result = TruncateResult {
                region_id: truncate.region_id,
                sender,
                result,
                truncated_entry_id: truncate.truncated_entry_id,
                truncated_sequence: truncate.truncated_sequence,
            };
            let _ = request_sender
                .send(WorkerRequest::Background {
                    region_id: truncate.region_id,
                    notify: BackgroundNotify::Truncate(truncate_result),
                })
                .await
                .inspect_err(|_| warn!("failed to send truncate result"));
        });
    }

    /// Writes region change action to the manifest and then applies it to the region in background.
    pub(crate) fn handle_manifest_region_change(
        &self,
        region: MitoRegionRef,
        change: RegionChange,
        sender: OptionOutputTx,
    ) {
        // Marks the region as altering.
        if let Err(e) = region.set_altering() {
            sender.send(Err(e));
            return;
        }

        let request_sender = self.sender.clone();
        // Now the region is in altering state.
        common_runtime::spawn_global(async move {
            let new_meta = change.metadata.clone();
            let action_list = RegionMetaActionList::with_action(RegionMetaAction::Change(change));

            let result = region
                .manifest_ctx
                .update_manifest(RegionState::Altering, action_list)
                .await;
            let notify = WorkerRequest::Background {
                region_id: region.region_id,
                notify: BackgroundNotify::RegionChange(RegionChangeResult {
                    region_id: region.region_id,
                    sender,
                    result,
                    new_meta,
                }),
            };

            if let Err(res) = request_sender.send(notify).await {
                warn!(
                    "Failed to send region change result back to the worker, region_id: {}, res: {:?}",
                    region.region_id, res
                );
            }
        });
    }

    /// Handles region change result.
    pub(crate) fn handle_manifest_region_change_result(&self, change_result: RegionChangeResult) {
        let region = match self.regions.get_region(change_result.region_id) {
            Some(region) => region,
            None => {
                change_result.sender.send(
                    RegionNotFoundSnafu {
                        region_id: change_result.region_id,
                    }
                    .fail(),
                );
                return;
            }
        };

        if change_result.result.is_ok() {
            // Apply the metadata to region's version.
            region
                .version_control
                .alter_schema(change_result.new_meta, &region.memtable_builder);

            info!(
                "Region {} is altered, schema version is {}",
                region.region_id,
                region.metadata().schema_version
            );
        }

        // Sets the region as writable.
        region.switch_state_to_writable(RegionState::Altering);

        change_result.sender.send(change_result.result.map(|_| 0));
    }
}

/// Checks the edit, writes and applies it.
async fn edit_region(region: &MitoRegionRef, edit: RegionEdit) -> Result<()> {
    let region_id = region.region_id;
    info!("Applying {edit:?} to region {}", region_id);

    let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit));
    region
        .manifest_ctx
        .update_manifest(RegionState::Editing, action_list)
        .await
}
