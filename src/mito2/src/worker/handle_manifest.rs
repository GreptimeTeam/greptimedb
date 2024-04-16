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

use common_telemetry::{info, warn};
use snafu::ensure;
use store_api::storage::RegionId;
use tokio::sync::oneshot::Sender;

use crate::error::{InvalidRequestSnafu, Result};
use crate::manifest::action::{
    RegionChange, RegionEdit, RegionMetaAction, RegionMetaActionList, RegionTruncate,
};
use crate::region::{MitoRegionRef, RegionState};
use crate::request::{BackgroundNotify, OptionOutputTx, TruncateResult, WorkerRequest};
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    /// Handles region edit request.
    pub(crate) async fn handle_region_edit(
        &self,
        region_id: RegionId,
        edit: RegionEdit,
        sender: Sender<Result<()>>,
    ) {
        let region = match self.regions.writable_region(region_id) {
            Ok(region) => region,
            Err(e) => {
                let _ = sender.send(Err(e));
                return;
            }
        };

        // Marks the region as editing.
        if let Err(e) = region.set_editing() {
            let _ = sender.send(Err(e));
            return;
        }

        // Now the region is in editing state.
        // Updates manifest in background.
        common_runtime::spawn_bg(async move {
            let result = edit_region(&region, edit).await;

            let _ = sender.send(result);

            // Sets the region as writable. For simplicity, we don't send the result
            // back to the worker.
            region.switch_state_to_writable(RegionState::Editing);
        });
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
            let _ = sender.send(Err(e));
            return;
        }
        // Now the region is in truncating state.

        let request_sender = self.sender.clone();
        let manifest_ctx = region.manifest_ctx.clone();
        let version_control = region.version_control.clone();
        let memtable_builder = region.memtable_builder.clone();

        // Updates manifest in background.
        common_runtime::spawn_bg(async move {
            // Write region truncated to manifest.
            let action_list =
                RegionMetaActionList::with_action(RegionMetaAction::Truncate(truncate.clone()));

            let result = manifest_ctx
                .update_manifest(RegionState::Truncating, action_list, || {
                    // Applies the truncate action to the region.
                    version_control.truncate(
                        truncate.truncated_entry_id,
                        truncate.truncated_sequence,
                        &memtable_builder,
                    );
                })
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
            let _ = sender.send(Err(e));
            return;
        }

        // Now the region is in altering state.
        common_runtime::spawn_bg(async move {
            let new_meta = change.metadata.clone();
            let action_list = RegionMetaActionList::with_action(RegionMetaAction::Change(change));

            let result = region
                .manifest_ctx
                .update_manifest(RegionState::Altering, action_list, || {
                    // Apply the metadata to region's version.
                    region
                        .version_control
                        .alter_schema(new_meta, &region.memtable_builder);
                })
                .await;

            // Sets the region as writable.
            region.switch_state_to_writable(RegionState::Altering);

            if result.is_ok() {
                info!(
                    "Region {} is altered, schema version is {}",
                    region.region_id,
                    region.metadata().schema_version
                );
            }

            sender.send(result.map(|_| 0));
        });
    }
}

/// Checks the edit, writes and applies it.
async fn edit_region(region: &MitoRegionRef, edit: RegionEdit) -> Result<()> {
    let region_id = region.region_id;
    for file_meta in &edit.files_to_add {
        let is_exist = region.access_layer.is_exist(file_meta).await?;
        ensure!(
            is_exist,
            InvalidRequestSnafu {
                region_id,
                reason: format!(
                    "trying to add a not exist file '{}' when editing region",
                    file_meta.file_id
                )
            }
        );
    }

    info!("Applying {edit:?} to region {}", region_id);

    let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
    region
        .manifest_ctx
        .update_manifest(RegionState::Editing, action_list, || {
            // Applies the edit to the region.
            region
                .version_control
                .apply_edit(edit, &[], region.file_purger.clone());
        })
        .await
}
