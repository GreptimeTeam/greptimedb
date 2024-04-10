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
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList, RegionTruncate};
use crate::memtable::{MemtableBuilderRef, MemtableId};
use crate::region::version::VersionControlRef;
use crate::region::{
    switch_state_to_writable, ManifestContextRef, MitoRegionRef, REGION_STATE_EDITING,
};
use crate::request::{BackgroundNotify, OptionOutputTx, TruncateResult, WorkerRequest};
use crate::sst::file_purger::FilePurgerRef;
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

        // Updates manifest in background.
        common_runtime::spawn_bg(async move {
            let result = edit_region(&region, edit).await;

            let _ = sender.send(result);

            // Sets the region as writable. For simplicity, we don't send the result
            // back to the worker.
            switch_state_to_writable(&region, REGION_STATE_EDITING);
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

        let request_sender = self.sender.clone();
        let manifest_ctx = region.manifest_ctx.clone();
        let version_control = region.version_control.clone();
        let memtable_builder = region.memtable_builder.clone();

        // Updates manifest in background.
        common_runtime::spawn_bg(async move {
            let result = write_and_apply_truncate_action(
                manifest_ctx,
                version_control,
                &truncate,
                memtable_builder,
            )
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

    write_and_apply_region_edit(
        region.region_id,
        &region.manifest_ctx,
        &region.version_control,
        &region.file_purger,
        edit,
        &[],
    )
    .await
}

/// Writes it to the manifest and then applies it to the region.
async fn write_and_apply_region_edit(
    region_id: RegionId,
    manifest_ctx: &ManifestContextRef,
    version_control: &VersionControlRef,
    file_purger: &FilePurgerRef,
    edit: RegionEdit,
    memtables_to_remove: &[MemtableId],
) -> Result<()> {
    info!("Applying {edit:?} to region {}", region_id);

    let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));

    manifest_ctx
        .update_manifest(action_list, || {
            // Applies the edit to the region.
            version_control.apply_edit(edit, memtables_to_remove, file_purger.clone());
        })
        .await
}

async fn write_and_apply_truncate_action(
    manifest_ctx: ManifestContextRef,
    version_control: VersionControlRef,
    truncate: &RegionTruncate,
    memtable_builder: MemtableBuilderRef,
) -> Result<()> {
    // Write region truncated to manifest.
    let action_list =
        RegionMetaActionList::with_action(RegionMetaAction::Truncate(truncate.clone()));

    manifest_ctx
        .update_manifest(action_list, || {
            // Applies the truncate action to the region.
            version_control.truncate(
                truncate.truncated_entry_id,
                truncate.truncated_sequence,
                &memtable_builder,
            );
        })
        .await
}
