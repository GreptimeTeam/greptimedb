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

use snafu::ensure;
use store_api::storage::RegionId;
use tokio::sync::oneshot::Sender;

use crate::error::{InvalidRequestSnafu, Result};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList, RegionTruncate};
use crate::manifest::manager::RegionManifestManagerRef;
use crate::region::version::VersionControlRef;
use crate::region::MitoRegionRef;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    /// Handles region edit request.
    pub(crate) async fn handle_region_edit(
        &self,
        region_id: RegionId,
        edit: RegionEdit,
        sender: Sender<Result<()>>,
    ) {
        todo!()
    }

    /// Writes truncate action to the manifest and then applies it to the region in background.
    pub(crate) fn handle_manifest_truncate_action(
        &self,
        region: MitoRegionRef,
        truncate: RegionTruncate,
        sender: Sender<Result<()>>,
    ) -> Result<()> {
        let request_sender = self.sender.clone();
        let manifest_manager = region.manifest_manager.clone();
        let version_control = region.version_control.clone();

        // Updates manifest in background.
        common_runtime::spawn_bg(async move {
            //
        });

        todo!()
    }

    /// Checks the region edit, writes it to the manifest and then applies it to the region.
    async fn edit_region(&self, region_id: RegionId, edit: RegionEdit) -> Result<()> {
        let region = self.regions.writable_region(region_id)?;

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

        // Applying region edit directly has nothing to do with memtables (at least for now).
        region.apply_edit(edit, &[]).await
    }
}

async fn write_and_apply_truncate_action(manifest_manager: RegionManifestManagerRef, version_control: VersionControlRef, truncate: RegionTruncate, sender: ) -> Result<()> {
    // Write region truncated to manifest.
    let action_list =
        RegionMetaActionList::with_action(RegionMetaAction::Truncate(truncate.clone()));

    // Acquires the write lock of the manifest manager.
    let mut manifest_manager = manifest_manager.write().await;
    manifest_manager.update(action_list).await?;

    todo!()
}
