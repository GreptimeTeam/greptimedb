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

use chrono::Utc;
use common_telemetry::{debug, info};
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::region_request::ApplyStagingManifestRequest;
use store_api::storage::RegionId;
use tokio::sync::oneshot;

use crate::error::{
    RegionStateSnafu, Result, SerdeJsonSnafu, StagingPartitionExprMismatchSnafu, UnexpectedSnafu,
};
use crate::manifest::action::{RegionEdit, RegionManifest};
use crate::manifest::storage::manifest_dir;
use crate::manifest::storage::staging::{StagingBlobStorage, staging_blob_path};
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::{OptionOutputTx, RegionEditRequest, WorkerRequest, WorkerRequestWithTime};
use crate::sst::location::region_dir_from_table_dir;
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_apply_staging_manifest_request(
        &mut self,
        region_id: RegionId,
        request: ApplyStagingManifestRequest,
        sender: OptionOutputTx,
    ) {
        let region = match self.regions.writable_region(region_id) {
            Ok(region) => region,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };

        if !region.is_staging() {
            let manifest_partition_expr = region.metadata().partition_expr.as_ref().cloned();
            let is_match = manifest_partition_expr.as_ref() == Some(&request.partition_expr);
            debug!(
                "region {} manifest partition expr: {:?}, request partition expr: {:?}",
                region_id, manifest_partition_expr, request.partition_expr
            );
            if is_match {
                // If current partition expr is already the same as the request,
                // treats the region already applied the staging manifest.
                info!(
                    "Region {} already applied the staging manifest, partition expr: {}, ignore the apply staging manifest request",
                    region_id, request.partition_expr
                );
                sender.send(Ok(0));
                return;
            }

            sender.send(
                RegionStateSnafu {
                    region_id,
                    state: region.state(),
                    expect: RegionRoleState::Leader(RegionLeaderState::Staging),
                }
                .fail(),
            );
            return;
        }

        let staging_partition_info = region.staging_partition_info.lock().unwrap().clone();
        // If the partition expr mismatch, return error.
        if staging_partition_info
            .as_ref()
            .and_then(|info| info.partition_expr().map(ToString::to_string))
            .as_ref()
            != Some(&request.partition_expr)
        {
            sender.send(
                StagingPartitionExprMismatchSnafu {
                    manifest_expr: staging_partition_info
                        .as_ref()
                        .and_then(|info| info.partition_expr().map(ToString::to_string)),
                    request_expr: request.partition_expr,
                }
                .fail(),
            );
            return;
        }

        let worker_sender = self.sender.clone();
        common_runtime::spawn_global(async move {
            let staging_manifest = match Self::fetch_staging_manifest(
                &region,
                request.central_region_id,
                &request.manifest_path,
            )
            .await
            {
                Ok(staging_manifest) => staging_manifest,
                Err(e) => {
                    sender.send(Err(e));
                    return;
                }
            };
            if staging_manifest.metadata.partition_expr.as_ref() != Some(&request.partition_expr) {
                sender.send(Err(StagingPartitionExprMismatchSnafu {
                    manifest_expr: staging_manifest.metadata.partition_expr.clone(),
                    request_expr: request.partition_expr,
                }
                .build()));
                return;
            }

            let files_to_add = staging_manifest.files.values().cloned().collect::<Vec<_>>();
            let edit = RegionEdit {
                files_to_add,
                files_to_remove: vec![],
                timestamp_ms: Some(Utc::now().timestamp_millis()),
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            };

            let (tx, rx) = oneshot::channel();
            info!(
                "Applying staging manifest request to region {}",
                region.region_id,
            );
            let _ = worker_sender
                .send(WorkerRequestWithTime::new(WorkerRequest::EditRegion(
                    RegionEditRequest {
                        region_id: region.region_id,
                        edit,
                        tx,
                    },
                )))
                .await;

            // Await the result from the region edit and forward the outcome to the original sender.
            // If the operation completes successfully, respond with Ok(0); otherwise, respond with an appropriate error.
            if let Ok(result) = rx.await {
                let Ok(()) = result else {
                    sender.send(result.map(|_| 0));
                    return;
                };
                let mut manager = region.manifest_ctx.manifest_manager.write().await;
                match region.exit_staging_on_success(&mut manager).await {
                    Ok(()) => {
                        sender.send(Ok(0));
                    }
                    Err(e) => sender.send(Err(e)),
                }
            } else {
                sender.send(
                    UnexpectedSnafu {
                        reason: "edit region receiver channel closed",
                    }
                    .fail(),
                );
            }
        });
    }

    /// Fetches the staging manifest from the central region's staging blob storage.
    ///
    /// The `central_region_id` is used to locate the staging directory because the staging
    /// manifest was created by the central region during `remap_manifests` operation.
    async fn fetch_staging_manifest(
        region: &MitoRegionRef,
        central_region_id: RegionId,
        manifest_path: &str,
    ) -> Result<RegionManifest> {
        let region_dir =
            region_dir_from_table_dir(region.table_dir(), central_region_id, region.path_type());
        let staging_blob_path = staging_blob_path(&manifest_dir(&region_dir));
        let staging_blob_storage = StagingBlobStorage::new(
            staging_blob_path,
            region.access_layer().object_store().clone(),
        );
        let staging_manifest = staging_blob_storage.get(manifest_path).await?;

        serde_json::from_slice::<RegionManifest>(&staging_manifest).context(SerdeJsonSnafu)
    }
}
