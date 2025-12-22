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
use common_telemetry::{debug, info, warn};
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::region_request::ApplyStagingManifestRequest;
use store_api::storage::RegionId;
use tokio::sync::oneshot;

use crate::error::{
    RegionStateSnafu, SerdeJsonSnafu, StagingPartitionExprMismatchSnafu, UnexpectedSnafu,
};
use crate::manifest::action::RegionEdit;
use crate::region::{RegionLeaderState, RegionRoleState};
use crate::request::{OptionOutputTx, RegionEditRequest};
use crate::sst::file::FileMeta;
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
                warn!(
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

        let staging_partition_expr = region.staging_partition_expr.lock().unwrap().clone();
        // If the partition expr mismatch, return error.
        if staging_partition_expr.as_ref() != Some(&request.partition_expr) {
            sender.send(
                StagingPartitionExprMismatchSnafu {
                    manifest_expr: staging_partition_expr,
                    request_expr: request.partition_expr,
                }
                .fail(),
            );
            return;
        }

        let (tx, rx) = oneshot::channel();
        let files_to_add = match serde_json::from_slice::<Vec<FileMeta>>(&request.files_to_add)
            .context(SerdeJsonSnafu)
        {
            Ok(files_to_add) => files_to_add,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };

        info!("Applying staging manifest request to region {}", region_id);
        self.handle_region_edit(RegionEditRequest {
            region_id,
            edit: RegionEdit {
                files_to_add,
                files_to_remove: vec![],
                timestamp_ms: Some(Utc::now().timestamp_millis()),
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            },
            tx,
        });

        common_runtime::spawn_global(async move {
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
}
