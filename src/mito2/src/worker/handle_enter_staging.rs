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

use std::sync::Arc;
use std::time::Instant;

use common_telemetry::{error, info, warn};
use store_api::logstore::LogStore;
use store_api::region_request::EnterStagingRequest;
use store_api::storage::RegionId;

use crate::error::{RegionNotFoundSnafu, StagingPartitionExprMismatchSnafu};
use crate::flush::FlushReason;
use crate::manifest::action::{RegionChange, RegionMetaAction, RegionMetaActionList};
use crate::region::{MitoRegionRef, RegionLeaderState};
use crate::request::{
    BackgroundNotify, DdlRequest, EnterStagingResult, OptionOutputTx, SenderDdlRequest,
    WorkerRequest, WorkerRequestWithTime,
};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_enter_staging_request(
        &mut self,
        region_id: RegionId,
        partition_expr: String,
        mut sender: OptionOutputTx,
    ) {
        let Some(region) = self.regions.writable_region_or(region_id, &mut sender) else {
            return;
        };

        // If the region is already in staging mode, verify the partition expr matches.
        if region.is_staging() {
            let staging_partition_expr = region.staging_partition_expr.lock().unwrap().clone();
            // If the partition expr mismatch, return error.
            if staging_partition_expr.as_ref() != Some(&partition_expr) {
                sender.send(Err(StagingPartitionExprMismatchSnafu {
                    manifest_expr: staging_partition_expr,
                    request_expr: partition_expr,
                }
                .build()));
                return;
            }

            // If the partition expr matches, return success.
            sender.send(Ok(0));
            return;
        }

        let version = region.version();
        if !version.memtables.is_empty() {
            // If memtable is not empty, we can't enter staging directly and need to flush
            // all memtables first.
            info!("Flush region: {} before entering staging", region_id);
            debug_assert!(!region.is_staging());
            let task = self.new_flush_task(
                &region,
                FlushReason::EnterStaging,
                None,
                self.config.clone(),
                region.is_staging(),
            );
            if let Err(e) =
                self.flush_scheduler
                    .schedule_flush(region.region_id, &region.version_control, task)
            {
                // Unable to flush the region, send error to waiter.
                sender.send(Err(e));
                return;
            }

            // Safety: We have requested flush.
            self.flush_scheduler
                .add_ddl_request_to_pending(SenderDdlRequest {
                    region_id,
                    sender,
                    request: DdlRequest::EnterStaging(EnterStagingRequest { partition_expr }),
                });

            return;
        }

        self.handle_enter_staging(region, partition_expr, sender);
    }

    fn handle_enter_staging(
        &self,
        region: MitoRegionRef,
        partition_expr: String,
        sender: OptionOutputTx,
    ) {
        if let Err(e) = region.set_entering_staging() {
            sender.send(Err(e));
            return;
        }

        let listener = self.listener.clone();
        let request_sender = self.sender.clone();
        common_runtime::spawn_global(async move {
            let now = Instant::now();

            // First step: clear all staging manifest files.
            let manager = region.manifest_ctx.manifest_manager.write().await;
            match manager.store().clear_staging_manifests().await {
                Ok(_) => {
                    info!(
                        "Cleared all staging manifest files for region {}, elapsed: {:?}",
                        region.region_id,
                        now.elapsed(),
                    );
                }
                Err(e) => {
                    error!(
                        e;
                        "Failed to clear staging manifest files for region {}",
                        region.region_id
                    );
                    if let Err(e) = region.exit_entering_staging() {
                        error!(e; "Failed to exit entering staging after failed to clear staging manifest files");
                    }
                    sender.send(Err(e));
                    return;
                }
            }
            // Drop the write lock to avoid dead lock.
            drop(manager);

            // Second step: write new staging manifest.
            let mut new_meta = (*region.metadata()).clone();
            new_meta.partition_expr = Some(partition_expr.clone());
            let sst_format = region.version().options.sst_format.unwrap_or_default();
            let change = RegionChange {
                metadata: Arc::new(new_meta),
                sst_format,
            };
            let action_list = RegionMetaActionList::with_action(RegionMetaAction::Change(change));
            let result = region
                .manifest_ctx
                .update_manifest(RegionLeaderState::EnteringStaging, action_list, true)
                .await
                .map(|_| ());
            info!(
                "Created staging manifest for region {}, elapsed: {:?}",
                region.region_id,
                now.elapsed(),
            );

            let notify = WorkerRequest::Background {
                region_id: region.region_id,
                notify: BackgroundNotify::EnterStaging(EnterStagingResult {
                    region_id: region.region_id,
                    sender,
                    result,
                    partition_expr,
                }),
            };
            listener
                .on_enter_staging_result_begin(region.region_id)
                .await;

            if let Err(res) = request_sender
                .send(WorkerRequestWithTime::new(notify))
                .await
            {
                warn!(
                    "Failed to send enter staging result back to the worker, region_id: {}, res: {:?}",
                    region.region_id, res
                );
            }
        });
    }

    /// Handles enter staging result.
    pub(crate) async fn handle_enter_staging_result(
        &mut self,
        enter_staging_result: EnterStagingResult,
    ) {
        let region = match self.regions.get_region(enter_staging_result.region_id) {
            Some(region) => region,
            None => {
                self.reject_region_stalled_requests(&enter_staging_result.region_id);
                enter_staging_result.sender.send(
                    RegionNotFoundSnafu {
                        region_id: enter_staging_result.region_id,
                    }
                    .fail(),
                );
                return;
            }
        };

        let clean_staging_manifests = |region: MitoRegionRef| {
            common_runtime::spawn_global(async move {
                let mut manager = region.manifest_ctx.manifest_manager.write().await;
                if let Err(e) = manager.clear_staging_manifests().await {
                    error!(e; "Failed to clear staging manifests after failed to switch region state to staging");
                }
            });
        };

        if let Err(e) = region.switch_state_to_staging(RegionLeaderState::EnteringStaging) {
            error!(e; "Failed to switch region state to staging");
            enter_staging_result.sender.send(Err(e));
            clean_staging_manifests(region);
            return;
        }

        if enter_staging_result.result.is_ok() {
            info!(
                "Updating region {} staging partition expr to {}",
                region.region_id, enter_staging_result.partition_expr
            );
            Self::update_region_staging_partition_expr(
                &region,
                enter_staging_result.partition_expr,
            );
        } else {
            clean_staging_manifests(region);
        }
        enter_staging_result
            .sender
            .send(enter_staging_result.result.map(|_| 0));
        // Handles the stalled requests.
        self.handle_region_stalled_requests(&enter_staging_result.region_id)
            .await;
    }

    fn update_region_staging_partition_expr(region: &MitoRegionRef, partition_expr: String) {
        let mut staging_partition_expr = region.staging_partition_expr.lock().unwrap();
        debug_assert!(staging_partition_expr.is_none());
        *staging_partition_expr = Some(partition_expr);
    }
}
