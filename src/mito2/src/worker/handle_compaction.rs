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

use api::v1::region::compact_request;
use common_telemetry::{error, info, warn};
use store_api::region_request::RegionCompactRequest;
use store_api::storage::RegionId;

use crate::error::RegionNotFoundSnafu;
use crate::metrics::COMPACTION_REQUEST_COUNT;
use crate::region::MitoRegionRef;
use crate::request::{CompactionFailed, CompactionFinished, OnFailure, OptionOutputTx};
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    /// Handles compaction request submitted to region worker.
    pub(crate) async fn handle_compaction_request(
        &mut self,
        region_id: RegionId,
        req: RegionCompactRequest,
        mut sender: OptionOutputTx,
    ) {
        let Some(region) = self.regions.writable_region_or(region_id, &mut sender) else {
            return;
        };
        COMPACTION_REQUEST_COUNT.inc();
        if let Err(e) = self
            .compaction_scheduler
            .schedule_compaction(
                region.region_id,
                req.options,
                &region.version_control,
                &region.access_layer,
                sender,
                &region.manifest_ctx,
                self.schema_metadata_manager.clone(),
                // TODO(yingwen): expose this to frontend
                1,
            )
            .await
        {
            error!(e; "Failed to schedule compaction task for region: {}", region_id);
        } else {
            info!(
                "Successfully scheduled compaction task for region: {}",
                region_id
            );
        }
    }

    /// Handles compaction finished, update region version and manifest, deleted compacted files.
    pub(crate) async fn handle_compaction_finished(
        &mut self,
        region_id: RegionId,
        mut request: CompactionFinished,
    ) {
        let region = match self.regions.get_region(region_id) {
            Some(region) => region,
            None => {
                request.on_failure(RegionNotFoundSnafu { region_id }.build());
                return;
            }
        };
        region.update_compaction_millis();

        region
            .version_control
            .apply_edit(request.edit.clone(), &[], region.file_purger.clone());

        // compaction finished.
        request.on_success();

        // Schedule next compaction if necessary.
        self.compaction_scheduler
            .on_compaction_finished(
                region_id,
                &region.manifest_ctx,
                self.schema_metadata_manager.clone(),
            )
            .await;
    }

    /// When compaction fails, we simply log the error.
    pub(crate) async fn handle_compaction_failure(&mut self, req: CompactionFailed) {
        error!(req.err; "Failed to compact region: {}", req.region_id);

        self.compaction_scheduler
            .on_compaction_failed(req.region_id, req.err);
    }

    /// Schedule compaction for the region if necessary.
    pub(crate) async fn schedule_compaction(&mut self, region: &MitoRegionRef) {
        let now = self.time_provider.current_time_millis();
        if now - region.last_compaction_millis()
            >= self.config.min_compaction_interval.as_millis() as i64
        {
            if let Err(e) = self
                .compaction_scheduler
                .schedule_compaction(
                    region.region_id,
                    compact_request::Options::Regular(Default::default()),
                    &region.version_control,
                    &region.access_layer,
                    OptionOutputTx::none(),
                    &region.manifest_ctx,
                    self.schema_metadata_manager.clone(),
                    1,
                )
                .await
            {
                warn!(
                    "Failed to schedule compaction for region: {}, err: {}",
                    region.region_id, e
                );
            }
        }
    }
}
