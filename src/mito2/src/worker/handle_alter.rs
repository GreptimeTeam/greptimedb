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

//! Handling alter related requests.

use std::sync::Arc;

use common_telemetry::{debug, info};
use snafu::ResultExt;
use store_api::metadata::{RegionMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::region_request::RegionAlterRequest;
use store_api::storage::RegionId;

use crate::error::{
    InvalidMetadataSnafu, InvalidRegionRequestSchemaVersionSnafu, InvalidRegionRequestSnafu, Result,
};
use crate::flush::FlushReason;
use crate::manifest::action::RegionChange;
use crate::request::{DdlRequest, OptionOutputTx, SenderDdlRequest};
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_alter_request(
        &mut self,
        region_id: RegionId,
        request: RegionAlterRequest,
        mut sender: OptionOutputTx,
    ) {
        let Some(region) = self.regions.writable_region_or(region_id, &mut sender) else {
            return;
        };

        info!("Try to alter region: {}, request: {:?}", region_id, request);

        // Get the version before alter.
        let version = region.version();
        if version.metadata.schema_version != request.schema_version {
            // This is possible if we retry the request.
            debug!(
                "Ignores alter request, region id:{}, region schema version {} is not equal to request schema version {}",
                region_id, version.metadata.schema_version, request.schema_version
            );
            // Returns an error.
            sender.send(
                InvalidRegionRequestSchemaVersionSnafu {
                    expect: version.metadata.schema_version,
                    actual: request.schema_version,
                }
                .fail(),
            );
            return;
        }
        // Validate request.
        if let Err(e) = request.validate(&version.metadata) {
            // Invalid request.
            sender.send(Err(e).context(InvalidRegionRequestSnafu));
            return;
        }
        // Checks whether we need to alter the region.
        if !request.need_alter(&version.metadata) {
            debug!(
                "Ignores alter request as it alters nothing, region_id: {}, request: {:?}",
                region_id, request
            );
            sender.send(Ok(0));
            return;
        }

        // Checks whether we can alter the region directly.
        if !version.memtables.is_empty() {
            // If memtable is not empty, we can't alter it directly and need to flush
            // all memtables first.
            info!("Flush region: {} before alteration", region_id);

            // Try to submit a flush task.
            let task = self.new_flush_task(&region, FlushReason::Alter, None, self.config.clone());
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
                    request: DdlRequest::Alter(request),
                });

            return;
        }

        info!(
            "Try to alter region {} from version {} to {}",
            region_id,
            version.metadata.schema_version,
            region.metadata().schema_version
        );

        let new_meta = match metadata_after_alteration(&version.metadata, request) {
            Ok(new_meta) => new_meta,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };
        // Persist the metadata to region's manifest.
        let change = RegionChange {
            metadata: new_meta.clone(),
        };
        self.handle_manifest_region_change(region, change, sender)
    }
}

/// Creates a metadata after applying the alter `request` to the old `metadata`.
///
/// Returns an error if the `request` is invalid.
fn metadata_after_alteration(
    metadata: &RegionMetadata,
    request: RegionAlterRequest,
) -> Result<RegionMetadataRef> {
    let mut builder = RegionMetadataBuilder::from_existing(metadata.clone());
    builder
        .alter(request.kind)
        .context(InvalidRegionRequestSnafu)?
        .bump_version();
    let new_meta = builder.build().context(InvalidMetadataSnafu)?;
    assert_eq!(request.schema_version + 1, new_meta.schema_version);

    Ok(Arc::new(new_meta))
}
