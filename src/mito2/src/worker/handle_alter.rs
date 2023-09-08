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

use common_query::Output;
use common_telemetry::{error, info};
use snafu::ResultExt;
use store_api::metadata::{RegionMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::region_request::RegionAlterRequest;
use store_api::storage::RegionId;
use tokio::sync::oneshot;

use crate::error::{InvalidMetadataSnafu, InvalidRegionRequestSnafu, RegionNotFoundSnafu, Result};
use crate::flush::FlushReason;
use crate::manifest::action::{RegionChange, RegionMetaAction, RegionMetaActionList};
use crate::memtable::MemtableBuilderRef;
use crate::region::version::Version;
use crate::region::MitoRegionRef;
use crate::request::{DdlRequest, SenderDdlRequest};
use crate::worker::{send_result, RegionWorkerLoop};

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_alter_request(
        &mut self,
        region_id: RegionId,
        request: RegionAlterRequest,
        sender: Option<oneshot::Sender<Result<Output>>>,
    ) {
        let Some(region) = self.regions.get_region(region_id) else {
            send_result(sender, RegionNotFoundSnafu { region_id }.fail());
            return;
        };

        info!("Try to alter region: {}, request: {:?}", region_id, request);

        let version = region.version();
        // Checks whether we can alter the region directly.
        if !can_alter_directly(&version) {
            // We need to flush all memtables first.
            info!("Flush region: {} before alteration", region_id);

            // Try to submit a flush task.
            let task = self.new_flush_task(&region, FlushReason::Alter);
            if let Err(e) = self.flush_scheduler.schedule_flush(&region, task) {
                // Unable to flush the region, send error to waiter.
                send_result(sender, Err(e));
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

        // Now we can alter the region directly.
        if let Err(e) =
            alter_region_schema(&region, &version, request, &self.memtable_builder).await
        {
            error!(e; "Failed to alter region schema, region_id: {}", region_id);
            send_result(sender, Err(e));
            return;
        }

        info!("Schema of region {} is altered", region_id);

        // Notifies waiters.
        send_result(sender, Ok(Output::AffectedRows(0)));
    }
}

/// Alter the schema of the region.
async fn alter_region_schema(
    region: &MitoRegionRef,
    version: &Version,
    request: RegionAlterRequest,
    builder: &MemtableBuilderRef,
) -> Result<()> {
    let new_meta = metadata_after_alteration(&version.metadata, request)?;
    // Persist the metadata to region's manifest.
    let change = RegionChange {
        metadata: new_meta.clone(),
    };
    let action_list = RegionMetaActionList::with_action(RegionMetaAction::Change(change));
    region.manifest_manager.update(action_list).await?;

    // Apply the metadata to region's version.
    region.version_control.alter_schema(new_meta, builder);
    Ok(())
}

/// Checks whether all memtables are empty.
fn can_alter_directly(version: &Version) -> bool {
    version.memtables.mutable.is_empty() && version.memtables.immutables().is_empty()
}

/// Creates a metadata after applying the alter `request` to the old `metadata`.
///
/// Returns an error if the `request` is invalid.
fn metadata_after_alteration(
    metadata: &RegionMetadata,
    request: RegionAlterRequest,
) -> Result<RegionMetadataRef> {
    // Validates request.
    request
        .validate(metadata)
        .context(InvalidRegionRequestSnafu)?;

    let mut builder = RegionMetadataBuilder::from_existing(metadata.clone());
    builder
        .alter(request.kind)
        .context(InvalidRegionRequestSnafu)?
        .bump_version();
    let new_meta = builder.build().context(InvalidMetadataSnafu)?;
    assert_eq!(request.schema_version + 1, new_meta.schema_version);

    Ok(Arc::new(new_meta))
}
