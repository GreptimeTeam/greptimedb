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

use std::str::FromStr;
use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use common_telemetry::{debug, info};
use humantime_serde::re::humantime;
use snafu::ResultExt;
use store_api::metadata::{
    InvalidSetRegionOptionRequestSnafu, MetadataError, RegionMetadata, RegionMetadataBuilder,
    RegionMetadataRef,
};
use store_api::mito_engine_options;
use store_api::region_request::{AlterKind, RegionAlterRequest, SetRegionOption};
use store_api::storage::RegionId;

use crate::error::{
    InvalidMetadataSnafu, InvalidRegionRequestSchemaVersionSnafu, InvalidRegionRequestSnafu, Result,
};
use crate::flush::FlushReason;
use crate::manifest::action::RegionChange;
use crate::region::options::CompactionOptions::Twcs;
use crate::region::options::TwcsOptions;
use crate::region::version::VersionRef;
use crate::region::MitoRegionRef;
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

        // fast path for memory state changes like options.
        match request.kind {
            AlterKind::SetRegionOptions { options } => {
                match self.handle_alter_region_options(region, version, options) {
                    Ok(_) => sender.send(Ok(0)),
                    Err(e) => sender.send(Err(e).context(InvalidMetadataSnafu)),
                }
                return;
            }
            AlterKind::UnsetRegionOptions { keys } => {
                // Converts the keys to SetRegionOption.
                //
                // It passes an empty string to achieve the purpose of unset
                match self.handle_alter_region_options(
                    region,
                    version,
                    keys.iter().map(Into::into).collect(),
                ) {
                    Ok(_) => sender.send(Ok(0)),
                    Err(e) => sender.send(Err(e).context(InvalidMetadataSnafu)),
                }
                return;
            }
            _ => {}
        }

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
        self.handle_alter_region_metadata(region, version, request, sender);
    }

    /// Handles region metadata changes.
    fn handle_alter_region_metadata(
        &mut self,
        region: MitoRegionRef,
        version: VersionRef,
        request: RegionAlterRequest,
        sender: OptionOutputTx,
    ) {
        let new_meta = match metadata_after_alteration(&version.metadata, request) {
            Ok(new_meta) => new_meta,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };
        // Persist the metadata to region's manifest.
        let change = RegionChange { metadata: new_meta };
        self.handle_manifest_region_change(region, change, sender)
    }

    /// Handles requests that changes region options, like TTL. It only affects memory state
    /// since changes are persisted in the `DatanodeTableValue` in metasrv.
    fn handle_alter_region_options(
        &mut self,
        region: MitoRegionRef,
        version: VersionRef,
        options: Vec<SetRegionOption>,
    ) -> std::result::Result<(), MetadataError> {
        let mut current_options = version.options.clone();
        for option in options {
            match option {
                SetRegionOption::TTL(new_ttl) => {
                    info!(
                        "Update region ttl: {}, previous: {:?} new: {:?}",
                        region.region_id, current_options.ttl, new_ttl
                    );
                    current_options.ttl = new_ttl;
                }
                SetRegionOption::Twsc(key, value) => {
                    let Twcs(options) = &mut current_options.compaction;
                    set_twcs_options(
                        options,
                        &TwcsOptions::default(),
                        &key,
                        &value,
                        region.region_id,
                    )?;
                }
            }
        }
        region.version_control.alter_options(current_options);
        Ok(())
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

fn set_twcs_options(
    options: &mut TwcsOptions,
    default_option: &TwcsOptions,
    key: &str,
    value: &str,
    region_id: RegionId,
) -> std::result::Result<(), MetadataError> {
    match key {
        mito_engine_options::TWCS_MAX_ACTIVE_WINDOW_RUNS => {
            let runs = parse_usize_with_default(key, value, default_option.max_active_window_runs)?;
            log_option_update(region_id, key, options.max_active_window_runs, runs);
            options.max_active_window_runs = runs;
        }
        mito_engine_options::TWCS_MAX_ACTIVE_WINDOW_FILES => {
            let files =
                parse_usize_with_default(key, value, default_option.max_active_window_files)?;
            log_option_update(region_id, key, options.max_active_window_files, files);
            options.max_active_window_files = files;
        }
        mito_engine_options::TWCS_MAX_INACTIVE_WINDOW_RUNS => {
            let runs =
                parse_usize_with_default(key, value, default_option.max_inactive_window_runs)?;
            log_option_update(region_id, key, options.max_inactive_window_runs, runs);
            options.max_inactive_window_runs = runs;
        }
        mito_engine_options::TWCS_MAX_INACTIVE_WINDOW_FILES => {
            let files =
                parse_usize_with_default(key, value, default_option.max_inactive_window_files)?;
            log_option_update(region_id, key, options.max_inactive_window_files, files);
            options.max_inactive_window_files = files;
        }
        mito_engine_options::TWCS_MAX_OUTPUT_FILE_SIZE => {
            let size = if value.is_empty() {
                default_option.max_output_file_size
            } else {
                Some(
                    ReadableSize::from_str(value)
                        .map_err(|_| InvalidSetRegionOptionRequestSnafu { key, value }.build())?,
                )
            };
            log_option_update(region_id, key, options.max_output_file_size, size);
            options.max_output_file_size = size;
        }
        mito_engine_options::TWCS_TIME_WINDOW => {
            let window = if value.is_empty() {
                default_option.time_window
            } else {
                Some(
                    humantime::parse_duration(value)
                        .map_err(|_| InvalidSetRegionOptionRequestSnafu { key, value }.build())?,
                )
            };
            log_option_update(region_id, key, options.time_window, window);
            options.time_window = window;
        }
        _ => return InvalidSetRegionOptionRequestSnafu { key, value }.fail(),
    }
    Ok(())
}

fn parse_usize_with_default(
    key: &str,
    value: &str,
    default: usize,
) -> std::result::Result<usize, MetadataError> {
    if value.is_empty() {
        Ok(default)
    } else {
        value
            .parse::<usize>()
            .map_err(|_| InvalidSetRegionOptionRequestSnafu { key, value }.build())
    }
}

fn log_option_update<T: std::fmt::Debug>(
    region_id: RegionId,
    option_name: &str,
    prev_value: T,
    cur_value: T,
) {
    info!(
        "Update region {}: {}, previous: {:?}, new: {:?}",
        option_name, region_id, prev_value, cur_value
    );
}
