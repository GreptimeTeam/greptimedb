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
use common_telemetry::info;
use common_telemetry::tracing::warn;
use humantime_serde::re::humantime;
use snafu::{ResultExt, ensure};
use common_wal::options::WalOptions;
use store_api::logstore::LogStore;
use store_api::metadata::{
    InvalidSetRegionOptionRequestSnafu, MetadataError, RegionMetadata, RegionMetadataBuilder,
    RegionMetadataRef,
};
use store_api::mito_engine_options;
use store_api::region_request::{AlterKind, RegionAlterRequest, SetRegionOption};
use store_api::storage::RegionId;

use crate::error::{InvalidMetadataSnafu, InvalidRegionRequestSnafu, Result};
use crate::flush::FlushReason;
use crate::manifest::action::RegionChange;
use crate::region::MitoRegionRef;
use crate::region::options::CompactionOptions::Twcs;
use crate::region::options::{RegionOptions, TwcsOptions};
use crate::region::version::VersionRef;
use crate::request::{DdlRequest, OptionOutputTx, SenderDdlRequest};
use crate::sst::FormatType;
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_alter_request(
        &mut self,
        region_id: RegionId,
        request: RegionAlterRequest,
        sender: OptionOutputTx,
    ) {
        let region = match self.regions.writable_non_staging_region(region_id) {
            Ok(region) => region,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };

        info!("Try to alter region: {}, request: {:?}", region_id, request);

        // Gets the version before alter.
        let mut version = region.version();

        // fast path for memory state changes like options.
        let set_options = match &request.kind {
            AlterKind::SetRegionOptions { options } => options.clone(),
            AlterKind::UnsetRegionOptions { keys } => {
                // Converts the keys to SetRegionOption.
                //
                // It passes an empty string to achieve the purpose of unset
                keys.iter().map(Into::into).collect()
            }
            _ => Vec::new(),
        };
        if !set_options.is_empty() {
            match self.handle_alter_region_options_fast(&region, version, set_options) {
                Ok(new_version) => {
                    let Some(new_version) = new_version else {
                        // We don't have options to alter after flush.
                        sender.send(Ok(0));
                        return;
                    };
                    version = new_version;
                }
                Err(e) => {
                    sender.send(Err(e).context(InvalidMetadataSnafu));
                    return;
                }
            }
        }

        // Validates request.
        if let Err(e) = request.validate(&version.metadata) {
            // Invalid request.
            sender.send(Err(e).context(InvalidRegionRequestSnafu));
            return;
        }

        // Checks whether we need to alter the region.
        if !request.need_alter(&version.metadata) {
            warn!(
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
            "Try to alter region {}, version.metadata: {:?}, version.options: {:?}, request: {:?}",
            region_id, version.metadata, version.options, request,
        );
        self.handle_alter_region_with_empty_memtable(region, version, request, sender);
    }

    // TODO(yingwen): Optional new options and sst format.
    /// Handles region metadata and format changes when the region memtable is empty.
    fn handle_alter_region_with_empty_memtable(
        &mut self,
        region: MitoRegionRef,
        version: VersionRef,
        request: RegionAlterRequest,
        sender: OptionOutputTx,
    ) {
        let need_index = need_change_index(&request.kind);
        let new_options = new_region_options_on_empty_memtable(&version.options, &request.kind);
        let new_meta = match metadata_after_alteration(&version.metadata, request) {
            Ok(new_meta) => new_meta,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };
        // Persist the metadata to region's manifest.
        let change = RegionChange {
            metadata: new_meta,
            sst_format: new_options
                .as_ref()
                .unwrap_or(&version.options)
                .sst_format
                .unwrap_or_default(),
        };
        self.handle_manifest_region_change(region, change, need_index, new_options, sender);
    }

    /// Handles requests that changes region options, like TTL. It only affects memory state
    /// since changes are persisted in the `DatanodeTableValue` in metasrv.
    ///
    /// If the options require empty memtable, it only does validation.
    ///
    /// Returns a new version with the updated options if it needs further alteration.
    fn handle_alter_region_options_fast(
        &mut self,
        region: &MitoRegionRef,
        version: VersionRef,
        options: Vec<SetRegionOption>,
    ) -> std::result::Result<Option<VersionRef>, MetadataError> {
        assert!(!options.is_empty());

        let mut all_options_altered = true;
        let mut current_options = version.options.clone();
        for option in options {
            match option {
                SetRegionOption::Ttl(new_ttl) => {
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
                SetRegionOption::Format(format_str) => {
                    let new_format = format_str.parse::<FormatType>().map_err(|_| {
                        store_api::metadata::InvalidRegionRequestSnafu {
                            region_id: region.region_id,
                            err: format!("Invalid format type: {}", format_str),
                        }
                        .build()
                    })?;
                    // If the format is unchanged, we also consider the option is altered.
                    if new_format != current_options.sst_format.unwrap_or_default() {
                        all_options_altered = false;

                        // Validates the format type.
                        ensure!(
                            new_format == FormatType::Flat,
                            store_api::metadata::InvalidRegionRequestSnafu {
                                region_id: region.region_id,
                                err: "Only allow changing format type to flat",
                            }
                        );
                    }
                }
                SetRegionOption::SkipWal(skip_wal) => {
                    info!(
                        "Update region skip_wal: {}, previous: {:?} new: {}",
                        region.region_id, current_options.wal_options, skip_wal
                    );
                    if skip_wal {
                        // Disable WAL by setting to Noop
                        current_options.wal_options = WalOptions::Noop;
                    } else {
                        // Enable WAL: restore to default (RaftEngine)
                        // TODO: In distributed mode, this should be allocated by metasrv,
                        // but for simplicity, we use RaftEngine as default here.
                        // The actual WAL options will be persisted in metasrv.
                        // We should read the correct WAL options from DatanodeTableValue
                        // or pass them through the AlterRegionRequest.
                        current_options.wal_options = WalOptions::RaftEngine;
                    }
                }
            }
        }
        region.version_control.alter_options(current_options);
        if all_options_altered {
            Ok(None)
        } else {
            Ok(Some(region.version()))
        }
    }
}

/// Returns the new region options if there are updates to the options.
fn new_region_options_on_empty_memtable(
    current_options: &RegionOptions,
    kind: &AlterKind,
) -> Option<RegionOptions> {
    let AlterKind::SetRegionOptions { options } = kind else {
        return None;
    };

    if options.is_empty() {
        return None;
    }

    let mut current_options = current_options.clone();
    for option in options {
        match option {
            SetRegionOption::Ttl(_) | SetRegionOption::Twsc(_, _) => (),
            SetRegionOption::Format(format_str) => {
                // Safety: handle_alter_region_options_fast() has validated this.
                let new_format = format_str.parse::<FormatType>().unwrap();
                assert_eq!(FormatType::Flat, new_format);

                current_options.sst_format = Some(new_format);
            }
            SetRegionOption::SkipWal(skip_wal) => {
                if *skip_wal {
                    current_options.wal_options = WalOptions::Noop;
                } else {
                    current_options.wal_options = WalOptions::RaftEngine;
                }
            }
        }
    }
    Some(current_options)
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
        mito_engine_options::TWCS_TRIGGER_FILE_NUM => {
            let files = parse_usize_with_default(key, value, default_option.trigger_file_num)?;
            log_option_update(region_id, key, options.trigger_file_num, files);
            options.trigger_file_num = files;
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

/// Used to determine whether we can build index directly after schema change.
fn need_change_index(kind: &AlterKind) -> bool {
    match kind {
        // `SetIndexes` is a fast-path operation because it can build indexes for existing SSTs
        // in the background, without needing to wait for a flush or compaction cycle.
        AlterKind::SetIndexes { options: _ } => true,
        // For AddColumns, DropColumns, UnsetIndexes and ModifyColumnTypes, we don't treat them as index changes.
        // Index files still need to be rebuilt after schema changes,
        // but this will happen automatically during flush or compaction.
        _ => false,
    }
}
