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

//! Handling truncate related requests.

use common_telemetry::{debug, info};
use store_api::logstore::LogStore;
use store_api::region_request::RegionTruncateRequest;
use store_api::storage::RegionId;

use crate::error::RegionNotFoundSnafu;
use crate::manifest::action::{RegionTruncate, TruncateKind};
use crate::region::RegionLeaderState;
use crate::request::{OptionOutputTx, TruncateResult};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_truncate_request(
        &mut self,
        region_id: RegionId,
        req: RegionTruncateRequest,
        sender: OptionOutputTx,
    ) {
        let region = match self.regions.writable_non_staging_region(region_id) {
            Ok(region) => region,
            Err(e) => {
                sender.send(Err(e));
                return;
            }
        };

        let version_data = region.version_control.current();

        match req {
            RegionTruncateRequest::All => {
                info!("Try to fully truncate region {}", region_id);

                let truncated_entry_id = version_data.last_entry_id;
                let truncated_sequence = version_data.committed_sequence;

                // Write region truncated to manifest.
                let truncate = RegionTruncate {
                    region_id,
                    kind: TruncateKind::All {
                        truncated_entry_id,
                        truncated_sequence,
                    },
                    timestamp_ms: None,
                };

                self.handle_manifest_truncate_action(region, truncate, sender);
            }
            RegionTruncateRequest::ByTimeRanges { time_ranges } => {
                info!(
                    "Try to partially truncate region {} by time ranges: {:?}",
                    region_id, time_ranges
                );
                // find all files that are fully contained in the time ranges
                let mut files_to_truncate = Vec::new();
                for level in version_data.version.ssts.levels() {
                    for file in level.files() {
                        let file_time_range = file.time_range();
                        // TODO(discord9): This is a naive way to check if is contained, we should
                        // optimize it later.
                        let is_subset = time_ranges.iter().any(|(start, end)| {
                            file_time_range.0 >= *start && file_time_range.1 <= *end
                        });

                        if is_subset {
                            files_to_truncate.push(file.meta_ref().clone());
                        }
                    }
                }
                debug!(
                    "Found {} files to partially truncate in region {}",
                    files_to_truncate.len(),
                    region_id
                );

                // this could happen if all files are not fully contained in the time ranges
                if files_to_truncate.is_empty() {
                    info!("No files to truncate in region {}", region_id);
                    // directly send success back as no files to truncate
                    // and no background notify is needed
                    sender.send(Ok(0));
                    return;
                }
                let truncate = RegionTruncate {
                    region_id,
                    kind: TruncateKind::Partial {
                        files_to_remove: files_to_truncate,
                    },
                    timestamp_ms: None,
                };
                self.handle_manifest_truncate_action(region, truncate, sender);
            }
        };
    }

    /// Handles truncate result.
    pub(crate) async fn handle_truncate_result(&mut self, truncate_result: TruncateResult) {
        let region_id = truncate_result.region_id;
        let Some(region) = self.regions.get_region(region_id) else {
            truncate_result.sender.send(
                RegionNotFoundSnafu {
                    region_id: truncate_result.region_id,
                }
                .fail(),
            );
            return;
        };

        // We are already in the worker loop so we can set the state first.
        region.switch_state_to_writable(RegionLeaderState::Truncating);

        match truncate_result.result {
            Ok(()) => {
                // Applies the truncate action to the region.
                region
                    .version_control
                    .truncate(truncate_result.kind.clone(), &region.memtable_builder);
            }
            Err(e) => {
                // Unable to truncate the region.
                truncate_result.sender.send(Err(e));
                return;
            }
        }

        // Notifies flush scheduler.
        self.flush_scheduler.on_region_truncated(region_id);
        // Notifies compaction scheduler.
        self.compaction_scheduler.on_region_truncated(region_id);
        // Notifies index build scheduler.
        self.index_build_scheduler
            .on_region_truncated(region_id)
            .await;

        if let TruncateKind::All {
            truncated_entry_id,
            truncated_sequence: _,
        } = &truncate_result.kind
        {
            // Make all data obsolete.
            if let Err(e) = self
                .wal
                .obsolete(region_id, *truncated_entry_id, &region.provider)
                .await
            {
                truncate_result.sender.send(Err(e));
                return;
            }
        }

        info!(
            "Complete truncating region: {}, kind: {:?}.",
            region_id, truncate_result.kind
        );

        truncate_result.sender.send(Ok(0));
    }
}
