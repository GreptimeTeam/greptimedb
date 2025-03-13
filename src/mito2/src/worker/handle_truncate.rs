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

use common_telemetry::info;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;

use crate::error::RegionNotFoundSnafu;
use crate::manifest::action::RegionTruncate;
use crate::manifest::notifier::ManifestChangeEvent;
use crate::region::RegionLeaderState;
use crate::request::{OptionOutputTx, TruncateResult};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_truncate_request(
        &mut self,
        region_id: RegionId,
        mut sender: OptionOutputTx,
    ) {
        let Some(region) = self.regions.writable_region_or(region_id, &mut sender) else {
            return;
        };

        info!("Try to truncate region {}", region_id);

        let version_data = region.version_control.current();
        let truncated_entry_id = version_data.last_entry_id;
        let truncated_sequence = version_data.committed_sequence;

        // Write region truncated to manifest.
        let truncate = RegionTruncate {
            region_id,
            truncated_entry_id,
            truncated_sequence,
        };
        self.handle_manifest_truncate_action(region, truncate, sender);
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
            Ok(manifest) => {
                // Applies the truncate action to the region.
                region.version_control.truncate(
                    truncate_result.truncated_entry_id,
                    truncate_result.truncated_sequence,
                    &region.memtable_builder,
                );

                // Notifies the manifest change to the downstream.
                self.notify_manifest_change(&region, manifest, ManifestChangeEvent::Truncate)
                    .await;
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

        // Make all data obsolete.
        if let Err(e) = self
            .wal
            .obsolete(
                region_id,
                truncate_result.truncated_entry_id,
                &region.provider,
            )
            .await
        {
            truncate_result.sender.send(Err(e));
            return;
        }

        info!(
            "Complete truncating region: {}, entry id: {} and sequence: {}.",
            region_id, truncate_result.truncated_entry_id, truncate_result.truncated_sequence
        );

        truncate_result.sender.send(Ok(0));
    }
}
