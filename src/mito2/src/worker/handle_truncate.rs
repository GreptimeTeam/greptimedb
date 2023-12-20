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
use store_api::region_request::AffectedRows;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::manifest::action::{RegionMetaAction, RegionMetaActionList, RegionTruncate};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_truncate_request(
        &mut self,
        region_id: RegionId,
    ) -> Result<AffectedRows> {
        let region = self.regions.writable_region(region_id)?;

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
        let action_list =
            RegionMetaActionList::with_action(RegionMetaAction::Truncate(truncate.clone()));
        region.manifest_manager.update(action_list).await?;

        // Notifies flush scheduler.
        self.flush_scheduler.on_region_truncated(region_id);
        // Notifies compaction scheduler.
        self.compaction_scheduler.on_region_truncated(region_id);

        // Reset region's version and mark all SSTs deleted.
        region.version_control.truncate(
            truncated_entry_id,
            truncated_sequence,
            &self.memtable_builder,
        );

        // Make all data obsolete.
        self.wal
            .obsolete(region_id, truncated_entry_id, &region.wal_options)
            .await?;
        info!(
            "Complete truncating region: {}, entry id: {} and sequence: {}.",
            region_id, truncated_entry_id, truncated_sequence
        );

        Ok(0)
    }
}
