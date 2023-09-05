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

//! Handling flush related requests.

use common_query::Output;
use common_telemetry::info;
use store_api::logstore::LogStore;
use store_api::storage::RegionId;

use crate::error::{RegionNotFoundSnafu, Result};
use crate::manifest::action::{RegionMetaAction, RegionMetaActionList, RegionTruncate};
use crate::worker::RegionWorkerLoop;

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_truncate_request(&mut self, region_id: RegionId) -> Result<Output> {
        let Some(region) = self.regions.get_region(region_id) else {
            return RegionNotFoundSnafu { region_id }.fail();
        };
        info!("Try to truncate region {}", region_id);

        // Write region truncated to manifest.
        let truncate = RegionTruncate { region_id };
        let action_list =
            RegionMetaActionList::with_action(RegionMetaAction::Truncate(truncate.clone()));
        region.manifest_manager.update(action_list).await?;

        // Notifies flush scheduler.
        self.flush_scheduler.on_region_truncating(region_id);

        // Maek all data obsolete.
        let version_data = region.version_control.current();
        let commited_sequence = version_data.committed_sequence;
        self.wal.obsolete(region_id, commited_sequence).await?;

        // Reset region's version and mark all SSTs deleted.
        region.version_control.reset(0, &self.memtable_builder);
        Ok(Output::AffectedRows(0))
    }
}
