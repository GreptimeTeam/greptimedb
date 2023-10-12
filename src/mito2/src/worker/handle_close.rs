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

//! Handling close request.

use common_query::Output;
use common_telemetry::info;
use metrics::decrement_gauge;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::metrics::REGION_COUNT;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_close_request(&mut self, region_id: RegionId) -> Result<Output> {
        let Some(region) = self.regions.get_region(region_id) else {
            return Ok(Output::AffectedRows(0));
        };

        info!("Try to close region {}", region_id);

        region.stop().await?;
        self.regions.remove_region(region_id);
        // Clean flush status.
        self.flush_scheduler.on_region_closed(region_id);
        // Clean compaction status.
        self.compaction_scheduler.on_region_closed(region_id);

        info!("Region {} closed", region_id);

        decrement_gauge!(REGION_COUNT, 1.0);

        Ok(Output::AffectedRows(0))
    }
}
