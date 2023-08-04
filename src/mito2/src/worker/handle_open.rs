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

//! Handling open request.

use std::sync::Arc;

use common_telemetry::info;

use crate::error::Result;
use crate::region::opener::RegionOpener;
use crate::request::OpenRequest;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_open_request(&mut self, request: OpenRequest) -> Result<()> {
        if self.regions.is_region_exists(request.region_id) {
            return Ok(());
        }

        info!("Try to open region {}", request.region_id);

        // Open region from specific region dir.
        let region = RegionOpener::new(
            request.region_id,
            self.memtable_builder.clone(),
            self.object_store.clone(),
        )
        .region_dir(&request.region_dir)
        .open(&self.config)
        .await?;

        info!("Region {} is opened", request.region_id);

        // Insert the MitoRegion into the RegionMap.
        self.regions.insert_region(Arc::new(region));

        Ok(())
    }
}
