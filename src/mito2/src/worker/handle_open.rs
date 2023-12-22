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
use object_store::util::join_path;
use snafu::{OptionExt, ResultExt};
use store_api::logstore::LogStore;
use store_api::region_request::{AffectedRows, RegionOpenRequest};
use store_api::storage::RegionId;

use crate::error::{ObjectStoreNotFoundSnafu, OpenDalSnafu, RegionNotFoundSnafu, Result};
use crate::metrics::REGION_COUNT;
use crate::region::opener::RegionOpener;
use crate::worker::handle_drop::remove_region_dir_once;
use crate::worker::{RegionWorkerLoop, DROPPING_MARKER_FILE};

impl<S: LogStore> RegionWorkerLoop<S> {
    pub(crate) async fn handle_open_request(
        &mut self,
        region_id: RegionId,
        request: RegionOpenRequest,
    ) -> Result<AffectedRows> {
        if self.regions.is_region_exists(region_id) {
            return Ok(0);
        }
        let object_store = if let Some(storage_name) = request.options.get("storage") {
            self.object_store_manager
                .find(storage_name)
                .context(ObjectStoreNotFoundSnafu {
                    object_store: storage_name.to_string(),
                })?
        } else {
            self.object_store_manager.default_object_store()
        };
        // Check if this region is pending drop. And clean the entire dir if so.
        if !self.dropping_regions.is_region_exists(region_id)
            && object_store
                .is_exist(&join_path(&request.region_dir, DROPPING_MARKER_FILE))
                .await
                .context(OpenDalSnafu)?
        {
            let result = remove_region_dir_once(&request.region_dir, object_store).await;
            info!("Region {} is dropped, result: {:?}", region_id, result);
            return RegionNotFoundSnafu { region_id }.fail();
        }

        info!("Try to open region {}", region_id);

        // Open region from specific region dir.
        let region = RegionOpener::new(
            region_id,
            &request.region_dir,
            self.memtable_builder.clone(),
            self.object_store_manager.clone(),
            self.scheduler.clone(),
        )
        .skip_wal_replay(request.skip_wal_replay)
        .parse_options(request.options)?
        .cache(Some(self.cache_manager.clone()))
        .open(&self.config, &self.wal)
        .await?;

        info!("Region {} is opened", region_id);

        REGION_COUNT.inc();

        // Insert the MitoRegion into the RegionMap.
        self.regions.insert_region(Arc::new(region));

        Ok(0)
    }
}
