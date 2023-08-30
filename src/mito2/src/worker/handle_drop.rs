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

//! Handling drop request.

use std::time::Duration;

use common_query::Output;
use common_telemetry::info;
use common_telemetry::tracing::warn;
use futures::StreamExt;
use object_store::util::join_path;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::storage::RegionId;
use tokio::time::sleep;

use crate::error::{OpenDalSnafu, RegionNotFoundSnafu, Result};
use crate::worker::RegionWorkerLoop;

const DROPING_MARKER_FILE: &str = ".dropping";
const GC_TASK_INTERVAL_SEC: u64 = 5 * 60;

impl<S> RegionWorkerLoop<S> {
    pub(crate) async fn handle_drop_request(&mut self, region_id: RegionId) -> Result<Output> {
        let Some(region) = self.regions.get_region(region_id) else {
            return RegionNotFoundSnafu { region_id }.fail();
        };

        info!("Try to drop region {}", region_id);
        region.stop().await?;

        // write dropping marker
        let marker_path = join_path(&region.region_dir, DROPING_MARKER_FILE);
        self.object_store
            .write(&marker_path, vec![])
            .await
            .context(OpenDalSnafu)?;

        // remove this region from region map to prevent other requests from accessing this region
        self.regions.remove_region(region_id);

        // mark region version as dropped
        region.version_control.mark_dropped();
        info!(
            "Region {} is dropped logically, but some files are not deleted yet",
            region_id
        );

        // detach a background task to delete the region dir
        let region_dir = region.region_dir.clone();
        let object_store = self.object_store.clone();
        common_runtime::spawn_bg(async move {
            later_drop_task(region_dir, object_store).await;
        });

        Ok(Output::AffectedRows(0))
    }
}

/// Background GC task to remove the entire region path once it find there is no
/// parquet file left.
///
/// This task will keep running until finished. Any resource captured by it will
/// not be released before then. Be sure to only pass weak reference if something
/// is depended on ref-count machanism.
async fn later_drop_task(region_path: String, object_store: ObjectStore) {
    loop {
        let result: Result<()> = try {
            sleep(Duration::from_secs(GC_TASK_INTERVAL_SEC)).await;

            // list all files under the given region path to check if there are un-deleted parquet files
            let mut has_parquet_file = false;
            let mut files = object_store
                .list(&region_path)
                .await
                .context(OpenDalSnafu)?;
            while let Some(file) = files.next().await {
                let file = file.context(OpenDalSnafu)?;
                if file.path().ends_with(".parquet") {
                    has_parquet_file = true;
                    break;
                }
            }

            if !has_parquet_file {
                // no parquet file found, delete the region path
                object_store
                    .delete(&region_path)
                    .await
                    .context(OpenDalSnafu)?;
                info!("Region {} is dropped", region_path);
            }
        };
        if result.is_err() {
            warn!(
                "Error occurs during trying to GC region dir {}: {:?}",
                region_path, result
            );
        }
    }
}
