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

use bytes::Bytes;
use common_telemetry::{error, info, warn};
use futures::TryStreamExt;
use object_store::util::join_path;
use object_store::{EntryMode, ObjectStore};
use snafu::ResultExt;
use store_api::logstore::LogStore;
use store_api::region_request::AffectedRows;
use store_api::storage::RegionId;
use tokio::time::sleep;

use crate::error::{OpenDalSnafu, Result};
use crate::region::{RegionMapRef, RegionState};
use crate::worker::{RegionWorkerLoop, DROPPING_MARKER_FILE};

const GC_TASK_INTERVAL_SEC: u64 = 5 * 60; // 5 minutes
const MAX_RETRY_TIMES: u64 = 288; // 24 hours (5m * 288)

impl<S> RegionWorkerLoop<S>
where
    S: LogStore,
{
    pub(crate) async fn handle_drop_request(
        &mut self,
        region_id: RegionId,
    ) -> Result<AffectedRows> {
        let region = self.regions.writable_region(region_id)?;

        info!("Try to drop region: {}, worker: {}", region_id, self.id);

        // Marks the region as dropping.
        region.set_dropping()?;
        // Writes dropping marker
        // We rarely drop a region so we still operate in the worker loop.
        let marker_path = join_path(region.access_layer.region_dir(), DROPPING_MARKER_FILE);
        region
            .access_layer
            .object_store()
            .write(&marker_path, Bytes::new())
            .await
            .context(OpenDalSnafu)
            .inspect_err(|e| {
                error!(e; "Failed to write the drop marker file for region {}", region_id);

                // Sets the state back to writable. It's possible that the marker file has been written.
                // We set the state back to writable so we can retry the drop operation.
                region.switch_state_to_writable(RegionState::Dropping);
            })?;

        region.stop().await;
        // Removes this region from region map to prevent other requests from accessing this region
        self.regions.remove_region(region_id);
        self.dropping_regions.insert_region(region.clone());

        // Delete region data in WAL.
        self.wal
            .obsolete(
                region_id,
                region.version_control.current().last_entry_id,
                &region.provider,
            )
            .await?;
        // Notifies flush scheduler.
        self.flush_scheduler.on_region_dropped(region_id);
        // Notifies compaction scheduler.
        self.compaction_scheduler.on_region_dropped(region_id);

        // Marks region version as dropped
        region
            .version_control
            .mark_dropped(&region.memtable_builder);
        info!(
            "Region {} is dropped logically, but some files are not deleted yet",
            region_id
        );

        self.region_count.dec();

        // Detaches a background task to delete the region dir
        let region_dir = region.access_layer.region_dir().to_owned();
        let object_store = region.access_layer.object_store().clone();
        let dropping_regions = self.dropping_regions.clone();
        let listener = self.listener.clone();
        common_runtime::spawn_global(async move {
            let gc_duration = listener
                .on_later_drop_begin(region_id)
                .unwrap_or(Duration::from_secs(GC_TASK_INTERVAL_SEC));
            let removed = later_drop_task(
                region_id,
                region_dir,
                object_store,
                dropping_regions,
                gc_duration,
            )
            .await;
            listener.on_later_drop_end(region_id, removed);
        });

        Ok(0)
    }
}

/// Background GC task to remove the entire region path once it find there is no
/// parquet file left. Returns whether the path is removed.
///
/// This task will keep running until finished. Any resource captured by it will
/// not be released before then. Be sure to only pass weak reference if something
/// is depended on ref-count mechanism.
async fn later_drop_task(
    region_id: RegionId,
    region_path: String,
    object_store: ObjectStore,
    dropping_regions: RegionMapRef,
    gc_duration: Duration,
) -> bool {
    for _ in 0..MAX_RETRY_TIMES {
        sleep(gc_duration).await;
        let result = remove_region_dir_once(&region_path, &object_store).await;
        match result {
            Err(err) => {
                warn!(
                    "Error occurs during trying to GC region dir {}: {}",
                    region_path, err
                );
            }
            Ok(true) => {
                dropping_regions.remove_region(region_id);
                info!("Region {} is dropped", region_path);
                return true;
            }
            Ok(false) => (),
        }
    }

    warn!(
        "Failed to GC region dir {} after {} retries, giving up",
        region_path, MAX_RETRY_TIMES
    );

    false
}

// TODO(ruihang): place the marker in a separate dir
/// Removes region dir if there is no parquet files, returns whether the directory is removed.
pub(crate) async fn remove_region_dir_once(
    region_path: &str,
    object_store: &ObjectStore,
) -> Result<bool> {
    // list all files under the given region path to check if there are un-deleted parquet files
    let mut has_parquet_file = false;
    // record all paths that neither ends with .parquet nor the marker file
    let mut files_to_remove_first = vec![];
    let mut files = object_store
        .lister_with(region_path)
        .await
        .context(OpenDalSnafu)?;
    while let Some(file) = files.try_next().await.context(OpenDalSnafu)? {
        if file.path().ends_with(".parquet") {
            has_parquet_file = true;
            break;
        } else if !file.path().ends_with(DROPPING_MARKER_FILE) {
            let meta = file.metadata();
            if meta.mode() == EntryMode::FILE {
                files_to_remove_first.push(file.path().to_string());
            }
        }
    }

    if !has_parquet_file {
        // no parquet file found, delete the region path
        // first delete all files other than the marker
        object_store
            .remove(files_to_remove_first)
            .await
            .context(OpenDalSnafu)?;
        // then remove the marker with this dir
        object_store
            .remove_all(region_path)
            .await
            .context(OpenDalSnafu)?;
        Ok(true)
    } else {
        Ok(false)
    }
}
