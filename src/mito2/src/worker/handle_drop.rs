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
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::{AffectedRows, PathType};
use store_api::storage::RegionId;
use tokio::time::sleep;

use crate::cache::CacheManagerRef;
use crate::engine::region_hook::RegionHookRef;
use crate::error::{OpenDalSnafu, Result};
use crate::region::{RegionLeaderState, RegionMapRef};
use crate::sst::index::intermediate::IntermediateManager;
use crate::worker::{DROPPING_MARKER_FILE, RegionWorkerLoop};

const GC_TASK_INTERVAL_SEC: u64 = 5 * 60; // 5 minutes
const MAX_RETRY_TIMES: u64 = 12; // 1 hours (5m * 12)

impl<S> RegionWorkerLoop<S>
where
    S: LogStore,
{
    pub(crate) async fn handle_drop_request(
        &mut self,
        region_id: RegionId,
        partial_drop: bool,
    ) -> Result<AffectedRows> {
        let region = self.regions.writable_region(region_id)?;

        info!("Try to drop region: {}, worker: {}", region_id, self.id);

        let is_staging = region.is_staging();
        let expect_state = if is_staging {
            RegionLeaderState::Staging
        } else {
            RegionLeaderState::Writable
        };
        // Marks the region as dropping.
        region.set_dropping(expect_state)?;
        // Writes dropping marker
        // We rarely drop a region so we still operate in the worker loop.
        let region_dir = region.access_layer.build_region_dir(region_id);
        let path_type = region.access_layer.path_type();
        let table_dir = region.access_layer.table_dir().to_string();
        let marker_path = join_path(&region_dir, DROPPING_MARKER_FILE);
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
                region.switch_state_to_writable(RegionLeaderState::Dropping);
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
        self.cleanup_dropped_region_runtime_state(region_id).await;

        // Marks region version as dropped
        region.version_control.mark_dropped();
        info!(
            "Region {} is dropped logically, but some files are not deleted yet",
            region_id
        );

        self.region_count.dec();

        // Notify registered hooks that the region has been logically dropped, and
        // prepare a payload for the background GC task to fire the terminal
        // `on_region_files_removed`. When no hook is registered this allocates
        // nothing — no metadata snapshot is taken and no payload is built.
        let hook_payload = match region.manifest_ctx.hook() {
            Some(hook) => {
                let region_metadata = region.metadata();
                hook.on_region_dropped(region_id, &region_metadata).await;
                Some(DropHookPayload {
                    hook,
                    metadata: region_metadata,
                })
            }
            None => None,
        };

        let object_store = region.access_layer.object_store().clone();
        let dropping_regions = self.dropping_regions.clone();
        let listener = self.listener.clone();
        let intm_manager = self.intermediate_manager.clone();
        let cache_manager = self.cache_manager.clone();
        let gc_enabled = self.file_ref_manager.is_gc_enabled();

        common_runtime::spawn_global(async move {
            let removed = if gc_enabled {
                later_drop_task_with_global_gc(
                    region_id,
                    region_dir.clone(),
                    path_type,
                    object_store,
                    dropping_regions,
                    partial_drop,
                    hook_payload.as_ref(),
                )
                .await
            } else {
                let gc_duration = listener
                    .on_later_drop_begin(region_id)
                    .unwrap_or(Duration::from_secs(GC_TASK_INTERVAL_SEC));

                later_drop_task_without_global_gc(
                    region_id,
                    region_dir.clone(),
                    object_store,
                    dropping_regions,
                    gc_duration,
                    hook_payload.as_ref(),
                )
                .await
            };

            cleanup_region_file_artifacts(region_id, &table_dir, &intm_manager, &cache_manager)
                .await;

            listener.on_later_drop_end(region_id, removed);
        });

        Ok(0)
    }

    /// Cleans runtime state for a region that is no longer available to serve requests.
    pub(crate) async fn cleanup_dropped_region_runtime_state(&mut self, region_id: RegionId) {
        // Notifies flush scheduler.
        self.flush_scheduler.on_region_dropped(region_id);
        // Notifies compaction scheduler.
        self.compaction_scheduler.on_region_dropped(region_id);
        // Notifies index build scheduler.
        self.index_build_scheduler
            .on_region_dropped(region_id)
            .await;
    }
}

/// Cleans files and caches that are produced at runtime but are not part of the
/// primary region directory deletion.
pub(crate) async fn cleanup_region_file_artifacts(
    region_id: RegionId,
    table_dir: &str,
    intermediate_manager: &IntermediateManager,
    cache_manager: &CacheManagerRef,
) {
    if let Err(err) = intermediate_manager.prune_region_dir(&region_id).await {
        warn!(err; "Failed to prune intermediate region directory, region_id: {}", region_id);
    }

    if let Some(write_cache) = cache_manager.write_cache()
        && let Some(manifest_cache) = write_cache.manifest_cache()
    {
        manifest_cache.clean_manifests(table_dir).await;
    }
}

/// Removes a region directory for full-drop style cleanup.
///
/// Full drop and purge/offline cleanup force physical deletion. Only partial
/// drop may leave data files for global GC.
pub(crate) async fn remove_region_dir_for_full_drop(
    region_path: &str,
    object_store: &ObjectStore,
) -> Result<()> {
    remove_region_dir_once(region_path, object_store, true).await?;
    Ok(())
}

/// Carries the region hook and region metadata into the background GC task so
/// it can fire [`RegionHook::on_region_files_removed`] once the dropped region's
/// directory is physically deleted.
///
/// Only constructed when a hook is registered; the task receives it as
/// `Option<&DropHookPayload>` so the no-hook path allocates nothing.
///
/// [`RegionHook::on_region_files_removed`]: crate::engine::region_hook::RegionHook::on_region_files_removed
struct DropHookPayload {
    hook: RegionHookRef,
    metadata: RegionMetadataRef,
}

/// Background GC task to remove the entire region path once one of the following
/// conditions is true:
/// - It finds there is no parquet file left.
/// - After `gc_duration`.
///
/// Returns whether the path is removed.
///
/// This task will retry on failure and keep running until finished. Any resource
/// captured by it will not be released before then. Be sure to only pass weak reference
/// if something is depended on ref-count mechanism.
async fn later_drop_task_without_global_gc(
    region_id: RegionId,
    region_path: String,
    object_store: ObjectStore,
    dropping_regions: RegionMapRef,
    gc_duration: Duration,
    hook_payload: Option<&DropHookPayload>,
) -> bool {
    remove_region_with_retry(
        region_id,
        region_path,
        object_store,
        dropping_regions,
        Some(gc_duration),
        false,
        hook_payload,
    )
    .await
}

async fn remove_region_with_retry(
    region_id: RegionId,
    region_path: String,
    object_store: ObjectStore,
    dropping_regions: std::sync::Arc<crate::region::RegionMap>,
    gc_duration: Option<Duration>,
    mut force: bool,
    hook_payload: Option<&DropHookPayload>,
) -> bool {
    for _ in 0..MAX_RETRY_TIMES {
        let result = remove_region_dir_once(&region_path, &object_store, force).await;
        match result {
            Err(err) => {
                warn!(
                    "Error occurs during trying to GC region dir {}: {}",
                    region_path, err
                );
            }
            Ok(true) => {
                dropping_regions.remove_region(region_id);
                info!("Region {} is dropped, force: {}", region_path, force);
                // The region directory has been physically deleted; fire the
                // terminal file-lifecycle event. The partial-drop/global-GC path
                // never reaches here (it does not delete the directory itself).
                if let Some(hook_payload) = hook_payload {
                    hook_payload
                        .hook
                        .on_region_files_removed(region_id, &hook_payload.metadata)
                        .await;
                }
                return true;
            }
            Ok(false) => (),
        }
        if let Some(duration) = gc_duration {
            sleep(duration).await;
        }
        // Force recycle after gc duration.
        force = true;
    }

    warn!(
        "Failed to GC region dir {} after {} retries, giving up",
        region_path, MAX_RETRY_TIMES
    );

    false
}

async fn later_drop_task_with_global_gc(
    region_id: RegionId,
    region_path: String,
    path_type: PathType,
    object_store: ObjectStore,
    dropping_regions: RegionMapRef,
    partial_drop: bool,
    hook_payload: Option<&DropHookPayload>,
) -> bool {
    // For metadata regions or regions marked for full deletion (such as when dropping a table)
    // the region directory is forcefully removed immediately.
    //
    // TODO(discord9): Evaluate removing files instantly rather than waiting for the GC period.
    if should_force_remove_region_dir(path_type, partial_drop) {
        remove_region_with_retry(
            region_id,
            region_path,
            object_store,
            dropping_regions,
            None,
            true,
            hook_payload,
        )
        .await
    } else {
        // left for global gc
        dropping_regions.remove_region(region_id);
        true
    }
}

fn should_force_remove_region_dir(path_type: PathType, partial_drop: bool) -> bool {
    path_type == PathType::Metadata || !partial_drop
}

// TODO(ruihang): place the marker in a separate dir
/// Removes region dir if there is no parquet files, returns whether the directory is removed.
/// If `force = true`, always removes the dir.
pub(crate) async fn remove_region_dir_once(
    region_path: &str,
    object_store: &ObjectStore,
    force: bool,
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
        if !force && file.path().ends_with(".parquet") {
            // If not in force mode, we only remove the region dir if there is no parquet file
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
            .delete_iter(files_to_remove_first)
            .await
            .context(OpenDalSnafu)?;
        // then remove the marker with this dir
        object_store
            .delete_with(region_path)
            .recursive(true)
            .await
            .context(OpenDalSnafu)?;
        Ok(true)
    } else {
        Ok(false)
    }
}
