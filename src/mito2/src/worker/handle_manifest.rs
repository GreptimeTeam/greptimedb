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

//! Handles manifest.
//!
//! It updates the manifest and applies the changes to the region in background.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use common_telemetry::{info, warn};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;

use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::CacheManagerRef;
use crate::error::{RegionBusySnafu, RegionNotFoundSnafu, Result};
use crate::manifest::action::{
    RegionChange, RegionEdit, RegionMetaAction, RegionMetaActionList, RegionTruncate,
};
use crate::metrics::WRITE_CACHE_INFLIGHT_DOWNLOAD;
use crate::region::version::VersionBuilder;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::{
    BackgroundNotify, OptionOutputTx, RegionChangeResult, RegionEditRequest, RegionEditResult,
    RegionSyncRequest, TruncateResult, WorkerRequest,
};
use crate::sst::location;
use crate::worker::{RegionWorkerLoop, WorkerListener};

pub(crate) type RegionEditQueues = HashMap<RegionId, RegionEditQueue>;

/// A queue for temporary store region edit requests, if the region is in the "Editing" state.
/// When the current region edit request is completed, the next (if there exists) request in the
/// queue will be processed.
/// Everything is done in the region worker loop.
pub(crate) struct RegionEditQueue {
    region_id: RegionId,
    requests: VecDeque<RegionEditRequest>,
}

impl RegionEditQueue {
    const QUEUE_MAX_LEN: usize = 128;

    fn new(region_id: RegionId) -> Self {
        Self {
            region_id,
            requests: VecDeque::new(),
        }
    }

    fn enqueue(&mut self, request: RegionEditRequest) {
        if self.requests.len() > Self::QUEUE_MAX_LEN {
            let _ = request.tx.send(
                RegionBusySnafu {
                    region_id: self.region_id,
                }
                .fail(),
            );
            return;
        };
        self.requests.push_back(request);
    }

    fn dequeue(&mut self) -> Option<RegionEditRequest> {
        self.requests.pop_front()
    }
}

impl<S: LogStore> RegionWorkerLoop<S> {
    /// Handles region change result.
    pub(crate) async fn handle_manifest_region_change_result(
        &mut self,
        change_result: RegionChangeResult,
    ) {
        let region = match self.regions.get_region(change_result.region_id) {
            Some(region) => region,
            None => {
                self.reject_region_stalled_requests(&change_result.region_id);
                change_result.sender.send(
                    RegionNotFoundSnafu {
                        region_id: change_result.region_id,
                    }
                    .fail(),
                );
                return;
            }
        };

        if change_result.result.is_ok() {
            // Apply the metadata to region's version.
            region
                .version_control
                .alter_schema(change_result.new_meta, &region.memtable_builder);

            let version = region.version();
            info!(
                "Region {} is altered, metadata is {:?}, options: {:?}",
                region.region_id, version.metadata, version.options,
            );
        }

        // Sets the region as writable.
        region.switch_state_to_writable(RegionLeaderState::Altering);
        // Sends the result.
        change_result.sender.send(change_result.result.map(|_| 0));

        // Handles the stalled requests.
        self.handle_region_stalled_requests(&change_result.region_id)
            .await;
    }

    /// Handles region sync request.
    ///
    /// Updates the manifest to at least the given version.
    /// **Note**: The installed version may be greater than the given version.
    pub(crate) async fn handle_region_sync(&mut self, request: RegionSyncRequest) {
        let region_id = request.region_id;
        let sender = request.sender;
        let region = match self.regions.follower_region(region_id) {
            Ok(region) => region,
            Err(e) => {
                let _ = sender.send(Err(e));
                return;
            }
        };

        let original_manifest_version = region.manifest_ctx.manifest_version().await;
        let manifest = match region
            .manifest_ctx
            .install_manifest_to(request.manifest_version)
            .await
        {
            Ok(manifest) => manifest,
            Err(e) => {
                let _ = sender.send(Err(e));
                return;
            }
        };
        let version = region.version();
        if !version.memtables.is_empty() {
            let current = region.version_control.current();
            warn!(
                "Region {} memtables is not empty, which should not happen, manifest version: {}, last entry id: {}",
                region.region_id, manifest.manifest_version, current.last_entry_id
            );
        }
        let region_options = version.options.clone();
        let new_mutable = Arc::new(
            region
                .version()
                .memtables
                .mutable
                .new_with_part_duration(version.compaction_time_window),
        );
        let metadata = manifest.metadata.clone();
        let version = VersionBuilder::new(metadata, new_mutable)
            .add_files(region.file_purger.clone(), manifest.files.values().cloned())
            .flushed_entry_id(manifest.flushed_entry_id)
            .flushed_sequence(manifest.flushed_sequence)
            .truncated_entry_id(manifest.truncated_entry_id)
            .compaction_time_window(manifest.compaction_time_window)
            .options(region_options)
            .build();
        region.version_control.overwrite_current(Arc::new(version));

        let updated = manifest.manifest_version > original_manifest_version;
        let _ = sender.send(Ok((manifest.manifest_version, updated)));
    }
}

impl<S> RegionWorkerLoop<S> {
    /// Handles region edit request.
    pub(crate) async fn handle_region_edit(&mut self, request: RegionEditRequest) {
        let region_id = request.region_id;
        let Some(region) = self.regions.get_region(region_id) else {
            let _ = request.tx.send(RegionNotFoundSnafu { region_id }.fail());
            return;
        };

        if !region.is_writable() {
            if region.state() == RegionRoleState::Leader(RegionLeaderState::Editing) {
                self.region_edit_queues
                    .entry(region_id)
                    .or_insert_with(|| RegionEditQueue::new(region_id))
                    .enqueue(request);
            } else {
                let _ = request.tx.send(RegionBusySnafu { region_id }.fail());
            }
            return;
        }

        let RegionEditRequest {
            region_id: _,
            edit,
            tx: sender,
        } = request;

        // Marks the region as editing.
        if let Err(e) = region.set_editing() {
            let _ = sender.send(Err(e));
            return;
        }

        let request_sender = self.sender.clone();
        let cache_manager = self.cache_manager.clone();
        let listener = self.listener.clone();
        // Now the region is in editing state.
        // Updates manifest in background.
        common_runtime::spawn_global(async move {
            let result = edit_region(&region, edit.clone(), cache_manager, listener).await;
            let notify = WorkerRequest::Background {
                region_id,
                notify: BackgroundNotify::RegionEdit(RegionEditResult {
                    region_id,
                    sender,
                    edit,
                    result,
                }),
            };
            // We don't set state back as the worker loop is already exited.
            if let Err(res) = request_sender.send(notify).await {
                warn!(
                    "Failed to send region edit result back to the worker, region_id: {}, res: {:?}",
                    region_id, res
                );
            }
        });
    }

    /// Handles region edit result.
    pub(crate) async fn handle_region_edit_result(&mut self, edit_result: RegionEditResult) {
        let region = match self.regions.get_region(edit_result.region_id) {
            Some(region) => region,
            None => {
                let _ = edit_result.sender.send(
                    RegionNotFoundSnafu {
                        region_id: edit_result.region_id,
                    }
                    .fail(),
                );
                return;
            }
        };

        let need_compaction =
            edit_result.result.is_ok() && !edit_result.edit.files_to_add.is_empty();

        if edit_result.result.is_ok() {
            // Applies the edit to the region.
            region
                .version_control
                .apply_edit(edit_result.edit, &[], region.file_purger.clone());
        }

        // Sets the region as writable.
        region.switch_state_to_writable(RegionLeaderState::Editing);

        let _ = edit_result.sender.send(edit_result.result);

        if let Some(edit_queue) = self.region_edit_queues.get_mut(&edit_result.region_id) {
            if let Some(request) = edit_queue.dequeue() {
                self.handle_region_edit(request).await;
            }
        }

        if need_compaction {
            self.schedule_compaction(&region).await;
        }
    }

    /// Writes truncate action to the manifest and then applies it to the region in background.
    pub(crate) fn handle_manifest_truncate_action(
        &self,
        region: MitoRegionRef,
        truncate: RegionTruncate,
        sender: OptionOutputTx,
    ) {
        // Marks the region as truncating.
        // This prevents the region from being accessed by other write requests.
        if let Err(e) = region.set_truncating() {
            sender.send(Err(e));
            return;
        }
        // Now the region is in truncating state.

        let request_sender = self.sender.clone();
        let manifest_ctx = region.manifest_ctx.clone();

        // Updates manifest in background.
        common_runtime::spawn_global(async move {
            // Write region truncated to manifest.
            let action_list =
                RegionMetaActionList::with_action(RegionMetaAction::Truncate(truncate.clone()));

            let result = manifest_ctx
                .update_manifest(RegionLeaderState::Truncating, action_list)
                .await
                .map(|_| ());

            // Sends the result back to the request sender.
            let truncate_result = TruncateResult {
                region_id: truncate.region_id,
                sender,
                result,
                truncated_entry_id: truncate.truncated_entry_id,
                truncated_sequence: truncate.truncated_sequence,
            };
            let _ = request_sender
                .send(WorkerRequest::Background {
                    region_id: truncate.region_id,
                    notify: BackgroundNotify::Truncate(truncate_result),
                })
                .await
                .inspect_err(|_| warn!("failed to send truncate result"));
        });
    }

    /// Writes region change action to the manifest and then applies it to the region in background.
    pub(crate) fn handle_manifest_region_change(
        &self,
        region: MitoRegionRef,
        change: RegionChange,
        sender: OptionOutputTx,
    ) {
        // Marks the region as altering.
        if let Err(e) = region.set_altering() {
            sender.send(Err(e));
            return;
        }
        let listener = self.listener.clone();
        let request_sender = self.sender.clone();
        // Now the region is in altering state.
        common_runtime::spawn_global(async move {
            let new_meta = change.metadata.clone();
            let action_list = RegionMetaActionList::with_action(RegionMetaAction::Change(change));

            let result = region
                .manifest_ctx
                .update_manifest(RegionLeaderState::Altering, action_list)
                .await
                .map(|_| ());
            let notify = WorkerRequest::Background {
                region_id: region.region_id,
                notify: BackgroundNotify::RegionChange(RegionChangeResult {
                    region_id: region.region_id,
                    sender,
                    result,
                    new_meta,
                }),
            };
            listener
                .on_notify_region_change_result_begin(region.region_id)
                .await;

            if let Err(res) = request_sender.send(notify).await {
                warn!(
                    "Failed to send region change result back to the worker, region_id: {}, res: {:?}",
                    region.region_id, res
                );
            }
        });
    }
}

/// Checks the edit, writes and applies it.
async fn edit_region(
    region: &MitoRegionRef,
    edit: RegionEdit,
    cache_manager: CacheManagerRef,
    listener: WorkerListener,
) -> Result<()> {
    let region_id = region.region_id;
    if let Some(write_cache) = cache_manager.write_cache() {
        for file_meta in &edit.files_to_add {
            let write_cache = write_cache.clone();
            let layer = region.access_layer.clone();
            let listener = listener.clone();

            let index_key = IndexKey::new(region_id, file_meta.file_id, FileType::Parquet);
            let remote_path = location::sst_file_path(layer.region_dir(), file_meta.file_id);

            let is_index_exist = file_meta.exists_index();
            let index_file_size = file_meta.index_file_size();

            let index_file_index_key =
                IndexKey::new(region_id, file_meta.file_id, FileType::Puffin);
            let index_remote_path =
                location::index_file_path(layer.region_dir(), file_meta.file_id);

            let file_size = file_meta.file_size;
            common_runtime::spawn_global(async move {
                WRITE_CACHE_INFLIGHT_DOWNLOAD.add(1);

                if write_cache
                    .download(index_key, &remote_path, layer.object_store(), file_size)
                    .await
                    .is_ok()
                {
                    // Triggers the filling of the parquet metadata cache.
                    // The parquet file is already downloaded.
                    let _ = write_cache
                        .file_cache()
                        .get_parquet_meta_data(index_key)
                        .await;

                    listener.on_file_cache_filled(index_key.file_id);
                }
                if is_index_exist {
                    // also download puffin file
                    if let Err(err) = write_cache
                        .download(
                            index_file_index_key,
                            &index_remote_path,
                            layer.object_store(),
                            index_file_size,
                        )
                        .await
                    {
                        common_telemetry::error!(
                            err; "Failed to download puffin file, region_id: {}, index_file_index_key: {:?}, index_remote_path: {}", region_id, index_file_index_key, index_remote_path
                        );
                    }
                }

                WRITE_CACHE_INFLIGHT_DOWNLOAD.sub(1);
            });
        }
    }

    info!("Applying {edit:?} to region {}", region_id);

    let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit));
    region
        .manifest_ctx
        .update_manifest(RegionLeaderState::Editing, action_list)
        .await
        .map(|_| ())
}
