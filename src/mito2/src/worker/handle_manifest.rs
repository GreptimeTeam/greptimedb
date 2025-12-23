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
use std::num::NonZeroU64;
use std::sync::Arc;

use common_telemetry::{info, warn};
use store_api::logstore::LogStore;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::cache::CacheManagerRef;
use crate::cache::file_cache::{FileType, IndexKey};
use crate::config::IndexBuildMode;
use crate::error::{RegionBusySnafu, RegionNotFoundSnafu, Result};
use crate::manifest::action::{
    RegionChange, RegionEdit, RegionMetaAction, RegionMetaActionList, RegionTruncate,
};
use crate::memtable::MemtableBuilderProvider;
use crate::metrics::WRITE_CACHE_INFLIGHT_DOWNLOAD;
use crate::region::opener::{sanitize_region_options, version_builder_from_manifest};
use crate::region::options::RegionOptions;
use crate::region::version::VersionControlRef;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::{
    BackgroundNotify, BuildIndexRequest, OptionOutputTx, RegionChangeResult, RegionEditRequest,
    RegionEditResult, RegionSyncRequest, TruncateResult, WorkerRequest, WorkerRequestWithTime,
};
use crate::sst::index::IndexBuildType;
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
            // Updates the region metadata and format.
            Self::update_region_version(
                &region.version_control,
                change_result.new_meta,
                change_result.new_options,
                &self.memtable_builder_provider,
            );
        }

        // Sets the region as writable.
        region.switch_state_to_writable(RegionLeaderState::Altering);
        // Sends the result.
        change_result.sender.send(change_result.result.map(|_| 0));

        // In async mode, rebuild index after index metadata changed.
        if self.config.index.build_mode == IndexBuildMode::Async && change_result.need_index {
            self.handle_rebuild_index(
                BuildIndexRequest {
                    region_id: region.region_id,
                    build_type: IndexBuildType::SchemaChange,
                    file_metas: Vec::new(),
                },
                OptionOutputTx::new(None),
            )
            .await;
        }
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
        let mut region_options = version.options.clone();
        let old_format = region_options.sst_format.unwrap_or_default();
        // Updates the region options with the manifest.
        sanitize_region_options(&manifest, &mut region_options);
        if !version.memtables.is_empty() {
            let current = region.version_control.current();
            warn!(
                "Region {} memtables is not empty, which should not happen, manifest version: {}, last entry id: {}",
                region.region_id, manifest.manifest_version, current.last_entry_id
            );
        }

        // We should sanitize the region options before creating a new memtable.
        let memtable_builder = if old_format != region_options.sst_format.unwrap_or_default() {
            // Format changed, also needs to replace the memtable builder.
            Some(
                self.memtable_builder_provider
                    .builder_for_options(&region_options),
            )
        } else {
            None
        };
        let new_mutable = Arc::new(
            region
                .version()
                .memtables
                .mutable
                .new_with_part_duration(version.compaction_time_window, memtable_builder),
        );
        // Here it assumes the leader has backfilled the partition_expr of the metadata.
        let metadata = manifest.metadata.clone();

        let version_builder = version_builder_from_manifest(
            &manifest,
            metadata,
            region.file_purger.clone(),
            new_mutable,
            region_options,
        );
        let version = version_builder.build();
        region.version_control.overwrite_current(Arc::new(version));

        let updated = manifest.manifest_version > original_manifest_version;
        let _ = sender.send(Ok((manifest.manifest_version, updated)));
    }
}

impl<S> RegionWorkerLoop<S> {
    /// Handles region edit request.
    pub(crate) fn handle_region_edit(&mut self, request: RegionEditRequest) {
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
            mut edit,
            tx: sender,
        } = request;
        let file_sequence = region.version_control.committed_sequence() + 1;
        edit.committed_sequence = Some(file_sequence);

        // For every file added through region edit, we should fill the file sequence
        for file in &mut edit.files_to_add {
            file.sequence = NonZeroU64::new(file_sequence);
        }

        // Allow retrieving `is_staging` before spawn the edit region task.
        let is_staging = region.is_staging();
        let expect_state = if is_staging {
            RegionLeaderState::Staging
        } else {
            RegionLeaderState::Writable
        };
        // Marks the region as editing.
        if let Err(e) = region.set_editing(expect_state) {
            let _ = sender.send(Err(e));
            return;
        }

        let request_sender = self.sender.clone();
        let cache_manager = self.cache_manager.clone();
        let listener = self.listener.clone();
        // Now the region is in editing state.
        // Updates manifest in background.
        common_runtime::spawn_global(async move {
            let result =
                edit_region(&region, edit.clone(), cache_manager, listener, is_staging).await;
            let notify = WorkerRequest::Background {
                region_id,
                notify: BackgroundNotify::RegionEdit(RegionEditResult {
                    region_id,
                    sender,
                    edit,
                    result,
                    // we always need to restore region state after region edit
                    update_region_state: true,
                    is_staging,
                }),
            };

            // We don't set state back as the worker loop is already exited.
            if let Err(res) = request_sender
                .send(WorkerRequestWithTime::new(notify))
                .await
            {
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

        let need_compaction = if edit_result.is_staging {
            if edit_result.update_region_state {
                // For staging regions, edits are not applied immediately,
                // as they remain invisible until the region exits the staging state.
                region.switch_state_to_staging(RegionLeaderState::Editing);
            }

            false
        } else {
            // Only apply the edit if the result is ok and region is not in staging state.
            if edit_result.result.is_ok() {
                // Applies the edit to the region.
                region.version_control.apply_edit(
                    Some(edit_result.edit),
                    &[],
                    region.file_purger.clone(),
                );
            }
            if edit_result.update_region_state {
                region.switch_state_to_writable(RegionLeaderState::Editing);
            }

            true
        };

        let _ = edit_result.sender.send(edit_result.result);

        if let Some(edit_queue) = self.region_edit_queues.get_mut(&edit_result.region_id)
            && let Some(request) = edit_queue.dequeue()
        {
            self.handle_region_edit(request);
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
        let is_staging = region.is_staging();

        // Updates manifest in background.
        common_runtime::spawn_global(async move {
            // Write region truncated to manifest.
            let action_list =
                RegionMetaActionList::with_action(RegionMetaAction::Truncate(truncate.clone()));

            let result = manifest_ctx
                .update_manifest(RegionLeaderState::Truncating, action_list, is_staging)
                .await
                .map(|_| ());

            // Sends the result back to the request sender.
            let truncate_result = TruncateResult {
                region_id: truncate.region_id,
                sender,
                result,
                kind: truncate.kind,
            };
            let _ = request_sender
                .send(WorkerRequestWithTime::new(WorkerRequest::Background {
                    region_id: truncate.region_id,
                    notify: BackgroundNotify::Truncate(truncate_result),
                }))
                .await
                .inspect_err(|_| warn!("failed to send truncate result"));
        });
    }

    /// Writes region change action to the manifest and then applies it to the region in background.
    pub(crate) fn handle_manifest_region_change(
        &self,
        region: MitoRegionRef,
        change: RegionChange,
        need_index: bool,
        new_options: Option<RegionOptions>,
        sender: OptionOutputTx,
    ) {
        // Marks the region as altering.
        if let Err(e) = region.set_altering() {
            sender.send(Err(e));
            return;
        }
        let listener = self.listener.clone();
        let request_sender = self.sender.clone();
        let is_staging = region.is_staging();
        // Now the region is in altering state.
        common_runtime::spawn_global(async move {
            let new_meta = change.metadata.clone();
            let action_list = RegionMetaActionList::with_action(RegionMetaAction::Change(change));

            let result = region
                .manifest_ctx
                .update_manifest(RegionLeaderState::Altering, action_list, is_staging)
                .await
                .map(|_| ());
            let notify = WorkerRequest::Background {
                region_id: region.region_id,
                notify: BackgroundNotify::RegionChange(RegionChangeResult {
                    region_id: region.region_id,
                    sender,
                    result,
                    new_meta,
                    need_index,
                    new_options,
                }),
            };
            listener
                .on_notify_region_change_result_begin(region.region_id)
                .await;

            if let Err(res) = request_sender
                .send(WorkerRequestWithTime::new(notify))
                .await
            {
                warn!(
                    "Failed to send region change result back to the worker, region_id: {}, res: {:?}",
                    region.region_id, res
                );
            }
        });
    }

    fn update_region_version(
        version_control: &VersionControlRef,
        new_meta: RegionMetadataRef,
        new_options: Option<RegionOptions>,
        memtable_builder_provider: &MemtableBuilderProvider,
    ) {
        let options_changed = new_options.is_some();
        let region_id = new_meta.region_id;
        if let Some(new_options) = new_options {
            // Needs to update the region with new format and memtables.
            // Creates a new memtable builder for the new options as it may change the memtable type.
            let new_memtable_builder = memtable_builder_provider.builder_for_options(&new_options);
            version_control.alter_schema_and_format(new_meta, new_options, new_memtable_builder);
        } else {
            // Only changes the schema.
            version_control.alter_schema(new_meta);
        }

        let version_data = version_control.current();
        let version = version_data.version;
        info!(
            "Region {} is altered, metadata is {:?}, options: {:?}, options_changed: {}",
            region_id, version.metadata, version.options, options_changed,
        );
    }
}

/// Checks the edit, writes and applies it.
async fn edit_region(
    region: &MitoRegionRef,
    edit: RegionEdit,
    cache_manager: CacheManagerRef,
    listener: WorkerListener,
    is_staging: bool,
) -> Result<()> {
    let region_id = region.region_id;
    if let Some(write_cache) = cache_manager.write_cache() {
        for file_meta in &edit.files_to_add {
            let write_cache = write_cache.clone();
            let layer = region.access_layer.clone();
            let listener = listener.clone();

            let index_key = IndexKey::new(region_id, file_meta.file_id, FileType::Parquet);
            let remote_path =
                location::sst_file_path(layer.table_dir(), file_meta.file_id(), layer.path_type());

            let is_index_exist = file_meta.exists_index();
            let index_file_size = file_meta.index_file_size();

            let index_file_index_key = IndexKey::new(
                region_id,
                file_meta.index_id().file_id.file_id(),
                FileType::Puffin(file_meta.index_version),
            );
            let index_remote_path = location::index_file_path(
                layer.table_dir(),
                file_meta.index_id(),
                layer.path_type(),
            );

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

    info!(
        "Applying {edit:?} to region {}, is_staging: {}",
        region_id, is_staging
    );

    let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit));
    region
        .manifest_ctx
        .update_manifest(RegionLeaderState::Editing, action_list, is_staging)
        .await
        .map(|_| ())
}
