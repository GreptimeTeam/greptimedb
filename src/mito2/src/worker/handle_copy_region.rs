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

use common_telemetry::{debug, error, info};
use snafu::OptionExt;
use store_api::region_engine::MitoCopyRegionFromResponse;
use store_api::storage::{FileId, RegionId};

use crate::error::{InvalidRequestSnafu, MissingManifestSnafu, Result};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::region::{FileDescriptor, MitoRegionRef, RegionFileCopier, RegionMetadataLoader};
use crate::request::{
    BackgroundNotify, CopyRegionFromFinished, CopyRegionFromRequest, WorkerRequest,
};
use crate::sst::location::region_dir_from_table_dir;
use crate::worker::{RegionWorkerLoop, WorkerRequestWithTime};

impl<S> RegionWorkerLoop<S> {
    pub(crate) fn handle_copy_region_from_request(&mut self, request: CopyRegionFromRequest) {
        let region_id = request.region_id;
        let source_region_id = request.source_region_id;
        let sender = request.sender;
        let region = match self.regions.writable_region(region_id) {
            Ok(region) => region,
            Err(e) => {
                let _ = sender.send(Err(e));
                return;
            }
        };

        let same_table = source_region_id.table_id() == region_id.table_id();
        if !same_table {
            let _ = sender.send(
                InvalidRequestSnafu {
                    region_id,
                    reason: format!("Source and target regions must be from the same table, source_region_id: {source_region_id}, target_region_id: {region_id}"),
                }
                .fail(),
            );
            return;
        }
        if source_region_id == region_id {
            let _ = sender.send(
                InvalidRequestSnafu {
                    region_id,
                    reason: format!("Source and target regions must be different, source_region_id: {source_region_id}, target_region_id: {region_id}"),
                }
                .fail(),
            );
            return;
        }

        let region_metadata_loader =
            RegionMetadataLoader::new(self.config.clone(), self.object_store_manager.clone());
        let worker_sender = self.sender.clone();

        common_runtime::spawn_global(async move {
            let (region_edit, file_ids) = match Self::copy_region_from(
                &region,
                region_metadata_loader,
                source_region_id,
                region_id,
                request.parallelism.max(1),
            )
            .await
            {
                Ok(region_files) => region_files,
                Err(e) => {
                    let _ = sender.send(Err(e));
                    return;
                }
            };

            match region_edit {
                Some(region_edit) => {
                    if let Err(e) = worker_sender
                        .send(WorkerRequestWithTime::new(WorkerRequest::Background {
                            region_id,
                            notify: BackgroundNotify::CopyRegionFromFinished(
                                CopyRegionFromFinished {
                                    region_id,
                                    edit: region_edit,
                                    sender,
                                },
                            ),
                        }))
                        .await
                    {
                        error!(e; "Failed to send copy region from finished notification to worker, region_id: {}", region_id);
                    }
                }
                None => {
                    let _ = sender.send(Ok(MitoCopyRegionFromResponse {
                        copied_file_ids: file_ids,
                    }));
                }
            }
        });
    }

    pub(crate) fn handle_copy_region_from_finished(&mut self, request: CopyRegionFromFinished) {
        let region_id = request.region_id;
        let sender = request.sender;
        let region = match self.regions.writable_region(region_id) {
            Ok(region) => region,
            Err(e) => {
                let _ = sender.send(Err(e));
                return;
            }
        };

        let copied_file_ids = request
            .edit
            .files_to_add
            .iter()
            .map(|file_meta| file_meta.file_id)
            .collect();

        region
            .version_control
            .apply_edit(Some(request.edit), &[], region.file_purger.clone());

        let _ = sender.send(Ok(MitoCopyRegionFromResponse { copied_file_ids }));
    }

    /// Returns the region edit and the file ids that were copied from the source region to the target region.
    ///
    /// If no need to copy files, returns (None, file_ids).
    async fn copy_region_from(
        region: &MitoRegionRef,
        region_metadata_loader: RegionMetadataLoader,
        source_region_id: RegionId,
        target_region_id: RegionId,
        parallelism: usize,
    ) -> Result<(Option<RegionEdit>, Vec<FileId>)> {
        let table_dir = region.table_dir();
        let path_type = region.path_type();
        let region_dir = region_dir_from_table_dir(table_dir, source_region_id, path_type);
        info!(
            "Loading source region manifest from region dir: {region_dir}, target region: {target_region_id}"
        );
        let source_region_manifest = region_metadata_loader
            .load_manifest(&region_dir, &region.version().options.storage)
            .await?
            .context(MissingManifestSnafu {
                region_id: source_region_id,
            })?;
        let mut files_to_copy = vec![];
        let target_region_manifest = region.manifest_ctx.manifest().await;
        let file_ids = source_region_manifest
            .files
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        debug!(
            "source region files: {:?}, source region id: {}",
            source_region_manifest.files, source_region_id
        );
        for (file_id, file_meta) in &source_region_manifest.files {
            if !target_region_manifest.files.contains_key(file_id) {
                let mut new_file_meta = file_meta.clone();
                new_file_meta.region_id = target_region_id;
                files_to_copy.push(new_file_meta);
            }
        }
        if files_to_copy.is_empty() {
            return Ok((None, file_ids));
        }

        let file_descriptors = files_to_copy
            .iter()
            .flat_map(|file_meta| {
                if file_meta.exists_index() {
                    let region_index_id = file_meta.index_id();
                    let file_id = region_index_id.file_id.file_id();
                    let version = region_index_id.version;
                    let file_size = file_meta.file_size;
                    let index_file_size = file_meta.index_file_size();
                    vec![
                        FileDescriptor::Data {
                            file_id: file_meta.file_id,
                            size: file_size,
                        },
                        FileDescriptor::Index {
                            file_id,
                            version,
                            size: index_file_size,
                        },
                    ]
                } else {
                    let file_size = file_meta.file_size;
                    vec![FileDescriptor::Data {
                        file_id: file_meta.file_id,
                        size: file_size,
                    }]
                }
            })
            .collect();
        debug!("File descriptors to copy: {:?}", file_descriptors);
        let copier = RegionFileCopier::new(region.access_layer());
        // TODO(weny): ensure the target region is empty.
        copier
            .copy_files(
                source_region_id,
                target_region_id,
                file_descriptors,
                parallelism,
            )
            .await?;
        let edit = RegionEdit {
            files_to_add: files_to_copy,
            files_to_remove: vec![],
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
        };
        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        info!("Applying {edit:?} to region {target_region_id}, reason: CopyRegionFrom");
        let version = region
            .manifest_ctx
            .manifest_manager
            .write()
            .await
            .update(action_list, false)
            .await?;
        info!(
            "Successfully update manifest version to {version}, region: {target_region_id}, reason: CopyRegionFrom"
        );

        Ok((Some(edit), file_ids))
    }
}
