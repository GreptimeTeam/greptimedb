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

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use common_telemetry::{debug, error, info};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::region_engine::MitoCopyRegionFromResponse;
use store_api::region_request::PathType;
use store_api::storage::{FileId, RegionId};

use crate::error::{InvalidRequestSnafu, MissingManifestSnafu, Result};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::region::{
    FileDescriptor, MitoRegionRef, RegionFileCopier, RegionLeaderState, RegionMetadataLoader,
};
use crate::request::{
    BackgroundNotify, CopyRegionFromFinished, CopyRegionFromRequest, WorkerRequest,
};
use crate::sst::file::{self, FileMeta};
use crate::sst::location::region_dir_from_table_dir;
use crate::worker::{RegionWorkerLoop, WorkerRequestWithTime};

/// COPY locks shared by one region worker and its background tasks.
#[derive(Clone, Default)]
pub(super) struct CopyRegionLocks {
    entries: Arc<Mutex<HashMap<RegionId, CopyRegionLock>>>,
}

struct CopyRegionLock {
    mutex: Arc<tokio::sync::Mutex<()>>,
    users: usize,
}

/// Registers a COPY before it is spawned, including while it waits for the lock.
struct CopyRegionGuard {
    locks: CopyRegionLocks,
    region_id: RegionId,
    mutex: Arc<tokio::sync::Mutex<()>>,
}

impl CopyRegionLocks {
    fn register(&self, region_id: RegionId) -> CopyRegionGuard {
        let mut entries = self.entries.lock().unwrap();
        let entry = entries.entry(region_id).or_insert_with(|| CopyRegionLock {
            mutex: Arc::new(tokio::sync::Mutex::new(())),
            users: 0,
        });
        entry.users += 1;
        CopyRegionGuard {
            locks: self.clone(),
            region_id,
            mutex: entry.mutex.clone(),
        }
    }
}

impl Drop for CopyRegionGuard {
    fn drop(&mut self) {
        // Registration and removal share the map lock, so a queued COPY cannot
        // lose its entry and race a new COPY using a different mutex.
        let mut entries = self.locks.entries.lock().unwrap();
        let entry = entries.get_mut(&self.region_id).unwrap();
        entry.users -= 1;
        if entry.users == 0 {
            entries.remove(&self.region_id);
        }
    }
}

impl<S> RegionWorkerLoop<S> {
    pub(crate) fn handle_copy_region_from_request(&mut self, request: CopyRegionFromRequest) {
        let region_id = request.region_id;
        let source_region_id = request.source_region_id;
        let sender = request.sender;
        let region = match self.regions.writable_non_staging_region(region_id) {
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

        let region_metadata_loader = RegionMetadataLoader::new(
            self.config.clone(),
            self.object_store_manager.clone(),
            self.node_lease.is_enabled(),
            self.file_ref_manager.file_metadata_interner.clone(),
        );
        let worker_sender = self.sender.clone();

        let copy_guard = self.copy_region_locks.register(region_id);
        common_runtime::spawn_global(async move {
            let _copy_lock = copy_guard.mutex.lock().await;
            let (region_edit, source_file_ids) = match Self::copy_region_from(
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
                Some(region_edit)
                    if region_edit
                        .files_to_add
                        .iter()
                        .any(|file| file.level == file::LEVEL_SHARED) =>
                {
                    // Shared references enter the serialized
                    // edit lane for destination admission sequences.
                    // The response lists installed references, including shared SSTs
                    // whose objects stay in place; it is not a physical COPY count.
                    let copied_file_ids = region_edit
                        .files_to_add
                        .iter()
                        .map(|file| file.file_id)
                        .collect();
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    if worker_sender
                        .send(WorkerRequestWithTime::new(WorkerRequest::EditRegion(
                            crate::request::RegionEditRequest::new(
                                region_id,
                                region_edit,
                                false,
                                tx,
                            ),
                        )))
                        .await
                        .is_err()
                    {
                        let _ = sender.send(crate::error::RegionNotFoundSnafu { region_id }.fail());
                        return;
                    }
                    let result = match rx.await {
                        Ok(result) => {
                            result.map(|_| MitoCopyRegionFromResponse { copied_file_ids })
                        }
                        Err(error) => Err(error).context(crate::error::RecvSnafu),
                    };
                    let _ = sender.send(result);
                }
                Some(region_edit) => {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    if let Err(e) = worker_sender
                        .send(WorkerRequestWithTime::new(WorkerRequest::Background {
                            region_id,
                            notify: BackgroundNotify::CopyRegionFromFinished(
                                CopyRegionFromFinished {
                                    region_id,
                                    edit: region_edit,
                                    sender: tx,
                                },
                            ),
                        }))
                        .await
                    {
                        error!(e; "Failed to send copy region from finished notification, region_id: {}", region_id);
                        let _ = sender.send(crate::error::RegionNotFoundSnafu { region_id }.fail());
                        return;
                    }
                    // Keep ownership until the worker installs the published version.
                    let result = match rx.await {
                        Ok(result) => result,
                        Err(error) => Err(error).context(crate::error::RecvSnafu),
                    };
                    let _ = sender.send(result);
                }
                None => {
                    let _ = sender.send(Ok(MitoCopyRegionFromResponse {
                        copied_file_ids: source_file_ids,
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

    /// Returns the region edit and source file IDs, including shared logical references.
    ///
    /// If no need to copy files, returns (None, source_file_ids).
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
            .load_manifest(&region_dir, &region.version().options)
            .await?
            .context(MissingManifestSnafu {
                region_id: source_region_id,
            })?;
        let narthex_format =
            region.version().options.sst_format == Some(crate::sst::FormatType::Narthex);
        ensure!(
            (source_region_manifest.sst_format == crate::sst::FormatType::Narthex)
                == narthex_format,
            InvalidRequestSnafu {
                region_id: target_region_id,
                reason: "Cannot copy between Parquet and Narthex regions",
            }
        );
        // Metadata sync transforms copied rows and then removes these references.
        // It cannot retire shared inputs through the ordinary edit API, so reject
        // them before either physical copying or extending the target manifest.
        ensure!(
            path_type != PathType::Metadata
                || source_region_manifest
                    .files
                    .values()
                    .all(|file| file.level != file::LEVEL_SHARED),
            InvalidRequestSnafu {
                region_id: target_region_id,
                reason: "Shared SST references are not supported in metadata-region copies",
            }
        );
        let mut new_file_metas = vec![];
        let target_region_manifest = region.manifest_ctx.manifest().await;
        let source_file_ids = source_region_manifest
            .files
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        debug!(
            "source region files: {:?}, source region id: {}",
            source_region_manifest.files, source_region_id
        );
        let mut file_descriptors = Vec::new();
        for (file_id, file_meta) in &source_region_manifest.files {
            if target_region_manifest.files.contains_key(file_id) {
                continue;
            }
            if file_meta.level == file::LEVEL_SHARED {
                new_file_metas.push(file_meta.as_ref().clone());
            } else if narthex_format {
                let (copied, objects) = prepare_narthex_copy(region, file_meta).await?;
                new_file_metas.push(copied);
                file_descriptors.extend(objects);
            } else {
                let copied = remap_copied_file_meta(file_meta, target_region_id);
                file_descriptors.extend(file_descriptors_for_meta(&copied));
                new_file_metas.push(copied);
            }
        }
        let target_samples: std::collections::HashMap<_, _> = target_region_manifest
            .files
            .values()
            .map(|meta| meta.as_ref())
            .chain(new_file_metas.iter())
            .filter_map(|target| {
                let source = source_region_manifest.files.get(&target.file_id)?;
                Some((source.file_id(), target.file_id()))
            })
            .collect();
        let mut new_series_indexes = Vec::new();
        for (id, index) in &source_region_manifest.series_indexes {
            if matches!(&index.format, file::FileFormat::NarthexSeriesIndex(meta) if meta.coverage.is_some())
            {
                // Destination imports have their own admission sequences.
                continue;
            }
            if target_region_manifest.series_indexes.contains_key(id) {
                continue;
            }
            let (mut copied, objects) = prepare_narthex_copy(region, index).await?;
            if let file::FileFormat::NarthexSeriesIndex(narthex) = &mut copied.format {
                for source in &mut narthex.sources {
                    // A retained index may refer to samples absent from this copy.
                    if let Some(target) = target_samples.get(source) {
                        *source = *target;
                    }
                }
            }
            // This index has its own physical target objects, even when some
            // samples remain shared references to a different owner.
            copied.level = 0;
            new_series_indexes.push(copied);
            file_descriptors.extend(objects);
        }
        if new_file_metas.is_empty() && new_series_indexes.is_empty() {
            return Ok((None, source_file_ids));
        }

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
            series_indexes_to_add: new_series_indexes,
            series_indexes_to_remove: Vec::new(),
            files_to_add: new_file_metas,
            files_to_remove: vec![],
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
            ..Default::default()
        };
        if edit
            .files_to_add
            .iter()
            .any(|file| file.level == file::LEVEL_SHARED)
        {
            return Ok((Some(edit), source_file_ids));
        }
        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        info!(
            "Applying {:?} to region {target_region_id}, reason: CopyRegionFrom",
            edit
        );
        let version = region
            .manifest_ctx
            .update_manifest(RegionLeaderState::Writable, action_list, false)
            .await?;
        info!(
            "Successfully update manifest version to {version}, region: {target_region_id}, reason: CopyRegionFrom"
        );

        Ok((Some(edit), source_file_ids))
    }
}

fn remap_copied_file_meta(file_meta: &FileMeta, target_region_id: RegionId) -> FileMeta {
    let mut new_file_meta = file_meta.clone();
    new_file_meta.region_id = target_region_id;
    // The target region has an independent sequence domain: the physical
    // per-row sequences in the copied file belong to the source region, so they
    // must not be trusted for exact sequence-range reads on the target. Clear
    // the `preserve_row_sequence` marker and source-domain max sequence to fail
    // closed until the scan provably cannot intersect the copied rows (see
    // `files_allow_exact_sequence_range`); otherwise an exact request would
    // replay source-domain rows as target sequences, and an unmarked file with
    // a stale source-domain `sequence` hint could be silently skipped as
    // "proven disjoint".
    new_file_meta.preserve_row_sequence = false;
    new_file_meta.sequence = None;
    new_file_meta
}

fn file_descriptors_for_meta(file_meta: &FileMeta) -> Vec<FileDescriptor> {
    let data = FileDescriptor::Data {
        file_id: file_meta.file_id,
        size: file_meta.file_size,
    };
    if file_meta.exists_index() {
        let region_index_id = file_meta.index_id();
        vec![
            data,
            FileDescriptor::Index {
                file_id: region_index_id.file_id.file_id(),
                version: region_index_id.version,
                size: file_meta.index_file_size(),
            },
        ]
    } else {
        vec![data]
    }
}

/// Prepares a physical narthex copy without rewriting encoded parts. The artifact
/// manifest records the physical owner; paths are resolved from FileMeta on read.
async fn prepare_narthex_copy(
    region: &MitoRegionRef,
    source: &FileMeta,
) -> Result<(FileMeta, Vec<FileDescriptor>)> {
    use crate::series_index::columnar::{invalid, require};
    use crate::sst::file::FileFormat;

    let layer = region.access_layer();
    let source_objects = source
        .narthex_objects(&layer.build_region_dir(source.region_id))
        .ok_or_else(|| invalid("expected Narthex copy source"))?;
    let (source_manifest, parts) = source_objects
        .split_last()
        .expect("Narthex inventory includes manifest");
    let bytes = layer
        .object_store()
        .read(&source_manifest.path)
        .await
        .context(crate::error::OpenDalSnafu)?
        .to_bytes();
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|error| invalid(error.to_string()))?;
    require(
        manifest["region_id"].as_u64() == Some(source.region_id.as_u64())
            && manifest["generation"].as_str() == Some(source.file_id.to_string().as_str()),
        "narthex copy manifest identity mismatch",
    )?;
    manifest["region_id"] = serde_json::json!(region.region_id.as_u64());
    let bytes = bytes::Bytes::from(
        serde_json::to_vec(&manifest).map_err(|error| invalid(error.to_string()))?,
    );
    let target_region_dir = layer.build_region_dir(region.region_id);
    let mut copied = remap_copied_file_meta(source, region.region_id);
    let manifest_size = bytes.len() as u64;
    match &mut copied.format {
        FileFormat::NarthexSamples(meta) => meta.manifest_size = manifest_size,
        FileFormat::NarthexSeriesIndex(meta) => meta.manifest_size = manifest_size,
        FileFormat::Parquet => return Err(invalid("expected Narthex copy source")),
    }
    let targets = copied
        .narthex_objects(&target_region_dir)
        .expect("Narthex copy");
    let descriptors: Vec<_> = source_objects
        .iter()
        .zip(targets)
        .enumerate()
        .map(|(i, (object, target))| FileDescriptor::NarthexObject {
            source: object.path.clone(),
            target: target.path,
            size: target.file_size,
            manifest: (i == parts.len()).then(|| bytes.clone()),
        })
        .collect();
    copied.file_size = descriptors.iter().try_fold(0u64, |size, object| {
        size.checked_add(object.size())
            .ok_or_else(|| invalid("narthex copy size overflow"))
    })?;
    Ok((copied, descriptors))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn copy_lock_guard_cleans_up_after_waiter_cancellation() {
        let locks = CopyRegionLocks::default();
        let region_id = RegionId::new(1, 1);
        let first = locks.register(region_id);
        let held = first.mutex.lock().await;
        let waiting = locks.register(region_id);
        assert!(Arc::ptr_eq(&first.mutex, &waiting.mutex));
        let task = tokio::spawn(async move {
            let _held = waiting.mutex.lock().await;
        });
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(locks.entries.lock().unwrap()[&region_id].users, 1);
        drop(held);
        drop(first);
        assert!(locks.entries.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn copy_lock_guard_retains_entry_for_waiting_copy() {
        let locks = CopyRegionLocks::default();
        let region_id = RegionId::new(1, 1);
        let first = locks.register(region_id);
        let held = first.mutex.lock().await;
        let waiting = locks.register(region_id);
        assert!(waiting.mutex.try_lock().is_err());
        drop(held);
        drop(first);
        let next = locks.register(region_id);
        assert!(Arc::ptr_eq(&waiting.mutex, &next.mutex));
        let held = waiting.mutex.lock().await;
        assert!(next.mutex.try_lock().is_err());
        drop(held);
        drop(waiting);
        assert_eq!(locks.entries.lock().unwrap()[&region_id].users, 1);
        drop(next);
        assert!(locks.entries.lock().unwrap().is_empty());
    }
}
