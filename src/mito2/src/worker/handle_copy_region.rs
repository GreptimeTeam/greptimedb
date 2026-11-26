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

use common_telemetry::{debug, info};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::region_engine::MitoCopyRegionFromResponse;
use store_api::region_request::PathType;
use store_api::storage::{FileId, RegionId};

use crate::error::{InvalidRequestSnafu, MissingManifestSnafu, Result};
use crate::manifest::action::RegionEdit;
use crate::region::{FileDescriptor, MitoRegionRef, RegionFileCopier, RegionMetadataLoader};
use crate::request::{CopyInstalledRequest, CopyRegionFromRequest, WorkerRequest};
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

/// What a COPY prepared for its target.
struct CopyPreparation {
    /// The edit to publish and install, or `None` when the target manifest already lists
    /// every source sample.
    edit: Option<RegionEdit>,
    /// Sample file ids of the source manifest.
    source_file_ids: Vec<FileId>,
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
            let preparation = match Self::copy_region_from(
                &region,
                region_metadata_loader,
                source_region_id,
                region_id,
                request.parallelism.max(1),
            )
            .await
            {
                Ok(preparation) => preparation,
                Err(e) => {
                    let _ = sender.send(Err(e));
                    return;
                }
            };

            let (tx, rx) = tokio::sync::oneshot::channel();
            let result = match preparation.edit {
                Some(region_edit) => {
                    // The response lists installed entries, including shared SSTs whose
                    // objects stay in place; it is not a physical COPY count.
                    let copied_file_ids = region_edit
                        .files_to_add
                        .iter()
                        .map(|file| file.file_id)
                        .collect();
                    // Publish and install the edit on the worker's edit lane: the region
                    // stays `Editing` from admission until the worker applied the edit, so
                    // no ordinary edit can observe a published version that is not
                    // installed yet, and this request resolves only once the edit is live.
                    let request = if region_edit
                        .files_to_add
                        .iter()
                        .any(|file| file.level == file::LEVEL_SHARED)
                    {
                        // Shared references enter the edit lane for destination admission
                        // sequences. The edit stays bound to the instance that prepared it,
                        // like every other COPY edit.
                        crate::request::RegionEditRequest::new(region_id, region_edit, false, tx)
                            .prepared_against(region.clone())
                    } else {
                        // A physical copy installs the metadata it prepared: its objects are
                        // already copied and its files keep the cleared source sequence
                        // trust. The edit stays bound to the instance that prepared it.
                        crate::request::RegionEditRequest::for_physical_copy(
                            region.clone(),
                            region_edit,
                            tx,
                        )
                    };
                    Self::submit_copy_request(
                        &worker_sender,
                        region_id,
                        WorkerRequest::EditRegion(request),
                        rx,
                    )
                    .await
                    .map(|()| MitoCopyRegionFromResponse { copied_file_ids })
                }
                None => {
                    // Every entry is manifested already. The manifest may hold an edit that
                    // the worker has not installed yet, so acknowledge only what the live
                    // version actually serves.
                    let copied_file_ids = preparation.source_file_ids.clone();
                    Self::submit_copy_request(
                        &worker_sender,
                        region_id,
                        WorkerRequest::CopyInstalled(CopyInstalledRequest {
                            region_id,
                            file_ids: preparation.source_file_ids,
                            sender: tx,
                        }),
                        rx,
                    )
                    .await
                    .map(|()| MitoCopyRegionFromResponse { copied_file_ids })
                }
            };
            let _ = sender.send(result);
        });
    }

    /// Submits a COPY request to the worker and waits for its answer.
    async fn submit_copy_request(
        worker_sender: &tokio::sync::mpsc::Sender<WorkerRequestWithTime>,
        region_id: RegionId,
        request: WorkerRequest,
        rx: tokio::sync::oneshot::Receiver<Result<()>>,
    ) -> Result<()> {
        if worker_sender
            .send(WorkerRequestWithTime::new(request))
            .await
            .is_err()
        {
            return crate::error::RegionNotFoundSnafu { region_id }.fail();
        }
        match rx.await {
            Ok(result) => result,
            Err(error) => Err(error).context(crate::error::RecvSnafu),
        }
    }

    /// Checks that the entries a COPY found in the target manifest are installed in the live
    /// version of the region.
    ///
    /// Manifest membership is not proof: another edit may have published the same entries
    /// and not installed them yet, in which case acknowledging the COPY would report rows the
    /// region does not serve.
    pub(crate) fn handle_copy_installed(&mut self, request: CopyInstalledRequest) {
        let region_id = request.region_id;
        let Some(region) = self.regions.get_region(region_id) else {
            let _ = request
                .sender
                .send(crate::error::RegionNotFoundSnafu { region_id }.fail());
            return;
        };
        let version = region.version();
        let levels = version.ssts.levels();
        let missing = request
            .file_ids
            .iter()
            .find(|id| !levels.iter().any(|level| level.files.contains_key(id)));
        let result = match missing {
            None => Ok(()),
            Some(missing) => {
                info!(
                    "COPY of region {} found sample {} in the manifest but not in the live version",
                    region_id, missing
                );
                Err(crate::error::RegionBusySnafu { region_id }.build())
            }
        };
        let _ = request.sender.send(result);
    }

    /// Prepares the entries the target manifest lacks, including shared logical references.
    ///
    /// The returned edit is prepared but not published: the caller installs it on the
    /// worker's edit lane. If the target manifest lists every source entry there is no edit,
    /// and the caller must verify the entries are installed instead of acknowledging them.
    async fn copy_region_from(
        region: &MitoRegionRef,
        region_metadata_loader: RegionMetadataLoader,
        source_region_id: RegionId,
        target_region_id: RegionId,
        parallelism: usize,
    ) -> Result<CopyPreparation> {
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
            } else if file_meta.region_id == target_region_id {
                // Ownership, not target manifest membership, decides whether the
                // objects already live in the target: a remap moves references
                // between manifests but keeps the physical owner. Installing such
                // an entry publishes this reference only, so the copy plans no
                // object operation for it. A planned copy would target every object
                // onto itself and let a failure elsewhere in the copy delete an
                // artifact that still serves its source.
                //
                // The reference also carries the admission sequence of the region it
                // came from, which cannot bound the rows of a file whose physical
                // sequences belong to the target. Clear it together with the trust
                // marker so an exact sequence-range read fails closed instead of
                // pruning the file and returning a wrong empty result.
                new_file_metas.push(remap_copied_file_meta(file_meta, target_region_id));
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
        // Native series coverage belongs to the source region's sequence domain, so a COPY
        // installs samples only and leaves the target's series indexes alone.
        if new_file_metas.is_empty() {
            return Ok(CopyPreparation {
                edit: None,
                source_file_ids,
            });
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
            files_to_add: new_file_metas,
            files_to_remove: vec![],
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
            ..Default::default()
        };
        // The edit is not published here: the caller publishes and installs it on the
        // worker's edit lane, which keeps publication and installation serialized against
        // every other edit of the region.
        Ok(CopyPreparation {
            edit: Some(edit),
            source_file_ids,
        })
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
