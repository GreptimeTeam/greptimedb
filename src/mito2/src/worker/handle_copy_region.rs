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

use std::collections::HashSet;

use common_telemetry::{debug, error, info};
use snafu::{OptionExt, ensure};
use store_api::logstore::LogStore;
use store_api::region_engine::MitoCopyRegionFromResponse;
use store_api::region_request::PathType;
use store_api::storage::{FileId, RegionId};

use crate::error::{InvalidRequestSnafu, MissingManifestSnafu, RegionNotFoundSnafu, Result};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::region::{
    FileDescriptor, MitoRegionRef, RegionFileCopier, RegionLeaderState, RegionMetadataLoader,
};
use crate::request::{
    BackgroundNotify, CopyRegionFromFinished, CopyRegionFromRequest, WorkerRequest,
};
use crate::sst::file::{self, FileMeta};
use crate::sst::location::region_dir_from_table_dir;
use crate::worker::handle_reload::notify_if_fenced;
use crate::worker::{RegionWorkerLoop, WorkerRequestWithTime, resume_queued_writes};

impl<S: LogStore> RegionWorkerLoop<S> {
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

        // The copy itself runs with the region writable: moving the objects of the source
        // region changes nothing this region publishes, so a publish window held for the
        // whole copy would stall every write of the region for the duration of the object
        // copy. The window is opened where its admission barrier `B` is allocated and the
        // copied edit applied instead (see `handle_copy_region_from_finished`), which is what
        // keeps `B` above every sequence already attributed to a row.
        common_runtime::spawn_global(async move {
            let (files_to_add, source_file_ids) = match Self::copy_region_files(
                &region,
                region_metadata_loader,
                source_region_id,
                region_id,
                request.parallelism.max(1),
            )
            .await
            {
                Ok(copied) => copied,
                Err(e) => {
                    // No window was opened and nothing was published: the region stays
                    // writable and keeps the writes it admitted during the copy.
                    let _ = sender.send(Err(e));
                    return;
                }
            };

            match files_to_add {
                // The target already holds every file of the source, so there is nothing to
                // publish: no barrier is allocated and no window is opened. The response lists
                // installed references, including shared SSTs whose objects stay in place; it
                // is not a physical COPY count.
                None => {
                    let _ = sender.send(Ok(MitoCopyRegionFromResponse {
                        copied_file_ids: source_file_ids,
                    }));
                }
                // The worker loop opens the publish window when it applies the copied files:
                // it is the only place that can prove no write context is in flight, so the
                // barrier the publish allocates inside it is a real admission boundary.
                Some(files_to_add) => {
                    if let Err(e) = worker_sender
                        .send(WorkerRequestWithTime::new(WorkerRequest::Background {
                            region_id,
                            notify: BackgroundNotify::CopyRegionFromFinished(
                                CopyRegionFromFinished {
                                    region_id,
                                    files_to_add,
                                    source_file_ids,
                                    sender,
                                },
                            ),
                        }))
                        .await
                    {
                        error!(e; "Failed to send copy region from finished notification to worker, region_id: {}", region_id);
                    }
                }
            }
        });
    }

    /// Publishes the files a finished copy installed, inside the publish window it opens.
    ///
    /// The copy runs with the region writable (see [`Self::handle_copy_region_from_request`]),
    /// so the window is opened here instead, in the worker loop. The loop is the only place that
    /// can prove no write context is in flight: every write it admits from now on is queued until
    /// the copied files are visible (see `handle_write.rs`), so the admission barrier the publish
    /// allocates in the window is above every sequence already attributed to a row. `Editing` is
    /// the state of the window, so the region cannot be looked up through `writable_region`.
    pub(crate) fn handle_copy_region_from_finished(&mut self, request: CopyRegionFromFinished) {
        let region_id = request.region_id;
        let sender = request.sender;
        let Some(region) = self.regions.get_region(region_id) else {
            let _ = sender.send(RegionNotFoundSnafu { region_id }.fail());
            return;
        };

        if let Err(e) = region.begin_publish_window(RegionLeaderState::Writable) {
            let _ = sender.send(Err(e));
            return;
        }
        // The window is released in the background, so a write the release admits can reach the
        // loop before the notification that admits the writes the window stalled. The window
        // therefore registers them as an ordered drain, which the notification drops and handles
        // in one turn (see `stalled_requests::add_staging_drain` and
        // `handle_editing_window_closed`), so a later write cannot overtake them.
        self.stalled_requests.add_staging_drain(region_id);
        let worker_sender = self.sender.clone();
        let files_to_add = request.files_to_add;
        let source_file_ids = request.source_file_ids;

        common_runtime::spawn_global(async move {
            let result = Self::publish_copied_files(&region, files_to_add).await;
            match result {
                Ok(()) => {
                    let _ = sender.send(Ok(MitoCopyRegionFromResponse {
                        copied_file_ids: source_file_ids,
                    }));
                }
                Err(e) => {
                    let _ = sender.send(Err(e));
                }
            }
            // An uncertain write fenced the region for a reload, which the worker has to start.
            notify_if_fenced(&worker_sender, &region).await;
            // The publish released the window on both paths, so the writes it queued can be
            // admitted now; on success they obtain a sequence above the barrier (see
            // `handle_editing_window_closed`).
            resume_queued_writes(&worker_sender, region_id).await;
        });
    }

    /// Persists the copied files at the admission barrier it allocates inside the publish window,
    /// publishes them in one version, then releases the window the worker loop opened.
    ///
    /// The window is released to `Writable` once the layout is visible, and also when the publish
    /// failed without the manifest possibly holding it. A failure that may have persisted the edit
    /// fences the region for a reload instead: the manifest may hold the copied files at the
    /// barrier while this instance's version does not, so a write admitted now would be attributed
    /// a sequence at or below a committed one, and reopening the region is what makes its version
    /// agree with the manifest again. The caller admits the writes the window queued.
    async fn publish_copied_files(
        region: &MitoRegionRef,
        mut files_to_add: Vec<FileMeta>,
    ) -> Result<()> {
        let region_id = region.region_id;
        // The copy filtered the files against the version it read when it started, and it ran
        // with the region writable: publishing a file the region already holds (or has
        // compacted away meanwhile) would undo that step, so drop it here too.
        let visible_files = visible_file_ids(region);
        files_to_add.retain(|file| !visible_files.contains(&file.file_id));

        if !files_to_add.is_empty() {
            // Allocated inside the window, under the manifest lock that serializes it with every
            // commit: no write can be admitted at or below it, and no sequence attributed to a
            // row — including a tail replay whose edit landed before the window opened — is above
            // it. A region whose sequence domain is exhausted allocates none, and releases the
            // window so the writes it stalled are admitted again.
            let barrier = match region.admission_barrier().await {
                Ok(barrier) => barrier,
                Err(e) => {
                    Self::release_copy_window(region).await;
                    return Err(e);
                }
            };
            // Backfill the barrier on the copied files in the same publish: a barrier-less
            // copy would be reported as an incomplete discovery candidate set forever, so
            // the degraded path could never make progress. The rows are virtualized at the
            // barrier, which is exactly what `remap_copied_file_meta` refuses to trust from
            // the source domain.
            for file in &mut files_to_add {
                file.sequence = std::num::NonZeroU64::new(barrier);
                file.preserve_row_sequence = false;
            }
            let edit = RegionEdit {
                files_to_add,
                files_to_remove: vec![],
                timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                // Publish a committed watermark with the files, so a later full rebuild
                // starts above the copied rows instead of re-reading them.
                committed_sequence: Some(barrier),
                ..Default::default()
            };
            // Shared references are persisted on this path too. Handing them to the region
            // edit lane instead would drop them back onto the publish window this publication
            // holds, so they must be persisted and published in the window like physically
            // copied files (see `handle_copy_region_from_request`).
            let action_list =
                RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
            info!(
                "Applying {:?} to region {region_id}, reason: CopyRegionFrom",
                edit
            );
            match region
                .manifest_ctx
                .update_manifest(RegionLeaderState::Editing, action_list, false)
                .await
            {
                Ok(version) => {
                    info!(
                        "Successfully update manifest version to {version}, region: {region_id}, reason: CopyRegionFrom"
                    );
                    // Make the persisted files visible, then release the window.
                    region
                        .version_control
                        .apply_edit(Some(edit), &[], region.file_purger.clone());
                }
                Err(e) => {
                    if e.may_have_persisted_manifest_update() {
                        // The manifest may hold this edit — the copied files at `barrier`, with
                        // `committed_sequence = barrier` — while this instance's version does
                        // not. Admitting writes now would attribute sequences at or below a
                        // boundary the manifest already committed, so the region stops serving
                        // them and is reloaded from the manifest, which is the only way its
                        // version learns what the edit published. The ambiguity is reported to
                        // the caller like any other failed publish.
                        let mut manager = region.manifest_ctx.manifest_manager.write().await;
                        region
                            .manifest_ctx
                            .fence_for_reload(&mut manager, region_id)
                            .await;
                        return Err(e);
                    }
                    // Nothing was published, so the region stays writable.
                    Self::release_copy_window(region).await;
                    return Err(e);
                }
            }
        }

        Self::release_copy_window(region).await;
        Ok(())
    }

    /// Releases the publish window of a copy that published nothing, so the writes the window
    /// stalled are admitted again.
    async fn release_copy_window(region: &MitoRegionRef) {
        if let Err(e) = region.end_publish_window(RegionLeaderState::Writable) {
            error!(e; "Failed to release the copy publish window of region {}", region.region_id);
        }
    }

    /// Copies the files of the source region the target does not hold yet into the target's
    /// storage, with the target region writable, and returns the files to publish (without a
    /// sequence, see [`remap_copied_file_meta`]) together with the ids of all files of the
    /// source region — including shared references, whose objects stay in place.
    ///
    /// Nothing is published here: no barrier is allocated and no manifest is written, so the
    /// region's `last_sequence` and visible file set are untouched while the copy runs. The
    /// caller publishes the returned files inside the publish window (see
    /// [`Self::handle_copy_region_from_finished`]); `None` means there is nothing to publish.
    async fn copy_region_files(
        region: &MitoRegionRef,
        region_metadata_loader: RegionMetadataLoader,
        source_region_id: RegionId,
        target_region_id: RegionId,
        parallelism: usize,
    ) -> Result<(Option<Vec<FileMeta>>, Vec<FileId>)> {
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
        // The files the target already holds are read from its visible version, which needs no
        // manifest lock: the copy runs with the region writable, so it must not hold the lock
        // that the publishes of the region need. The publish re-checks the set before it adds
        // anything (see `publish_copied_files`).
        let target_files = visible_file_ids(region);
        let source_file_ids = source_region_manifest
            .files
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        debug!(
            "source region files: {:?}, source region id: {}",
            source_region_manifest.files, source_region_id
        );
        for (file_id, file_meta) in &source_region_manifest.files {
            if !target_files.contains(file_id) {
                new_file_metas.push(if file_meta.level == file::LEVEL_SHARED {
                    file_meta.as_ref().clone()
                } else {
                    remap_copied_file_meta(file_meta, target_region_id)
                });
            }
        }
        if new_file_metas.is_empty() {
            return Ok((None, source_file_ids));
        }

        let file_descriptors = new_file_metas
            .iter()
            .filter(|file| file.level != file::LEVEL_SHARED)
            .flat_map(file_descriptors_for_meta)
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

        // The files are published at the barrier the publish allocates inside the window the
        // worker loop opens: no sequence is allocated here, so the copy cannot hold back the
        // writes of the region (see `handle_copy_region_from_finished`).
        Ok((Some(new_file_metas), source_file_ids))
    }
}

/// Returns the ids of the files the visible version of `region` holds.
///
/// It reads the published version instead of the manifest, so the caller needs no manifest
/// lock: a copy runs with the region writable and must not hold the lock the region's
/// publishes need.
fn visible_file_ids(region: &MitoRegionRef) -> HashSet<FileId> {
    region
        .version()
        .ssts
        .levels()
        .iter()
        .flat_map(|level| level.files())
        .map(|file| file.meta_ref().file_id)
        .collect()
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
