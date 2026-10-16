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

//! Handles index build requests.

use std::collections::HashMap;
use std::sync::Arc;

use api::v1::region::build_index_request;
use common_telemetry::{debug, warn};
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::RegionBuildIndexRequest;
use store_api::storage::{FileId, RegionId};
use tokio::sync::mpsc;

use crate::cache::CacheStrategy;
use crate::error::{
    IndexBuildIncompleteSnafu, InvalidRequestSnafu, RegionStateSnafu, Result, WorkerStoppedSnafu,
};
use crate::manifest::action::RegionEdit;
use crate::metrics::INDEX_PUBLICATION_STALE_TOTAL;
use crate::region::options::IndexOptions;
use crate::region::{IndexBuildSource, MitoRegionRef, RegionLeaderState, RegionRoleState};
use crate::request::{BuildIndexRequest, IndexBuildFinished, IndexBuildStopped, OptionOutputTx};
use crate::sst::file::{FileHandle, FileMeta, RegionFileId, RegionIndexId};
use crate::sst::index::{
    IndexBuildOutcome, IndexBuildTask, IndexBuildType, IndexerBuilderImpl, ResultMpscSender,
    cleanup_stale_index_caches,
};
use crate::worker::RegionWorkerLoop;

/// Parts of a region version that index build tasks need.
///
/// Holding this instead of the whole version avoids pinning memtables and the
/// SST version while the worker awaits.
pub(crate) struct IndexBuildSnapshot {
    metadata: RegionMetadataRef,
    index_options: IndexOptions,
}

impl<S> RegionWorkerLoop<S> {
    pub(crate) fn new_index_build_task(
        &self,
        region: &MitoRegionRef,
        snapshot: &IndexBuildSnapshot,
        file: FileHandle,
        file_meta: FileMeta,
        build_type: IndexBuildType,
        result_senders: Vec<ResultMpscSender>,
    ) -> IndexBuildTask {
        let access_layer = region.access_layer.clone();

        let puffin_manager = if let Some(write_cache) = self.cache_manager.write_cache() {
            write_cache.build_puffin_manager()
        } else {
            access_layer.build_puffin_manager()
        };

        let intermediate_manager = if let Some(write_cache) = self.cache_manager.write_cache() {
            write_cache.intermediate_manager().clone()
        } else {
            access_layer.intermediate_manager().clone()
        };

        let indexer_builder_ref = Arc::new(IndexerBuilderImpl {
            build_type: build_type.clone(),
            metadata: snapshot.metadata.clone(),
            inverted_index_config: self.config.inverted_index.clone(),
            fulltext_index_config: self.config.fulltext_index.clone(),
            bloom_filter_index_config: self.config.bloom_filter_index.clone(),
            index_options: snapshot.index_options.clone(),
            intermediate_manager,
            puffin_manager,
            write_cache_enabled: self.cache_manager.write_cache().is_some(),
        });

        IndexBuildTask {
            region_id: region.region_id,
            file: file.clone(),
            target_region_metadata: snapshot.metadata.clone(),
            source: IndexBuildSource::new(file_meta, snapshot.metadata.schema_version),
            index_options: snapshot.index_options.clone(),
            reason: build_type,
            access_layer: access_layer.clone(),
            listener: self.listener.clone(),
            manifest_ctx: region.manifest_ctx.clone(),
            write_cache: self.cache_manager.write_cache().cloned(),
            cache_manager: Some(self.cache_manager.clone()),
            file_purger: file.file_purger(),
            request_sender: self.sender.clone(),
            indexer_builder: indexer_builder_ref.clone(),
            result_senders,
        }
    }

    /// Handles manual build index requests.
    pub(crate) async fn handle_build_index_request(
        &mut self,
        region_id: RegionId,
        req: RegionBuildIndexRequest,
        sender: OptionOutputTx,
    ) {
        match req.options {
            Some(build_index_request::Options::SeriesIndex(_)) => {
                self.handle_build_series_index_request(region_id, sender);
            }
            None | Some(build_index_request::Options::SstIndex(_)) => {
                self.handle_rebuild_index(
                    BuildIndexRequest {
                        region_id,
                        build_type: IndexBuildType::Manual,
                        file_metas: Vec::new(),
                        result_senders: Vec::new(),
                    },
                    sender,
                )
                .await;
            }
        }
    }

    /// Submits manual reconciliation without blocking the region worker on index I/O.
    fn handle_build_series_index_request(&self, region_id: RegionId, mut sender: OptionOutputTx) {
        let Some(region) = self.regions.writable_region_or(region_id, &mut sender) else {
            return;
        };
        let Some(state) = &self.series_index_task_state else {
            sender.send(
                InvalidRequestSnafu {
                    region_id,
                    reason: "series index is disabled; enable experimental_enable_series_index",
                }
                .fail(),
            );
            return;
        };
        let receiver = match state.try_reconcile(region) {
            Ok(receiver) => receiver,
            Err(error) => {
                sender.send(Err(error));
                return;
            }
        };
        let worker_id = self.id;
        common_runtime::spawn_global(async move {
            let result = receiver
                .await
                .unwrap_or_else(|_| WorkerStoppedSnafu { id: worker_id }.fail());
            sender.send(result.map(|_| 0));
        });
    }

    pub(crate) async fn handle_rebuild_index(
        &mut self,
        request: BuildIndexRequest,
        mut sender: OptionOutputTx,
    ) {
        let region_id = request.region_id;
        let Some(region) = self.regions.writable_region_or(region_id, &mut sender) else {
            return;
        };
        // Staging regions are writable but must not publish indexes.
        if region.should_abort_index() {
            sender.send(
                RegionStateSnafu {
                    region_id,
                    state: region.state(),
                    expect: RegionRoleState::Leader(RegionLeaderState::Writable),
                }
                .fail(),
            );
            return;
        }

        // Only keep the parts of the version that index builds need, so the
        // memtables and SST version are not pinned across the awaits below.
        let (snapshot, all_files) = {
            let version = region.version_control.current().version;
            let all_files: HashMap<FileId, FileHandle> = version
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files.iter())
                .filter(|(_, handle)| !handle.is_deleted() && !handle.compacting())
                .map(|(id, handle)| (*id, handle.clone()))
                .collect();
            let snapshot = IndexBuildSnapshot {
                metadata: version.metadata.clone(),
                index_options: version.options.index_options.clone(),
            };
            (snapshot, all_files)
        };
        // A committed index publication may not have reached version control
        // yet. Use the manifest's FileMeta as the conditional publication
        // source while keeping the builder's schema generation from the version.
        let manifest = region.manifest_ctx.manifest().await;
        let current_file_meta = |file_id| {
            let file_meta = manifest.files.get(&file_id);
            if file_meta.is_none() {
                debug!(
                    "Skipping index build because file is absent from manifest, region: {}, file_id: {}",
                    region_id, file_id
                );
            }
            file_meta.cloned()
        };

        let build_tasks = if request.file_metas.is_empty() {
            // If no specific files are provided, find files whose index is inconsistent with the region metadata.
            // Check the version rather than the manifest: a publication committed
            // to the manifest but not yet applied is still invisible to queries.
            // Its build still holds the file, so the manual task waits for it.
            all_files
                .values()
                .filter(|file| {
                    !file.meta_ref().is_index_consistent_with_region(
                        &snapshot.metadata.column_metadatas,
                        &snapshot.index_options,
                    )
                })
                .filter_map(|file| {
                    let file_meta = current_file_meta(file.meta_ref().file_id)?;
                    Some((file.clone(), file_meta))
                })
                .collect::<Vec<_>>()
        } else {
            request
                .file_metas
                .iter()
                .filter_map(|meta| {
                    let file = all_files.get(&meta.file_id)?;
                    let file_meta = current_file_meta(meta.file_id)?;
                    Some((file.clone(), file_meta))
                })
                .collect::<Vec<_>>()
        };

        if build_tasks.is_empty() {
            debug!(
                "No files need to build index for region {}, request: {:?}",
                region_id, request
            );
            sender.send(Ok(0));
            return;
        }

        let file_ids = build_tasks
            .iter()
            .map(|(file, _)| file.meta_ref().file_id)
            .collect::<Vec<_>>();
        // Each task holds a sender and reports at most once, so sends never block.
        let (tx, mut rx) = mpsc::channel::<Result<IndexBuildOutcome>>(build_tasks.len());
        for (file_handle, file_meta) in build_tasks {
            debug!(
                "Scheduling index build for region {}, file_id {}",
                region_id,
                file_handle.meta_ref().file_id
            );

            let mut result_senders = request.result_senders.clone();
            result_senders.push(tx.clone());
            let task = self.new_index_build_task(
                &region,
                &snapshot,
                file_handle,
                file_meta,
                request.build_type.clone(),
                result_senders,
            );
            self.index_build_scheduler
                .schedule_build(&region.version_control, task)
                .await;
        }
        drop(tx);

        // Wait for all index build tasks to finish and notify the caller.
        common_runtime::spawn_global(async move {
            // The channel closes once every task has reported or been dropped.
            while let Some(result) = rx.recv().await {
                if let Err(e) = result {
                    warn!(e; "Index build task failed for region: {}", region_id);
                    sender.send(Err(e));
                    return;
                }
            }

            // Aborted and dropped tasks are fine as long as another build or a
            // compaction has left the file with the expected index.
            let manifest = region.manifest_ctx.manifest().await;
            let incomplete = file_ids
                .into_iter()
                .filter(|file_id| {
                    manifest.files.get(file_id).is_some_and(|file_meta| {
                        !file_meta.is_index_consistent_with_region(
                            &snapshot.metadata.column_metadatas,
                            &snapshot.index_options,
                        )
                    })
                })
                .collect::<Vec<_>>();
            let result = if incomplete.is_empty() {
                Ok(0)
            } else {
                IndexBuildIncompleteSnafu {
                    region_id,
                    file_ids: incomplete,
                }
                .fail()
            };
            sender.send(result);
        });
    }

    pub(crate) async fn handle_index_build_finished(
        &mut self,
        region_id: RegionId,
        request: IndexBuildFinished,
    ) {
        let region = match self.regions.get_region(region_id) {
            Some(region) => region,
            None => {
                warn!(
                    "Region not found for index build finished, region_id: {}",
                    region_id
                );
                return;
            }
        };

        let file_meta = request.file_meta;
        let region_file_id = RegionFileId::new(file_meta.region_id, file_meta.file_id);
        let index_id = RegionIndexId::new(region_file_id, file_meta.index_version);

        // Clean old puffin-related cache before making the new metadata visible.
        let cache_strategy = CacheStrategy::EnableAll(self.cache_manager.clone());
        cache_strategy.evict_puffin_cache(index_id).await;

        let manager = region.manifest_ctx.manifest_manager.read().await;
        let manifest = manager.manifest();
        let is_current = manifest.files.get(&file_meta.file_id) == Some(&file_meta);
        if is_current {
            region.version_control.apply_edit(
                Some(RegionEdit {
                    files_to_add: vec![file_meta.clone()],
                    files_to_remove: Vec::new(),
                    timestamp_ms: None,
                    flushed_sequence: None,
                    flushed_entry_id: None,
                    committed_sequence: None,
                    compaction_time_window: None,
                }),
                &[],
                region.file_purger.clone(),
            );
        }
        drop(manager);

        let outcome = if is_current {
            self.listener.on_index_build_finish(region_file_id).await;
            IndexBuildOutcome::Finished
        } else {
            INDEX_PUBLICATION_STALE_TOTAL
                .with_label_values(&["worker_apply"])
                .inc();
            warn!(
                "Ignores stale index build result, region: {}, file_id: {}, index_version: {}, committed manifest version: {}",
                region_id, file_meta.file_id, file_meta.index_version, request.manifest_version
            );
            cleanup_stale_index_caches(
                index_id,
                &region.access_layer,
                Some(&self.cache_manager),
                self.cache_manager.write_cache(),
            )
            .await;
            self.listener.on_index_build_abort(region_file_id).await;
            IndexBuildOutcome::Aborted(format!(
                "Index publication was superseded before worker apply, region: {}, file_id: {}",
                region_id, file_meta.file_id
            ))
        };
        // Notify waiters after the version is updated, so queries issued after a
        // manual build returns can use the new index. `try_send` keeps the worker
        // loop from waiting on a caller; a missing result is caught by the
        // waiter's manifest check.
        for sender in &request.result_senders {
            let _ = sender.try_send(Ok(outcome.clone()));
        }
    }

    pub(crate) async fn handle_index_build_stopped(
        &mut self,
        region_id: RegionId,
        request: IndexBuildStopped,
    ) {
        self.index_build_scheduler
            .on_task_stopped(region_id, request.file_id);
    }
}
