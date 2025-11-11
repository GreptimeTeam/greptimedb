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

use common_telemetry::{debug, error, warn};
use store_api::region_request::RegionBuildIndexRequest;
use store_api::storage::{FileId, RegionId};
use tokio::sync::mpsc;

use crate::cache::CacheStrategy;
use crate::error::Result;
use crate::region::MitoRegionRef;
use crate::request::{
    BuildIndexRequest, IndexBuildFailed, IndexBuildFinished, IndexBuildStopped, OptionOutputTx,
};
use crate::sst::file::{FileHandle, RegionFileId};
use crate::sst::index::{
    IndexBuildOutcome, IndexBuildTask, IndexBuildType, IndexerBuilderImpl, ResultMpscSender,
};
use crate::sst::parquet::WriteOptions;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) fn new_index_build_task(
        &self,
        region: &MitoRegionRef,
        file: FileHandle,
        build_type: IndexBuildType,
        result_sender: ResultMpscSender,
    ) -> IndexBuildTask {
        let version = region.version();
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
            metadata: version.metadata.clone(),
            inverted_index_config: self.config.inverted_index.clone(),
            fulltext_index_config: self.config.fulltext_index.clone(),
            bloom_filter_index_config: self.config.bloom_filter_index.clone(),
            index_options: version.options.index_options.clone(),
            row_group_size: WriteOptions::default().row_group_size,
            intermediate_manager,
            puffin_manager,
        });

        IndexBuildTask {
            file_meta: file.meta_ref().clone(),
            reason: build_type,
            access_layer: access_layer.clone(),
            listener: self.listener.clone(),
            manifest_ctx: region.manifest_ctx.clone(),
            write_cache: self.cache_manager.write_cache().cloned(),
            file_purger: file.file_purger(),
            request_sender: self.sender.clone(),
            indexer_builder: indexer_builder_ref.clone(),
            result_sender,
        }
    }

    /// Handles manual build index requests.
    pub(crate) async fn handle_build_index_request(
        &mut self,
        region_id: RegionId,
        _req: RegionBuildIndexRequest,
        sender: OptionOutputTx,
    ) {
        self.handle_rebuild_index(
            BuildIndexRequest {
                region_id,
                build_type: IndexBuildType::Manual,
                file_metas: Vec::new(),
            },
            sender,
        )
        .await;
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

        let version_control = region.version_control.clone();
        let version = version_control.current().version;

        let all_files: HashMap<FileId, FileHandle> = version
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files.iter())
            .filter(|(_, handle)| !handle.is_deleted() && !handle.compacting())
            .map(|(id, handle)| (*id, handle.clone()))
            .collect();

        let build_tasks = if request.file_metas.is_empty() {
            if request.build_type == IndexBuildType::Manual {
                // Find all files whose index is inconsistent with the region metadata.
                all_files
                    .values()
                    .filter(|file| {
                        !file
                            .meta_ref()
                            .is_index_consistent_with_region(&version.metadata.column_metadatas)
                    })
                    .cloned()
                    .collect::<Vec<_>>()
            } else {
                all_files.values().cloned().collect::<Vec<_>>()
            }
        } else {
            request
                .file_metas
                .iter()
                .filter_map(|meta| all_files.get(&meta.file_id).cloned())
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

        let num_tasks = build_tasks.len();
        let (tx, mut rx) = mpsc::channel::<Result<IndexBuildOutcome>>(num_tasks);

        for file_handle in build_tasks {
            debug!(
                "Scheduling index build for region {}, file_id {}",
                region_id,
                file_handle.meta_ref().file_id
            );

            if region.should_abort_index() {
                warn!(
                    "Region {} is in state {:?}, abort index rebuild process for file_id {}",
                    region_id,
                    region.state(),
                    file_handle.meta_ref().file_id
                );
                break;
            }

            let task = self.new_index_build_task(
                &region,
                file_handle.clone(),
                request.build_type.clone(),
                tx.clone(),
            );
            let _ = self
                .index_build_scheduler
                .schedule_build(&region.version_control, task)
                .await;
        }
        // Wait for all index build tasks to finish and notify the caller.
        common_runtime::spawn_global(async move {
            for _ in 0..num_tasks {
                if let Some(Err(e)) = rx.recv().await {
                    warn!(e; "Index build task failed for region: {}", region_id);
                    sender.send(Err(e));
                    return;
                }
            }
            sender.send(Ok(0));
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

        // Clean old puffin-related cache for all rebuilt files.
        let cache_strategy = CacheStrategy::EnableAll(self.cache_manager.clone());
        for file_meta in &request.edit.files_to_add {
            let region_file_id = RegionFileId::new(region_id, file_meta.file_id);
            cache_strategy.evict_puffin_cache(region_file_id).await;
        }

        region.version_control.apply_edit(
            Some(request.edit.clone()),
            &[],
            region.file_purger.clone(),
        );

        for file_meta in &request.edit.files_to_add {
            self.listener
                .on_index_build_finish(RegionFileId::new(region_id, file_meta.file_id))
                .await;
        }
    }

    pub(crate) async fn handle_index_build_failed(
        &mut self,
        region_id: RegionId,
        request: IndexBuildFailed,
    ) {
        error!(request.err; "Index build failed for region: {}", region_id);
        self.index_build_scheduler
            .on_failure(region_id, request.err.clone())
            .await;
    }

    pub(crate) async fn handle_index_build_stopped(
        &mut self,
        region_id: RegionId,
        request: IndexBuildStopped,
    ) {
        let Some(region) = self.regions.get_region(region_id) else {
            warn!(
                "Region not found for index build stopped, region_id: {}",
                region_id
            );
            return;
        };
        self.index_build_scheduler.on_task_stopped(
            region_id,
            request.file_id,
            &region.version_control,
        );
    }
}
