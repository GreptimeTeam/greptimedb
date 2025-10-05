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

use common_telemetry::{error, warn};
use store_api::storage::{FileId, RegionId};
use tokio::sync::oneshot;

use crate::region::MitoRegionRef;
use crate::request::{IndexBuildFailed, IndexBuildFinished, RegionBuildIndexRequest};
use crate::sst::file::FileHandle;
use crate::sst::index::{IndexBuildOutcome, IndexBuildTask, IndexBuildType, IndexerBuilderImpl};
use crate::sst::parquet::WriteOptions;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) fn new_index_build_task(
        &self,
        region: &MitoRegionRef,
        file: FileHandle,
        build_type: IndexBuildType,
        result_sender: Option<oneshot::Sender<IndexBuildOutcome>>,
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
            manifest_ctx: region.manifest_ctx.clone(),
            write_cache: self.cache_manager.write_cache().cloned(),
            file_purger: file.file_purger(),
            request_sender: self.sender.clone(),
            indexer_builder: indexer_builder_ref.clone(),
            result_sender,
        }
    }

    pub(crate) async fn handle_rebuild_index(&mut self, request: RegionBuildIndexRequest) {
        let region_id = request.region_id;
        let Some(region) = self.regions.get_region(region_id) else {
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
            // NOTE: Currently, rebuilding the index will reconstruct the index for all
            // files in the region, which is a simplified approach and is not yet available for
            // production use; further optimization is required.
            all_files.values().cloned().collect::<Vec<_>>()
        } else {
            request
                .file_metas
                .iter()
                .filter_map(|meta| all_files.get(&meta.file_id).cloned())
                .collect::<Vec<_>>()
        };

        for file_handle in build_tasks {
            let task =
                self.new_index_build_task(&region, file_handle, request.build_type.clone(), None);
            let _ = self
                .index_build_scheduler
                .schedule_build(&region.version_control, task);
        }
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
        region.version_control.apply_edit(
            Some(request.edit.clone()),
            &[],
            region.file_purger.clone(),
        );
    }

    pub(crate) async fn handle_index_build_failed(
        &mut self,
        region_id: RegionId,
        request: IndexBuildFailed,
    ) {
        error!(request.err; "Index build failed for region: {}", region_id);
        // TODO(SNC123): Implement error handling logic after IndexBuildScheduler optimization.
    }
}
