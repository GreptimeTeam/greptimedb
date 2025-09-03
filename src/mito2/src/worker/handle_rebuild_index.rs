use std::collections::HashMap;
use std::sync::Arc;

use common_telemetry::{debug, warn};
use puffin::puffin_manager;
use store_api::region_request::PathType;
use store_api::storage::RegionId;

use crate::access_layer::{self, OperationType};
use crate::manifest::action::{RegionMetaAction, RegionMetaActionList};
use crate::region::{MitoRegion, MitoRegionRef, RegionLeaderState};
use crate::request::{self, IndexBuildFailed, IndexBuildFinished, RegionBuildIndexRequest};
use crate::sst::file::{FileHandle, FileId, RegionFileId};
use crate::sst::index::{IndexBuildTask, IndexBuildType, IndexerBuilderImpl};
use crate::sst::location::{self, sst_file_path};
use crate::sst::parquet::WriteOptions;
use crate::worker::RegionWorkerLoop;

impl<S> RegionWorkerLoop<S> {
    pub(crate) fn new_index_build_task(
        &self,
        region: &MitoRegionRef,
        file: FileHandle,
        build_type: IndexBuildType,
    ) -> IndexBuildTask {
        let version = region.version();
        let access_layer = region.access_layer.clone();

        let puffin_manager = if let Some(write_cache) = self.cache_manager.write_cache() {
            write_cache.build_puffin_manager()
        } else {
            access_layer.build_puffin_manager()
        };

        let indexer_builder_ref = Arc::new(IndexerBuilderImpl {
            op_type: OperationType::Flush, //TODO(SNC123): Temporarily set to Flush, 4 BuildTypes will be introduced later.
            metadata: version.metadata.clone(),
            inverted_index_config: self.config.inverted_index.clone(),
            fulltext_index_config: self.config.fulltext_index.clone(),
            bloom_filter_index_config: self.config.bloom_filter_index.clone(),
            index_options: version.options.index_options.clone(),
            row_group_size: WriteOptions::default().row_group_size,
            intermediate_manager: self.intermediate_manager.clone(),
            puffin_manager,
        });

        IndexBuildTask {
            file_meta: file.meta_ref().clone(),
            reason: build_type,
            flushed_entry_id: Some(version.flushed_entry_id),
            flushed_sequence: Some(version.flushed_sequence),
            access_layer: access_layer.clone(),
            write_cache: self.cache_manager.write_cache().cloned(),
            manifest_ctx: region.manifest_ctx.clone(),
            file_purger: file.file_purger(),
            request_sender: self.sender.clone(),
            indexer_builder: indexer_builder_ref.clone(),
        }
    }

    pub(crate) async fn handle_rebuild_index(&mut self, request: RegionBuildIndexRequest) {
        let region_id = request.region_id;
        let Some(region) = self.regions.get_region(region_id) else {
            return;
        };

        let version = region.version();

        let all_files: HashMap<FileId, FileHandle> = version
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files.iter())
            .filter(|(_, handle)| !handle.is_deleted())
            .map(|(id, handle)| (*id, handle.clone()))
            .collect();

        let build_tasks = if request.file_metas.is_empty() {
            all_files.values().cloned().collect::<Vec<_>>()
        } else {
            request
                .file_metas
                .iter()
                .filter_map(|meta| all_files.get(&meta.file_id).cloned())
                .collect::<Vec<_>>()
        };

        for file_handle in build_tasks {
            let _ = self
                .index_build_scheduler
                .schedule_build(self.new_index_build_task(
                    &region,
                    file_handle,
                    request.build_type.clone(),
                ));
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
        let version = region.version();

        for file_meta in &request.edit.files_to_add {
            let file_id = file_meta.file_id;

            // Check if the file exists in the region version.
            let found_in_version = version
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files.iter())
                .any(|(id, handle)| *id == file_id && !handle.is_deleted() && !handle.compacting());
            if !found_in_version {
                warn!(
                    "File id {} not found in region version for index build, region: {}",
                    file_id, region_id
                );
                return;
            }

            // Check if the file exists in the object store.
            let path = location::sst_file_path(
                region.access_layer.table_dir(), 
                RegionFileId::new(file_meta.region_id, file_id),
                PathType::Bare,
            );
            region.access_layer.object_store().exists(&path).await.map_err(|e| {
                warn!(e; "SST file not found for index build, region: {}, file_id: {}", region_id, file_meta.file_id);
                return;
            }).ok();
        }

        // Safety: The corresponding file exists.
        let version_result = region
            .manifest_ctx
            .update_manifest(
                RegionLeaderState::Writable,
                RegionMetaActionList::with_action(RegionMetaAction::Edit(request.edit.clone())),
            )
            .await;

        match version_result {
            Ok(version) => {
                debug!(
                    "Successfully update manifest version to {version}, region: {region_id}, reason : index build"
                );
                region.version_control.apply_edit(
                    request.edit.clone(),
                    &[],
                    region.file_purger.clone(),
                );
            }
            Err(e) => {
                warn!(
                    "Failed to update manifest for index build, region: {}, error: {:?}",
                    region_id, e
                );
                return;
            }
        }
    }

    pub(crate) async fn handle_index_build_failed(
        &mut self,
        region_id: RegionId,
        request: IndexBuildFailed,
    ) {
        // todo!("index build failed handling not implemented yet");
    }
}
