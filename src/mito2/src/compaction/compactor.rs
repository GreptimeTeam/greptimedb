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
use std::sync::Arc;
use std::time::Duration;

use api::v1::region::compact_request;
use common_telemetry::info;
use object_store::manager::ObjectStoreManager;
use object_store::util::join_dir;
use smallvec::SmallVec;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::path_utils::region_dir;
use store_api::storage::RegionId;

use crate::access_layer::{AccessLayer, AccessLayerRef, SstWriteRequest};
use crate::cache::{CacheManager, CacheManagerRef};
use crate::compaction::build_sst_reader;
use crate::compaction::picker::{Picker, PickerOutput};
use crate::config::MitoConfig;
use crate::error::{EmptyRegionDirSnafu, JoinSnafu, ObjectStoreNotFoundSnafu, Result};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::manifest::storage::manifest_compress_type;
use crate::memtable::time_partition::TimePartitions;
use crate::memtable::MemtableBuilderProvider;
use crate::new_picker;
use crate::read::Source;
use crate::region::options::RegionOptions;
use crate::region::version::{VersionBuilder, VersionControl, VersionControlRef};
use crate::region::ManifestContext;
use crate::region::RegionState::Writable;
use crate::schedule::scheduler::LocalScheduler;
use crate::sst::file::{FileMeta, IndexType};
use crate::sst::file_purger::{FilePurgerRef, LocalFilePurger};
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::parquet::WriteOptions;

#[derive(Clone)]
/// CompactionRegion represents a region that needs to be compacted.
/// It's the subset of MitoRegion.
pub struct CompactionRegion {
    pub region_id: RegionId,
    pub region_options: RegionOptions,
    pub engine_config: Arc<MitoConfig>,
    pub region_metadata: RegionMetadataRef,
    pub cache_manager: CacheManagerRef,
    pub access_layer: AccessLayerRef,
    pub file_purger: FilePurgerRef,

    pub(crate) manifest_ctx: Arc<ManifestContext>,
    pub(crate) version_control: VersionControlRef,
}

/// CompactionRequest represents the request to compact a region.
pub struct CompactionRequest {
    pub catalog: String,
    pub schema: String,
    pub region_id: RegionId,
    pub options: HashMap<String, String>,
    pub compaction_options: compact_request::Options,
}

/// Open a compaction region from a compaction request.
/// It's simple version of RegionOpener::open().
pub async fn open_compaction_region(
    req: &CompactionRequest,
    data_home: &str,
    object_store_manager: ObjectStoreManager,
) -> Result<CompactionRegion> {
    let region_options = RegionOptions::try_from(&req.options)?;
    let region_dir = region_dir(
        format!("{}/{}", &req.catalog, &req.schema).as_str(),
        req.region_id,
    );

    let object_store = {
        let name = &region_options.storage;
        if let Some(name) = name {
            object_store_manager
                .find(name)
                .context(ObjectStoreNotFoundSnafu {
                    object_store: name.to_string(),
                })?
        } else {
            object_store_manager.default_object_store()
        }
    };

    let mut mito_config = MitoConfig::default();
    mito_config.sanitize(data_home)?;

    let access_layer = {
        let intermediate_manager =
            IntermediateManager::init_fs(mito_config.inverted_index.intermediate_path.clone())
                .await?;

        Arc::new(AccessLayer::new(
            region_dir.to_string(),
            object_store.clone(),
            intermediate_manager,
        ))
    };

    let manifest_manager = {
        let region_manifest_options = RegionManifestOptions {
            manifest_dir: join_dir(&region_dir, "manifest"),
            object_store: object_store.clone(),
            compress_type: manifest_compress_type(mito_config.compress_manifest),
            checkpoint_distance: mito_config.manifest_checkpoint_distance,
        };

        RegionManifestManager::open(region_manifest_options, Default::default())
            .await?
            .context(EmptyRegionDirSnafu {
                region_id: req.region_id,
                region_dir,
            })?
    };

    let manifest = manifest_manager.manifest();
    let region_metadata = manifest.metadata.clone();
    let manifest_ctx = Arc::new(ManifestContext::new(manifest_manager, Writable));

    let file_purger = {
        let purge_scheduler = Arc::new(LocalScheduler::new(mito_config.max_background_jobs));
        Arc::new(LocalFilePurger::new(
            purge_scheduler.clone(),
            access_layer.clone(),
            None,
        ))
    };

    let version_control = {
        let memtable_builder = MemtableBuilderProvider::new(None, Arc::new(mito_config.clone()))
            .builder_for_options(
                region_options.memtable.as_ref(),
                !region_options.append_mode,
            );

        // Initial memtable id is 0.
        let mutable = Arc::new(TimePartitions::new(
            region_metadata.clone(),
            memtable_builder.clone(),
            0,
            region_options.compaction.time_window(),
        ));

        let version = VersionBuilder::new(region_metadata.clone(), mutable)
            .add_files(file_purger.clone(), manifest.files.values().cloned())
            .flushed_entry_id(manifest.flushed_entry_id)
            .flushed_sequence(manifest.flushed_sequence)
            .truncated_entry_id(manifest.truncated_entry_id)
            .compaction_time_window(manifest.compaction_time_window)
            .options(region_options.clone())
            .build();
        Arc::new(VersionControl::new(version))
    };

    Ok(CompactionRegion {
        region_options: region_options.clone(),
        manifest_ctx,
        access_layer,
        version_control,
        region_id: req.region_id,
        cache_manager: Arc::new(CacheManager::default()),
        engine_config: Arc::new(mito_config.clone()),
        region_metadata: region_metadata.clone(),
        file_purger: file_purger.clone(),
    })
}

/// MergeOutput represents the output of merging SST files.
pub struct MergeOutput {
    pub files_to_add: Option<Vec<FileMeta>>,
    pub files_to_remove: Option<Vec<FileMeta>>,
    pub compaction_time_window: Option<i64>,
}

impl MergeOutput {
    pub fn is_empty(&self) -> bool {
        self.files_to_add.is_none() && self.files_to_remove.is_none()
    }
}

#[async_trait::async_trait]
pub trait Compactor: Send + Sync + 'static {
    async fn merge_ssts(
        &self,
        compaction_region: CompactionRegion,
        picker_output: Option<PickerOutput>,
    ) -> Result<MergeOutput> {
        if let Some(picker_output) = picker_output {
            do_merge_ssts(compaction_region, picker_output).await
        } else {
            Ok(MergeOutput {
                files_to_add: None,
                files_to_remove: None,
                compaction_time_window: None,
            })
        }
    }

    async fn update_manifest(
        &self,
        compaction_region: CompactionRegion,
        merge_output: MergeOutput,
    ) -> Result<()> {
        let files_to_add = {
            if let Some(files) = merge_output.files_to_add {
                files
            } else {
                return Ok(());
            }
        };

        let files_to_remove = {
            if let Some(files) = merge_output.files_to_remove {
                files
            } else {
                return Ok(());
            }
        };

        // Write region edit to manifest.
        let edit = RegionEdit {
            files_to_add,
            files_to_remove,
            compaction_time_window: merge_output
                .compaction_time_window
                .map(|seconds| Duration::from_secs(seconds as u64)),
            flushed_entry_id: None,
            flushed_sequence: None,
        };

        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        // We might leak files if we fail to update manifest. We can add a cleanup task to
        // remove them later.
        compaction_region
            .manifest_ctx
            .update_manifest(Writable, action_list, || {
                compaction_region.version_control.apply_edit(
                    edit,
                    &[],
                    compaction_region.file_purger.clone(),
                );
            })
            .await?;
        Ok(())
    }

    fn picker(&self) -> Arc<dyn Picker>;

    async fn compact(&self, compaction_region: CompactionRegion) -> Result<()> {
        let picker_output = self
            .picker()
            .pick(compaction_region.version_control.current().version);
        let merge_output = self
            .merge_ssts(compaction_region.clone(), picker_output)
            .await?;
        if merge_output.is_empty() {
            info!(
                "No files to compact for region_id: {}",
                compaction_region.region_id
            );
            return Ok(());
        }
        self.update_manifest(compaction_region, merge_output).await
    }
}

async fn do_merge_ssts(
    compaction_region: CompactionRegion,
    mut picker_output: PickerOutput,
) -> Result<MergeOutput> {
    let current_version = compaction_region.version_control.current().version;
    let mut futs = Vec::with_capacity(picker_output.outputs.len());
    let mut compacted_inputs =
        Vec::with_capacity(picker_output.outputs.iter().map(|o| o.inputs.len()).sum());
    for output in picker_output.outputs.drain(..) {
        compacted_inputs.extend(output.inputs.iter().map(|f| f.meta_ref().clone()));

        info!(
            "Compaction region {} output [{}]-> {}",
            compaction_region.region_id,
            output
                .inputs
                .iter()
                .map(|f| f.file_id().to_string())
                .collect::<Vec<_>>()
                .join(","),
            output.output_file_id
        );

        let write_opts = WriteOptions {
            write_buffer_size: compaction_region.engine_config.sst_write_buffer_size,
            ..Default::default()
        };
        let create_inverted_index = compaction_region
            .engine_config
            .inverted_index
            .create_on_compaction
            .auto();
        let mem_threshold_index_create = compaction_region
            .engine_config
            .inverted_index
            .mem_threshold_on_create
            .map(|m| m.as_bytes() as _);
        let index_write_buffer_size = Some(
            compaction_region
                .engine_config
                .inverted_index
                .write_buffer_size
                .as_bytes() as usize,
        );

        let region_metadata = compaction_region.region_metadata.clone();
        let sst_layer = compaction_region.access_layer.clone();
        let region_id = compaction_region.region_id;
        let file_id = output.output_file_id;
        let cache_manager = compaction_region.cache_manager.clone();
        let storage = compaction_region.region_options.storage.clone();
        let index_options = current_version.options.index_options.clone();
        let append_mode = current_version.options.append_mode;
        futs.push(async move {
            let reader = build_sst_reader(
                region_metadata.clone(),
                sst_layer.clone(),
                Some(cache_manager.clone()),
                &output.inputs,
                append_mode,
                output.filter_deleted,
                output.output_time_range,
            )
            .await?;
            let file_meta_opt = sst_layer
                .write_sst(
                    SstWriteRequest {
                        file_id,
                        metadata: region_metadata,
                        source: Source::Reader(reader),
                        cache_manager,
                        storage,
                        create_inverted_index,
                        mem_threshold_index_create,
                        index_write_buffer_size,
                        index_options,
                    },
                    &write_opts,
                )
                .await?
                .map(|sst_info| FileMeta {
                    region_id,
                    file_id,
                    time_range: sst_info.time_range,
                    level: output.output_level,
                    file_size: sst_info.file_size,
                    available_indexes: sst_info
                        .inverted_index_available
                        .then(|| SmallVec::from_iter([IndexType::InvertedIndex]))
                        .unwrap_or_default(),
                    index_file_size: sst_info.index_file_size,
                });
            Ok(file_meta_opt)
        });
    }
    let mut output_files = Vec::with_capacity(futs.len());
    while !futs.is_empty() {
        let mut task_chunk = Vec::with_capacity(crate::compaction::task::MAX_PARALLEL_COMPACTION);
        for _ in 0..crate::compaction::task::MAX_PARALLEL_COMPACTION {
            if let Some(task) = futs.pop() {
                task_chunk.push(common_runtime::spawn_bg(task));
            }
        }
        let metas = futures::future::try_join_all(task_chunk)
            .await
            .context(JoinSnafu)?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;
        output_files.extend(metas.into_iter().flatten());
    }

    let mut inputs: Vec<_> = compacted_inputs.into_iter().collect();
    inputs.extend(
        picker_output
            .expired_ssts
            .iter()
            .map(|f| f.meta_ref().clone()),
    );

    Ok(MergeOutput {
        files_to_add: Some(output_files),
        files_to_remove: Some(inputs),
        compaction_time_window: Some(picker_output.time_window_size),
    })
}

pub struct DefaultCompactor {
    picker: Arc<dyn Picker>,
}

#[async_trait::async_trait]
impl Compactor for DefaultCompactor {
    fn picker(&self) -> Arc<dyn Picker> {
        self.picker.clone()
    }
}

impl DefaultCompactor {
    pub fn new_with_picker(picker: Arc<dyn Picker>) -> Self {
        Self { picker }
    }

    pub fn new_from_request(req: &CompactionRequest) -> Result<Self> {
        let region_options = RegionOptions::try_from(&req.options)?;
        let compaction_options = region_options.compaction;
        let picker = new_picker(req.compaction_options.clone(), &compaction_options);
        Ok(Self { picker })
    }
}
