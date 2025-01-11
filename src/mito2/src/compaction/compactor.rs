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

use std::num::NonZero;
use std::sync::Arc;
use std::time::Duration;

use api::v1::region::compact_request;
use common_meta::key::SchemaMetadataManagerRef;
use common_telemetry::{info, warn};
use common_time::TimeToLive;
use object_store::manager::ObjectStoreManagerRef;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::access_layer::{AccessLayer, AccessLayerRef, OperationType, SstWriteRequest};
use crate::cache::{CacheManager, CacheManagerRef};
use crate::compaction::picker::{new_picker, PickerOutput};
use crate::compaction::{find_ttl, CompactionSstReaderBuilder};
use crate::config::MitoConfig;
use crate::error::{EmptyRegionDirSnafu, JoinSnafu, ObjectStoreNotFoundSnafu, Result};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::manifest::storage::manifest_compress_type;
use crate::read::Source;
use crate::region::opener::new_manifest_dir;
use crate::region::options::RegionOptions;
use crate::region::version::VersionRef;
use crate::region::{ManifestContext, RegionLeaderState, RegionRoleState};
use crate::schedule::scheduler::LocalScheduler;
use crate::sst::file::FileMeta;
use crate::sst::file_purger::LocalFilePurger;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::sst::parquet::WriteOptions;
use crate::sst::version::{SstVersion, SstVersionRef};

/// Region version for compaction that does not hold memtables.
#[derive(Clone)]
pub struct CompactionVersion {
    /// Metadata of the region.
    ///
    /// Altering metadata isn't frequent, storing metadata in Arc to allow sharing
    /// metadata and reuse metadata when creating a new `Version`.
    pub(crate) metadata: RegionMetadataRef,
    /// Options of the region.
    pub(crate) options: RegionOptions,
    /// SSTs of the region.
    pub(crate) ssts: SstVersionRef,
    /// Inferred compaction time window.
    pub(crate) compaction_time_window: Option<Duration>,
}

impl From<VersionRef> for CompactionVersion {
    fn from(value: VersionRef) -> Self {
        Self {
            metadata: value.metadata.clone(),
            options: value.options.clone(),
            ssts: value.ssts.clone(),
            compaction_time_window: value.compaction_time_window,
        }
    }
}

/// CompactionRegion represents a region that needs to be compacted.
/// It's the subset of MitoRegion.
#[derive(Clone)]
pub struct CompactionRegion {
    pub region_id: RegionId,
    pub region_options: RegionOptions,
    pub region_dir: String,

    pub(crate) engine_config: Arc<MitoConfig>,
    pub(crate) region_metadata: RegionMetadataRef,
    pub(crate) cache_manager: CacheManagerRef,
    pub(crate) access_layer: AccessLayerRef,
    pub(crate) manifest_ctx: Arc<ManifestContext>,
    pub(crate) current_version: CompactionVersion,
    pub(crate) file_purger: Option<Arc<LocalFilePurger>>,
    pub(crate) ttl: Option<TimeToLive>,
}

/// OpenCompactionRegionRequest represents the request to open a compaction region.
#[derive(Debug, Clone)]
pub struct OpenCompactionRegionRequest {
    pub region_id: RegionId,
    pub region_dir: String,
    pub region_options: RegionOptions,
}

/// Open a compaction region from a compaction request.
/// It's simple version of RegionOpener::open().
pub async fn open_compaction_region(
    req: &OpenCompactionRegionRequest,
    mito_config: &MitoConfig,
    object_store_manager: ObjectStoreManagerRef,
    schema_metadata_manager: SchemaMetadataManagerRef,
) -> Result<CompactionRegion> {
    let object_store = {
        let name = &req.region_options.storage;
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

    let access_layer = {
        let puffin_manager_factory = PuffinManagerFactory::new(
            &mito_config.index.aux_path,
            mito_config.index.staging_size.as_bytes(),
            Some(mito_config.index.write_buffer_size.as_bytes() as _),
        )
        .await?;
        let intermediate_manager =
            IntermediateManager::init_fs(mito_config.index.aux_path.clone()).await?;

        Arc::new(AccessLayer::new(
            req.region_dir.as_str(),
            object_store.clone(),
            puffin_manager_factory,
            intermediate_manager,
        ))
    };

    let manifest_manager = {
        let region_manifest_options = RegionManifestOptions {
            manifest_dir: new_manifest_dir(req.region_dir.as_str()),
            object_store: object_store.clone(),
            compress_type: manifest_compress_type(mito_config.compress_manifest),
            checkpoint_distance: mito_config.manifest_checkpoint_distance,
        };

        RegionManifestManager::open(region_manifest_options, Default::default())
            .await?
            .context(EmptyRegionDirSnafu {
                region_id: req.region_id,
                region_dir: req.region_dir.as_str(),
            })?
    };

    let manifest = manifest_manager.manifest();
    let region_metadata = manifest.metadata.clone();
    let manifest_ctx = Arc::new(ManifestContext::new(
        manifest_manager,
        RegionRoleState::Leader(RegionLeaderState::Writable),
    ));

    let file_purger = {
        let purge_scheduler = Arc::new(LocalScheduler::new(mito_config.max_background_purges));
        Arc::new(LocalFilePurger::new(
            purge_scheduler.clone(),
            access_layer.clone(),
            None,
        ))
    };

    let current_version = {
        let mut ssts = SstVersion::new();
        ssts.add_files(file_purger.clone(), manifest.files.values().cloned());
        CompactionVersion {
            metadata: region_metadata.clone(),
            options: req.region_options.clone(),
            ssts: Arc::new(ssts),
            compaction_time_window: manifest.compaction_time_window,
        }
    };

    let ttl = find_ttl(
        req.region_id.table_id(),
        current_version.options.ttl,
        &schema_metadata_manager,
    )
    .await
    .unwrap_or_else(|e| {
        warn!(e; "Failed to get ttl for region: {}", region_metadata.region_id);
        TimeToLive::default()
    });
    Ok(CompactionRegion {
        region_id: req.region_id,
        region_options: req.region_options.clone(),
        region_dir: req.region_dir.clone(),
        engine_config: Arc::new(mito_config.clone()),
        region_metadata: region_metadata.clone(),
        cache_manager: Arc::new(CacheManager::default()),
        access_layer,
        manifest_ctx,
        current_version,
        file_purger: Some(file_purger),
        ttl: Some(ttl),
    })
}

impl CompactionRegion {
    pub fn file_purger(&self) -> Option<Arc<LocalFilePurger>> {
        self.file_purger.clone()
    }
}

/// `[MergeOutput]` represents the output of merging SST files.
#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub struct MergeOutput {
    pub files_to_add: Vec<FileMeta>,
    pub files_to_remove: Vec<FileMeta>,
    pub compaction_time_window: Option<i64>,
}

impl MergeOutput {
    pub fn is_empty(&self) -> bool {
        self.files_to_add.is_empty() && self.files_to_remove.is_empty()
    }
}

/// Compactor is the trait that defines the compaction logic.
#[async_trait::async_trait]
pub trait Compactor: Send + Sync + 'static {
    /// Merge SST files for a region.
    async fn merge_ssts(
        &self,
        compaction_region: &CompactionRegion,
        picker_output: PickerOutput,
    ) -> Result<MergeOutput>;

    /// Update the manifest after merging SST files.
    async fn update_manifest(
        &self,
        compaction_region: &CompactionRegion,
        merge_output: MergeOutput,
    ) -> Result<RegionEdit>;

    /// Execute compaction for a region.
    async fn compact(
        &self,
        compaction_region: &CompactionRegion,
        compact_request_options: compact_request::Options,
    ) -> Result<()>;
}

/// DefaultCompactor is the default implementation of Compactor.
pub struct DefaultCompactor;

#[async_trait::async_trait]
impl Compactor for DefaultCompactor {
    async fn merge_ssts(
        &self,
        compaction_region: &CompactionRegion,
        mut picker_output: PickerOutput,
    ) -> Result<MergeOutput> {
        let mut futs = Vec::with_capacity(picker_output.outputs.len());
        let mut compacted_inputs =
            Vec::with_capacity(picker_output.outputs.iter().map(|o| o.inputs.len()).sum());

        for output in picker_output.outputs.drain(..) {
            compacted_inputs.extend(output.inputs.iter().map(|f| f.meta_ref().clone()));

            info!(
                "Region {} compaction input: [{}]",
                compaction_region.region_id,
                output
                    .inputs
                    .iter()
                    .map(|f| f.file_id().to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            );

            let write_opts = WriteOptions {
                write_buffer_size: compaction_region.engine_config.sst_write_buffer_size,
                max_file_size: picker_output.max_file_size,
                ..Default::default()
            };

            let region_metadata = compaction_region.region_metadata.clone();
            let sst_layer = compaction_region.access_layer.clone();
            let region_id = compaction_region.region_id;
            let cache_manager = compaction_region.cache_manager.clone();
            let storage = compaction_region.region_options.storage.clone();
            let index_options = compaction_region
                .current_version
                .options
                .index_options
                .clone();
            let append_mode = compaction_region.current_version.options.append_mode;
            let merge_mode = compaction_region.current_version.options.merge_mode();
            let inverted_index_config = compaction_region.engine_config.inverted_index.clone();
            let fulltext_index_config = compaction_region.engine_config.fulltext_index.clone();
            let bloom_filter_index_config =
                compaction_region.engine_config.bloom_filter_index.clone();
            let max_sequence = output
                .inputs
                .iter()
                .map(|f| f.meta_ref().sequence)
                .max()
                .flatten();
            futs.push(async move {
                let reader = CompactionSstReaderBuilder {
                    metadata: region_metadata.clone(),
                    sst_layer: sst_layer.clone(),
                    cache: cache_manager.clone(),
                    inputs: &output.inputs,
                    append_mode,
                    filter_deleted: output.filter_deleted,
                    time_range: output.output_time_range,
                    merge_mode,
                }
                .build_sst_reader()
                .await?;
                let file_meta_opt = sst_layer
                    .write_sst(
                        SstWriteRequest {
                            op_type: OperationType::Compact,
                            metadata: region_metadata,
                            source: Source::Reader(reader),
                            cache_manager,
                            storage,
                            max_sequence: max_sequence.map(NonZero::get),
                            index_options,
                            inverted_index_config,
                            fulltext_index_config,
                            bloom_filter_index_config,
                        },
                        &write_opts,
                    )
                    .await?
                    .into_iter()
                    .map(|sst_info| FileMeta {
                        region_id,
                        file_id: sst_info.file_id,
                        time_range: sst_info.time_range,
                        level: output.output_level,
                        file_size: sst_info.file_size,
                        available_indexes: sst_info.index_metadata.build_available_indexes(),
                        index_file_size: sst_info.index_metadata.file_size,
                        num_rows: sst_info.num_rows as u64,
                        num_row_groups: sst_info.num_row_groups,
                        sequence: max_sequence,
                    })
                    .collect::<Vec<_>>();
                Ok(file_meta_opt)
            });
        }
        let mut output_files = Vec::with_capacity(futs.len());
        while !futs.is_empty() {
            let mut task_chunk =
                Vec::with_capacity(crate::compaction::task::MAX_PARALLEL_COMPACTION);
            for _ in 0..crate::compaction::task::MAX_PARALLEL_COMPACTION {
                if let Some(task) = futs.pop() {
                    task_chunk.push(common_runtime::spawn_compact(task));
                }
            }
            let metas = futures::future::try_join_all(task_chunk)
                .await
                .context(JoinSnafu)?
                .into_iter()
                .collect::<Result<Vec<Vec<_>>>>()?;
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
            files_to_add: output_files,
            files_to_remove: inputs,
            compaction_time_window: Some(picker_output.time_window_size),
        })
    }

    async fn update_manifest(
        &self,
        compaction_region: &CompactionRegion,
        merge_output: MergeOutput,
    ) -> Result<RegionEdit> {
        // Write region edit to manifest.
        let edit = RegionEdit {
            files_to_add: merge_output.files_to_add,
            files_to_remove: merge_output.files_to_remove,
            compaction_time_window: merge_output
                .compaction_time_window
                .map(|seconds| Duration::from_secs(seconds as u64)),
            flushed_entry_id: None,
            flushed_sequence: None,
        };

        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        // TODO: We might leak files if we fail to update manifest. We can add a cleanup task to remove them later.
        compaction_region
            .manifest_ctx
            .update_manifest(RegionLeaderState::Writable, action_list)
            .await?;

        Ok(edit)
    }

    // The default implementation of compact combines the merge_ssts and update_manifest functions.
    // Note: It's local compaction and only used for testing purpose.
    async fn compact(
        &self,
        compaction_region: &CompactionRegion,
        compact_request_options: compact_request::Options,
    ) -> Result<()> {
        let picker_output = {
            let picker_output = new_picker(
                &compact_request_options,
                &compaction_region.region_options.compaction,
                compaction_region.region_options.append_mode,
            )
            .pick(compaction_region);

            if let Some(picker_output) = picker_output {
                picker_output
            } else {
                info!(
                    "No files to compact for region_id: {}",
                    compaction_region.region_id
                );
                return Ok(());
            }
        };

        let merge_output = self.merge_ssts(compaction_region, picker_output).await?;
        if merge_output.is_empty() {
            info!(
                "No files to compact for region_id: {}",
                compaction_region.region_id
            );
            return Ok(());
        }
        self.update_manifest(compaction_region, merge_output)
            .await?;

        Ok(())
    }
}
