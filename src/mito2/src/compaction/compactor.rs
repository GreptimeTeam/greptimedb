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
use either::Either;
use itertools::Itertools;
use object_store::manager::ObjectStoreManagerRef;
use partition::expr::PartitionExpr;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::PathType;
use store_api::storage::RegionId;

use crate::access_layer::{AccessLayer, AccessLayerRef, OperationType, SstWriteRequest, WriteType};
use crate::cache::{CacheManager, CacheManagerRef};
use crate::compaction::picker::{PickerOutput, new_picker};
use crate::compaction::{CompactionSstReaderBuilder, find_ttl};
use crate::config::MitoConfig;
use crate::error::{
    EmptyRegionDirSnafu, InvalidPartitionExprSnafu, JoinSnafu, ObjectStoreNotFoundSnafu, Result,
};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions, RemoveFileOptions};
use crate::manifest::storage::manifest_compress_type;
use crate::metrics;
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
use crate::sst::location::region_dir_from_table_dir;
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

    pub(crate) engine_config: Arc<MitoConfig>,
    pub(crate) region_metadata: RegionMetadataRef,
    pub(crate) cache_manager: CacheManagerRef,
    /// Access layer to get the table path and path type.
    pub access_layer: AccessLayerRef,
    pub(crate) manifest_ctx: Arc<ManifestContext>,
    pub(crate) current_version: CompactionVersion,
    pub(crate) file_purger: Option<Arc<LocalFilePurger>>,
    pub(crate) ttl: Option<TimeToLive>,

    /// Controls the parallelism of this compaction task. Default is 1.
    ///
    /// The parallel is inside this compaction task, not across different compaction tasks.
    /// It can be different windows of the same compaction task or something like this.
    pub max_parallelism: usize,
}

/// OpenCompactionRegionRequest represents the request to open a compaction region.
#[derive(Debug, Clone)]
pub struct OpenCompactionRegionRequest {
    pub region_id: RegionId,
    pub table_dir: String,
    pub path_type: PathType,
    pub region_options: RegionOptions,
    pub max_parallelism: usize,
}

/// Open a compaction region from a compaction request.
/// It's simple version of RegionOpener::open().
pub async fn open_compaction_region(
    req: &OpenCompactionRegionRequest,
    mito_config: &MitoConfig,
    object_store_manager: ObjectStoreManagerRef,
    ttl_provider: Either<TimeToLive, SchemaMetadataManagerRef>,
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
            mito_config.index.staging_ttl,
        )
        .await?;
        let intermediate_manager =
            IntermediateManager::init_fs(mito_config.index.aux_path.clone()).await?;

        Arc::new(AccessLayer::new(
            &req.table_dir,
            req.path_type,
            object_store.clone(),
            puffin_manager_factory,
            intermediate_manager,
        ))
    };

    let manifest_manager = {
        let region_manifest_options = RegionManifestOptions {
            manifest_dir: new_manifest_dir(&region_dir_from_table_dir(
                &req.table_dir,
                req.region_id,
                req.path_type,
            )),
            object_store: object_store.clone(),
            compress_type: manifest_compress_type(mito_config.compress_manifest),
            checkpoint_distance: mito_config.manifest_checkpoint_distance,
            remove_file_options: RemoveFileOptions {
                keep_count: mito_config.experimental_manifest_keep_removed_file_count,
                keep_ttl: mito_config.experimental_manifest_keep_removed_file_ttl,
            },
        };

        RegionManifestManager::open(
            region_manifest_options,
            Default::default(),
            Default::default(),
        )
        .await?
        .context(EmptyRegionDirSnafu {
            region_id: req.region_id,
            region_dir: &region_dir_from_table_dir(&req.table_dir, req.region_id, req.path_type),
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

    let ttl = match ttl_provider {
        // Use the specified ttl.
        Either::Left(ttl) => ttl,
        // Get the ttl from the schema metadata manager.
        Either::Right(schema_metadata_manager) => find_ttl(
            req.region_id.table_id(),
            current_version.options.ttl,
            &schema_metadata_manager,
        )
        .await
        .unwrap_or_else(|e| {
            warn!(e; "Failed to get ttl for region: {}", region_metadata.region_id);
            TimeToLive::default()
        }),
    };

    Ok(CompactionRegion {
        region_id: req.region_id,
        region_options: req.region_options.clone(),
        engine_config: Arc::new(mito_config.clone()),
        region_metadata: region_metadata.clone(),
        cache_manager: Arc::new(CacheManager::default()),
        access_layer,
        manifest_ctx,
        current_version,
        file_purger: Some(file_purger),
        ttl: Some(ttl),
        max_parallelism: req.max_parallelism,
    })
}

impl CompactionRegion {
    /// Get the file purger of the compaction region.
    pub fn file_purger(&self) -> Option<Arc<LocalFilePurger>> {
        self.file_purger.clone()
    }

    /// Stop the file purger scheduler of the compaction region.
    pub async fn stop_purger_scheduler(&self) -> Result<()> {
        if let Some(file_purger) = &self.file_purger {
            file_purger.stop_scheduler().await
        } else {
            Ok(())
        }
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

    pub fn input_file_size(&self) -> u64 {
        self.files_to_remove.iter().map(|f| f.file_size).sum()
    }

    pub fn output_file_size(&self) -> u64 {
        self.files_to_add.iter().map(|f| f.file_size).sum()
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
        let internal_parallelism = compaction_region.max_parallelism.max(1);

        for output in picker_output.outputs.drain(..) {
            compacted_inputs.extend(output.inputs.iter().map(|f| f.meta_ref().clone()));
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
            let index_config = compaction_region.engine_config.index.clone();
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
            let region_metadata_for_filemeta = region_metadata.clone();
            futs.push(async move {
                let input_file_names = output
                    .inputs
                    .iter()
                    .map(|f| f.file_id().to_string())
                    .join(",");
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
                let (sst_infos, metrics) = sst_layer
                    .write_sst(
                        SstWriteRequest {
                            op_type: OperationType::Compact,
                            metadata: region_metadata,
                            source: Source::Reader(reader),
                            cache_manager,
                            storage,
                            max_sequence: max_sequence.map(NonZero::get),
                            index_options,
                            index_config,
                            inverted_index_config,
                            fulltext_index_config,
                            bloom_filter_index_config,
                        },
                        &write_opts,
                        WriteType::Compaction,
                    )
                    .await?;
                // Convert partition expression once outside the map
                let partition_expr = match &region_metadata_for_filemeta.partition_expr {
                    None => None,
                    Some(json_str) if json_str.is_empty() => None,
                    Some(json_str) => {
                        PartitionExpr::from_json_str(json_str).with_context(|_| {
                            InvalidPartitionExprSnafu {
                                expr: json_str.clone(),
                            }
                        })?
                    }
                };

                let output_files = sst_infos
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
                        partition_expr: partition_expr.clone(),
                    })
                    .collect::<Vec<_>>();
                let output_file_names =
                    output_files.iter().map(|f| f.file_id.to_string()).join(",");
                info!(
                    "Region {} compaction inputs: [{}], outputs: [{}], metrics: {:?}",
                    region_id, input_file_names, output_file_names, metrics
                );
                metrics.observe();
                Ok(output_files)
            });
        }
        let mut output_files = Vec::with_capacity(futs.len());
        while !futs.is_empty() {
            let mut task_chunk = Vec::with_capacity(internal_parallelism);
            for _ in 0..internal_parallelism {
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
            // Use current timestamp as the edit timestamp.
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
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

        metrics::COMPACTION_INPUT_BYTES.inc_by(merge_output.input_file_size() as f64);
        metrics::COMPACTION_OUTPUT_BYTES.inc_by(merge_output.output_file_size() as f64);
        self.update_manifest(compaction_region, merge_output)
            .await?;

        Ok(())
    }
}
