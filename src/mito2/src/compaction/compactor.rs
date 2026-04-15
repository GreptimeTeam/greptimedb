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

use crate::access_layer::{
    AccessLayer, AccessLayerRef, Metrics, OperationType, SstWriteRequest, WriteType,
};
use crate::cache::{CacheManager, CacheManagerRef};
use crate::compaction::picker::PickerOutput;
use crate::compaction::{CompactionOutput, CompactionSstReaderBuilder, find_dynamic_options};
use crate::config::MitoConfig;
use crate::error;
use crate::error::{
    EmptyRegionDirSnafu, InvalidPartitionExprSnafu, ObjectStoreNotFoundSnafu, Result,
};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::read::FlatSource;
use crate::region::options::RegionOptions;
use crate::region::version::VersionRef;
use crate::region::{ManifestContext, RegionLeaderState, RegionRoleState};
use crate::schedule::scheduler::LocalScheduler;
use crate::sst::FormatType;
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
                .with_context(|| ObjectStoreNotFoundSnafu {
                    object_store: name.clone(),
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
        let region_dir = region_dir_from_table_dir(&req.table_dir, req.region_id, req.path_type);
        let region_manifest_options =
            RegionManifestOptions::new(mito_config, &region_dir, object_store);

        RegionManifestManager::open(region_manifest_options, &Default::default())
            .await?
            .with_context(|| EmptyRegionDirSnafu {
                region_id: req.region_id,
                region_dir: region_dir_from_table_dir(&req.table_dir, req.region_id, req.path_type),
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
        Either::Right(schema_metadata_manager) => {
            let (_, ttl) = find_dynamic_options(
                req.region_id.table_id(),
                &req.region_options,
                &schema_metadata_manager,
            )
            .await
            .unwrap_or_else(|e| {
                warn!(e; "Failed to get ttl for region: {}", region_metadata.region_id);
                (
                    crate::region::options::CompactionOptions::default(),
                    TimeToLive::default(),
                )
            });
            ttl
        }
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
}

/// Trait for merging a single compaction output into SST files.
///
/// This is extracted from `DefaultCompactor` to allow injecting mock
/// implementations in tests.
#[async_trait::async_trait]
pub trait SstMerger: Send + Sync + 'static {
    async fn merge_single_output(
        &self,
        compaction_region: CompactionRegion,
        output: CompactionOutput,
        write_opts: WriteOptions,
    ) -> Result<Vec<FileMeta>>;
}

/// The production [`SstMerger`] that reads, merges, and writes SST files.
#[derive(Clone)]
pub struct DefaultSstMerger;

#[async_trait::async_trait]
impl SstMerger for DefaultSstMerger {
    async fn merge_single_output(
        &self,
        compaction_region: CompactionRegion,
        output: CompactionOutput,
        write_opts: WriteOptions,
    ) -> Result<Vec<FileMeta>> {
        let region_id = compaction_region.region_id;
        let storage = compaction_region.region_options.storage.clone();
        let index_options = compaction_region
            .current_version
            .options
            .index_options
            .clone();
        let append_mode = compaction_region.current_version.options.append_mode;
        let merge_mode = compaction_region.current_version.options.merge_mode();
        let flat_format = compaction_region
            .region_options
            .sst_format
            .map(|format| format == FormatType::Flat)
            .unwrap_or(compaction_region.engine_config.default_flat_format);

        let index_config = compaction_region.engine_config.index.clone();
        let inverted_index_config = compaction_region.engine_config.inverted_index.clone();
        let fulltext_index_config = compaction_region.engine_config.fulltext_index.clone();
        let bloom_filter_index_config = compaction_region.engine_config.bloom_filter_index.clone();
        #[cfg(feature = "vector_index")]
        let vector_index_config = compaction_region.engine_config.vector_index.clone();

        let input_file_names = output
            .inputs
            .iter()
            .map(|f| f.file_id().to_string())
            .join(",");
        let max_sequence = output
            .inputs
            .iter()
            .map(|f| f.meta_ref().sequence)
            .max()
            .flatten();
        let builder = CompactionSstReaderBuilder {
            metadata: compaction_region.region_metadata.clone(),
            sst_layer: compaction_region.access_layer.clone(),
            cache: compaction_region.cache_manager.clone(),
            inputs: &output.inputs,
            append_mode,
            filter_deleted: output.filter_deleted,
            time_range: output.output_time_range,
            merge_mode,
        };
        let reader = builder.build_flat_sst_reader().await?;
        let source = FlatSource::Stream(reader);
        let mut metrics = Metrics::new(WriteType::Compaction);
        let region_metadata = compaction_region.region_metadata.clone();
        let sst_infos = compaction_region
            .access_layer
            .write_sst(
                SstWriteRequest {
                    op_type: OperationType::Compact,
                    metadata: region_metadata.clone(),
                    source,
                    cache_manager: compaction_region.cache_manager.clone(),
                    storage,
                    max_sequence: max_sequence.map(NonZero::get),
                    sst_write_format: if flat_format {
                        FormatType::Flat
                    } else {
                        FormatType::PrimaryKey
                    },
                    index_options,
                    index_config,
                    inverted_index_config,
                    fulltext_index_config,
                    bloom_filter_index_config,
                    #[cfg(feature = "vector_index")]
                    vector_index_config,
                },
                &write_opts,
                &mut metrics,
            )
            .await?;
        // Convert partition expression once outside the map
        let partition_expr = match &region_metadata.partition_expr {
            None => None,
            Some(json_str) if json_str.is_empty() => None,
            Some(json_str) => PartitionExpr::from_json_str(json_str).with_context(|_| {
                InvalidPartitionExprSnafu {
                    expr: json_str.clone(),
                }
            })?,
        };

        let output_files = sst_infos
            .into_iter()
            .map(|sst_info| FileMeta {
                region_id,
                file_id: sst_info.file_id,
                time_range: sst_info.time_range,
                level: output.output_level,
                file_size: sst_info.file_size,
                max_row_group_uncompressed_size: sst_info.max_row_group_uncompressed_size,
                available_indexes: sst_info.index_metadata.build_available_indexes(),
                indexes: sst_info.index_metadata.build_indexes(),
                index_file_size: sst_info.index_metadata.file_size,
                index_version: 0,
                num_rows: sst_info.num_rows as u64,
                num_row_groups: sst_info.num_row_groups,
                sequence: max_sequence,
                partition_expr: partition_expr.clone(),
                num_series: sst_info.num_series,
            })
            .collect::<Vec<_>>();
        let output_file_names = output_files.iter().map(|f| f.file_id.to_string()).join(",");
        info!(
            "Region {} compaction inputs: [{}], outputs: [{}], flat_format: {}, metrics: {:?}",
            region_id, input_file_names, output_file_names, flat_format, metrics
        );
        metrics.observe();
        Ok(output_files)
    }
}

/// DefaultCompactor is the default implementation of Compactor.
///
/// It is parameterized by an [`SstMerger`] to allow injecting mock
/// implementations in tests.
pub struct DefaultCompactor<M = DefaultSstMerger> {
    merger: M,
}

impl Default for DefaultCompactor {
    fn default() -> Self {
        Self {
            merger: DefaultSstMerger,
        }
    }
}

impl<M: SstMerger> DefaultCompactor<M> {
    pub fn with_merger(merger: M) -> Self {
        Self { merger }
    }
}

#[async_trait::async_trait]
impl<M: SstMerger> Compactor for DefaultCompactor<M>
where
    M: Clone,
{
    async fn merge_ssts(
        &self,
        compaction_region: &CompactionRegion,
        mut picker_output: PickerOutput,
    ) -> Result<MergeOutput> {
        let internal_parallelism = compaction_region.max_parallelism.max(1);
        let compaction_time_window = picker_output.time_window_size;
        let region_id = compaction_region.region_id;

        // Build tasks along with their input file metas so we can track which
        // inputs correspond to each task.
        let mut tasks: Vec<(Vec<FileMeta>, _)> = Vec::with_capacity(picker_output.outputs.len());

        for output in picker_output.outputs.drain(..) {
            let inputs_to_remove: Vec<_> =
                output.inputs.iter().map(|f| f.meta_ref().clone()).collect();
            let write_opts = WriteOptions {
                write_buffer_size: compaction_region.engine_config.sst_write_buffer_size,
                max_file_size: picker_output.max_file_size,
                ..Default::default()
            };
            let merger = self.merger.clone();
            let compaction_region = compaction_region.clone();
            let fut = async move {
                merger
                    .merge_single_output(compaction_region, output, write_opts)
                    .await
            };
            tasks.push((inputs_to_remove, fut));
        }

        let mut output_files = Vec::with_capacity(tasks.len());
        let mut compacted_inputs = Vec::with_capacity(
            tasks.iter().map(|(inputs, _)| inputs.len()).sum::<usize>()
                + picker_output.expired_ssts.len(),
        );

        while !tasks.is_empty() {
            let mut chunk: Vec<(Vec<FileMeta>, _)> = Vec::with_capacity(internal_parallelism);
            for _ in 0..internal_parallelism {
                if let Some(task) = tasks.pop() {
                    chunk.push(task);
                }
            }
            let spawned: Vec<_> = chunk
                .into_iter()
                .map(|(inputs, fut)| {
                    let handle = common_runtime::spawn_compact(fut);
                    (inputs, handle)
                })
                .collect();

            for (inputs, handle) in spawned {
                match handle.await {
                    Ok(Ok(files)) => {
                        output_files.extend(files);
                        compacted_inputs.extend(inputs);
                    }
                    Ok(Err(e)) => {
                        warn!(
                            e; "Region {} failed to merge compaction output with inputs: [{}], skipping",
                            region_id,
                            inputs.iter().map(|f| f.file_id.to_string()).join(",")
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Region {} compaction task join error for inputs: [{}], skipping: {}",
                            region_id,
                            inputs.iter().map(|f| f.file_id.to_string()).join(","),
                            e
                        );
                        return Err(e).context(error::JoinSnafu);
                    }
                }
            }
        }

        // Include expired SSTs in removals — these don't depend on merge success.
        compacted_inputs.extend(
            picker_output
                .expired_ssts
                .iter()
                .map(|f| f.meta_ref().clone()),
        );

        Ok(MergeOutput {
            files_to_add: output_files,
            files_to_remove: compacted_inputs,
            compaction_time_window: Some(compaction_time_window),
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
            committed_sequence: None,
        };

        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        // TODO: We might leak files if we fail to update manifest. We can add a cleanup task to remove them later.
        compaction_region
            .manifest_ctx
            .update_manifest(RegionLeaderState::Writable, action_list, false)
            .await?;

        Ok(edit)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use store_api::storage::{FileId, RegionId};

    use super::*;
    use crate::cache::CacheManager;
    use crate::compaction::picker::PickerOutput;
    use crate::sst::file::FileHandle;
    use crate::sst::file_purger::NoopFilePurger;
    use crate::sst::version::SstVersion;
    use crate::test_util::memtable_util::metadata_for_test;
    use crate::test_util::scheduler_util::SchedulerEnv;

    fn dummy_file_meta() -> FileMeta {
        FileMeta {
            region_id: RegionId::new(1, 1),
            file_id: FileId::random(),
            file_size: 100,
            ..Default::default()
        }
    }

    fn new_file_handle(meta: FileMeta) -> FileHandle {
        FileHandle::new(meta, Arc::new(NoopFilePurger))
    }

    /// Build a minimal [`CompactionRegion`] suitable for tests where the
    /// [`SstMerger`] is mocked and never touches the access layer.
    async fn new_test_compaction_region() -> CompactionRegion {
        let env = SchedulerEnv::new().await;
        let metadata = metadata_for_test();
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        CompactionRegion {
            region_id: RegionId::new(1, 1),
            region_options: RegionOptions::default(),
            engine_config: Arc::new(MitoConfig::default()),
            region_metadata: metadata.clone(),
            cache_manager: Arc::new(CacheManager::default()),
            access_layer: env.access_layer.clone(),
            manifest_ctx,
            current_version: CompactionVersion {
                metadata,
                options: RegionOptions::default(),
                ssts: Arc::new(SstVersion::new()),
                compaction_time_window: None,
            },
            file_purger: None,
            ttl: None,
            max_parallelism: 1,
        }
    }

    /// An [`SstMerger`] that returns pre-configured results per call index.
    ///
    /// Call 0 gets `results[0]`, call 1 gets `results[1]`, etc.
    #[derive(Clone)]
    struct MockMerger {
        results: Arc<Mutex<Vec<Result<Vec<FileMeta>>>>>,
        call_idx: Arc<AtomicUsize>,
    }

    impl MockMerger {
        fn new(results: Vec<Result<Vec<FileMeta>>>) -> Self {
            Self {
                results: Arc::new(Mutex::new(results)),
                call_idx: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    #[async_trait::async_trait]
    impl SstMerger for MockMerger {
        async fn merge_single_output(
            &self,
            _compaction_region: CompactionRegion,
            _output: CompactionOutput,
            _write_opts: WriteOptions,
        ) -> Result<Vec<FileMeta>> {
            let idx = self.call_idx.fetch_add(1, Ordering::SeqCst);
            match self.results.lock().unwrap().get(idx) {
                Some(Ok(files)) => Ok(files.clone()),
                Some(Err(_)) => error::InvalidMetaSnafu {
                    reason: format!("simulated failure at index {idx}"),
                }
                .fail(),
                None => panic!("MockMerger: no result configured for call index {idx}"),
            }
        }
    }

    #[tokio::test]
    async fn test_partial_merge_failure_collects_only_successful_outputs() {
        common_telemetry::init_default_ut_logging();

        let compaction_region = new_test_compaction_region().await;

        // Prepare 3 compaction outputs: output 0 and 2 succeed, output 1 fails.
        let input_meta_0 = dummy_file_meta();
        let input_meta_1 = dummy_file_meta();
        let input_meta_2 = dummy_file_meta();

        let output_meta_0 = vec![dummy_file_meta()];
        let output_meta_2 = vec![dummy_file_meta(), dummy_file_meta()];

        let merger = MockMerger::new(vec![
            Ok(output_meta_0.clone()),
            Err(error::InvalidMetaSnafu {
                reason: "boom".to_string(),
            }
            .build()),
            Ok(output_meta_2.clone()),
        ]);
        let compactor = DefaultCompactor::with_merger(merger);

        let picker_output = PickerOutput {
            outputs: vec![
                CompactionOutput {
                    output_level: 1,
                    inputs: vec![new_file_handle(input_meta_0.clone())],
                    filter_deleted: false,
                    output_time_range: None,
                },
                CompactionOutput {
                    output_level: 1,
                    inputs: vec![new_file_handle(input_meta_1.clone())],
                    filter_deleted: false,
                    output_time_range: None,
                },
                CompactionOutput {
                    output_level: 1,
                    inputs: vec![new_file_handle(input_meta_2.clone())],
                    filter_deleted: false,
                    output_time_range: None,
                },
            ],
            expired_ssts: vec![],
            time_window_size: 3600,
            max_file_size: None,
        };

        let merge_output = compactor
            .merge_ssts(&compaction_region, picker_output)
            .await
            .unwrap();

        // Outputs 0 and 2 succeeded (1 + 2 = 3 files added).
        assert_eq!(merge_output.files_to_add.len(), 3);
        // Only inputs from successful merges should be removed.
        assert_eq!(merge_output.files_to_remove.len(), 2);

        let removed_ids: Vec<_> = merge_output
            .files_to_remove
            .iter()
            .map(|f| f.file_id)
            .collect();
        assert!(removed_ids.contains(&input_meta_0.file_id));
        assert!(removed_ids.contains(&input_meta_2.file_id));
        // The failed output's input must NOT be removed.
        assert!(!removed_ids.contains(&input_meta_1.file_id));
    }

    #[tokio::test]
    async fn test_all_outputs_succeed() {
        common_telemetry::init_default_ut_logging();

        let compaction_region = new_test_compaction_region().await;
        let input_meta = dummy_file_meta();
        let output_meta = vec![dummy_file_meta()];

        let merger = MockMerger::new(vec![Ok(output_meta.clone())]);
        let compactor = DefaultCompactor::with_merger(merger);

        let picker_output = PickerOutput {
            outputs: vec![CompactionOutput {
                output_level: 1,
                inputs: vec![new_file_handle(input_meta.clone())],
                filter_deleted: false,
                output_time_range: None,
            }],
            expired_ssts: vec![],
            time_window_size: 3600,
            max_file_size: None,
        };

        let merge_output = compactor
            .merge_ssts(&compaction_region, picker_output)
            .await
            .unwrap();

        assert_eq!(merge_output.files_to_add.len(), 1);
        assert_eq!(merge_output.files_to_add[0].file_id, output_meta[0].file_id);
        assert_eq!(merge_output.files_to_remove.len(), 1);
        assert_eq!(merge_output.files_to_remove[0].file_id, input_meta.file_id);
    }

    #[tokio::test]
    async fn test_expired_ssts_always_removed() {
        common_telemetry::init_default_ut_logging();

        let compaction_region = new_test_compaction_region().await;
        let input_meta = dummy_file_meta();
        let expired_meta = dummy_file_meta();

        // The single merge output fails, but expired SSTs should still be removed.
        let merger = MockMerger::new(vec![Err(error::InvalidMetaSnafu {
            reason: "fail".to_string(),
        }
        .build())]);
        let compactor = DefaultCompactor::with_merger(merger);

        let picker_output = PickerOutput {
            outputs: vec![CompactionOutput {
                output_level: 1,
                inputs: vec![new_file_handle(input_meta.clone())],
                filter_deleted: false,
                output_time_range: None,
            }],
            expired_ssts: vec![new_file_handle(expired_meta.clone())],
            time_window_size: 3600,
            max_file_size: None,
        };

        let merge_output = compactor
            .merge_ssts(&compaction_region, picker_output)
            .await
            .unwrap();

        // No files added (merge failed).
        assert!(merge_output.files_to_add.is_empty());
        // Only the expired SST should be in files_to_remove (not the failed merge's input).
        assert_eq!(merge_output.files_to_remove.len(), 1);
        assert_eq!(
            merge_output.files_to_remove[0].file_id,
            expired_meta.file_id
        );
    }
}
