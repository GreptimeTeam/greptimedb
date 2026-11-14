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

pub(crate) mod bloom_filter;
pub mod build;
mod column;
#[cfg(test)]
mod column_test;
pub(crate) mod fulltext_index;
pub mod handler;
mod indexer;
pub mod intermediate;
pub(crate) mod inverted_index;
pub mod kind;
mod primary_key;
pub mod puffin_manager;
pub mod registry;
#[cfg(test)]
mod sparse_test;
mod statistics;
pub(crate) mod store;

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

use bytes::Bytes;
use common_error::ext::BoxedError;
use common_telemetry::{debug, error, info, warn};
use datatypes::arrow::record_batch::RecordBatch;
use index::target::IndexTarget;
use mito_codec::row_converter::DensePrimaryKeyCodec;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use puffin_manager::SstPuffinManager;
use smallvec::SmallVec;
use snafu::{IntoError, ResultExt};
#[cfg(feature = "enterprise")]
pub use statistics::{Statistics, TimerGuard};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, FileId, RegionId};
use strum::IntoStaticStr;
use tokio::sync::mpsc::Sender;

use crate::access_layer::{AccessLayerRef, FilePathProvider, OperationType, RegionFilePathFactory};
use crate::batch_size::{estimate_batch_size, largest_row_group_statistic};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::write_cache::{UploadOptions, UploadTracker, WriteCacheRef};
use crate::cache::{CacheManagerRef, CacheStrategy};
#[cfg(test)]
use crate::config::{BloomFilterConfig, InvertedIndexConfig};
use crate::error::{
    BuildIndexAsyncSnafu, Error, ExternalSnafu, InvalidConfigSnafu, JoinSnafu, RegionClosedSnafu,
    RegionDroppedSnafu, RegionTruncatedSnafu, Result,
};
use crate::metrics::{
    INDEX_ARTIFACT_CLEANUP_FAILURE_TOTAL, INDEX_CREATE_DISCARDED_TOTAL, INDEX_CREATE_MEMORY_USAGE,
    INDEX_DISCARD_MEMORY_BUDGET_EXCEEDED, INDEX_PUBLICATION_STALE_TOTAL,
};
use crate::read::Batch;
use crate::region::options::IndexOptions;
use crate::region::version::VersionControlRef;
use crate::region::{
    IndexBuildSource, IndexPublication, IndexPublicationStale, ManifestContextRef,
};
use crate::request::{
    BackgroundNotify, BuildIndexRequest, IndexBuildFinished, IndexBuildStopped, WorkerRequest,
    WorkerRequestWithTime,
};
use crate::schedule::scheduler::{Job, SchedulerRef};
use crate::sst::file::{
    ColumnIndexMetadata, FileHandle, IndexType, IndexTypes, RegionFileId, RegionIndexId,
};
use crate::sst::file_purger::FilePurgerRef;
#[cfg(feature = "testing")]
use crate::sst::index::bloom_filter::creator::BloomFilterIndexer;
#[cfg(test)]
use crate::sst::index::fulltext_index::FULLTEXT_BLOOM_VERSION;
use crate::sst::index::intermediate::IntermediateManager;
#[cfg(feature = "testing")]
use crate::sst::index::inverted_index::creator::InvertedIndexer;
use crate::sst::parquet::SstInfo;
use crate::worker::WorkerListener;

pub(crate) const TYPE_EXTENSION_INDEX: &str = "extension";
pub(crate) const TYPE_INVERTED_INDEX: &str = kind::IndexKind::Inverted.as_str();
pub(crate) const TYPE_FULLTEXT_INDEX: &str = kind::IndexKind::Fulltext.as_str();
pub(crate) const TYPE_BLOOM_FILTER_INDEX: &str = kind::IndexKind::Bloom.as_str();

/// Evicts local state for a stale index artifact.
///
/// The remote artifact may already be referenced by another region manifest
/// because repartitioned regions share physical SST and index paths. With GC
/// enabled, remote deletion therefore stays in the reference-aware GC lifecycle.
pub(crate) async fn cleanup_stale_index_caches(
    index_id: RegionIndexId,
    access_layer: &AccessLayerRef,
    cache_manager: Option<&CacheManagerRef>,
    write_cache: Option<&WriteCacheRef>,
) {
    if let Some(cache_manager) = cache_manager {
        CacheStrategy::EnableAll(cache_manager.clone())
            .evict_puffin_cache(index_id)
            .await;
    }
    if let Some(write_cache) = write_cache {
        write_cache
            .remove(IndexKey::new(
                index_id.region_id(),
                index_id.file_id(),
                FileType::Puffin(index_id.version),
            ))
            .await;
    }
    if let Err(e) = access_layer
        .puffin_manager_factory()
        .purge_stager(index_id)
        .await
    {
        INDEX_ARTIFACT_CLEANUP_FAILURE_TOTAL.inc();
        warn!(
            e;
            "Failed to purge stale index artifact from stager, region: {}, file_id: {}, index_version: {}",
            index_id.region_id(),
            index_id.file_id(),
            index_id.version
        );
    }
}

/// Triggers background download of an index file to the local cache.
pub(crate) fn trigger_index_background_download(
    file_cache: Option<&FileCacheRef>,
    file_id: &RegionIndexId,
    file_size_hint: Option<u64>,
    path_factory: &RegionFilePathFactory,
    object_store: &ObjectStore,
) {
    if let (Some(file_cache), Some(file_size)) = (file_cache, file_size_hint) {
        let index_key = IndexKey::new(
            file_id.region_id(),
            file_id.file_id(),
            FileType::Puffin(file_id.version),
        );
        let remote_path = path_factory.build_index_file_path(file_id.file_id);
        file_cache.maybe_download_background(
            index_key,
            remote_path,
            object_store.clone(),
            file_size,
        );
    }
}

/// One index successfully written into the enclosing Puffin file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedEntry {
    /// Indexed column. `None` is reserved for file-level built-in indexes, such as
    /// JSON-path-only indexes; these appear only in `available_indexes`.
    pub column_id: Option<ColumnId>,
    /// Registered index identity.
    pub kind: crate::sst::index::kind::IndexKind,
    /// Fulltext bloom version published in column metadata. Extension format
    /// versions belong to their Puffin blob properties and are not published here.
    pub format_version: u32,
}

impl CompletedEntry {
    fn fulltext_bloom_version(&self) -> Result<u8> {
        u8::try_from(self.format_version).map_err(|_| {
            InvalidConfigSnafu {
                reason: format!("fulltext bloom version {} exceeds u8", self.format_version),
            }
            .build()
        })
    }

    fn validate(&self) -> Result<()> {
        if self.column_id.is_none() && !self.kind.is_builtin() {
            return InvalidConfigSnafu {
                reason: "extension index completion requires a column id".to_string(),
            }
            .fail();
        }
        if self.kind.index_type() == IndexType::FulltextIndex {
            self.fulltext_bloom_version()?;
        }
        Ok(())
    }
}

/// Output published only after the enclosing Puffin file finishes.
#[derive(Debug, Clone, Default)]
pub struct IndexOutput {
    /// Size of the file.
    pub file_size: u64,
    /// Index version.
    pub version: u64,
    /// Successfully written column indexes.
    pub entries: Vec<CompletedEntry>,
}

impl IndexOutput {
    pub fn build_available_indexes(&self) -> IndexTypes {
        let mut kinds = SmallVec::<[kind::IndexKind; 6]>::new();
        for entry in &self.entries {
            if !kinds.contains(&entry.kind) {
                kinds.push(entry.kind);
            }
        }
        kinds.sort_unstable();
        kinds.into_iter().map(kind::IndexKind::index_type).collect()
    }

    /// Builds column metadata, rejecting values that cannot be represented on disk.
    pub fn build_indexes(&self) -> Result<Vec<ColumnIndexMetadata>> {
        let mut map: BTreeMap<ColumnId, ColumnIndexMetadata> = BTreeMap::new();
        for kind in self.build_available_indexes() {
            for entry in self
                .entries
                .iter()
                .filter(|entry| entry.kind.index_type() == kind)
            {
                let Some(column_id) = entry.column_id else {
                    continue;
                };
                let metadata = map.entry(column_id).or_default();
                metadata.column_id = column_id;
                metadata.created_indexes.push(kind);
                if kind == IndexType::FulltextIndex {
                    metadata.fulltext_bloom_version = entry.fulltext_bloom_version()?;
                }
            }
        }
        Ok(map.into_values().collect())
    }
}

/// Executes planned creators through the existing Puffin lifecycle.
#[derive(Default)]
pub struct Indexer {
    file_id: FileId,
    region_id: RegionId,
    physical_region_id: RegionId,
    index_version: u64,
    puffin_manager: Option<SstPuffinManager>,
    write_cache_enabled: bool,
    creators: Vec<ActiveIndex>,
    /// Shared retained-memory ceiling for all injected creators.
    extension_memory_limit: Option<usize>,
    dense_pk_decoder: Option<DensePrimaryKeyCodec>,
    intermediate_manager: Option<IntermediateManager>,
    /// First failure retained even if best-effort execution discards a creator.
    failure: Option<Arc<Error>>,
    /// Optional index kinds dropped by the operation's resource policy.
    discarded: Vec<kind::IndexKind>,
}

impl Indexer {
    /// Reports failures even when surviving creators produced a partial result.
    pub fn check_success(&self) -> Result<(), Arc<Error>> {
        self.failure.clone().map_or(Ok(()), Err)
    }

    /// Index kinds intentionally discarded because their memory budget was exceeded.
    pub fn discarded(&self) -> &[kind::IndexKind] {
        &self.discarded
    }

    /// Whether any active creator requires decoded rows.
    pub fn requires_row_scan(&self) -> bool {
        self.creators
            .iter()
            .any(|active| active.creator.requires_row_scan())
    }

    /// Returns true if no index creator is active.
    pub(crate) fn is_empty(&self) -> bool {
        self.creators.is_empty()
    }

    #[cfg(test)]
    fn has_kind(&self, kind: kind::IndexKind) -> bool {
        self.creators
            .iter()
            .any(|active| active.kinds.contains(&kind))
    }

    fn push_creator(
        &mut self,
        kinds: Vec<kind::IndexKind>,
        creator: Box<dyn handler::IndexCreator>,
        memory_limit: Option<usize>,
        label: &'static str,
    ) {
        self.creators.push(ActiveIndex {
            kinds,
            creator,
            memory_limit,
            memory: IndexMemoryUsage { label, bytes: 0 },
        });
    }

    /// Wraps real creators for update-only benchmarks without a Puffin output.
    #[cfg(feature = "testing")]
    pub fn for_bench(
        metadata: &RegionMetadataRef,
        inverted_indexer: Option<InvertedIndexer>,
        bloom_filter_indexer: Option<BloomFilterIndexer>,
    ) -> Self {
        let mut indexer = Self {
            region_id: metadata.region_id,
            ..Default::default()
        };
        if let Some(creator) = inverted_indexer {
            indexer.push_creator(
                vec![kind::IndexKind::Inverted],
                Box::new(creator),
                None,
                TYPE_INVERTED_INDEX,
            );
        }
        if let Some(creator) = bloom_filter_indexer {
            indexer.push_creator(
                vec![kind::IndexKind::Bloom],
                Box::new(creator),
                None,
                TYPE_BLOOM_FILTER_INDEX,
            );
        }
        indexer.prepare_dense_pk_decoder(metadata);
        indexer
    }

    fn prepare_dense_pk_decoder(&mut self, metadata: &RegionMetadataRef) {
        if metadata.primary_key_encoding != PrimaryKeyEncoding::Dense
            || metadata.primary_key.is_empty()
        {
            return;
        }
        let requested: HashSet<_> = self
            .creators
            .iter()
            .filter(|active| active.creator.needs_dense_pk())
            .flat_map(|active| active.creator.indexed_targets())
            .filter_map(|target| match target {
                IndexTarget::ColumnId(id) => Some(id),
                IndexTarget::JsonPath(_) => None,
            })
            .collect();
        if metadata.primary_key.iter().all(|id| requested.contains(id)) {
            self.dense_pk_decoder = Some(DensePrimaryKeyCodec::new(metadata));
        }
    }

    fn prepare_primary_key(&self, batch: &mut Batch) -> Result<()> {
        if let Some(codec) = &self.dense_pk_decoder {
            batch.ensure_dense_pk_decoded(codec)?;
        }
        Ok(())
    }

    /// Updates creators that require rows using the borrowed batch.
    pub async fn update(&mut self, batch: &mut Batch) {
        self.do_update(batch).await;
        self.enforce_memory_limits().await;
        self.flush_mem_metrics();
    }

    /// Updates creators that require rows using the borrowed flat batch.
    pub async fn update_flat(&mut self, batch: &RecordBatch) {
        self.do_update_flat(batch).await;
        self.enforce_memory_limits().await;
        self.flush_mem_metrics();
    }

    /// Finalizes the index creation.
    pub async fn finish(&mut self) -> IndexOutput {
        let output = self.do_finish().await;
        self.flush_mem_metrics();
        output
    }

    /// Aborts the index creation.
    pub async fn abort(&mut self) {
        self.do_abort().await;
        self.flush_mem_metrics();
    }

    fn record_failure(&mut self, source: BoxedError) {
        self.failure.get_or_insert_with(|| {
            Arc::new(
                ExternalSnafu {
                    context: "index creation".to_string(),
                }
                .into_error(source),
            )
        });
    }

    async fn enforce_memory_limits(&mut self) {
        let mut position = 0;
        let mut extension_bytes = 0usize;
        while position < self.creators.len() {
            let active = &self.creators[position];
            if active.memory_limit.is_none()
                && (active.is_builtin() || self.extension_memory_limit.is_none())
            {
                position += 1;
                continue;
            }
            let bytes = active.creator.memory_usage();
            let retained = extension_bytes.saturating_add(bytes);
            let exceeds_shared_limit = !active.is_builtin()
                && self
                    .extension_memory_limit
                    .is_some_and(|limit| retained > limit);
            if exceeds_shared_limit || active.memory_limit.is_some_and(|limit| bytes > limit) {
                warn!(
                    "Discarding index exceeding memory budget, region_id: {}, file_id: {}, kinds: {:?}",
                    self.region_id, self.file_id, active.kinds
                );
                for kind in &active.kinds {
                    INDEX_CREATE_DISCARDED_TOTAL
                        .with_label_values(&[kind.as_str(), INDEX_DISCARD_MEMORY_BUDGET_EXCEEDED])
                        .inc();
                    if !self.discarded.contains(kind) {
                        self.discarded.push(*kind);
                    }
                }
                self.abort_creator(position).await;
            } else {
                if !active.is_builtin() {
                    extension_bytes = retained;
                }
                position += 1;
            }
        }
    }

    fn flush_mem_metrics(&mut self) {
        for active in &mut self.creators {
            let bytes = active.creator.memory_usage();
            INDEX_CREATE_MEMORY_USAGE
                .with_label_values(&[active.memory.label])
                .add(bytes as i64 - active.memory.bytes as i64);
            active.memory.bytes = bytes;
        }
    }
}

/// One handler's active creator and its shared memory accounting.
struct ActiveIndex {
    kinds: Vec<kind::IndexKind>,
    creator: Box<dyn handler::IndexCreator>,
    memory_limit: Option<usize>,
    memory: IndexMemoryUsage,
}

impl ActiveIndex {
    fn is_builtin(&self) -> bool {
        self.kinds.iter().all(|kind| kind.is_builtin())
    }
}

/// Releases each creator's gauge contribution on completion or cancellation.
struct IndexMemoryUsage {
    label: &'static str,
    bytes: usize,
}

impl Drop for IndexMemoryUsage {
    fn drop(&mut self) {
        INDEX_CREATE_MEMORY_USAGE
            .with_label_values(&[self.label])
            .sub(self.bytes as i64);
    }
}

#[async_trait::async_trait]
pub trait IndexerBuilder {
    /// Builds an indexer for the physical SST file.
    async fn build(
        &self,
        region_file_id: RegionFileId,
        index_version: u64,
        row_group_size: Option<usize>,
    ) -> Indexer;
}
#[derive(Clone)]
pub(crate) struct IndexerBuilderImpl {
    pub(crate) index_build_inputs: Option<build::IndexBuildInputs>,
    pub(crate) registry: registry::IndexRegistryRef,
    pub(crate) build_type: IndexBuildType,
    pub(crate) metadata: RegionMetadataRef,
    pub(crate) puffin_manager: SstPuffinManager,
    pub(crate) write_cache_enabled: bool,
    pub(crate) intermediate_manager: IntermediateManager,
    pub(crate) index_options: IndexOptions,
}

#[async_trait::async_trait]
impl IndexerBuilder for IndexerBuilderImpl {
    /// Sanity check for arguments and create a new [Indexer] if arguments are valid.
    async fn build(
        &self,
        region_file_id: RegionFileId,
        index_version: u64,
        row_group_size: Option<usize>,
    ) -> Indexer {
        let mut indexer = Indexer {
            file_id: region_file_id.file_id(),
            region_id: self.metadata.region_id,
            physical_region_id: region_file_id.region_id(),
            index_version,
            write_cache_enabled: self.write_cache_enabled,
            ..Default::default()
        };

        let plan = if let Some(inputs) = &self.index_build_inputs {
            inputs.plan.clone()
        } else {
            let context = handler::PlanContext {
                metadata: &self.metadata,
                index_options: &self.index_options,
                build_type: self.build_type.clone(),
                existing_indexes: &[],
            };
            Arc::new(self.registry.plan(&context))
        };
        indexer.failure = plan.check_success().err();
        let mut context = handler::BuildContext {
            metadata: &self.metadata,
            output: RegionIndexId::new(region_file_id, index_version),
            puffin_manager: &self.puffin_manager,
            intermediate_manager: &self.intermediate_manager,
            inputs: self
                .index_build_inputs
                .as_ref()
                .map_or(&[], |inputs| &inputs.inputs),
            input_puffin_manager: self
                .index_build_inputs
                .as_ref()
                .map_or(&self.puffin_manager, |inputs| &inputs.input_puffin_manager),
            can_merge_inputs: self
                .index_build_inputs
                .as_ref()
                .is_some_and(|inputs| inputs.can_merge_inputs),
            memory_limit: self
                .index_build_inputs
                .as_ref()
                .and_then(|inputs| inputs.memory_limit),
            row_group_size,
            output_rows: self
                .index_build_inputs
                .as_ref()
                .and_then(|inputs| inputs.output_rows),
        };
        indexer.extension_memory_limit = context.memory_limit;
        for planned in plan.planned() {
            let Some(first_kind) = planned.kinds().first() else {
                let err = BoxedError::new(
                    InvalidConfigSnafu {
                        reason: "index plan must own at least one kind",
                    }
                    .build(),
                );
                warn!(err; "Failed to create index, region_id: {}", self.metadata.region_id);
                indexer.record_failure(err);
                continue;
            };
            let builtin = planned.kinds().iter().all(|kind| kind.is_builtin());
            // Optional creators share the operation allocation. Pass only the
            // remaining retained budget before allowing another creator to build.
            context.memory_limit = if builtin {
                indexer.extension_memory_limit
            } else {
                let remaining = indexer.extension_memory_limit.map(|limit| {
                    let retained = indexer
                        .creators
                        .iter()
                        .filter(|active| !active.is_builtin())
                        .fold(0usize, |bytes, active| {
                            bytes.saturating_add(active.creator.memory_usage())
                        });
                    limit.saturating_sub(retained)
                });
                remaining.into_iter().chain(planned.memory_limit()).min()
            };
            match planned.build(&context).await {
                Ok(Some(creator)) => {
                    let label = if builtin {
                        first_kind.as_str()
                    } else {
                        TYPE_EXTENSION_INDEX
                    };
                    let limit = if builtin { None } else { context.memory_limit };
                    indexer.push_creator(planned.kinds().to_vec(), creator, limit, label);
                    if !builtin {
                        indexer.enforce_memory_limits().await;
                    }
                }
                Ok(None) => {}
                Err(err) => {
                    if planned.kinds().iter().all(|kind| kind.is_builtin())
                        && cfg!(any(test, feature = "test"))
                    {
                        panic!("Failed to create index: {err:?}");
                    }
                    warn!(err; "Failed to create index, region_id: {}, kinds: {:?}", self.metadata.region_id, planned.kinds());
                    indexer.record_failure(err);
                }
            }
        }
        indexer.prepare_dense_pk_decoder(&self.metadata);
        indexer.intermediate_manager = Some(self.intermediate_manager.clone());

        if indexer.is_empty() {
            indexer.abort().await;
            return indexer;
        }

        indexer.puffin_manager = Some(self.puffin_manager.clone());
        indexer
    }
}

/// Builds the indexes of an SST from its encoded parquet bytes.
///
/// Encoded bulk parts are flushed without passing through [ParquetWriter], so their indexes
/// are built from the bytes still in memory instead of reading the uploaded SST back. Like
/// the writer, a failure only drops the index and leaves the SST unindexed.
///
/// [ParquetWriter]: crate::sst::parquet::writer::ParquetWriter
pub(crate) async fn build_index_from_parquet_bytes(
    builder: &IndexerBuilderImpl,
    region_file_id: RegionFileId,
    data: &Bytes,
) -> IndexOutput {
    let reader_builder = match ParquetRecordBatchReaderBuilder::try_new(data.clone()) {
        Ok(reader_builder) => reader_builder,
        Err(e) => {
            warn!(e; "Failed to read encoded SST {:?} for index creation", region_file_id);
            return IndexOutput::default();
        }
    };
    let row_group_size = reader_builder
        .metadata()
        .row_groups()
        .first()
        .map(|row_group| row_group.num_rows() as usize)
        .filter(|size| *size > 0);
    let mut indexer = builder.build(region_file_id, 0, row_group_size).await;
    if indexer.is_empty() {
        return IndexOutput::default();
    }
    // Follow the batch size the flush reads use, so wide rows are not decoded 8192 at a time.
    let batch_size = estimate_batch_size(largest_row_group_statistic(reader_builder.metadata()));

    // TODO(dennis): decode only the columns the enabled indexers need, including the encoded
    // primary key for tag indexes. This needs the indexer to stop locating columns by position.
    let mut reader = match reader_builder.with_batch_size(batch_size).build() {
        Ok(reader) => reader,
        Err(e) => {
            warn!(e; "Failed to read encoded SST {:?} for index creation", region_file_id);
            indexer.abort().await;
            return IndexOutput::default();
        }
    };
    loop {
        // Decoding is CPU-bound, so each batch is decoded on the blocking pool. Only one batch
        // is in flight, and an aborted flush stops after the batch being decoded.
        let decoded = common_runtime::spawn_blocking_global(move || {
            let batch = reader.next();
            (reader, batch)
        })
        .await;
        let batch = match decoded {
            Ok((next_reader, batch)) => {
                reader = next_reader;
                batch
            }
            Err(e) => {
                warn!(e; "Failed to decode encoded SST {:?} for index creation", region_file_id);
                indexer.abort().await;
                return IndexOutput::default();
            }
        };
        match batch {
            Some(Ok(batch)) => indexer.update_flat(&batch).await,
            Some(Err(e)) => {
                warn!(e; "Failed to decode encoded SST {:?} for index creation", region_file_id);
                indexer.abort().await;
                return IndexOutput::default();
            }
            None => break,
        }
    }
    indexer.finish().await
}

/// Type of an index build task.
#[derive(Debug, Clone, IntoStaticStr, PartialEq)]
pub enum IndexBuildType {
    /// Build index when schema change.
    SchemaChange,
    /// Create or update index after flush.
    Flush,
    /// Create or update index after compact.
    Compact,
    /// Manually build index.
    Manual,
}

impl IndexBuildType {
    fn as_str(&self) -> &'static str {
        self.into()
    }

    // Higher value means higher priority.
    fn priority(&self) -> u8 {
        match self {
            IndexBuildType::Manual => 3,
            IndexBuildType::SchemaChange => 2,
            IndexBuildType::Flush => 1,
            IndexBuildType::Compact => 0,
        }
    }
}

impl From<OperationType> for IndexBuildType {
    fn from(op_type: OperationType) -> Self {
        match op_type {
            OperationType::Flush => IndexBuildType::Flush,
            OperationType::Compact => IndexBuildType::Compact,
        }
    }
}

/// Outcome of an index build task.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IndexBuildOutcome {
    Finished,
    Aborted(String),
}

/// How an index build task reports its result.
enum IndexBuildReport {
    /// The task sends the outcome to its waiters.
    Done(IndexBuildOutcome),
    /// The worker or a retry task owns the waiters, so the task must not
    /// report to them.
    Deferred,
}

/// Mpsc output result sender.
pub type ResultMpscSender = Sender<Result<IndexBuildOutcome>>;

#[derive(Clone)]
pub struct IndexBuildTask {
    /// The logical region whose manifest and in-memory version this task updates.
    pub region_id: RegionId,
    /// The SST file handle to build index for.
    pub file: FileHandle,
    /// The target region metadata used to decode rows from the SST.
    ///
    /// An SST may originate in another region while being visible in the target
    /// manifest. This metadata defines the target schema and sequence domain;
    /// applying the staging manifest only makes imported files visible. Index
    /// rebuild happens later when a flush, compaction, schema change, or manual
    /// index build request schedules it.
    pub(crate) target_region_metadata: RegionMetadataRef,
    /// The manifest state this build is based on.
    pub(crate) source: IndexBuildSource,
    /// Index options of the target region when the task was created.
    pub(crate) index_options: IndexOptions,
    pub reason: IndexBuildType,
    pub access_layer: AccessLayerRef,
    pub(crate) listener: WorkerListener,
    pub(crate) manifest_ctx: ManifestContextRef,
    pub write_cache: Option<WriteCacheRef>,
    pub cache_manager: Option<CacheManagerRef>,
    pub file_purger: FilePurgerRef,
    /// When write cache is enabled, the indexer builder should be built from the write cache.
    /// Otherwise, it should be built from the access layer.
    pub indexer_builder: Arc<dyn IndexerBuilder + Send + Sync>,
    /// Request sender to notify the region worker.
    pub(crate) request_sender: Sender<WorkerRequestWithTime>,
    /// Senders of callers waiting for this build. Coalesced tasks and
    /// schema-change retries hand their senders over to the surviving task.
    pub(crate) result_senders: Vec<ResultMpscSender>,
}

impl std::fmt::Debug for IndexBuildTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndexBuildTask")
            .field("region_id", &self.region_id)
            .field("origin_region_id", &self.source.file_meta.region_id)
            .field("file_id", &self.source.file_meta.file_id)
            .field("schema_version", &self.source.schema_version)
            .field("reason", &self.reason)
            .finish()
    }
}

impl IndexBuildTask {
    /// Notify the caller the job is success.
    pub async fn on_success(&self, outcome: IndexBuildOutcome) {
        for sender in &self.result_senders {
            let _ = sender.send(Ok(outcome.clone())).await;
        }
    }

    /// Send index build error to waiter.
    pub async fn on_failure(&self, err: Arc<Error>) {
        for sender in &self.result_senders {
            let _ = sender
                .send(Err(err.clone()).context(BuildIndexAsyncSnafu {
                    region_id: self.region_id,
                }))
                .await;
        }
    }

    fn into_index_build_job(mut self, version_control: VersionControlRef) -> Job {
        Box::pin(async move {
            self.do_index_build(version_control).await;
        })
    }

    async fn do_index_build(&mut self, version_control: VersionControlRef) {
        self.listener
            .on_index_build_begin(self.region_file_id())
            .await;
        let result = if self.reason == IndexBuildType::Compact {
            let mut task = self.clone();
            // Keep the scheduler slot occupied until the compact runtime finishes the build.
            match common_runtime::spawn_compact(
                async move { task.index_build(version_control).await },
            )
            .await
            {
                Ok(result) => result,
                Err(err) => Err(err).context(JoinSnafu),
            }
        } else {
            self.index_build(version_control).await
        };
        match result {
            Ok(IndexBuildReport::Done(outcome)) => {
                if let IndexBuildOutcome::Aborted(reason) = &outcome {
                    debug!("Index build aborted: {}", reason);
                }
                self.on_success(outcome).await
            }
            Ok(IndexBuildReport::Deferred) => {}
            Err(e) => {
                warn!(
                    e; "Index build task failed, region: {}, file_id: {}",
                    self.region_id, self.source.file_meta.file_id,
                );
                self.on_failure(e.into()).await
            }
        }
        self.notify_worker(BackgroundNotify::IndexBuildStopped(IndexBuildStopped {
            file_id: self.source.file_meta.file_id,
        }))
        .await;
    }

    fn region_file_id(&self) -> RegionFileId {
        RegionFileId::new(
            self.source.file_meta.region_id,
            self.source.file_meta.file_id,
        )
    }

    async fn notify_worker(&self, notify: BackgroundNotify) {
        let worker_request = WorkerRequest::Background {
            region_id: self.region_id,
            notify,
        };
        let _ = self
            .request_sender
            .send(WorkerRequestWithTime::new(worker_request))
            .await;
    }

    /// Builds an aborted outcome without a listener event. Builds that create
    /// no index stop here, and the listener observes them as neither finished
    /// nor aborted.
    fn aborted(&self, reason: &str) -> IndexBuildReport {
        IndexBuildReport::Done(IndexBuildOutcome::Aborted(format!(
            "{reason}, region: {}, file_id: {}",
            self.region_id, self.source.file_meta.file_id
        )))
    }

    async fn abort_and_notify(
        &self,
        region_file_id: RegionFileId,
        reason: &str,
    ) -> IndexBuildReport {
        self.listener.on_index_build_abort(region_file_id).await;
        self.aborted(reason)
    }

    // Checks if the SST file still exists in object store and version to avoid conflict with compaction.
    async fn check_sst_file_exists(&self, version_control: &VersionControlRef) -> bool {
        let file_id = self.source.file_meta.file_id;
        let level = self.source.file_meta.level;
        // We should check current version instead of the version when the job is created.
        let version = version_control.current().version;

        let Some(level_files) = version.ssts.levels().get(level as usize) else {
            warn!(
                "File id {} not found in level {} for index build, region: {}",
                file_id, level, self.region_id
            );
            return false;
        };

        match level_files.files.get(&file_id) {
            Some(handle) if !handle.is_deleted() && !handle.compacting() => {
                // If the file's metadata is present in the current version, the physical SST file
                // is guaranteed to exist on object store. The file purger removes the physical
                // file only after its metadata is removed from the version.
                true
            }
            _ => {
                warn!(
                    "File id {} not found in region version for index build, region: {}",
                    file_id, self.region_id
                );
                false
            }
        }
    }

    async fn index_build(
        &mut self,
        version_control: VersionControlRef,
    ) -> Result<IndexBuildReport> {
        // Taken before `prepare_source` replaces the source, and used for the
        // index files and listener events of this build.
        let region_file_id = self.region_file_id();
        if let ControlFlow::Break(report) =
            self.prepare_source(region_file_id, &version_control).await
        {
            return Ok(report);
        }
        let new_index_version = self
            .source
            .file_meta
            .index_version()
            .map_or(0, |version| version + 1);
        let (mut indexer, index_output) =
            match self.create_index(region_file_id, new_index_version).await? {
                ControlFlow::Continue(created) => created,
                ControlFlow::Break(report) => return Ok(report),
            };

        // Check SST file existence again after building index.
        if !self.check_sst_file_exists(&version_control).await {
            // Calls abort to clean up index files.
            indexer.abort().await;
            return Ok(self
                .abort_and_notify(region_file_id, "SST file not found during index build")
                .await);
        }

        self.publish(region_file_id, index_output, new_index_version)
            .await
    }

    /// Checks that the SST still exists and moves the build source to the
    /// file's current manifest state.
    async fn prepare_source(
        &mut self,
        region_file_id: RegionFileId,
        version_control: &VersionControlRef,
    ) -> ControlFlow<IndexBuildReport> {
        // Check SST file existence before building index to avoid failure of parquet reader.
        if !self.check_sst_file_exists(version_control).await {
            return ControlFlow::Break(
                self.abort_and_notify(region_file_id, "SST file not found during index build")
                    .await,
            );
        }

        // A task may have waited behind another build of the same SST. Build on
        // the index that build published, otherwise this publication is stale.
        let current_file_meta = self
            .manifest_ctx
            .manifest()
            .await
            .files
            .get(&self.source.file_meta.file_id)
            .cloned();
        let Some(current_file_meta) = current_file_meta else {
            return ControlFlow::Break(
                self.abort_and_notify(
                    region_file_id,
                    "SST file not found in manifest during index build",
                )
                .await,
            );
        };
        if self.reason == IndexBuildType::Manual
            && current_file_meta.is_index_consistent_with_region(
                &self.target_region_metadata.column_metadatas,
                &self.index_options,
            )
        {
            return ControlFlow::Break(
                self.abort_and_notify(region_file_id, "Index is already up to date")
                    .await,
            );
        }
        self.source.file_meta = current_file_meta.as_ref().clone();
        ControlFlow::Continue(())
    }

    /// Reads the SST and creates the index files.
    async fn create_index(
        &self,
        region_file_id: RegionFileId,
        new_index_version: u64,
    ) -> Result<ControlFlow<IndexBuildReport, (Indexer, IndexOutput)>> {
        let mut parquet_reader = self
            .access_layer
            .read_sst(self.file.clone()) // use the latest file handle instead of creating a new one
            .expected_metadata(Some(self.target_region_metadata.clone()))
            .build()
            .await?;

        let row_group_size = parquet_reader.as_ref().and_then(|reader| {
            reader
                .parquet_metadata()
                .row_groups()
                .first()
                .map(|row_group| row_group.num_rows() as usize)
                .filter(|size| *size > 0)
        });

        let mut indexer = self
            .indexer_builder
            .build(region_file_id, new_index_version, row_group_size)
            .await;
        // The build type or index options may leave nothing to build. Return before reading
        // any row; an empty indexer finishes with no output, which publishes nothing.
        if indexer.is_empty() {
            return Ok(ControlFlow::Break(self.aborted("No index to build")));
        }

        if let Some(mut parquet_reader) = parquet_reader.take() {
            // TODO(SNC123): optimize index batch
            loop {
                match parquet_reader.next_record_batch().await {
                    Ok(Some(batch)) => {
                        indexer.update_flat(&batch).await;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        indexer.abort().await;
                        return Err(e);
                    }
                }
            }
        }
        let index_output = indexer.finish().await;

        if index_output.file_size == 0 {
            // The indexer logs and drops index creation failures.
            return Ok(ControlFlow::Break(self.aborted("No index was created")));
        }
        Ok(ControlFlow::Continue((indexer, index_output)))
    }

    /// Uploads the index files and commits them to the manifest.
    async fn publish(
        &self,
        region_file_id: RegionFileId,
        index_output: IndexOutput,
        new_index_version: u64,
    ) -> Result<IndexBuildReport> {
        // Use the same file_id but with new version for index file.
        self.maybe_upload_index_file(
            index_output.clone(),
            region_file_id.file_id(),
            new_index_version,
        )
        .await?;

        self.listener
            .on_index_build_before_manifest_commit(region_file_id)
            .await;

        // The commit may have reached storage even on error, so the uploaded
        // index file is left for GC instead of being deleted here.
        match self
            .update_manifest(index_output, new_index_version)
            .await?
        {
            IndexPublication::Committed {
                manifest_version,
                file_meta,
            } => {
                self.listener
                    .on_index_build_manifest_committed(region_file_id)
                    .await;
                self.notify_worker(BackgroundNotify::IndexBuildFinished(IndexBuildFinished {
                    manifest_version,
                    file_meta,
                    result_senders: self.result_senders.clone(),
                }))
                .await;
                Ok(IndexBuildReport::Deferred)
            }
            IndexPublication::Stale(stale) => Ok(self
                .handle_stale_publication(region_file_id, stale, new_index_version)
                .await),
        }
    }

    async fn handle_stale_publication(
        &self,
        region_file_id: RegionFileId,
        stale: IndexPublicationStale,
        new_index_version: u64,
    ) -> IndexBuildReport {
        INDEX_PUBLICATION_STALE_TOTAL
            .with_label_values(&["manifest_commit"])
            .inc();
        // If no successor publishes the same version, GC collects the
        // remote artifact. Repartition requires GC, while local mode
        // accepts this narrow orphan window when GC is disabled.
        cleanup_stale_index_caches(
            RegionIndexId::new(region_file_id, new_index_version),
            &self.access_layer,
            self.cache_manager.as_ref(),
            self.write_cache.as_ref(),
        )
        .await;
        if stale != IndexPublicationStale::SchemaChanged {
            return self
                .abort_and_notify(
                    region_file_id,
                    "Index build source changed before publication",
                )
                .await;
        }

        self.listener.on_index_build_abort(region_file_id).await;
        // An index-unrelated schema change also invalidates the generation
        // fence. Retry to avoid leaving the SST unindexed; per-SST coalescing
        // limits this to one active and one pending task, so repeated changes
        // cannot storm.
        self.notify_worker(BackgroundNotify::IndexBuildRetry(BuildIndexRequest {
            region_id: self.region_id,
            build_type: IndexBuildType::SchemaChange,
            file_metas: vec![self.source.file_meta.clone()],
            result_senders: self.result_senders.clone(),
        }))
        .await;
        IndexBuildReport::Deferred
    }

    async fn maybe_upload_index_file(
        &self,
        output: IndexOutput,
        index_file_id: FileId,
        index_version: u64,
    ) -> Result<()> {
        if let Some(write_cache) = &self.write_cache {
            let file_id = self.source.file_meta.file_id;
            let region_id = self.source.file_meta.region_id;
            let remote_store = self.access_layer.object_store();
            let mut upload_tracker = UploadTracker::new(region_id);
            let mut err = None;
            let puffin_key =
                IndexKey::new(region_id, index_file_id, FileType::Puffin(output.version));
            let index_id = RegionIndexId::new(RegionFileId::new(region_id, file_id), index_version);
            let puffin_path = RegionFilePathFactory::new(
                self.access_layer.table_dir().to_string(),
                self.access_layer.path_type(),
            )
            .build_index_file_path_with_version(index_id);
            if let Err(e) = write_cache
                // Index rebuild is background maintenance work, so its uploads
                // are reported as compaction uploads.
                .upload(
                    puffin_key,
                    &puffin_path,
                    remote_store,
                    UploadOptions {
                        op_type: OperationType::Compact,
                        write_buffer_size: crate::sst::DEFAULT_WRITE_BUFFER_SIZE,
                    },
                )
                .await
            {
                err = Some(e);
            }
            upload_tracker.push_uploaded_file(puffin_path);
            if let Some(err) = err {
                // Cleans index files on failure.
                upload_tracker
                    .clean(
                        &[SstInfo {
                            file_id,
                            index_metadata: output,
                            ..Default::default()
                        }],
                        &write_cache.file_cache(),
                        remote_store,
                    )
                    .await;
                return Err(err);
            }
        } else {
            debug!("write cache is not available, skip uploading index file");
        }
        Ok(())
    }

    async fn update_manifest(
        &self,
        output: IndexOutput,
        new_index_version: u64,
    ) -> Result<IndexPublication> {
        let mut updated = self.source.file_meta.clone();
        updated.available_indexes = output.build_available_indexes();
        updated.indexes = output.build_indexes()?;
        updated.index_file_size = output.file_size;
        updated.index_version = new_index_version;
        let publication = self
            .manifest_ctx
            .update_manifest_for_index(&self.source, updated)
            .await?;
        if let IndexPublication::Committed {
            manifest_version, ..
        } = &publication
        {
            info!(
                "Successfully update manifest version to {}, region: {}, reason: {}",
                manifest_version,
                self.region_id,
                self.reason.as_str()
            );
        }
        Ok(publication)
    }
}

impl PartialEq for IndexBuildTask {
    fn eq(&self, other: &Self) -> bool {
        self.reason.priority() == other.reason.priority()
    }
}

impl Eq for IndexBuildTask {}

impl PartialOrd for IndexBuildTask {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexBuildTask {
    fn cmp(&self, other: &Self) -> Ordering {
        self.reason.priority().cmp(&other.reason.priority())
    }
}

#[derive(Clone)]
struct PendingIndexBuild {
    task: IndexBuildTask,
    version_control: VersionControlRef,
}

impl PartialEq for PendingIndexBuild {
    fn eq(&self, other: &Self) -> bool {
        self.task == other.task
    }
}

impl Eq for PendingIndexBuild {}

impl PartialOrd for PendingIndexBuild {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PendingIndexBuild {
    fn cmp(&self, other: &Self) -> Ordering {
        self.task.cmp(&other.task)
    }
}

impl PendingIndexBuild {
    /// Returns whether this task should replace another pending task for the
    /// same SST.
    fn supersedes(&self, other: &Self) -> bool {
        match self
            .task
            .source
            .schema_version
            .cmp(&other.task.source.schema_version)
        {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => match self
                .task
                .source
                .file_meta
                .index_version()
                .cmp(&other.task.source.file_meta.index_version())
            {
                Ordering::Greater => true,
                Ordering::Less => false,
                Ordering::Equal => self.task.reason.priority() > other.task.reason.priority(),
            },
        }
    }
}

#[derive(Default)]
struct PendingIndexBuilds {
    tasks: HashMap<FileId, PendingIndexBuild>,
}

impl PendingIndexBuilds {
    /// Inserts a pending task and returns the task it replaces or that is dropped.
    ///
    /// The surviving task takes over the result senders of the returned task, so
    /// callers waiting on either task wait for the build that actually runs.
    fn insert(&mut self, mut pending: PendingIndexBuild) -> Option<IndexBuildTask> {
        let file_id = pending.task.source.file_meta.file_id;
        match self.tasks.entry(file_id) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(pending);
                None
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let survivor = entry.get_mut();
                if pending.supersedes(survivor) {
                    std::mem::swap(survivor, &mut pending);
                }
                let senders = std::mem::take(&mut pending.task.result_senders);
                survivor.task.result_senders.extend(senders);
                Some(pending.task)
            }
        }
    }

    fn highest_ready(&self, building_files: &HashSet<FileId>) -> Option<PendingIndexBuild> {
        self.tasks
            .values()
            .filter(|pending| !building_files.contains(&pending.task.source.file_meta.file_id))
            .max()
            .cloned()
    }

    fn remove(&mut self, file_id: FileId) {
        self.tasks.remove(&file_id);
    }

    fn drain(&mut self) -> impl Iterator<Item = PendingIndexBuild> {
        std::mem::take(&mut self.tasks).into_values()
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.tasks.len()
    }

    fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

/// Tracks the index build status of a region scheduled by the [IndexBuildScheduler].
struct IndexBuildStatus {
    building_files: HashSet<FileId>,
    /// At most one coalesced pending task is kept for each SST.
    pending_tasks: PendingIndexBuilds,
    /// Whether the active builds belong to a region incarnation that has
    /// stopped accepting index publications.
    retiring: bool,
}

impl IndexBuildStatus {
    fn new() -> Self {
        IndexBuildStatus {
            building_files: HashSet::new(),
            pending_tasks: PendingIndexBuilds::default(),
            retiring: false,
        }
    }

    async fn fail_pending(&mut self, err: Arc<Error>) {
        for pending in self.pending_tasks.drain() {
            pending.task.on_failure(err.clone()).await;
        }
    }

    async fn retire(&mut self, err: Arc<Error>) {
        self.retiring = !self.building_files.is_empty();
        self.fail_pending(err).await;
    }
}

pub struct IndexBuildScheduler {
    /// Background job scheduler.
    scheduler: SchedulerRef,
    /// Tracks regions need to build index.
    region_status: HashMap<RegionId, IndexBuildStatus>,
    /// Limit of files allowed to build index concurrently on this worker.
    files_limit: usize,
}

/// Manager background index build tasks of a worker.
impl IndexBuildScheduler {
    pub fn new(scheduler: SchedulerRef, files_limit: usize) -> Self {
        IndexBuildScheduler {
            scheduler,
            region_status: HashMap::new(),
            files_limit,
        }
    }

    pub(crate) async fn schedule_build(
        &mut self,
        version_control: &VersionControlRef,
        task: IndexBuildTask,
    ) {
        let status = self
            .region_status
            .entry(task.region_id)
            .or_insert_with(IndexBuildStatus::new);

        let file_id = task.source.file_meta.file_id;
        let region_file_id = RegionFileId::new(
            task.source.file_meta.region_id,
            task.source.file_meta.file_id,
        );
        // A manual build must not report success while an active build that may
        // not create the requested indexes is still running, so it waits as well.
        let can_wait_for_active = status.retiring
            || matches!(
                task.reason,
                IndexBuildType::SchemaChange | IndexBuildType::Manual
            );
        let (rejected, coalesced) =
            if status.building_files.contains(&file_id) && !can_wait_for_active {
                (Some(task), None)
            } else {
                let pending = PendingIndexBuild {
                    task,
                    version_control: version_control.clone(),
                };
                (None, status.pending_tasks.insert(pending))
            };
        let should_schedule = !status.retiring;

        if let Some(rejected) = rejected {
            debug!(
                "Rejecting index build because region file {:?} is already being built",
                region_file_id
            );
            rejected
                .on_success(IndexBuildOutcome::Aborted(format!(
                    "Index is already being built for region file {:?}",
                    region_file_id
                )))
                .await;
            rejected.listener.on_index_build_abort(region_file_id).await;
        }

        if let Some(coalesced) = coalesced {
            debug!(
                "Coalescing redundant index build task for region file {:?}",
                region_file_id
            );
            coalesced
                .on_success(IndexBuildOutcome::Aborted(format!(
                    "Index build was coalesced for region file {:?}",
                    region_file_id
                )))
                .await;
            coalesced
                .listener
                .on_index_build_abort(region_file_id)
                .await;
        }

        if should_schedule {
            self.schedule_next_build_batch();
        }
    }

    /// Schedule tasks until reaching the files limit or no more tasks.
    fn schedule_next_build_batch(&mut self) {
        let mut building_count = self
            .region_status
            .values()
            .map(|status| status.building_files.len())
            .sum::<usize>();

        while building_count < self.files_limit {
            let Some(pending) = self.find_next_task() else {
                break;
            };

            let task = pending.task;
            let region_id = task.region_id;
            let file_id = task.source.file_meta.file_id;
            let job = task.clone().into_index_build_job(pending.version_control);
            match self.scheduler.schedule(job) {
                Ok(()) => {
                    if let Some(status) = self.region_status.get_mut(&region_id) {
                        status.pending_tasks.remove(file_id);
                        status.building_files.insert(file_id);
                        building_count += 1;
                    } else {
                        error!(
                            "Region status not found when scheduling index build task, region: {}",
                            region_id
                        );
                    }
                }
                Err(err) => {
                    error!(
                        err;
                        "Failed to schedule index build job, region: {}, file_id: {}",
                        region_id,
                        file_id
                    );
                    if let Some(status) = self.region_status.get_mut(&region_id) {
                        status.pending_tasks.remove(file_id);
                    }
                    common_runtime::spawn_global(async move {
                        task.on_failure(Arc::new(err)).await;
                    });
                }
            }
        }

        self.region_status.retain(|_, status| {
            !status.building_files.is_empty() || !status.pending_tasks.is_empty()
        });
    }

    /// Find the ready task with the highest priority.
    fn find_next_task(&self) -> Option<PendingIndexBuild> {
        self.region_status
            .values()
            .filter(|status| !status.retiring)
            .filter_map(|status| status.pending_tasks.highest_ready(&status.building_files))
            .max()
    }

    pub(crate) fn on_task_stopped(&mut self, region_id: RegionId, file_id: FileId) {
        if let Some(status) = self.region_status.get_mut(&region_id) {
            if !status.building_files.remove(&file_id) {
                debug!(
                    "Index build task is not tracked as building, region: {}, file: {}",
                    region_id, file_id
                );
                return;
            }
            if status.building_files.is_empty() && status.pending_tasks.is_empty() {
                // No more tasks for this region, remove it.
                self.region_status.remove(&region_id);
            } else if status.building_files.is_empty() {
                // All builds from the previous region incarnation have stopped.
                status.retiring = false;
            }
        }

        self.schedule_next_build_batch();
    }

    /// Notifies the scheduler that the region is dropped.
    pub(crate) async fn on_region_dropped(&mut self, region_id: RegionId) {
        self.retire_region(
            region_id,
            Arc::new(RegionDroppedSnafu { region_id }.build()),
        )
        .await;
    }

    /// Notifies the scheduler that the region is closed.
    pub(crate) async fn on_region_closed(&mut self, region_id: RegionId) {
        self.retire_region(region_id, Arc::new(RegionClosedSnafu { region_id }.build()))
            .await;
    }

    /// Notifies the scheduler that the region is truncated.
    pub(crate) async fn on_region_truncated(&mut self, region_id: RegionId) {
        self.retire_region(
            region_id,
            Arc::new(RegionTruncatedSnafu { region_id }.build()),
        )
        .await;
    }

    async fn retire_region(&mut self, region_id: RegionId, err: Arc<Error>) {
        let Some(status) = self.region_status.get_mut(&region_id) else {
            return;
        };
        status.retire(err).await;
        if status.building_files.is_empty() {
            self.region_status.remove(&region_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_base::readable_size::ReadableSize;
    use datafusion_common::HashMap;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{
        ColumnSchema, FulltextOptions, SkippingIndexOptions, SkippingIndexType,
    };
    use datatypes::value::Value;
    use index::inverted_index::format::reader::InvertedIndexReader;
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use partition::expr::col;
    use puffin::puffin_manager::{PuffinManager, PuffinReader};
    use puffin_manager::PuffinManagerFactory;
    use smallvec::smallvec;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use tokio::sync::mpsc;

    use super::*;
    use crate::access_layer::{FilePathProvider, Metrics, SstWriteRequest, WriteType};
    use crate::cache::write_cache::WriteCache;
    use crate::config::{FulltextIndexConfig, IndexBuildMode, MitoConfig, Mode};
    use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
    use crate::memtable::time_partition::TimePartitions;
    use crate::region::RegionLeaderState;
    use crate::region::version::{VersionBuilder, VersionControl};
    use crate::schedule::scheduler::{LocalScheduler, Scheduler};
    use crate::sst::file::{FileMeta, RegionFileId};
    use crate::sst::file_purger::NoopFilePurger;
    use crate::sst::location;
    use crate::sst::parquet::WriteOptions;
    use crate::test_util::memtable_util::EmptyMemtableBuilder;
    use crate::test_util::scheduler_util::{SchedulerEnv, VecScheduler};
    use crate::test_util::sst_util::{
        new_flat_source_from_record_batches, new_record_batch_by_range, sst_region_metadata,
    };

    struct MetaConfig {
        with_inverted: bool,
        with_fulltext: bool,
        with_skipping_bloom: bool,
    }

    /// Receives the published index that the worker applies before notifying waiters.
    async fn recv_index_build_finished(
        rx: &mut mpsc::Receiver<WorkerRequestWithTime>,
    ) -> IndexBuildFinished {
        match rx.recv().await.unwrap().request {
            WorkerRequest::Background {
                notify: BackgroundNotify::IndexBuildFinished(finished),
                ..
            } => finished,
            other => panic!("Expect index build finished, got: {other:?}"),
        }
    }

    async fn seed_manifest_file(manifest_ctx: &ManifestContextRef, file_meta: &FileMeta) {
        manifest_ctx
            .update_manifest(
                RegionLeaderState::Writable,
                RegionMetaActionList::with_action(RegionMetaAction::Edit(RegionEdit {
                    files_to_add: vec![file_meta.clone()],
                    files_to_remove: Vec::new(),
                    timestamp_ms: None,
                    flushed_sequence: None,
                    flushed_entry_id: None,
                    committed_sequence: None,
                    compaction_time_window: None,
                    wal_recovery: Default::default(),
                })),
                false,
            )
            .await
            .unwrap();
    }

    fn mock_region_metadata(
        MetaConfig {
            with_inverted,
            with_fulltext,
            with_skipping_bloom,
        }: MetaConfig,
    ) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        let mut column_schema = ColumnSchema::new("a", ConcreteDataType::int64_datatype(), false);
        if with_inverted {
            column_schema = column_schema.with_inverted_index(true);
        }
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema,
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("b", ConcreteDataType::float64_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "c",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            });

        if with_fulltext {
            let column_schema =
                ColumnSchema::new("text", ConcreteDataType::string_datatype(), true)
                    .with_fulltext_options(FulltextOptions {
                        enable: true,
                        ..Default::default()
                    })
                    .unwrap();

            let column = ColumnMetadata {
                column_schema,
                semantic_type: SemanticType::Field,
                column_id: 4,
            };

            builder.push_column_metadata(column);
        }

        if with_skipping_bloom {
            let column_schema =
                ColumnSchema::new("bloom", ConcreteDataType::string_datatype(), false)
                    .with_skipping_options(SkippingIndexOptions::new_unchecked(
                        42,
                        0.01,
                        SkippingIndexType::BloomFilter,
                    ))
                    .unwrap();

            let column = ColumnMetadata {
                column_schema,
                semantic_type: SemanticType::Field,
                column_id: 5,
            };

            builder.push_column_metadata(column);
        }

        Arc::new(builder.build().unwrap())
    }

    fn mock_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap()
    }

    async fn mock_intm_mgr(path: impl AsRef<str>) -> IntermediateManager {
        IntermediateManager::init_fs(path).await.unwrap()
    }

    fn random_region_file_id() -> RegionFileId {
        RegionFileId::new(RegionId::new(1, 2), FileId::random())
    }

    struct NoopPathProvider;

    impl FilePathProvider for NoopPathProvider {
        fn build_index_file_path(&self, _file_id: RegionFileId) -> String {
            unreachable!()
        }

        fn build_index_file_path_with_version(&self, _index_id: RegionIndexId) -> String {
            unreachable!()
        }

        fn build_sst_file_path(&self, _file_id: RegionFileId) -> String {
            unreachable!()
        }
    }

    async fn mock_sst_file(
        metadata: RegionMetadataRef,
        env: &SchedulerEnv,
        build_mode: IndexBuildMode,
    ) -> SstInfo {
        let source = new_flat_source_from_record_batches(vec![
            new_record_batch_by_range(&["a", "d"], 0, 60),
            new_record_batch_by_range(&["b", "f"], 0, 40),
            new_record_batch_by_range(&["b", "h"], 100, 200),
        ]);
        let mut index_config = MitoConfig::default().index;
        index_config.build_mode = build_mode;
        let write_request = SstWriteRequest {
            index_build_inputs: None,
            op_type: OperationType::Flush,
            metadata: metadata.clone(),
            source,
            max_sequence: None,
            sst_write_format: Default::default(),
            cache_manager: Default::default(),
            preserve_row_sequence: false,
            index_options: IndexOptions::default(),
            index_config,
            inverted_index_config: Default::default(),
            fulltext_index_config: Default::default(),
            bloom_filter_index_config: Default::default(),
        };
        let mut metrics = Metrics::new(WriteType::Flush);
        env.access_layer
            .write_sst(write_request, &WriteOptions::default(), &mut metrics)
            .await
            .unwrap()
            .remove(0)
    }

    async fn mock_version_control(
        metadata: RegionMetadataRef,
        file_purger: FilePurgerRef,
        files: HashMap<FileId, FileMeta>,
    ) -> VersionControlRef {
        let mutable = TimePartitions::new(
            metadata.clone(),
            Arc::new(EmptyMemtableBuilder::default()),
            0,
            None,
        );
        let version_builder = VersionBuilder::new(metadata, mutable)
            .add_files(file_purger, files.values().cloned())
            .build();
        Arc::new(VersionControl::new(version_builder))
    }

    async fn mock_indexer_builder(
        metadata: RegionMetadataRef,
        env: &SchedulerEnv,
    ) -> Arc<IndexerBuilderImpl> {
        let (dir, factory) = PuffinManagerFactory::new_for_test_async("mock_indexer_builder").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;
        let puffin_manager = factory.build(
            env.access_layer.object_store().clone(),
            RegionFilePathFactory::new(
                env.access_layer.table_dir().to_string(),
                env.access_layer.path_type(),
            ),
        );
        Arc::new(IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Flush,
            metadata,
            puffin_manager,
            write_cache_enabled: false,
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            registry: Arc::new(crate::sst::index::registry::IndexRegistry::new(
                InvertedIndexConfig::default(),
                FulltextIndexConfig::default(),
                BloomFilterConfig::default(),
            )),
        })
    }

    #[tokio::test]
    async fn test_builtin_output_with_missing_or_failing_provider() {
        use datatypes::arrow::array::{
            Float64Array, Int64Array, StringArray, TimestampMillisecondArray, UInt8Array,
            UInt64Array,
        };

        for failure in [
            None,
            Some(ProviderFailure::Plan),
            Some(ProviderFailure::Finish),
        ] {
            let (dir, factory) =
                PuffinManagerFactory::new_for_test_async("builtin_provider_failure").await;
            let mut metadata = mock_region_metadata(MetaConfig {
                with_inverted: true,
                with_fulltext: true,
                with_skipping_bloom: true,
            });
            Arc::make_mut(&mut metadata).column_metadatas[0]
                .column_schema
                .mut_metadata()
                .insert(
                    datatypes::schema::HLL_KEY.into(),
                    "{\"precision\":10}".into(),
                );
            let registry =
                registry::IndexRegistry::new(
                    Default::default(),
                    Default::default(),
                    Default::default(),
                )
                .with_provider(failure.map(|failure| {
                    Arc::new(FailingProvider(failure)) as handler::IndexKindProviderRef
                }))
                .unwrap();
            let builder = IndexerBuilderImpl {
                index_build_inputs: None,
                build_type: IndexBuildType::Compact,
                metadata,
                puffin_manager: factory.build(
                    mock_object_store(),
                    RegionFilePathFactory::new(
                        "builtin-provider-test/".into(),
                        store_api::region_request::PathType::Bare,
                    ),
                ),
                write_cache_enabled: false,
                intermediate_manager: mock_intm_mgr(dir.path().to_string_lossy()).await,
                index_options: IndexOptions::default(),
                registry: Arc::new(registry),
            };
            let mut indexer = builder.build(random_region_file_id(), 0, Some(1024)).await;
            let mut batch = crate::read::BatchBuilder::new(Vec::new());
            batch
                .push_field_array(1, Arc::new(Int64Array::from(vec![42])))
                .unwrap();
            batch
                .push_field_array(2, Arc::new(Float64Array::from(vec![1.0])))
                .unwrap();
            batch
                .push_field_array(4, Arc::new(StringArray::from(vec!["hello world"])))
                .unwrap();
            batch
                .push_field_array(5, Arc::new(StringArray::from(vec!["hello"])))
                .unwrap();
            batch
                .timestamps_array(Arc::new(TimestampMillisecondArray::from(vec![0])))
                .unwrap();
            batch
                .sequences_array(Arc::new(UInt64Array::from(vec![0])))
                .unwrap();
            batch
                .op_types_array(Arc::new(UInt8Array::from(vec![0])))
                .unwrap();
            indexer.update(&mut batch.build().unwrap()).await;
            let output = indexer.finish().await;
            assert_eq!(
                indexer.check_success().is_err(),
                failure.is_some(),
                "{failure:?}"
            );
            if failure == Some(ProviderFailure::Finish) {
                assert_eq!(output.file_size, 0);
                assert!(output.entries.is_empty());
            } else {
                assert!(output.file_size > 0, "{failure:?}");
                assert_eq!(
                    output.build_available_indexes().as_slice(),
                    &[
                        crate::sst::file::IndexType::InvertedIndex,
                        crate::sst::file::IndexType::FulltextIndex,
                        crate::sst::file::IndexType::BloomFilterIndex,
                    ],
                    "{failure:?}"
                );
                assert!(!output.build_indexes().unwrap().is_empty());
            }
        }
    }

    #[rstest::rstest]
    #[case::batch(false)]
    #[case::flat(true)]
    #[tokio::test]
    async fn test_mixed_reused_and_scanning_creators(#[case] flat: bool) {
        use datatypes::arrow::array::{ArrayRef, Int64Array};
        use datatypes::vectors::{TimestampMillisecondVector, UInt8Vector, UInt64Vector};
        use index::inverted_index::format::reader::InvertedIndexBlobReader;

        let (dir, factory) = PuffinManagerFactory::new_for_test_async("mixed_reuse").await;
        let manager = factory.build(
            mock_object_store(),
            RegionFilePathFactory::new(
                "mixed-reuse/".into(),
                store_api::region_request::PathType::Bare,
            ),
        );
        let builder = IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Compact,
            metadata: mock_region_metadata(MetaConfig {
                with_inverted: true,
                with_fulltext: false,
                with_skipping_bloom: false,
            }),
            puffin_manager: manager.clone(),
            write_cache_enabled: false,
            intermediate_manager: mock_intm_mgr(dir.path().to_string_lossy()).await,
            index_options: IndexOptions::default(),
            registry: Arc::new(
                registry::IndexRegistry::new(
                    Default::default(),
                    Default::default(),
                    Default::default(),
                )
                .with_provider(Some(Arc::new(FailingProvider(ProviderFailure::Reused))))
                .unwrap(),
            ),
        };
        let file = random_region_file_id();
        let mut indexer = builder.build(file, 0, Some(1024)).await;
        assert!(indexer.requires_row_scan());
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        if flat {
            let batch = RecordBatch::try_from_iter([("a", values)]).unwrap();
            indexer.update_flat(&batch).await;
        } else {
            let mut batch = crate::read::BatchBuilder::with_required_columns(
                Vec::new(),
                Arc::new(TimestampMillisecondVector::from_vec(vec![0, 1, 2])),
                Arc::new(UInt64Vector::from_vec(vec![0; 3])),
                Arc::new(UInt8Vector::from_vec(vec![0; 3])),
            );
            batch.push_field_array(1, values).unwrap();
            indexer.update(&mut batch.build().unwrap()).await;
        }
        indexer.check_success().unwrap();
        let output = indexer.finish().await;
        indexer.check_success().unwrap();
        assert_eq!(
            output.build_available_indexes().as_slice(),
            &[IndexType::InvertedIndex, IndexType::HllIndex]
        );
        let reader = manager.reader(&RegionIndexId::new(file, 0)).await.unwrap();
        reader.blob("test-sketch").await.unwrap();
        let blob = reader.blob("greptime-inverted-index-v1").await.unwrap();
        let inverted = InvertedIndexBlobReader::new(blob.reader().await.unwrap());
        let metadata = inverted.metadata(None).await.unwrap();
        assert_eq!(metadata.total_row_count, 3);
    }

    #[derive(Debug, Default, Clone)]
    struct TestProvider {
        build_calls: Arc<std::sync::atomic::AtomicUsize>,
        kind: Option<kind::ExtensionIndexKind>,
        received_limits: Arc<std::sync::Mutex<Vec<Option<usize>>>>,
        hard_limit: Option<usize>,
    }

    impl handler::IndexKindProvider for TestProvider {
        fn handlers(&self) -> Vec<Arc<dyn handler::IndexKindHandler>> {
            vec![Arc::new(self.clone())]
        }
    }

    impl handler::IndexKindHandler for TestProvider {
        fn kinds(&self) -> &[kind::IndexKind] {
            match self.kind {
                Some(kind::ExtensionIndexKind::Kll) => {
                    &[kind::IndexKind::Extension(kind::ExtensionIndexKind::Kll)]
                }
                _ => &[kind::IndexKind::Extension(kind::ExtensionIndexKind::Hll)],
            }
        }
        fn plan(
            &self,
            _context: &handler::PlanContext<'_>,
        ) -> std::result::Result<
            Option<Arc<dyn handler::PlannedIndex>>,
            common_error::ext::BoxedError,
        > {
            Ok(Some(Arc::new(self.clone())))
        }
    }

    #[async_trait::async_trait]
    impl handler::PlannedIndex for TestProvider {
        fn kinds(&self) -> &[kind::IndexKind] {
            handler::IndexKindHandler::kinds(self)
        }
        fn entries(&self) -> &[handler::PlanEntry] {
            match self.kind {
                Some(kind::ExtensionIndexKind::Kll) => &[handler::PlanEntry {
                    column_id: 0,
                    kind: kind::IndexKind::Extension(kind::ExtensionIndexKind::Kll),
                    format_version: 2,
                }],
                _ => &[handler::PlanEntry {
                    column_id: 0,
                    kind: kind::IndexKind::Extension(kind::ExtensionIndexKind::Hll),
                    format_version: 2,
                }],
            }
        }

        fn memory_limit(&self) -> Option<usize> {
            self.hard_limit
        }

        async fn build(
            &self,
            context: &handler::BuildContext<'_>,
        ) -> std::result::Result<
            Option<Box<dyn handler::IndexCreator>>,
            common_error::ext::BoxedError,
        > {
            self.build_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.received_limits
                .lock()
                .unwrap()
                .push(context.memory_limit);
            Ok(Some(Box::new(TestCreator)))
        }
    }

    #[derive(Debug)]
    struct TestCreator;

    #[async_trait::async_trait]
    impl handler::IndexCreator for TestCreator {
        async fn update(
            &mut self,
            _batch: &mut Batch,
        ) -> std::result::Result<(), common_error::ext::BoxedError> {
            Ok(())
        }

        async fn update_flat(
            &mut self,
            _batch: &RecordBatch,
        ) -> std::result::Result<(), common_error::ext::BoxedError> {
            Ok(())
        }

        async fn finish(
            &mut self,
            _writer: &mut puffin_manager::SstPuffinWriter,
        ) -> std::result::Result<Vec<CompletedEntry>, common_error::ext::BoxedError> {
            Ok(Vec::new())
        }

        async fn abort(&mut self) -> std::result::Result<(), common_error::ext::BoxedError> {
            Ok(())
        }

        fn memory_usage(&self) -> usize {
            17
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ProviderFailure {
        None,
        Plan,
        EmptyKinds,
        Reused,
        Budget,
        Create,
        Update,
        UpdateFlat,
        Finish,
        Close,
    }

    #[derive(Debug, Clone)]
    struct FailingProvider(ProviderFailure);

    fn provider_test_error() -> common_error::ext::BoxedError {
        common_error::ext::BoxedError::new(common_error::ext::PlainError::new(
            "injected provider failure".into(),
            common_error::status_code::StatusCode::Internal,
        ))
    }

    impl handler::IndexKindProvider for FailingProvider {
        fn handlers(&self) -> Vec<Arc<dyn handler::IndexKindHandler>> {
            vec![Arc::new(self.clone())]
        }
    }

    impl handler::IndexKindHandler for FailingProvider {
        fn kinds(&self) -> &[kind::IndexKind] {
            &[kind::IndexKind::Extension(kind::ExtensionIndexKind::Hll)]
        }
        fn plan(
            &self,
            context: &handler::PlanContext<'_>,
        ) -> std::result::Result<
            Option<Arc<dyn handler::PlannedIndex>>,
            common_error::ext::BoxedError,
        > {
            if self.0 == ProviderFailure::Plan {
                return Err(provider_test_error());
            }
            if context.build_type == IndexBuildType::Flush {
                return Ok(None);
            }
            Ok(Some(Arc::new(self.clone())))
        }
    }

    #[async_trait::async_trait]
    impl handler::PlannedIndex for FailingProvider {
        fn kinds(&self) -> &[kind::IndexKind] {
            if self.0 == ProviderFailure::EmptyKinds {
                return &[];
            }
            handler::IndexKindHandler::kinds(self)
        }
        fn entries(&self) -> &[handler::PlanEntry] {
            &[handler::PlanEntry {
                column_id: 0,
                kind: kind::IndexKind::Extension(kind::ExtensionIndexKind::Hll),
                format_version: 2,
            }]
        }

        fn memory_limit(&self) -> Option<usize> {
            (self.0 == ProviderFailure::Budget).then_some(16)
        }

        async fn build(
            &self,
            _context: &handler::BuildContext<'_>,
        ) -> std::result::Result<
            Option<Box<dyn handler::IndexCreator>>,
            common_error::ext::BoxedError,
        > {
            if self.0 == ProviderFailure::Create {
                return Err(provider_test_error());
            }
            Ok(Some(Box::new(FailingCreator(self.0))))
        }
    }

    #[derive(Debug)]
    struct FailingCreator(ProviderFailure);

    #[async_trait::async_trait]
    impl handler::IndexCreator for FailingCreator {
        fn requires_row_scan(&self) -> bool {
            self.0 != ProviderFailure::Reused
        }

        async fn update(
            &mut self,
            _batch: &mut Batch,
        ) -> std::result::Result<(), common_error::ext::BoxedError> {
            if matches!(self.0, ProviderFailure::Update | ProviderFailure::Reused) {
                Err(provider_test_error())
            } else {
                Ok(())
            }
        }

        async fn update_flat(
            &mut self,
            _batch: &RecordBatch,
        ) -> std::result::Result<(), common_error::ext::BoxedError> {
            if matches!(
                self.0,
                ProviderFailure::UpdateFlat | ProviderFailure::Reused
            ) {
                Err(provider_test_error())
            } else {
                Ok(())
            }
        }

        async fn finish(
            &mut self,
            writer: &mut puffin_manager::SstPuffinWriter,
        ) -> std::result::Result<Vec<CompletedEntry>, common_error::ext::BoxedError> {
            use puffin::puffin_manager::PuffinWriter;

            writer
                .put_blob(
                    "test-sketch",
                    futures::io::Cursor::new(vec![1_u8, 2, 3]),
                    puffin::puffin_manager::PutOptions::default(),
                    Default::default(),
                )
                .await
                .map_err(common_error::ext::BoxedError::new)?;
            if self.0 == ProviderFailure::Finish {
                // Fail after writing to exercise partially written artifact handling.
                return Err(provider_test_error());
            }
            Ok(vec![CompletedEntry {
                column_id: Some(0),
                kind: kind::IndexKind::Extension(kind::ExtensionIndexKind::Hll),
                format_version: 2,
            }])
        }

        async fn abort(&mut self) -> std::result::Result<(), common_error::ext::BoxedError> {
            Ok(())
        }

        fn memory_usage(&self) -> usize {
            17
        }
    }

    struct FailingIndexClose {
        inner: object_store::layers::mock::Writer,
        attempts: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl object_store::layers::mock::Write for FailingIndexClose {
        async fn write(
            &mut self,
            bytes: object_store::layers::mock::Buffer,
        ) -> object_store::layers::mock::Result<()> {
            self.inner.write(bytes).await
        }

        async fn close(
            &mut self,
        ) -> object_store::layers::mock::Result<object_store::layers::mock::Metadata> {
            self.attempts
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(object_store::layers::mock::Error::new(
                object_store::layers::mock::ErrorKind::Unexpected,
                "injected Puffin close failure",
            ))
        }

        async fn abort(&mut self) -> object_store::layers::mock::Result<()> {
            self.inner.abort().await
        }
    }

    #[tokio::test]
    async fn test_provider_failures_do_not_publish_sketch_descriptors() {
        for failure in [
            ProviderFailure::None,
            ProviderFailure::Plan,
            ProviderFailure::EmptyKinds,
            ProviderFailure::Create,
            ProviderFailure::Update,
            ProviderFailure::UpdateFlat,
            ProviderFailure::Finish,
            ProviderFailure::Close,
        ] {
            let (dir, factory) = PuffinManagerFactory::new_for_test_async("provider_failure").await;
            let close_attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let store = if failure == ProviderFailure::Close {
                let attempts = close_attempts.clone();
                let layer = object_store::layers::mock::MockLayerBuilder::default()
                    .writer_factory(Arc::new(move |_, _, inner| {
                        Box::new(FailingIndexClose {
                            inner,
                            attempts: attempts.clone(),
                        })
                    }))
                    .build()
                    .unwrap();
                mock_object_store().layer(layer)
            } else {
                mock_object_store()
            };
            let manager = factory.build(
                store,
                crate::access_layer::RegionFilePathFactory::new(
                    "provider-test/".into(),
                    store_api::region_request::PathType::Bare,
                ),
            );
            let builder = IndexerBuilderImpl {
                index_build_inputs: None,
                build_type: IndexBuildType::Compact,
                metadata: mock_region_metadata(MetaConfig {
                    with_inverted: false,
                    with_fulltext: false,
                    with_skipping_bloom: false,
                }),
                puffin_manager: manager,
                write_cache_enabled: false,
                intermediate_manager: mock_intm_mgr(dir.path().to_string_lossy()).await,
                index_options: IndexOptions::default(),
                registry: Arc::new(
                    registry::IndexRegistry::new(
                        InvertedIndexConfig::default(),
                        FulltextIndexConfig::default(),
                        BloomFilterConfig::default(),
                    )
                    .with_provider(Some(Arc::new(FailingProvider(failure))))
                    .unwrap(),
                ),
            };
            let mut indexer = builder.build(random_region_file_id(), 0, Some(1024)).await;
            if failure == ProviderFailure::Update {
                let mut batch = crate::read::BatchBuilder::with_required_columns(
                    Vec::new(),
                    Arc::new(datatypes::vectors::TimestampMillisecondVector::from_vec(
                        vec![0],
                    )),
                    Arc::new(datatypes::vectors::UInt64Vector::from_vec(vec![0])),
                    Arc::new(datatypes::vectors::UInt8Vector::from_vec(vec![0])),
                )
                .build()
                .unwrap();
                indexer.update(&mut batch).await;
            } else {
                indexer
                    .update_flat(&new_record_batch_by_range(&["a", "b"], 0, 1))
                    .await;
            }
            let output = indexer.finish().await;
            if failure == ProviderFailure::None {
                assert_eq!(
                    output
                        .entries
                        .iter()
                        .filter(|entry| !entry.kind.is_builtin())
                        .count(),
                    1
                );
                assert!(output.file_size > 0);
                assert!(output.build_indexes().unwrap().iter().any(|index| {
                    index.created_indexes.iter().any(|kind| {
                        matches!(
                            kind,
                            crate::sst::file::IndexType::HllIndex
                                | crate::sst::file::IndexType::KllIndex
                                | crate::sst::file::IndexType::MgIndex
                        )
                    })
                }));
            } else {
                assert!(
                    output.entries.iter().all(|entry| entry.kind.is_builtin()),
                    "{failure:?}"
                );
                assert!(output.build_indexes().unwrap().is_empty(), "{failure:?}");
                assert_eq!(output.file_size, 0, "{failure:?}");
            }
            assert!(indexer.creators.is_empty());
            assert_eq!(
                indexer.check_success().is_err(),
                failure != ProviderFailure::None
            );
            assert_eq!(
                close_attempts.load(std::sync::atomic::Ordering::Relaxed),
                usize::from(failure == ProviderFailure::Close)
            );
        }
    }

    #[tokio::test]
    async fn test_provider_compaction_only_and_per_output() {
        let (dir, factory) = PuffinManagerFactory::new_for_test_async("provider_scope").await;
        let provider = Arc::new(TestProvider {
            hard_limit: Some(17),
            ..Default::default()
        });
        let mut builder = IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Flush,
            metadata: mock_region_metadata(MetaConfig {
                with_inverted: false,
                with_fulltext: false,
                with_skipping_bloom: false,
            }),
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            write_cache_enabled: false,
            intermediate_manager: mock_intm_mgr(dir.path().to_string_lossy()).await,
            index_options: IndexOptions::default(),
            registry: Arc::new(
                registry::IndexRegistry::new(
                    InvertedIndexConfig::default(),
                    FulltextIndexConfig::default(),
                    BloomFilterConfig::default(),
                )
                .with_provider(Some(provider.clone()))
                .unwrap(),
            ),
        };
        let flush = builder.build(random_region_file_id(), 0, Some(1024)).await;
        assert!(flush.creators.is_empty());
        assert_eq!(
            provider
                .build_calls
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );

        builder.build_type = IndexBuildType::Compact;
        let mut first = builder.build(random_region_file_id(), 0, Some(1024)).await;
        let mut second = builder.build(random_region_file_id(), 0, Some(1024)).await;
        assert_eq!(
            provider
                .build_calls
                .load(std::sync::atomic::Ordering::Relaxed),
            2
        );
        assert!(!first.is_empty());
        assert!(!second.is_empty());
        assert_eq!(first.creators[0].creator.memory_usage(), 17);
        first.abort().await;
        assert!(first.creators.is_empty());
        assert!(!second.creators.is_empty());
        second.abort().await;

        // Reject a creator at preparation when its retained state already exceeds
        // the caller's allocation; equality is allowed.
        let plan = builder.registry.plan(&handler::PlanContext {
            metadata: &builder.metadata,
            index_options: &builder.index_options,
            build_type: builder.build_type.clone(),
            existing_indexes: &[],
        });
        builder.index_build_inputs = Some(build::IndexBuildInputs {
            output_rows: None,
            plan: Arc::new(plan),
            inputs: Arc::from([]),
            input_puffin_manager: builder.puffin_manager.clone(),
            can_merge_inputs: false,
            memory_limit: Some(16),
        });
        let rejected = builder.build(random_region_file_id(), 0, Some(1024)).await;
        assert!(rejected.is_empty());
        builder.index_build_inputs.as_mut().unwrap().memory_limit = Some(17);
        let mut bounded = builder.build(random_region_file_id(), 0, Some(1024)).await;
        assert!(!bounded.creators.is_empty());
        bounded.creators[0].memory_limit = Some(16);
        bounded.enforce_memory_limits().await;
        bounded.flush_mem_metrics();
        assert!(bounded.creators.is_empty());

        // A larger operation allocation must not relax the handler's ceiling.
        builder.index_build_inputs.as_mut().unwrap().memory_limit = Some(34);
        let mut bounded = builder.build(random_region_file_id(), 0, Some(1024)).await;
        assert_eq!(bounded.creators[0].memory_limit, Some(17));
        bounded.abort().await;
    }

    #[tokio::test]
    async fn test_builder_passes_remaining_extension_budget() {
        let (dir, factory) = PuffinManagerFactory::new_for_test_async("build_budget").await;
        for (operation_limit, ceiling, expected, retained) in [
            (Some(30), None, vec![Some(30), Some(13)], 1),
            (Some(17), None, vec![Some(17), Some(0)], 1),
            (Some(30), Some(20), vec![Some(20), Some(13)], 1),
            (Some(40), Some(20), vec![Some(20), Some(20)], 2),
            (Some(16), None, vec![Some(16), Some(16)], 0),
            (None, None, vec![None, None], 2),
            (None, Some(17), vec![Some(17), Some(17)], 2),
        ] {
            let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
            let mut registry = registry::IndexRegistry::new(
                InvertedIndexConfig::default(),
                FulltextIndexConfig::default(),
                BloomFilterConfig::default(),
            );
            for kind in [kind::ExtensionIndexKind::Hll, kind::ExtensionIndexKind::Kll] {
                registry
                    .register(Arc::new(TestProvider {
                        kind: Some(kind),
                        received_limits: observed.clone(),
                        hard_limit: ceiling,
                        ..Default::default()
                    }))
                    .unwrap();
            }
            let mut builder = IndexerBuilderImpl {
                index_build_inputs: None,
                build_type: IndexBuildType::Compact,
                metadata: mock_region_metadata(MetaConfig {
                    with_inverted: false,
                    with_fulltext: false,
                    with_skipping_bloom: false,
                }),
                puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
                write_cache_enabled: false,
                intermediate_manager: mock_intm_mgr(dir.path().to_string_lossy()).await,
                index_options: IndexOptions::default(),
                registry: Arc::new(registry),
            };
            let plan = builder.registry.plan(&handler::PlanContext {
                metadata: &builder.metadata,
                index_options: &builder.index_options,
                build_type: builder.build_type.clone(),
                existing_indexes: &[],
            });
            builder.index_build_inputs = Some(build::IndexBuildInputs {
                output_rows: None,
                plan: Arc::new(plan),
                inputs: Arc::from([]),
                input_puffin_manager: builder.puffin_manager.clone(),
                can_merge_inputs: false,
                memory_limit: operation_limit,
            });
            let mut indexer = builder.build(random_region_file_id(), 0, Some(1024)).await;
            assert_eq!(*observed.lock().unwrap(), expected);
            assert_eq!(indexer.creators.len(), retained);
            indexer.abort().await;
        }
    }

    #[tokio::test]
    async fn test_extension_creators_share_memory_budget() {
        for (limit, expected_extensions) in [(34, 2), (33, 1), (16, 0)] {
            let mut indexer = Indexer {
                extension_memory_limit: Some(limit),
                ..Default::default()
            };
            // Built-in state uses its own spill budget and does not consume the
            // optional extension allocation. Each test creator retains 17 bytes.
            indexer.push_creator(
                vec![kind::IndexKind::Inverted],
                Box::new(TestCreator),
                None,
                TYPE_INVERTED_INDEX,
            );
            for kind in [kind::ExtensionIndexKind::Hll, kind::ExtensionIndexKind::Kll] {
                indexer.push_creator(
                    vec![kind::IndexKind::Extension(kind)],
                    Box::new(TestCreator),
                    Some(34),
                    TYPE_EXTENSION_INDEX,
                );
            }
            let before = INDEX_CREATE_DISCARDED_TOTAL
                .with_label_values(&["kll_index", "memory_budget_exceeded"])
                .get();
            indexer.enforce_memory_limits().await;
            assert_eq!(indexer.creators.len(), 1 + expected_extensions);
            assert!(indexer.check_success().is_ok());
            assert_eq!(indexer.discarded().len(), 2 - expected_extensions);
            let kinds = [
                kind::IndexKind::Extension(kind::ExtensionIndexKind::Hll),
                kind::IndexKind::Extension(kind::ExtensionIndexKind::Kll),
            ];
            assert_eq!(indexer.discarded(), &kinds[expected_extensions..]);
            if expected_extensions < 2 {
                assert!(
                    INDEX_CREATE_DISCARDED_TOTAL
                        .with_label_values(&["kll_index", "memory_budget_exceeded"])
                        .get()
                        > before
                );
            }
            assert!(indexer.creators[0].is_builtin());
        }
    }

    #[test]
    fn test_file_level_completion_preserves_json_only_index() {
        let output = IndexOutput {
            entries: vec![CompletedEntry {
                column_id: None,
                kind: kind::IndexKind::Inverted,
                format_version: 0,
            }],
            ..Default::default()
        };
        assert_eq!(
            output.build_available_indexes().as_slice(),
            &[IndexType::InvertedIndex]
        );
        assert!(output.build_indexes().unwrap().is_empty());
    }

    #[test]
    fn test_completed_entries_preserve_all_kinds_in_order() {
        use crate::sst::index::kind::{ExtensionIndexKind, IndexKind};

        let output = IndexOutput {
            entries: [
                IndexKind::Extension(ExtensionIndexKind::Mg),
                IndexKind::Fulltext,
                IndexKind::Extension(ExtensionIndexKind::Kll),
                IndexKind::Bloom,
                IndexKind::Extension(ExtensionIndexKind::Hll),
                IndexKind::Inverted,
                IndexKind::Extension(ExtensionIndexKind::Hll),
            ]
            .into_iter()
            .enumerate()
            .map(|(position, kind)| CompletedEntry {
                column_id: Some(if position == 6 { 2 } else { 1 }),
                kind,
                format_version: 0,
            })
            .collect(),
            ..Default::default()
        };
        let expected = [
            IndexType::InvertedIndex,
            IndexType::FulltextIndex,
            IndexType::BloomFilterIndex,
            IndexType::HllIndex,
            IndexType::KllIndex,
            IndexType::MgIndex,
        ];
        let available = output.build_available_indexes();
        assert_eq!(available.as_slice(), &expected);
        assert!(!available.spilled());
        let columns = output.build_indexes().unwrap();
        assert_eq!(columns.len(), 2);
        assert_eq!(columns[0].created_indexes.as_slice(), &expected);
        assert_eq!(
            columns[1].created_indexes.as_slice(),
            &[IndexType::HllIndex]
        );
    }

    #[test]
    fn test_completed_entries_preserve_builtin_metadata() {
        use crate::sst::index::kind::IndexKind;

        // Deliberately reverse creator order: the published builtin order is contractual.
        let output = IndexOutput {
            entries: vec![
                CompletedEntry {
                    column_id: Some(2),
                    kind: IndexKind::Bloom,
                    format_version: 0,
                },
                CompletedEntry {
                    column_id: Some(2),
                    kind: IndexKind::Fulltext,
                    format_version: u32::from(FULLTEXT_BLOOM_VERSION),
                },
                CompletedEntry {
                    column_id: Some(2),
                    kind: IndexKind::Inverted,
                    format_version: 0,
                },
                CompletedEntry {
                    column_id: Some(3),
                    kind: IndexKind::Fulltext,
                    format_version: 0,
                },
            ],
            ..Default::default()
        };
        let expected_kinds = vec![
            IndexType::InvertedIndex,
            IndexType::FulltextIndex,
            IndexType::BloomFilterIndex,
        ];
        assert_eq!(
            output.build_available_indexes().as_slice(),
            expected_kinds.as_slice()
        );
        let actual = output.build_indexes().unwrap();
        assert_eq!(
            actual,
            vec![
                ColumnIndexMetadata {
                    column_id: 2,
                    created_indexes: expected_kinds.into(),
                    fulltext_bloom_version: FULLTEXT_BLOOM_VERSION,
                },
                ColumnIndexMetadata {
                    column_id: 3,
                    created_indexes: vec![IndexType::FulltextIndex].into(),
                    fulltext_bloom_version: 0,
                },
            ]
        );
    }

    #[test]
    fn test_build_indexes_rejects_unrepresentable_fulltext_version() {
        let mut output = IndexOutput {
            entries: vec![CompletedEntry {
                column_id: Some(7),
                kind: kind::IndexKind::Fulltext,
                format_version: u8::MAX.into(),
            }],
            ..Default::default()
        };
        assert_eq!(
            output.build_indexes().unwrap()[0].fulltext_bloom_version,
            u8::MAX
        );
        output.entries[0].format_version += 1;
        let error = output.build_indexes().unwrap_err();
        assert!(
            error
                .to_string()
                .contains("fulltext bloom version 256 exceeds u8")
        );
    }

    #[test]
    fn test_completed_entry_validation() {
        let mut entry = CompletedEntry {
            column_id: Some(1),
            kind: kind::IndexKind::Fulltext,
            format_version: u8::MAX.into(),
        };
        assert!(entry.validate().is_ok());
        entry.format_version += 1;
        assert!(entry.validate().is_err());
        entry.kind = kind::IndexKind::Extension(kind::ExtensionIndexKind::Hll);
        assert!(entry.validate().is_ok()); // Extension versions belong to Puffin.
        entry.column_id = None;
        assert!(entry.validate().is_err());
    }

    #[tokio::test]
    async fn test_build_indexer_basic() {
        let (dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

        let metadata = mock_region_metadata(MetaConfig {
            with_inverted: true,
            with_fulltext: true,
            with_skipping_bloom: true,
        });
        let indexer = IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Flush,
            metadata,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            write_cache_enabled: false,
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            registry: Arc::new(crate::sst::index::registry::IndexRegistry::new(
                InvertedIndexConfig::default(),
                FulltextIndexConfig::default(),
                BloomFilterConfig::default(),
            )),
        }
        .build(random_region_file_id(), 0, Some(1024))
        .await;

        assert!(indexer.has_kind(kind::IndexKind::Inverted));
        assert!(indexer.has_kind(kind::IndexKind::Fulltext));
        assert!(indexer.has_kind(kind::IndexKind::Bloom));
    }

    #[tokio::test]
    async fn test_build_indexer_disable_create() {
        let (dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_disable_create_").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

        let metadata = mock_region_metadata(MetaConfig {
            with_inverted: true,
            with_fulltext: true,
            with_skipping_bloom: true,
        });
        let indexer = IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            write_cache_enabled: false,
            intermediate_manager: intm_manager.clone(),
            index_options: IndexOptions::default(),
            registry: Arc::new(crate::sst::index::registry::IndexRegistry::new(
                InvertedIndexConfig {
                    create_on_flush: Mode::Disable,
                    ..Default::default()
                },
                FulltextIndexConfig::default(),
                BloomFilterConfig::default(),
            )),
        }
        .build(random_region_file_id(), 0, Some(1024))
        .await;

        assert!(!indexer.has_kind(kind::IndexKind::Inverted));
        assert!(indexer.has_kind(kind::IndexKind::Fulltext));
        assert!(indexer.has_kind(kind::IndexKind::Bloom));

        let indexer = IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Compact,
            metadata: metadata.clone(),
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            write_cache_enabled: false,
            intermediate_manager: intm_manager.clone(),
            index_options: IndexOptions::default(),
            registry: Arc::new(crate::sst::index::registry::IndexRegistry::new(
                InvertedIndexConfig::default(),
                FulltextIndexConfig {
                    create_on_compaction: Mode::Disable,
                    ..Default::default()
                },
                BloomFilterConfig::default(),
            )),
        }
        .build(random_region_file_id(), 0, Some(1024))
        .await;

        assert!(indexer.has_kind(kind::IndexKind::Inverted));
        assert!(!indexer.has_kind(kind::IndexKind::Fulltext));
        assert!(indexer.has_kind(kind::IndexKind::Bloom));

        let indexer = IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Compact,
            metadata,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            write_cache_enabled: false,
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            registry: Arc::new(crate::sst::index::registry::IndexRegistry::new(
                InvertedIndexConfig::default(),
                FulltextIndexConfig::default(),
                BloomFilterConfig {
                    create_on_compaction: Mode::Disable,
                    ..Default::default()
                },
            )),
        }
        .build(random_region_file_id(), 0, Some(1024))
        .await;

        assert!(indexer.has_kind(kind::IndexKind::Inverted));
        assert!(indexer.has_kind(kind::IndexKind::Fulltext));
        assert!(!indexer.has_kind(kind::IndexKind::Bloom));
    }

    #[tokio::test]
    async fn test_build_indexer_no_required() {
        let (dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_no_required_").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

        let metadata = mock_region_metadata(MetaConfig {
            with_inverted: false,
            with_fulltext: true,
            with_skipping_bloom: true,
        });
        let indexer = IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            write_cache_enabled: false,
            intermediate_manager: intm_manager.clone(),
            index_options: IndexOptions::default(),
            registry: Arc::new(crate::sst::index::registry::IndexRegistry::new(
                InvertedIndexConfig::default(),
                FulltextIndexConfig::default(),
                BloomFilterConfig::default(),
            )),
        }
        .build(random_region_file_id(), 0, Some(1024))
        .await;

        assert!(!indexer.has_kind(kind::IndexKind::Inverted));
        assert!(indexer.has_kind(kind::IndexKind::Fulltext));
        assert!(indexer.has_kind(kind::IndexKind::Bloom));

        let metadata = mock_region_metadata(MetaConfig {
            with_inverted: true,
            with_fulltext: false,
            with_skipping_bloom: true,
        });
        let indexer = IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            write_cache_enabled: false,
            intermediate_manager: intm_manager.clone(),
            index_options: IndexOptions::default(),
            registry: Arc::new(crate::sst::index::registry::IndexRegistry::new(
                InvertedIndexConfig::default(),
                FulltextIndexConfig::default(),
                BloomFilterConfig::default(),
            )),
        }
        .build(random_region_file_id(), 0, Some(1024))
        .await;

        assert!(indexer.has_kind(kind::IndexKind::Inverted));
        assert!(!indexer.has_kind(kind::IndexKind::Fulltext));
        assert!(indexer.has_kind(kind::IndexKind::Bloom));

        let metadata = mock_region_metadata(MetaConfig {
            with_inverted: true,
            with_fulltext: true,
            with_skipping_bloom: false,
        });
        let indexer = IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            write_cache_enabled: false,
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            registry: Arc::new(crate::sst::index::registry::IndexRegistry::new(
                InvertedIndexConfig::default(),
                FulltextIndexConfig::default(),
                BloomFilterConfig::default(),
            )),
        }
        .build(random_region_file_id(), 0, Some(1024))
        .await;

        assert!(indexer.has_kind(kind::IndexKind::Inverted));
        assert!(indexer.has_kind(kind::IndexKind::Fulltext));
        assert!(!indexer.has_kind(kind::IndexKind::Bloom));
    }

    #[tokio::test]
    async fn test_build_indexer_zero_row_group_hint() {
        let (dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_zero_row_group_").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

        let metadata = mock_region_metadata(MetaConfig {
            with_inverted: true,
            with_fulltext: true,
            with_skipping_bloom: true,
        });
        let indexer = IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Flush,
            metadata,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            write_cache_enabled: false,
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            registry: Arc::new(crate::sst::index::registry::IndexRegistry::new(
                InvertedIndexConfig::default(),
                FulltextIndexConfig::default(),
                BloomFilterConfig::default(),
            )),
        }
        .build(random_region_file_id(), 0, Some(0))
        .await;

        assert!(indexer.has_kind(kind::IndexKind::Inverted));
    }

    #[tokio::test]
    async fn test_index_build_task_sst_not_exist() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);
        let mut scheduler = env.mock_index_build_scheduler(4);
        let metadata = Arc::new(sst_region_metadata());
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        let file_purger = Arc::new(NoopFilePurger {});
        let files = HashMap::new();
        let version_control =
            mock_version_control(metadata.clone(), file_purger.clone(), files).await;
        let region_id = metadata.region_id;
        let indexer_builder = mock_indexer_builder(metadata, &env).await;

        let file_meta = FileMeta {
            region_id,
            file_id: FileId::random(),
            file_size: 100,
            ..Default::default()
        };

        let file = FileHandle::new(file_meta.clone(), file_purger.clone());

        // Create mock task.
        let task = IndexBuildTask {
            region_id,
            file,
            target_region_metadata: version_control.current().version.metadata.clone(),
            source: IndexBuildSource::new(
                file_meta,
                version_control.current().version.metadata.schema_version,
            ),
            reason: IndexBuildType::Flush,
            access_layer: env.access_layer.clone(),
            listener: WorkerListener::default(),
            manifest_ctx,
            write_cache: None,
            cache_manager: None,
            file_purger,
            indexer_builder,
            request_sender: tx,
            index_options: IndexOptions::default(),
            result_senders: vec![result_tx],
        };

        // Schedule the build task and check result.
        scheduler.schedule_build(&version_control, task).await;
        match result_rx.recv().await.unwrap() {
            Ok(outcome) => {
                if outcome == IndexBuildOutcome::Finished {
                    panic!("Expect aborted result due to missing SST file")
                }
            }
            _ => panic!("Expect aborted result due to missing SST file"),
        }
    }

    #[tokio::test]
    async fn test_index_build_task_foreign_file_uses_target_metadata() {
        let env = SchedulerEnv::new().await;
        let mut scheduler = env.mock_index_build_scheduler(4);
        let source_metadata = Arc::new(sst_region_metadata());
        let mut target_metadata = (*source_metadata).clone();
        target_metadata.region_id = RegionId::new(1, 3);
        let mut target_builder = RegionMetadataBuilder::new(target_metadata.region_id);
        for mut column_metadata in target_metadata.column_metadatas.clone() {
            if column_metadata.column_id == 2 {
                column_metadata.column_schema =
                    column_metadata.column_schema.with_inverted_index(true);
            }
            target_builder.push_column_metadata(column_metadata);
        }
        let partition_expr = col("field_0")
            .gt_eq(Value::UInt64(100))
            .and(col("field_0").lt(Value::UInt64(200)));
        target_builder
            .primary_key(target_metadata.primary_key.clone())
            .partition_expr_json(Some(partition_expr.as_json_str().unwrap()))
            .bump_version();
        let target_metadata = Arc::new(target_builder.build().unwrap());
        let manifest_ctx = env.mock_manifest_context(target_metadata.clone()).await;
        let region_id = target_metadata.region_id;
        let file_purger = Arc::new(NoopFilePurger {});
        let sst_info = mock_sst_file(source_metadata.clone(), &env, IndexBuildMode::Async).await;
        let file_meta = FileMeta {
            region_id: source_metadata.region_id,
            file_id: sst_info.file_id,
            file_size: sst_info.file_size,
            max_row_group_uncompressed_size: sst_info.max_row_group_uncompressed_size,
            available_indexes: smallvec![IndexType::InvertedIndex],
            // Old manifests may publish an index without recording its size.
            index_file_size: 0,
            index_version: 0,
            num_rows: sst_info.num_rows as u64,
            num_row_groups: sst_info.num_row_groups,
            ..Default::default()
        };
        seed_manifest_file(&manifest_ctx, &file_meta).await;
        let files = HashMap::from([(file_meta.file_id, file_meta.clone())]);
        let version_control =
            mock_version_control(target_metadata.clone(), file_purger.clone(), files).await;
        let indexer_builder = mock_indexer_builder(target_metadata.clone(), &env).await;

        let file = FileHandle::new(file_meta.clone(), file_purger.clone());

        // Create mock task.
        let (tx, mut rx) = mpsc::channel(4);
        let (result_tx, _result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);
        let task = IndexBuildTask {
            region_id,
            file,
            target_region_metadata: version_control.current().version.metadata.clone(),
            source: IndexBuildSource::new(
                file_meta.clone(),
                version_control.current().version.metadata.schema_version,
            ),
            reason: IndexBuildType::Flush,
            access_layer: env.access_layer.clone(),
            listener: WorkerListener::default(),
            manifest_ctx,
            write_cache: None,
            cache_manager: None,
            file_purger,
            indexer_builder,
            request_sender: tx,
            index_options: IndexOptions::default(),
            result_senders: vec![result_tx],
        };

        scheduler.schedule_build(&version_control, task).await;

        // A notification should be sent to the worker to update the manifest.
        let worker_req = rx.recv().await.unwrap().request;
        match worker_req {
            WorkerRequest::Background {
                region_id: req_region_id,
                notify: BackgroundNotify::IndexBuildFinished(finished),
            } => {
                assert_eq!(req_region_id, region_id);
                let updated_meta = &finished.file_meta;

                // The mock indexer builder creates all index types.
                assert!(!updated_meta.available_indexes.is_empty());
                assert!(updated_meta.index_file_size > 0);
                assert_eq!(updated_meta.file_id, file_meta.file_id);
                assert_eq!(updated_meta.index_version, 1);
                let field_0_index = updated_meta
                    .indexes
                    .iter()
                    .find(|index| index.column_id == 2)
                    .expect("field_0 should have an inverted index");
                assert_eq!(
                    field_0_index.created_indexes.as_slice(),
                    [IndexType::InvertedIndex]
                );
            }
            _ => panic!("Unexpected worker request: {:?}", worker_req),
        }

        let puffin_reader = env
            .access_layer
            .build_puffin_manager()
            .reader(&RegionIndexId::new(
                RegionFileId::new(source_metadata.region_id, file_meta.file_id),
                1,
            ))
            .await
            .unwrap();
        let blob = puffin_reader
            .blob(inverted_index::INDEX_BLOB_TYPE)
            .await
            .unwrap();
        let blob_reader = blob.reader().await.unwrap();
        let index_metadata =
            index::inverted_index::format::reader::InvertedIndexBlobReader::new(blob_reader)
                .metadata(None)
                .await
                .unwrap();
        assert!(index_metadata.metas.contains_key("2"));
        assert_eq!(index_metadata.total_row_count, 100);
    }

    async fn schedule_index_build_task_with_mode(build_mode: IndexBuildMode) {
        let env = SchedulerEnv::new().await;
        let mut scheduler = env.mock_index_build_scheduler(4);
        let metadata = Arc::new(sst_region_metadata());
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        let file_purger = Arc::new(NoopFilePurger {});
        let region_id = metadata.region_id;
        let sst_info = mock_sst_file(metadata.clone(), &env, build_mode.clone()).await;
        let file_meta = FileMeta {
            region_id,
            file_id: sst_info.file_id,
            file_size: sst_info.file_size,
            max_row_group_uncompressed_size: sst_info.max_row_group_uncompressed_size,
            index_file_size: sst_info.index_metadata.file_size,
            num_rows: sst_info.num_rows as u64,
            num_row_groups: sst_info.num_row_groups,
            ..Default::default()
        };
        seed_manifest_file(&manifest_ctx, &file_meta).await;
        let files = HashMap::from([(file_meta.file_id, file_meta.clone())]);
        let version_control =
            mock_version_control(metadata.clone(), file_purger.clone(), files).await;
        let indexer_builder = mock_indexer_builder(metadata.clone(), &env).await;

        let file = FileHandle::new(file_meta.clone(), file_purger.clone());

        // Create mock task.
        let (tx, mut rx) = mpsc::channel(4);
        let (result_tx, _result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);
        let task = IndexBuildTask {
            region_id,
            file,
            target_region_metadata: version_control.current().version.metadata.clone(),
            source: IndexBuildSource::new(
                file_meta.clone(),
                version_control.current().version.metadata.schema_version,
            ),
            reason: IndexBuildType::Flush,
            access_layer: env.access_layer.clone(),
            listener: WorkerListener::default(),
            manifest_ctx,
            write_cache: None,
            cache_manager: None,
            file_purger,
            indexer_builder,
            request_sender: tx,
            index_options: IndexOptions::default(),
            result_senders: vec![result_tx],
        };

        scheduler.schedule_build(&version_control, task).await;

        let puffin_path = location::index_file_path(
            env.access_layer.table_dir(),
            RegionIndexId::new(RegionFileId::new(region_id, file_meta.file_id), 0),
            env.access_layer.path_type(),
        );

        if build_mode == IndexBuildMode::Async {
            // The index file should not exist before the task finishes.
            assert!(
                !env.access_layer
                    .object_store()
                    .exists(&puffin_path)
                    .await
                    .unwrap()
            );
        } else {
            // The index file should exist before the task finishes.
            assert!(
                env.access_layer
                    .object_store()
                    .exists(&puffin_path)
                    .await
                    .unwrap()
            );
        }

        // The worker is notified to apply the published index.
        recv_index_build_finished(&mut rx).await;

        // The index file should exist after the task finishes.
        assert!(
            env.access_layer
                .object_store()
                .exists(&puffin_path)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_index_build_task_build_mode() {
        schedule_index_build_task_with_mode(IndexBuildMode::Async).await;
        schedule_index_build_task_with_mode(IndexBuildMode::Sync).await;
    }

    #[tokio::test]
    async fn test_index_build_task_no_index() {
        let env = SchedulerEnv::new().await;
        let mut scheduler = env.mock_index_build_scheduler(4);
        let mut metadata = sst_region_metadata();
        // Unset indexes in metadata to simulate no index scenario.
        metadata.column_metadatas.iter_mut().for_each(|col| {
            col.column_schema.set_inverted_index(false);
            let _ = col.column_schema.unset_skipping_options();
        });
        let region_id = metadata.region_id;
        let metadata = Arc::new(metadata);
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        let file_purger = Arc::new(NoopFilePurger {});
        let sst_info = mock_sst_file(metadata.clone(), &env, IndexBuildMode::Async).await;
        let file_meta = FileMeta {
            region_id,
            file_id: sst_info.file_id,
            file_size: sst_info.file_size,
            max_row_group_uncompressed_size: sst_info.max_row_group_uncompressed_size,
            index_file_size: sst_info.index_metadata.file_size,
            num_rows: sst_info.num_rows as u64,
            num_row_groups: sst_info.num_row_groups,
            ..Default::default()
        };
        seed_manifest_file(&manifest_ctx, &file_meta).await;
        let files = HashMap::from([(file_meta.file_id, file_meta.clone())]);
        let version_control =
            mock_version_control(metadata.clone(), file_purger.clone(), files).await;
        let indexer_builder = mock_indexer_builder(metadata.clone(), &env).await;

        let file = FileHandle::new(file_meta.clone(), file_purger.clone());

        // Create mock task.
        let (tx, mut rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);
        let task = IndexBuildTask {
            region_id,
            file,
            target_region_metadata: version_control.current().version.metadata.clone(),
            source: IndexBuildSource::new(
                file_meta.clone(),
                version_control.current().version.metadata.schema_version,
            ),
            reason: IndexBuildType::Flush,
            access_layer: env.access_layer.clone(),
            listener: WorkerListener::default(),
            manifest_ctx,
            write_cache: None,
            cache_manager: None,
            file_purger,
            indexer_builder,
            request_sender: tx,
            index_options: IndexOptions::default(),
            result_senders: vec![result_tx],
        };

        scheduler.schedule_build(&version_control, task).await;

        // No index needs to be built, so the SST is not read.
        assert!(matches!(
            result_rx.recv().await.unwrap(),
            Ok(IndexBuildOutcome::Aborted(reason)) if reason.contains("No index to build")
        ));

        // Only the stop notification reaches the worker.
        assert!(matches!(
            rx.recv().await.unwrap().request,
            WorkerRequest::Background {
                notify: BackgroundNotify::IndexBuildStopped(_),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_index_build_task_with_write_cache() {
        let env = SchedulerEnv::new().await;
        let mut scheduler = env.mock_index_build_scheduler(4);
        let metadata = Arc::new(sst_region_metadata());
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        let file_purger = Arc::new(NoopFilePurger {});
        let region_id = metadata.region_id;

        let (dir, factory) = PuffinManagerFactory::new_for_test_async("test_write_cache").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

        // Create mock write cache
        let write_cache = Arc::new(
            WriteCache::new_fs(
                dir.path().to_str().unwrap(),
                ReadableSize::mb(10),
                None,
                None,
                true, // enable_background_worker
                factory,
                intm_manager,
                ReadableSize::mb(10),
            )
            .await
            .unwrap(),
        );
        // Indexer builder built from write cache.
        let indexer_builder = Arc::new(IndexerBuilderImpl {
            index_build_inputs: None,
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            puffin_manager: write_cache.build_puffin_manager().clone(),
            write_cache_enabled: true,
            intermediate_manager: write_cache.intermediate_manager().clone(),
            index_options: IndexOptions::default(),
            registry: Arc::new(crate::sst::index::registry::IndexRegistry::new(
                InvertedIndexConfig::default(),
                FulltextIndexConfig::default(),
                BloomFilterConfig::default(),
            )),
        });

        let sst_info = mock_sst_file(metadata.clone(), &env, IndexBuildMode::Async).await;
        let file_meta = FileMeta {
            region_id,
            file_id: sst_info.file_id,
            file_size: sst_info.file_size,
            index_file_size: sst_info.index_metadata.file_size,
            num_rows: sst_info.num_rows as u64,
            num_row_groups: sst_info.num_row_groups,
            ..Default::default()
        };
        seed_manifest_file(&manifest_ctx, &file_meta).await;
        let files = HashMap::from([(file_meta.file_id, file_meta.clone())]);
        let version_control =
            mock_version_control(metadata.clone(), file_purger.clone(), files).await;

        let file = FileHandle::new(file_meta.clone(), file_purger.clone());

        // Create mock task.
        let (tx, mut rx) = mpsc::channel(4);
        let (result_tx, _result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);
        let task = IndexBuildTask {
            region_id,
            file,
            target_region_metadata: version_control.current().version.metadata.clone(),
            source: IndexBuildSource::new(
                file_meta.clone(),
                version_control.current().version.metadata.schema_version,
            ),
            reason: IndexBuildType::Flush,
            access_layer: env.access_layer.clone(),
            listener: WorkerListener::default(),
            manifest_ctx,
            write_cache: Some(write_cache.clone()),
            cache_manager: None,
            file_purger,
            indexer_builder,
            request_sender: tx,
            index_options: IndexOptions::default(),
            result_senders: vec![result_tx],
        };

        scheduler.schedule_build(&version_control, task).await;

        recv_index_build_finished(&mut rx).await;

        // The write cache should contain the uploaded index file.
        let index_key = IndexKey::new(
            region_id,
            file_meta.file_id,
            FileType::Puffin(sst_info.index_metadata.version),
        );
        assert!(write_cache.file_cache().contains_key(&index_key));
    }

    async fn create_mock_task_for_schedule(
        env: &SchedulerEnv,
        file_id: FileId,
        region_id: RegionId,
        reason: IndexBuildType,
    ) -> IndexBuildTask {
        create_mock_task_for_schedule_with_result(env, file_id, region_id, reason)
            .await
            .0
    }

    /// Like [`create_mock_task_for_schedule`] but also returns the result receiver
    /// so tests can verify pending task cancellation errors.
    async fn create_mock_task_for_schedule_with_result(
        env: &SchedulerEnv,
        file_id: FileId,
        region_id: RegionId,
        reason: IndexBuildType,
    ) -> (IndexBuildTask, mpsc::Receiver<Result<IndexBuildOutcome>>) {
        let metadata = Arc::new(sst_region_metadata());
        let schema_version = metadata.schema_version;
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        let file_purger = Arc::new(NoopFilePurger {});
        let indexer_builder = mock_indexer_builder(metadata.clone(), env).await;
        let (tx, _rx) = mpsc::channel(4);
        let (result_tx, result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);

        let file_meta = FileMeta {
            region_id,
            file_id,
            file_size: 100,
            ..Default::default()
        };

        let file = FileHandle::new(file_meta.clone(), file_purger.clone());

        let task = IndexBuildTask {
            region_id,
            file,
            target_region_metadata: metadata,
            source: IndexBuildSource::new(file_meta, schema_version),
            reason,
            access_layer: env.access_layer.clone(),
            listener: WorkerListener::default(),
            manifest_ctx,
            write_cache: None,
            cache_manager: None,
            file_purger,
            indexer_builder,
            request_sender: tx,
            index_options: IndexOptions::default(),
            result_senders: vec![result_tx],
        };
        (task, result_rx)
    }

    #[tokio::test]
    async fn test_scheduler_coalesces_latest_schema_generation_per_sst() {
        let job_scheduler = Arc::new(VecScheduler::default());
        let env = SchedulerEnv::new().await.scheduler(job_scheduler.clone());
        let mut scheduler = env.mock_index_build_scheduler(2);
        let metadata = Arc::new(sst_region_metadata());
        let region_id = metadata.region_id;
        let file_id = FileId::random();
        let file_purger = Arc::new(NoopFilePurger {});
        let files = HashMap::from([(
            file_id,
            FileMeta {
                region_id,
                file_id,
                file_size: 100,
                ..Default::default()
            },
        )]);
        let version_control = mock_version_control(metadata, file_purger, files).await;

        let (active, _active_rx) = create_mock_task_for_schedule_with_result(
            &env,
            file_id,
            region_id,
            IndexBuildType::Manual,
        )
        .await;
        scheduler.schedule_build(&version_control, active).await;
        assert_eq!(job_scheduler.num_jobs(), 1);

        let (mut older, mut older_rx) = create_mock_task_for_schedule_with_result(
            &env,
            file_id,
            region_id,
            IndexBuildType::SchemaChange,
        )
        .await;
        older.source.schema_version = 1;
        scheduler.schedule_build(&version_control, older).await;

        let (mut latest, mut latest_rx) = create_mock_task_for_schedule_with_result(
            &env,
            file_id,
            region_id,
            IndexBuildType::SchemaChange,
        )
        .await;
        latest.source.schema_version = 2;
        scheduler.schedule_build(&version_control, latest).await;

        // The replaced task's caller waits for the surviving build instead of
        // being told the replaced build has completed.
        for rx in [&mut older_rx, &mut latest_rx] {
            assert!(matches!(
                rx.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }

        let status = &scheduler.region_status[&region_id];
        assert_eq!(status.building_files.len(), 1);
        assert_eq!(status.pending_tasks.len(), 1);
        let survivor = &status.pending_tasks.tasks[&file_id].task;
        assert_eq!(survivor.source.schema_version, 2);
        assert_eq!(survivor.result_senders.len(), 2);
        assert_eq!(job_scheduler.num_jobs(), 1);

        scheduler.on_task_stopped(region_id, file_id);
        let status = &scheduler.region_status[&region_id];
        assert_eq!(status.building_files.len(), 1);
        assert!(status.pending_tasks.is_empty());
        assert_eq!(job_scheduler.num_jobs(), 2);
    }

    #[tokio::test]
    async fn test_scheduler_completes_sender_when_job_is_rejected() {
        let job_scheduler = Arc::new(LocalScheduler::new(1));
        job_scheduler.stop(false).await.unwrap();
        let env = SchedulerEnv::new().await.scheduler(job_scheduler);
        let mut scheduler = env.mock_index_build_scheduler(1);
        let metadata = Arc::new(sst_region_metadata());
        let region_id = metadata.region_id;
        let file_id = FileId::random();
        let file_purger = Arc::new(NoopFilePurger {});
        let files = HashMap::from([(
            file_id,
            FileMeta {
                region_id,
                file_id,
                file_size: 100,
                ..Default::default()
            },
        )]);
        let version_control = mock_version_control(metadata, file_purger, files).await;
        let (task, mut result_rx) = create_mock_task_for_schedule_with_result(
            &env,
            file_id,
            region_id,
            IndexBuildType::Flush,
        )
        .await;

        scheduler.schedule_build(&version_control, task).await;

        let result = tokio::time::timeout(std::time::Duration::from_secs(5), result_rx.recv())
            .await
            .expect("scheduler rejection did not complete the result sender")
            .expect("result channel closed without a result");
        assert!(result.is_err());
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    #[tokio::test]
    async fn test_scheduler_comprehensive() {
        let env = SchedulerEnv::new().await;
        let mut scheduler = env.mock_index_build_scheduler(2);
        let metadata = Arc::new(sst_region_metadata());
        let region_id = metadata.region_id;
        let file_purger = Arc::new(NoopFilePurger {});

        // Prepare multiple files for testing
        let file_id1 = FileId::random();
        let file_id2 = FileId::random();
        let file_id3 = FileId::random();
        let file_id4 = FileId::random();
        let file_id5 = FileId::random();

        let mut files = HashMap::new();
        for file_id in [file_id1, file_id2, file_id3, file_id4, file_id5] {
            files.insert(
                file_id,
                FileMeta {
                    region_id,
                    file_id,
                    file_size: 100,
                    ..Default::default()
                },
            );
        }

        let version_control = mock_version_control(metadata, file_purger, files).await;

        // Test 1: Basic scheduling
        let task1 =
            create_mock_task_for_schedule(&env, file_id1, region_id, IndexBuildType::Flush).await;
        scheduler.schedule_build(&version_control, task1).await;
        assert!(scheduler.region_status.contains_key(&region_id));
        let status = scheduler.region_status.get(&region_id).unwrap();
        assert_eq!(status.building_files.len(), 1);
        assert!(status.building_files.contains(&file_id1));

        // Test 2: Duplicate file scheduling (should be skipped)
        let task1_dup =
            create_mock_task_for_schedule(&env, file_id1, region_id, IndexBuildType::Flush).await;
        scheduler.schedule_build(&version_control, task1_dup).await;
        let status = scheduler.region_status.get(&region_id).unwrap();
        assert_eq!(status.building_files.len(), 1); // Still only one

        // Test 3: Fill up to limit (2 building tasks)
        let task2 =
            create_mock_task_for_schedule(&env, file_id2, region_id, IndexBuildType::Flush).await;
        scheduler.schedule_build(&version_control, task2).await;
        let status = scheduler.region_status.get(&region_id).unwrap();
        assert_eq!(status.building_files.len(), 2); // Reached limit
        assert_eq!(status.pending_tasks.len(), 0);

        // Test 4: Add tasks with different priorities to pending queue
        // Now all new tasks will be pending since we reached the limit
        let task3 =
            create_mock_task_for_schedule(&env, file_id3, region_id, IndexBuildType::Compact).await;
        let task4 =
            create_mock_task_for_schedule(&env, file_id4, region_id, IndexBuildType::SchemaChange)
                .await;
        let task5 =
            create_mock_task_for_schedule(&env, file_id5, region_id, IndexBuildType::Manual).await;

        scheduler.schedule_build(&version_control, task3).await;
        scheduler.schedule_build(&version_control, task4).await;
        scheduler.schedule_build(&version_control, task5).await;

        let status = scheduler.region_status.get(&region_id).unwrap();
        assert_eq!(status.building_files.len(), 2); // Still at limit
        assert_eq!(status.pending_tasks.len(), 3); // Three pending

        // Test 5: Task completion triggers scheduling next highest priority task (Manual)
        scheduler.on_task_stopped(region_id, file_id1);
        let status = scheduler.region_status.get(&region_id).unwrap();
        assert!(!status.building_files.contains(&file_id1));
        assert_eq!(status.building_files.len(), 2); // Should schedule next task
        assert_eq!(status.pending_tasks.len(), 2); // One less pending
        // The highest priority task (Manual) should now be building
        assert!(status.building_files.contains(&file_id5));

        // Test 6: Complete another task, should schedule SchemaChange (second highest priority)
        scheduler.on_task_stopped(region_id, file_id2);
        let status = scheduler.region_status.get(&region_id).unwrap();
        assert_eq!(status.building_files.len(), 2);
        assert_eq!(status.pending_tasks.len(), 1); // One less pending
        assert!(status.building_files.contains(&file_id4)); // SchemaChange should be building

        // Test 7: Complete remaining tasks and cleanup
        scheduler.on_task_stopped(region_id, file_id5);
        scheduler.on_task_stopped(region_id, file_id4);

        let status = scheduler.region_status.get(&region_id).unwrap();
        assert_eq!(status.building_files.len(), 1); // Last task (Compact) should be building
        assert_eq!(status.pending_tasks.len(), 0);
        assert!(status.building_files.contains(&file_id3));

        scheduler.on_task_stopped(region_id, file_id3);

        // Region should be removed when all tasks complete
        assert!(!scheduler.region_status.contains_key(&region_id));
    }

    /// Helper to set up a scheduler with files_limit=1 and 3 scheduled tasks,
    /// returning the scheduler, the two pending-task result receivers, and the
    /// version control.
    async fn setup_scheduler_with_pending_tasks(
        env: &SchedulerEnv,
    ) -> (
        IndexBuildScheduler,
        mpsc::Receiver<Result<IndexBuildOutcome>>,
        mpsc::Receiver<Result<IndexBuildOutcome>>,
        VersionControlRef,
        RegionId,
        FileId, // building file_id for no-op assertion
    ) {
        let metadata = Arc::new(sst_region_metadata());
        let region_id = metadata.region_id;
        let file_purger = Arc::new(NoopFilePurger {});

        let file_id1 = FileId::random();
        let file_id2 = FileId::random();
        let file_id3 = FileId::random();

        let files = HashMap::from([
            (
                file_id1,
                FileMeta {
                    region_id,
                    file_id: file_id1,
                    file_size: 100,
                    ..Default::default()
                },
            ),
            (
                file_id2,
                FileMeta {
                    region_id,
                    file_id: file_id2,
                    file_size: 100,
                    ..Default::default()
                },
            ),
            (
                file_id3,
                FileMeta {
                    region_id,
                    file_id: file_id3,
                    file_size: 100,
                    ..Default::default()
                },
            ),
        ]);
        let version_control =
            mock_version_control(metadata.clone(), file_purger.clone(), files).await;

        let mut scheduler = env.mock_index_build_scheduler(1);

        // task1 becomes the "building" task (files_limit=1).
        // We intentionally drop its result receiver: the building task's late-stop
        // behavior is covered by the manual on_task_stopped no-op assertion below.
        let (task1, _rx1) = create_mock_task_for_schedule_with_result(
            env,
            file_id1,
            region_id,
            IndexBuildType::Flush,
        )
        .await;
        let (task2, rx2) = create_mock_task_for_schedule_with_result(
            env,
            file_id2,
            region_id,
            IndexBuildType::Flush,
        )
        .await;
        let (task3, rx3) = create_mock_task_for_schedule_with_result(
            env,
            file_id3,
            region_id,
            IndexBuildType::Flush,
        )
        .await;

        scheduler.schedule_build(&version_control, task1).await;
        scheduler.schedule_build(&version_control, task2).await;
        scheduler.schedule_build(&version_control, task3).await;

        // Verify: 1 building + 2 pending.
        assert!(scheduler.region_status.contains_key(&region_id));
        let status = scheduler.region_status.get(&region_id).unwrap();
        assert_eq!(status.building_files.len(), 1);
        assert_eq!(status.pending_tasks.len(), 2);

        (scheduler, rx2, rx3, version_control, region_id, file_id1)
    }

    /// Pattern‑matches a pending‑task cancellation error: outer **must** be
    /// [`crate::error::Error::BuildIndexAsync`] and the inner source **must** be
    /// the lifecycle variant named by `expected_source` (`"dropped"`, `"closed"`,
    /// or `"truncated"`).  Panics with a descriptive message on mismatch.
    fn assert_lifecycle_error(
        err: crate::error::Error,
        expected_source: &str,
        lifecycle_name: &str,
    ) {
        let crate::error::Error::BuildIndexAsync { source, .. } = err else {
            panic!("[{lifecycle_name}] Expected BuildIndexAsync outer error, got: {err:?}");
        };
        let actual = match &*source {
            crate::error::Error::RegionDropped { .. } => "dropped",
            crate::error::Error::RegionClosed { .. } => "closed",
            crate::error::Error::RegionTruncated { .. } => "truncated",
            other => panic!(
                "[{lifecycle_name}] Expected lifecycle source variant (RegionDropped/RegionClosed/RegionTruncated), got: {other:?}"
            ),
        };
        assert_eq!(
            actual, expected_source,
            "[{lifecycle_name}] Source variant mismatch"
        );
    }

    /// Receives a pending‑task error from `rx` with a 5‑second timeout, then
    /// delegates to [`assert_lifecycle_error`].
    async fn recv_lifecycle_error(
        rx: &mut mpsc::Receiver<Result<IndexBuildOutcome>>,
        expected_source: &str,
        lifecycle_name: &str,
    ) {
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "[{lifecycle_name}] Timeout (5s) waiting for lifecycle error from pending task"
                )
            });
        let result = result.unwrap_or_else(|| {
            panic!("[{lifecycle_name}] Channel closed without receiving lifecycle error")
        });
        let err = result.unwrap_err();
        assert_lifecycle_error(err, expected_source, lifecycle_name);
    }

    #[tokio::test]
    async fn test_scheduler_lifecycle_cleanup() {
        let env = SchedulerEnv::new().await;

        // --- on_region_dropped ---
        {
            let (mut scheduler, mut rx2, mut rx3, _version_control, region_id, building_file_id) =
                setup_scheduler_with_pending_tasks(&env).await;

            scheduler.on_region_dropped(region_id).await;

            let status = scheduler.region_status.get(&region_id).unwrap();
            assert!(status.retiring);
            assert_eq!(status.building_files.len(), 1);
            assert!(status.pending_tasks.is_empty());

            // Pending-task receivers get lifecycle errors (with timeout).
            recv_lifecycle_error(&mut rx2, "dropped", "on_region_dropped").await;
            recv_lifecycle_error(&mut rx3, "dropped", "on_region_dropped").await;

            // The active build keeps its lease until it stops.
            scheduler.on_task_stopped(region_id, building_file_id);
            assert!(!scheduler.region_status.contains_key(&region_id));
        }

        // --- on_region_closed ---
        {
            let (mut scheduler, mut rx2, mut rx3, version_control, region_id, building_file_id) =
                setup_scheduler_with_pending_tasks(&env).await;

            scheduler.on_region_closed(region_id).await;

            let status = scheduler.region_status.get(&region_id).unwrap();
            assert!(status.retiring);
            assert_eq!(status.building_files.len(), 1);
            assert!(status.pending_tasks.is_empty());

            recv_lifecycle_error(&mut rx2, "closed", "on_region_closed").await;
            recv_lifecycle_error(&mut rx3, "closed", "on_region_closed").await;

            // A build scheduled by the reopened region waits behind the old
            // incarnation's active lease.
            let (reopened_task, _reopened_rx) = create_mock_task_for_schedule_with_result(
                &env,
                building_file_id,
                region_id,
                IndexBuildType::Manual,
            )
            .await;
            scheduler
                .schedule_build(&version_control, reopened_task)
                .await;
            let status = scheduler.region_status.get(&region_id).unwrap();
            assert!(status.retiring);
            assert_eq!(status.building_files.len(), 1);
            assert_eq!(status.pending_tasks.len(), 1);

            scheduler.on_task_stopped(region_id, building_file_id);
            let status = scheduler.region_status.get(&region_id).unwrap();
            assert!(!status.retiring);
            assert_eq!(status.building_files.len(), 1);
            assert!(status.pending_tasks.is_empty());

            scheduler.on_task_stopped(region_id, building_file_id);
            assert!(!scheduler.region_status.contains_key(&region_id));
        }

        // --- on_region_truncated ---
        {
            let (mut scheduler, mut rx2, mut rx3, _version_control, region_id, building_file_id) =
                setup_scheduler_with_pending_tasks(&env).await;

            scheduler.on_region_truncated(region_id).await;

            let status = scheduler.region_status.get(&region_id).unwrap();
            assert!(status.retiring);
            assert_eq!(status.building_files.len(), 1);
            assert!(status.pending_tasks.is_empty());

            recv_lifecycle_error(&mut rx2, "truncated", "on_region_truncated").await;
            recv_lifecycle_error(&mut rx3, "truncated", "on_region_truncated").await;

            scheduler.on_task_stopped(region_id, building_file_id);
            assert!(!scheduler.region_status.contains_key(&region_id));
        }
    }
}
