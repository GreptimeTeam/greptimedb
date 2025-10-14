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
pub(crate) mod fulltext_index;
mod indexer;
pub mod intermediate;
pub(crate) mod inverted_index;
pub mod puffin_manager;
mod statistics;
pub(crate) mod store;

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bloom_filter::creator::BloomFilterIndexer;
use common_telemetry::{debug, info, warn};
use datatypes::arrow::record_batch::RecordBatch;
use puffin_manager::SstPuffinManager;
use smallvec::{SmallVec, smallvec};
use snafu::ResultExt;
use statistics::{ByteCount, RowCount};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, FileId, RegionId};
use strum::IntoStaticStr;
use tokio::sync::mpsc::Sender;

use crate::access_layer::{AccessLayerRef, FilePathProvider, OperationType, RegionFilePathFactory};
use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::write_cache::{UploadTracker, WriteCacheRef};
use crate::config::{BloomFilterConfig, FulltextIndexConfig, InvertedIndexConfig};
use crate::error::{BuildIndexAsyncSnafu, Error, Result};
use crate::manifest::action::{RegionEdit, RegionMetaAction, RegionMetaActionList};
use crate::metrics::INDEX_CREATE_MEMORY_USAGE;
use crate::read::{Batch, BatchReader};
use crate::region::options::IndexOptions;
use crate::region::version::{VersionControlData, VersionControlRef, VersionRef};
use crate::region::{ManifestContextRef, RegionLeaderState};
use crate::request::{
    BackgroundNotify, IndexBuildFailed, IndexBuildFinished, WorkerRequest, WorkerRequestWithTime,
};
use crate::schedule::scheduler::{Job, SchedulerRef};
use crate::sst::file::{
    ColumnIndexMetadata, FileHandle, FileMeta, IndexType, IndexTypes, RegionFileId,
};
use crate::sst::file_purger::FilePurgerRef;
use crate::sst::index::fulltext_index::creator::FulltextIndexer;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::inverted_index::creator::InvertedIndexer;
use crate::sst::parquet::SstInfo;

pub(crate) const TYPE_INVERTED_INDEX: &str = "inverted_index";
pub(crate) const TYPE_FULLTEXT_INDEX: &str = "fulltext_index";
pub(crate) const TYPE_BLOOM_FILTER_INDEX: &str = "bloom_filter_index";

/// Output of the index creation.
#[derive(Debug, Clone, Default)]
pub struct IndexOutput {
    /// Size of the file.
    pub file_size: u64,
    /// Inverted index output.
    pub inverted_index: InvertedIndexOutput,
    /// Fulltext index output.
    pub fulltext_index: FulltextIndexOutput,
    /// Bloom filter output.
    pub bloom_filter: BloomFilterOutput,
}

impl IndexOutput {
    pub fn build_available_indexes(&self) -> SmallVec<[IndexType; 4]> {
        let mut indexes = SmallVec::new();
        if self.inverted_index.is_available() {
            indexes.push(IndexType::InvertedIndex);
        }
        if self.fulltext_index.is_available() {
            indexes.push(IndexType::FulltextIndex);
        }
        if self.bloom_filter.is_available() {
            indexes.push(IndexType::BloomFilterIndex);
        }
        indexes
    }

    pub fn build_indexes(&self) -> Vec<ColumnIndexMetadata> {
        let mut map: HashMap<ColumnId, IndexTypes> = HashMap::new();

        if self.inverted_index.is_available() {
            for &col in &self.inverted_index.columns {
                map.entry(col).or_default().push(IndexType::InvertedIndex);
            }
        }
        if self.fulltext_index.is_available() {
            for &col in &self.fulltext_index.columns {
                map.entry(col).or_default().push(IndexType::FulltextIndex);
            }
        }
        if self.bloom_filter.is_available() {
            for &col in &self.bloom_filter.columns {
                map.entry(col)
                    .or_default()
                    .push(IndexType::BloomFilterIndex);
            }
        }

        map.into_iter()
            .map(|(column_id, created_indexes)| ColumnIndexMetadata {
                column_id,
                created_indexes,
            })
            .collect::<Vec<_>>()
    }
}

/// Base output of the index creation.
#[derive(Debug, Clone, Default)]
pub struct IndexBaseOutput {
    /// Size of the index.
    pub index_size: ByteCount,
    /// Number of rows in the index.
    pub row_count: RowCount,
    /// Available columns in the index.
    pub columns: Vec<ColumnId>,
}

impl IndexBaseOutput {
    pub fn is_available(&self) -> bool {
        self.index_size > 0
    }
}

/// Output of the inverted index creation.
pub type InvertedIndexOutput = IndexBaseOutput;
/// Output of the fulltext index creation.
pub type FulltextIndexOutput = IndexBaseOutput;
/// Output of the bloom filter creation.
pub type BloomFilterOutput = IndexBaseOutput;

/// The index creator that hides the error handling details.
#[derive(Default)]
pub struct Indexer {
    file_id: FileId,
    region_id: RegionId,
    puffin_manager: Option<SstPuffinManager>,
    inverted_indexer: Option<InvertedIndexer>,
    last_mem_inverted_index: usize,
    fulltext_indexer: Option<FulltextIndexer>,
    last_mem_fulltext_index: usize,
    bloom_filter_indexer: Option<BloomFilterIndexer>,
    last_mem_bloom_filter: usize,
    intermediate_manager: Option<IntermediateManager>,
}

impl Indexer {
    /// Updates the index with the given batch.
    pub async fn update(&mut self, batch: &mut Batch) {
        self.do_update(batch).await;

        self.flush_mem_metrics();
    }

    /// Updates the index with the given flat format RecordBatch.
    pub async fn update_flat(&mut self, batch: &RecordBatch) {
        self.do_update_flat(batch).await;

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

    fn flush_mem_metrics(&mut self) {
        let inverted_mem = self
            .inverted_indexer
            .as_ref()
            .map_or(0, |creator| creator.memory_usage());
        INDEX_CREATE_MEMORY_USAGE
            .with_label_values(&[TYPE_INVERTED_INDEX])
            .add(inverted_mem as i64 - self.last_mem_inverted_index as i64);
        self.last_mem_inverted_index = inverted_mem;

        let fulltext_mem = self
            .fulltext_indexer
            .as_ref()
            .map_or(0, |creator| creator.memory_usage());
        INDEX_CREATE_MEMORY_USAGE
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .add(fulltext_mem as i64 - self.last_mem_fulltext_index as i64);
        self.last_mem_fulltext_index = fulltext_mem;

        let bloom_filter_mem = self
            .bloom_filter_indexer
            .as_ref()
            .map_or(0, |creator| creator.memory_usage());
        INDEX_CREATE_MEMORY_USAGE
            .with_label_values(&[TYPE_BLOOM_FILTER_INDEX])
            .add(bloom_filter_mem as i64 - self.last_mem_bloom_filter as i64);
        self.last_mem_bloom_filter = bloom_filter_mem;
    }
}

#[async_trait::async_trait]
pub trait IndexerBuilder {
    /// Builds indexer of given file id to [index_file_path].
    async fn build(&self, file_id: FileId) -> Indexer;
}
#[derive(Clone)]
pub(crate) struct IndexerBuilderImpl {
    pub(crate) build_type: IndexBuildType,
    pub(crate) metadata: RegionMetadataRef,
    pub(crate) row_group_size: usize,
    pub(crate) puffin_manager: SstPuffinManager,
    pub(crate) intermediate_manager: IntermediateManager,
    pub(crate) index_options: IndexOptions,
    pub(crate) inverted_index_config: InvertedIndexConfig,
    pub(crate) fulltext_index_config: FulltextIndexConfig,
    pub(crate) bloom_filter_index_config: BloomFilterConfig,
}

#[async_trait::async_trait]
impl IndexerBuilder for IndexerBuilderImpl {
    /// Sanity check for arguments and create a new [Indexer] if arguments are valid.
    async fn build(&self, file_id: FileId) -> Indexer {
        let mut indexer = Indexer {
            file_id,
            region_id: self.metadata.region_id,
            ..Default::default()
        };

        indexer.inverted_indexer = self.build_inverted_indexer(file_id);
        indexer.fulltext_indexer = self.build_fulltext_indexer(file_id).await;
        indexer.bloom_filter_indexer = self.build_bloom_filter_indexer(file_id);
        indexer.intermediate_manager = Some(self.intermediate_manager.clone());
        if indexer.inverted_indexer.is_none()
            && indexer.fulltext_indexer.is_none()
            && indexer.bloom_filter_indexer.is_none()
        {
            indexer.abort().await;
            return Indexer::default();
        }

        indexer.puffin_manager = Some(self.puffin_manager.clone());
        indexer
    }
}

impl IndexerBuilderImpl {
    fn build_inverted_indexer(&self, file_id: FileId) -> Option<InvertedIndexer> {
        let create = match self.build_type {
            IndexBuildType::Flush => self.inverted_index_config.create_on_flush.auto(),
            IndexBuildType::Compact => self.inverted_index_config.create_on_compaction.auto(),
            _ => true,
        };

        if !create {
            debug!(
                "Skip creating inverted index due to config, region_id: {}, file_id: {}",
                self.metadata.region_id, file_id,
            );
            return None;
        }

        let indexed_column_ids = self.metadata.inverted_indexed_column_ids(
            self.index_options.inverted_index.ignore_column_ids.iter(),
        );
        if indexed_column_ids.is_empty() {
            debug!(
                "No columns to be indexed, skip creating inverted index, region_id: {}, file_id: {}",
                self.metadata.region_id, file_id,
            );
            return None;
        }

        let Some(mut segment_row_count) =
            NonZeroUsize::new(self.index_options.inverted_index.segment_row_count)
        else {
            warn!(
                "Segment row count is 0, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, file_id,
            );
            return None;
        };

        let Some(row_group_size) = NonZeroUsize::new(self.row_group_size) else {
            warn!(
                "Row group size is 0, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, file_id,
            );
            return None;
        };

        // if segment row count not aligned with row group size, adjust it to be aligned.
        if row_group_size.get() % segment_row_count.get() != 0 {
            segment_row_count = row_group_size;
        }

        let indexer = InvertedIndexer::new(
            file_id,
            &self.metadata,
            self.intermediate_manager.clone(),
            self.inverted_index_config.mem_threshold_on_create(),
            segment_row_count,
            indexed_column_ids,
        );

        Some(indexer)
    }

    async fn build_fulltext_indexer(&self, file_id: FileId) -> Option<FulltextIndexer> {
        let create = match self.build_type {
            IndexBuildType::Flush => self.fulltext_index_config.create_on_flush.auto(),
            IndexBuildType::Compact => self.fulltext_index_config.create_on_compaction.auto(),
            _ => true,
        };

        if !create {
            debug!(
                "Skip creating full-text index due to config, region_id: {}, file_id: {}",
                self.metadata.region_id, file_id,
            );
            return None;
        }

        let mem_limit = self.fulltext_index_config.mem_threshold_on_create();
        let creator = FulltextIndexer::new(
            &self.metadata.region_id,
            &file_id,
            &self.intermediate_manager,
            &self.metadata,
            self.fulltext_index_config.compress,
            mem_limit,
        )
        .await;

        let err = match creator {
            Ok(creator) => {
                if creator.is_none() {
                    debug!(
                        "Skip creating full-text index due to no columns require indexing, region_id: {}, file_id: {}",
                        self.metadata.region_id, file_id,
                    );
                }
                return creator;
            }
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to create full-text indexer, region_id: {}, file_id: {}, err: {:?}",
                self.metadata.region_id, file_id, err
            );
        } else {
            warn!(
                err; "Failed to create full-text indexer, region_id: {}, file_id: {}",
                self.metadata.region_id, file_id,
            );
        }

        None
    }

    fn build_bloom_filter_indexer(&self, file_id: FileId) -> Option<BloomFilterIndexer> {
        let create = match self.build_type {
            IndexBuildType::Flush => self.bloom_filter_index_config.create_on_flush.auto(),
            IndexBuildType::Compact => self.bloom_filter_index_config.create_on_compaction.auto(),
            _ => true,
        };

        if !create {
            debug!(
                "Skip creating bloom filter due to config, region_id: {}, file_id: {}",
                self.metadata.region_id, file_id,
            );
            return None;
        }

        let mem_limit = self.bloom_filter_index_config.mem_threshold_on_create();
        let indexer = BloomFilterIndexer::new(
            file_id,
            &self.metadata,
            self.intermediate_manager.clone(),
            mem_limit,
        );

        let err = match indexer {
            Ok(indexer) => {
                if indexer.is_none() {
                    debug!(
                        "Skip creating bloom filter due to no columns require indexing, region_id: {}, file_id: {}",
                        self.metadata.region_id, file_id,
                    );
                }
                return indexer;
            }
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to create bloom filter, region_id: {}, file_id: {}, err: {:?}",
                self.metadata.region_id, file_id, err
            );
        } else {
            warn!(
                err; "Failed to create bloom filter, region_id: {}, file_id: {}",
                self.metadata.region_id, file_id,
            );
        }

        None
    }
}

/// Type of an index build task.
#[derive(Debug, Clone, PartialEq, IntoStaticStr)]
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
/// Mpsc output result sender.
#[derive(Debug)]
pub struct ResultMpscSender(Sender<Result<IndexBuildOutcome>>);

impl ResultMpscSender {
    pub fn new(tx: Sender<Result<IndexBuildOutcome>>) -> Self {
        Self(tx)
    }
}

pub struct IndexBuildTask {
    /// The file meta to build index for.
    pub file_meta: FileMeta,
    pub reason: IndexBuildType,
    pub access_layer: AccessLayerRef,
    pub(crate) manifest_ctx: ManifestContextRef,
    pub write_cache: Option<WriteCacheRef>,
    pub file_purger: FilePurgerRef,
    /// When write cache is enabled, the indexer builder should be built from the write cache.
    /// Otherwise, it should be built from the access layer.
    pub indexer_builder: Arc<dyn IndexerBuilder + Send + Sync>,
    /// Request sender to notify the region worker.
    pub(crate) request_sender: Sender<WorkerRequestWithTime>,
    /// Index build result sender.
    pub(crate) result_sender: ResultMpscSender,
}

impl IndexBuildTask {
    /// Notify the caller the job is success.
    pub async fn on_success(&mut self, outcome: IndexBuildOutcome) {
        let _ = self.result_sender.0.send(Ok(outcome)).await;
    }

    /// Send index build error to waiter.
    pub async fn on_failure(&mut self, err: Arc<Error>) {
        let _ = self
            .result_sender
            .0
            .send(Err(err.clone()).context(BuildIndexAsyncSnafu {
                region_id: self.file_meta.region_id,
            }))
            .await;
    }

    fn into_index_build_job(mut self, version_control: &VersionControlRef) -> Job {
        let version_data = version_control.current();

        Box::pin(async move {
            self.do_index_build(version_data).await;
        })
    }

    async fn do_index_build(&mut self, version_data: VersionControlData) {
        match self.index_build(&version_data).await {
            Ok(outcome) => self.on_success(outcome).await,
            Err(e) => {
                warn!(
                    e; "Index build task failed, region: {}, file_id: {}",
                    self.file_meta.region_id, self.file_meta.file_id,
                );
                self.on_failure(e.into()).await
            }
        };
    }

    // Checks if the SST file still exists in object store and version to avoid conflict with compaction.
    async fn check_sst_file_exists(&self, version: &VersionRef) -> bool {
        let region_id = self.file_meta.region_id;
        let file_id = self.file_meta.file_id;

        let found_in_version = version
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files.iter())
            .any(|(id, handle)| {
                *id == self.file_meta.file_id && !handle.is_deleted() && !handle.compacting()
            });
        if !found_in_version {
            warn!(
                "File id {} not found in region version for index build, region: {}",
                file_id, region_id
            );
            false
        } else {
            // If the file's metadata is present in the current version, the physical SST file
            // is guaranteed to exist on object store. The file purger removes the physical
            // file only after its metadata is removed from the version.
            true
        }
    }

    async fn index_build(
        &mut self,
        version_data: &VersionControlData,
    ) -> Result<IndexBuildOutcome> {
        let version = &version_data.version;
        let mut indexer = self.indexer_builder.build(self.file_meta.file_id).await;

        // Check SST file existence before building index to avoid failure of parquet reader.
        if !self.check_sst_file_exists(version).await {
            // Calls abort to clean up index files.
            indexer.abort().await;
            return Ok(IndexBuildOutcome::Aborted(format!(
                "SST file not found during index build, region: {}, file_id: {}",
                self.file_meta.region_id, self.file_meta.file_id
            )));
        }

        let mut parquet_reader = self
            .access_layer
            .read_sst(FileHandle::new(
                self.file_meta.clone(),
                self.file_purger.clone(),
            ))
            .build()
            .await?;

        // TODO(SNC123): optimize index batch
        loop {
            match parquet_reader.next_batch().await {
                Ok(Some(batch)) => {
                    indexer.update(&mut batch.clone()).await;
                }
                Ok(None) => break,
                Err(e) => {
                    indexer.abort().await;
                    return Err(e);
                }
            }
        }
        let index_output = indexer.finish().await;

        if index_output.file_size > 0 {
            // Check SST file existence again after building index.
            if !self.check_sst_file_exists(version).await {
                // Calls abort to clean up index files.
                indexer.abort().await;
                return Ok(IndexBuildOutcome::Aborted(format!(
                    "SST file not found during index build, region: {}, file_id: {}",
                    self.file_meta.region_id, self.file_meta.file_id
                )));
            }

            // Upload index file if write cache is enabled.
            self.maybe_upload_index_file(index_output.clone()).await?;

            let worker_request = match self.update_manifest(index_output).await {
                Ok(edit) => {
                    let index_build_finished = IndexBuildFinished {
                        region_id: self.file_meta.region_id,
                        edit,
                    };
                    WorkerRequest::Background {
                        region_id: self.file_meta.region_id,
                        notify: BackgroundNotify::IndexBuildFinished(index_build_finished),
                    }
                }
                Err(e) => {
                    let err = Arc::new(e);
                    WorkerRequest::Background {
                        region_id: self.file_meta.region_id,
                        notify: BackgroundNotify::IndexBuildFailed(IndexBuildFailed { err }),
                    }
                }
            };

            let _ = self
                .request_sender
                .send(WorkerRequestWithTime::new(worker_request))
                .await;
        }
        Ok(IndexBuildOutcome::Finished)
    }

    async fn maybe_upload_index_file(&self, output: IndexOutput) -> Result<()> {
        if let Some(write_cache) = &self.write_cache {
            let file_id = self.file_meta.file_id;
            let region_id = self.file_meta.region_id;
            let remote_store = self.access_layer.object_store();
            let mut upload_tracker = UploadTracker::new(region_id);
            let mut err = None;
            let puffin_key = IndexKey::new(region_id, file_id, FileType::Puffin);
            let puffin_path = RegionFilePathFactory::new(
                self.access_layer.table_dir().to_string(),
                self.access_layer.path_type(),
            )
            .build_index_file_path(RegionFileId::new(region_id, file_id));
            if let Err(e) = write_cache
                .upload(puffin_key, &puffin_path, remote_store)
                .await
            {
                err = Some(e);
            }
            upload_tracker.push_uploaded_file(puffin_path);
            if let Some(err) = err {
                // Cleans index files on failure.
                upload_tracker
                    .clean(
                        &smallvec![SstInfo {
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

    async fn update_manifest(&mut self, output: IndexOutput) -> Result<RegionEdit> {
        self.file_meta.available_indexes = output.build_available_indexes();
        self.file_meta.indexes = output.build_indexes();
        self.file_meta.index_file_size = output.file_size;
        let edit = RegionEdit {
            files_to_add: vec![self.file_meta.clone()],
            files_to_remove: vec![],
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            flushed_sequence: None,
            flushed_entry_id: None,
            committed_sequence: None,
            compaction_time_window: None,
        };
        let version = self
            .manifest_ctx
            .update_manifest(
                RegionLeaderState::Writable,
                RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone())),
            )
            .await?;
        info!(
            "Successfully update manifest version to {version}, region: {}, reason: {}",
            self.file_meta.region_id,
            self.reason.as_str()
        );
        Ok(edit)
    }
}

#[derive(Clone)]
pub struct IndexBuildScheduler {
    scheduler: SchedulerRef,
}

impl IndexBuildScheduler {
    pub fn new(scheduler: SchedulerRef) -> Self {
        IndexBuildScheduler { scheduler }
    }

    pub(crate) fn schedule_build(
        &mut self,
        version_control: &VersionControlRef,
        task: IndexBuildTask,
    ) -> Result<()> {
        let job = task.into_index_build_job(version_control);
        self.scheduler.schedule(job)?;
        Ok(())
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
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use puffin_manager::PuffinManagerFactory;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use tokio::sync::mpsc;

    use super::*;
    use crate::access_layer::{FilePathProvider, SstWriteRequest, WriteType};
    use crate::cache::write_cache::WriteCache;
    use crate::config::{FulltextIndexConfig, IndexBuildMode, MitoConfig, Mode};
    use crate::memtable::time_partition::TimePartitions;
    use crate::region::version::{VersionBuilder, VersionControl};
    use crate::sst::file::RegionFileId;
    use crate::sst::file_purger::NoopFilePurger;
    use crate::sst::location;
    use crate::sst::parquet::WriteOptions;
    use crate::test_util::memtable_util::EmptyMemtableBuilder;
    use crate::test_util::scheduler_util::SchedulerEnv;
    use crate::test_util::sst_util::{new_batch_by_range, new_source, sst_region_metadata};

    struct MetaConfig {
        with_inverted: bool,
        with_fulltext: bool,
        with_skipping_bloom: bool,
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
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    async fn mock_intm_mgr(path: impl AsRef<str>) -> IntermediateManager {
        IntermediateManager::init_fs(path).await.unwrap()
    }
    struct NoopPathProvider;

    impl FilePathProvider for NoopPathProvider {
        fn build_index_file_path(&self, _file_id: RegionFileId) -> String {
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
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);
        let mut index_config = MitoConfig::default().index;
        index_config.build_mode = build_mode;
        let write_request = SstWriteRequest {
            op_type: OperationType::Flush,
            metadata: metadata.clone(),
            source: either::Left(source),
            storage: None,
            max_sequence: None,
            cache_manager: Default::default(),
            index_options: IndexOptions::default(),
            index_config,
            inverted_index_config: Default::default(),
            fulltext_index_config: Default::default(),
            bloom_filter_index_config: Default::default(),
        };
        env.access_layer
            .write_sst(write_request, &WriteOptions::default(), WriteType::Flush)
            .await
            .unwrap()
            .0
            .remove(0)
    }

    async fn mock_version_control(
        metadata: RegionMetadataRef,
        file_purger: FilePurgerRef,
        files: HashMap<FileId, FileMeta>,
    ) -> VersionControlRef {
        let mutable = Arc::new(TimePartitions::new(
            metadata.clone(),
            Arc::new(EmptyMemtableBuilder::default()),
            0,
            None,
        ));
        let version_builder = VersionBuilder::new(metadata, mutable)
            .add_files(file_purger, files.values().cloned())
            .build();
        Arc::new(VersionControl::new(version_builder))
    }

    async fn mock_indexer_builder(
        metadata: RegionMetadataRef,
        env: &SchedulerEnv,
    ) -> Arc<dyn IndexerBuilder + Send + Sync> {
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
            build_type: IndexBuildType::Flush,
            metadata,
            row_group_size: 1024,
            puffin_manager,
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
            bloom_filter_index_config: BloomFilterConfig::default(),
        })
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
            build_type: IndexBuildType::Flush,
            metadata,
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
            bloom_filter_index_config: BloomFilterConfig::default(),
        }
        .build(FileId::random())
        .await;

        assert!(indexer.inverted_indexer.is_some());
        assert!(indexer.fulltext_indexer.is_some());
        assert!(indexer.bloom_filter_indexer.is_some());
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
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            intermediate_manager: intm_manager.clone(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig {
                create_on_flush: Mode::Disable,
                ..Default::default()
            },
            fulltext_index_config: FulltextIndexConfig::default(),
            bloom_filter_index_config: BloomFilterConfig::default(),
        }
        .build(FileId::random())
        .await;

        assert!(indexer.inverted_indexer.is_none());
        assert!(indexer.fulltext_indexer.is_some());
        assert!(indexer.bloom_filter_indexer.is_some());

        let indexer = IndexerBuilderImpl {
            build_type: IndexBuildType::Compact,
            metadata: metadata.clone(),
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            intermediate_manager: intm_manager.clone(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig {
                create_on_compaction: Mode::Disable,
                ..Default::default()
            },
            bloom_filter_index_config: BloomFilterConfig::default(),
        }
        .build(FileId::random())
        .await;

        assert!(indexer.inverted_indexer.is_some());
        assert!(indexer.fulltext_indexer.is_none());
        assert!(indexer.bloom_filter_indexer.is_some());

        let indexer = IndexerBuilderImpl {
            build_type: IndexBuildType::Compact,
            metadata,
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
            bloom_filter_index_config: BloomFilterConfig {
                create_on_compaction: Mode::Disable,
                ..Default::default()
            },
        }
        .build(FileId::random())
        .await;

        assert!(indexer.inverted_indexer.is_some());
        assert!(indexer.fulltext_indexer.is_some());
        assert!(indexer.bloom_filter_indexer.is_none());
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
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            intermediate_manager: intm_manager.clone(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
            bloom_filter_index_config: BloomFilterConfig::default(),
        }
        .build(FileId::random())
        .await;

        assert!(indexer.inverted_indexer.is_none());
        assert!(indexer.fulltext_indexer.is_some());
        assert!(indexer.bloom_filter_indexer.is_some());

        let metadata = mock_region_metadata(MetaConfig {
            with_inverted: true,
            with_fulltext: false,
            with_skipping_bloom: true,
        });
        let indexer = IndexerBuilderImpl {
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            intermediate_manager: intm_manager.clone(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
            bloom_filter_index_config: BloomFilterConfig::default(),
        }
        .build(FileId::random())
        .await;

        assert!(indexer.inverted_indexer.is_some());
        assert!(indexer.fulltext_indexer.is_none());
        assert!(indexer.bloom_filter_indexer.is_some());

        let metadata = mock_region_metadata(MetaConfig {
            with_inverted: true,
            with_fulltext: true,
            with_skipping_bloom: false,
        });
        let indexer = IndexerBuilderImpl {
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
            bloom_filter_index_config: BloomFilterConfig::default(),
        }
        .build(FileId::random())
        .await;

        assert!(indexer.inverted_indexer.is_some());
        assert!(indexer.fulltext_indexer.is_some());
        assert!(indexer.bloom_filter_indexer.is_none());
    }

    #[tokio::test]
    async fn test_build_indexer_zero_row_group() {
        let (dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_zero_row_group_").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

        let metadata = mock_region_metadata(MetaConfig {
            with_inverted: true,
            with_fulltext: true,
            with_skipping_bloom: true,
        });
        let indexer = IndexerBuilderImpl {
            build_type: IndexBuildType::Flush,
            metadata,
            row_group_size: 0,
            puffin_manager: factory.build(mock_object_store(), NoopPathProvider),
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
            bloom_filter_index_config: BloomFilterConfig::default(),
        }
        .build(FileId::random())
        .await;

        assert!(indexer.inverted_indexer.is_none());
    }

    #[tokio::test]
    async fn test_index_build_task_sst_not_exist() {
        let env = SchedulerEnv::new().await;
        let (tx, _rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);
        let mut scheduler = env.mock_index_build_scheduler();
        let metadata = Arc::new(sst_region_metadata());
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        let file_purger = Arc::new(NoopFilePurger {});
        let files = HashMap::new();
        let version_control =
            mock_version_control(metadata.clone(), file_purger.clone(), files).await;
        let region_id = metadata.region_id;
        let indexer_builder = mock_indexer_builder(metadata, &env).await;

        // Create mock task.
        let task = IndexBuildTask {
            file_meta: FileMeta {
                region_id,
                file_id: FileId::random(),
                file_size: 100,
                ..Default::default()
            },
            reason: IndexBuildType::Flush,
            access_layer: env.access_layer.clone(),
            manifest_ctx,
            write_cache: None,
            file_purger,
            indexer_builder,
            request_sender: tx,
            result_sender: ResultMpscSender(result_tx),
        };

        // Schedule the build task and check result.
        scheduler.schedule_build(&version_control, task).unwrap();
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
    async fn test_index_build_task_sst_exist() {
        let env = SchedulerEnv::new().await;
        let mut scheduler = env.mock_index_build_scheduler();
        let metadata = Arc::new(sst_region_metadata());
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        let region_id = metadata.region_id;
        let file_purger = Arc::new(NoopFilePurger {});
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
        let files = HashMap::from([(file_meta.file_id, file_meta.clone())]);
        let version_control =
            mock_version_control(metadata.clone(), file_purger.clone(), files).await;
        let indexer_builder = mock_indexer_builder(metadata.clone(), &env).await;

        // Create mock task.
        let (tx, mut rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);
        let task = IndexBuildTask {
            file_meta: file_meta.clone(),
            reason: IndexBuildType::Flush,
            access_layer: env.access_layer.clone(),
            manifest_ctx,
            write_cache: None,
            file_purger,
            indexer_builder,
            request_sender: tx,
            result_sender: ResultMpscSender(result_tx),
        };

        scheduler.schedule_build(&version_control, task).unwrap();

        // The task should finish successfully.
        match result_rx.recv().await.unwrap() {
            Ok(outcome) => {
                assert_eq!(outcome, IndexBuildOutcome::Finished);
            }
            _ => panic!("Expect finished result"),
        }

        // A notification should be sent to the worker to update the manifest.
        let worker_req = rx.recv().await.unwrap().request;
        match worker_req {
            WorkerRequest::Background {
                region_id: req_region_id,
                notify: BackgroundNotify::IndexBuildFinished(finished),
            } => {
                assert_eq!(req_region_id, region_id);
                assert_eq!(finished.edit.files_to_add.len(), 1);
                let updated_meta = &finished.edit.files_to_add[0];

                // The mock indexer builder creates all index types.
                assert!(!updated_meta.available_indexes.is_empty());
                assert!(updated_meta.index_file_size > 0);
                assert_eq!(updated_meta.file_id, file_meta.file_id);
            }
            _ => panic!("Unexpected worker request: {:?}", worker_req),
        }
    }

    async fn schedule_index_build_task_with_mode(build_mode: IndexBuildMode) {
        let env = SchedulerEnv::new().await;
        let mut scheduler = env.mock_index_build_scheduler();
        let metadata = Arc::new(sst_region_metadata());
        let manifest_ctx = env.mock_manifest_context(metadata.clone()).await;
        let file_purger = Arc::new(NoopFilePurger {});
        let region_id = metadata.region_id;
        let sst_info = mock_sst_file(metadata.clone(), &env, build_mode.clone()).await;
        let file_meta = FileMeta {
            region_id,
            file_id: sst_info.file_id,
            file_size: sst_info.file_size,
            index_file_size: sst_info.index_metadata.file_size,
            num_rows: sst_info.num_rows as u64,
            num_row_groups: sst_info.num_row_groups,
            ..Default::default()
        };
        let files = HashMap::from([(file_meta.file_id, file_meta.clone())]);
        let version_control =
            mock_version_control(metadata.clone(), file_purger.clone(), files).await;
        let indexer_builder = mock_indexer_builder(metadata.clone(), &env).await;

        // Create mock task.
        let (tx, _rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);
        let task = IndexBuildTask {
            file_meta: file_meta.clone(),
            reason: IndexBuildType::Flush,
            access_layer: env.access_layer.clone(),
            manifest_ctx,
            write_cache: None,
            file_purger,
            indexer_builder,
            request_sender: tx,
            result_sender: ResultMpscSender(result_tx),
        };

        scheduler.schedule_build(&version_control, task).unwrap();

        let puffin_path = location::index_file_path(
            env.access_layer.table_dir(),
            RegionFileId::new(region_id, file_meta.file_id),
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

        // The task should finish successfully.
        match result_rx.recv().await.unwrap() {
            Ok(outcome) => {
                assert_eq!(outcome, IndexBuildOutcome::Finished);
            }
            _ => panic!("Expect finished result"),
        }

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
        let mut scheduler = env.mock_index_build_scheduler();
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
            index_file_size: sst_info.index_metadata.file_size,
            num_rows: sst_info.num_rows as u64,
            num_row_groups: sst_info.num_row_groups,
            ..Default::default()
        };
        let files = HashMap::from([(file_meta.file_id, file_meta.clone())]);
        let version_control =
            mock_version_control(metadata.clone(), file_purger.clone(), files).await;
        let indexer_builder = mock_indexer_builder(metadata.clone(), &env).await;

        // Create mock task.
        let (tx, mut rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);
        let task = IndexBuildTask {
            file_meta: file_meta.clone(),
            reason: IndexBuildType::Flush,
            access_layer: env.access_layer.clone(),
            manifest_ctx,
            write_cache: None,
            file_purger,
            indexer_builder,
            request_sender: tx,
            result_sender: ResultMpscSender(result_tx),
        };

        scheduler.schedule_build(&version_control, task).unwrap();

        // The task should finish successfully.
        match result_rx.recv().await.unwrap() {
            Ok(outcome) => {
                assert_eq!(outcome, IndexBuildOutcome::Finished);
            }
            _ => panic!("Expect finished result"),
        }

        // No index is built, so no notification should be sent to the worker.
        let _ = rx.recv().await.is_none();
    }

    #[tokio::test]
    async fn test_index_build_task_with_write_cache() {
        let env = SchedulerEnv::new().await;
        let mut scheduler = env.mock_index_build_scheduler();
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
                factory,
                intm_manager,
            )
            .await
            .unwrap(),
        );
        // Indexer builder built from write cache.
        let indexer_builder = Arc::new(IndexerBuilderImpl {
            build_type: IndexBuildType::Flush,
            metadata: metadata.clone(),
            row_group_size: 1024,
            puffin_manager: write_cache.build_puffin_manager().clone(),
            intermediate_manager: write_cache.intermediate_manager().clone(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
            bloom_filter_index_config: BloomFilterConfig::default(),
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
        let files = HashMap::from([(file_meta.file_id, file_meta.clone())]);
        let version_control =
            mock_version_control(metadata.clone(), file_purger.clone(), files).await;

        // Create mock task.
        let (tx, mut _rx) = mpsc::channel(4);
        let (result_tx, mut result_rx) = mpsc::channel::<Result<IndexBuildOutcome>>(4);
        let task = IndexBuildTask {
            file_meta: file_meta.clone(),
            reason: IndexBuildType::Flush,
            access_layer: env.access_layer.clone(),
            manifest_ctx,
            write_cache: Some(write_cache.clone()),
            file_purger,
            indexer_builder,
            request_sender: tx,
            result_sender: ResultMpscSender(result_tx),
        };

        scheduler.schedule_build(&version_control, task).unwrap();

        // The task should finish successfully.
        match result_rx.recv().await.unwrap() {
            Ok(outcome) => {
                assert_eq!(outcome, IndexBuildOutcome::Finished);
            }
            _ => panic!("Expect finished result"),
        }

        // The write cache should contain the uploaded index file.
        let index_key = IndexKey::new(region_id, file_meta.file_id, FileType::Puffin);
        assert!(write_cache.file_cache().contains_key(&index_key));
    }
}
