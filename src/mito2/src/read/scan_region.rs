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

//! Scans a region according to the scan request.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Instant;

use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{debug, error, tracing, warn};
use common_time::range::TimestampRange;
use datafusion_expr::utils::expr_to_columns;
use parquet::arrow::arrow_reader::RowSelection;
use smallvec::SmallVec;
use store_api::region_engine::{PartitionRange, RegionScannerRef};
use store_api::storage::{ColumnId, ScanRequest, TimeSeriesRowSelector};
use table::predicate::{build_time_range_predicate, Predicate};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio_stream::wrappers::ReceiverStream;

use crate::access_layer::AccessLayerRef;
use crate::cache::file_cache::FileCacheRef;
use crate::cache::CacheManagerRef;
use crate::error::Result;
use crate::memtable::{MemtableRange, MemtableRef};
use crate::metrics::READ_SST_COUNT;
use crate::read::compat::{self, CompatBatch};
use crate::read::projection::ProjectionMapper;
use crate::read::range::{RangeMeta, RowGroupIndex};
use crate::read::seq_scan::SeqScan;
use crate::read::unordered_scan::UnorderedScan;
use crate::read::{Batch, Source};
use crate::region::options::MergeMode;
use crate::region::version::VersionRef;
use crate::sst::file::FileHandle;
use crate::sst::index::fulltext_index::applier::builder::FulltextIndexApplierBuilder;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplierRef;
use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;
use crate::sst::index::inverted_index::applier::InvertedIndexApplierRef;
use crate::sst::parquet::file_range::{FileRange, FileRangeContextRef};
use crate::sst::parquet::reader::ReaderMetrics;

/// A scanner scans a region and returns a [SendableRecordBatchStream].
pub(crate) enum Scanner {
    /// Sequential scan.
    Seq(SeqScan),
    /// Unordered scan.
    Unordered(UnorderedScan),
}

impl Scanner {
    /// Returns a [SendableRecordBatchStream] to retrieve scan results from all partitions.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub(crate) async fn scan(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.build_stream(),
            Scanner::Unordered(unordered_scan) => unordered_scan.build_stream().await,
        }
    }

    /// Returns a [RegionScanner] to scan the region.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub(crate) fn region_scanner(self) -> Result<RegionScannerRef> {
        match self {
            Scanner::Seq(seq_scan) => Ok(Box::new(seq_scan)),
            Scanner::Unordered(unordered_scan) => Ok(Box::new(unordered_scan)),
        }
    }
}

#[cfg(test)]
impl Scanner {
    /// Returns number of files to scan.
    pub(crate) fn num_files(&self) -> usize {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.input().num_files(),
            Scanner::Unordered(unordered_scan) => unordered_scan.input().num_files(),
        }
    }

    /// Returns number of memtables to scan.
    pub(crate) fn num_memtables(&self) -> usize {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.input().num_memtables(),
            Scanner::Unordered(unordered_scan) => unordered_scan.input().num_memtables(),
        }
    }

    /// Returns SST file ids to scan.
    pub(crate) fn file_ids(&self) -> Vec<crate::sst::file::FileId> {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.input().file_ids(),
            Scanner::Unordered(unordered_scan) => unordered_scan.input().file_ids(),
        }
    }
}

#[cfg_attr(doc, aquamarine::aquamarine)]
/// Helper to scans a region by [ScanRequest].
///
/// [ScanRegion] collects SSTs and memtables to scan without actually reading them. It
/// creates a [Scanner] to actually scan these targets in [Scanner::scan()].
///
/// ```mermaid
/// classDiagram
/// class ScanRegion {
///     -VersionRef version
///     -ScanRequest request
///     ~scanner() Scanner
///     ~seq_scan() SeqScan
/// }
/// class Scanner {
///     <<enumeration>>
///     SeqScan
///     UnorderedScan
///     +scan() SendableRecordBatchStream
/// }
/// class SeqScan {
///     -ScanInput input
///     +build() SendableRecordBatchStream
/// }
/// class UnorderedScan {
///     -ScanInput input
///     +build() SendableRecordBatchStream
/// }
/// class ScanInput {
///     -ProjectionMapper mapper
///     -Option~TimeRange~ time_range
///     -Option~Predicate~ predicate
///     -Vec~MemtableRef~ memtables
///     -Vec~FileHandle~ files
/// }
/// class ProjectionMapper {
///     ~output_schema() SchemaRef
///     ~convert(Batch) RecordBatch
/// }
/// ScanRegion -- Scanner
/// ScanRegion o-- ScanRequest
/// Scanner o-- SeqScan
/// Scanner o-- UnorderedScan
/// SeqScan o-- ScanInput
/// UnorderedScan o-- ScanInput
/// Scanner -- SendableRecordBatchStream
/// ScanInput o-- ProjectionMapper
/// SeqScan -- SendableRecordBatchStream
/// UnorderedScan -- SendableRecordBatchStream
/// ```
pub(crate) struct ScanRegion {
    /// Version of the region at scan.
    version: VersionRef,
    /// Access layer of the region.
    access_layer: AccessLayerRef,
    /// Scan request.
    request: ScanRequest,
    /// Cache.
    cache_manager: Option<CacheManagerRef>,
    /// Parallelism to scan.
    parallelism: ScanParallelism,
    /// Whether to ignore inverted index.
    ignore_inverted_index: bool,
    /// Whether to ignore fulltext index.
    ignore_fulltext_index: bool,
    /// Start time of the scan task.
    start_time: Option<Instant>,
}

impl ScanRegion {
    /// Creates a [ScanRegion].
    pub(crate) fn new(
        version: VersionRef,
        access_layer: AccessLayerRef,
        request: ScanRequest,
        cache_manager: Option<CacheManagerRef>,
    ) -> ScanRegion {
        ScanRegion {
            version,
            access_layer,
            request,
            cache_manager,
            parallelism: ScanParallelism::default(),
            ignore_inverted_index: false,
            ignore_fulltext_index: false,
            start_time: None,
        }
    }

    /// Sets parallelism.
    #[must_use]
    pub(crate) fn with_parallelism(mut self, parallelism: ScanParallelism) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Sets whether to ignore inverted index.
    #[must_use]
    pub(crate) fn with_ignore_inverted_index(mut self, ignore: bool) -> Self {
        self.ignore_inverted_index = ignore;
        self
    }

    /// Sets whether to ignore fulltext index.
    #[must_use]
    pub(crate) fn with_ignore_fulltext_index(mut self, ignore: bool) -> Self {
        self.ignore_fulltext_index = ignore;
        self
    }

    #[must_use]
    pub(crate) fn with_start_time(mut self, now: Instant) -> Self {
        self.start_time = Some(now);
        self
    }

    /// Returns a [Scanner] to scan the region.
    pub(crate) fn scanner(self) -> Result<Scanner> {
        if self.version.options.append_mode && self.request.series_row_selector.is_none() {
            // If table is append only and there is no series row selector, we use unordered scan in query.
            // We still use seq scan in compaction.
            self.unordered_scan().map(Scanner::Unordered)
        } else {
            self.seq_scan().map(Scanner::Seq)
        }
    }

    /// Scan sequentially.
    pub(crate) fn seq_scan(self) -> Result<SeqScan> {
        let input = self.scan_input(true)?;
        Ok(SeqScan::new(input))
    }

    /// Unordered scan.
    pub(crate) fn unordered_scan(self) -> Result<UnorderedScan> {
        let input = self.scan_input(true)?;
        Ok(UnorderedScan::new(input))
    }

    #[cfg(test)]
    pub(crate) fn scan_without_filter_deleted(self) -> Result<SeqScan> {
        let input = self.scan_input(false)?;
        Ok(SeqScan::new(input))
    }

    /// Creates a scan input.
    fn scan_input(mut self, filter_deleted: bool) -> Result<ScanInput> {
        let time_range = self.build_time_range_predicate();

        let ssts = &self.version.ssts;
        let mut files = Vec::new();
        for level in ssts.levels() {
            for file in level.files.values() {
                // Finds SST files in range.
                if file_in_range(file, &time_range) {
                    files.push(file.clone());
                }
            }
        }

        let memtables = self.version.memtables.list_memtables();
        // Skip empty memtables and memtables out of time range.
        let memtables: Vec<_> = memtables
            .into_iter()
            .filter(|mem| {
                if mem.is_empty() {
                    return false;
                }
                let stats = mem.stats();
                // Safety: the memtable is not empty.
                let (start, end) = stats.time_range().unwrap();

                // The time range of the memtable is inclusive.
                let memtable_range = TimestampRange::new_inclusive(Some(start), Some(end));
                memtable_range.intersects(&time_range)
            })
            .collect();

        debug!(
            "Scan region {}, request: {:?}, time range: {:?}, memtables: {}, ssts_to_read: {}, append_mode: {}",
            self.version.metadata.region_id,
            self.request,
            time_range,
            memtables.len(),
            files.len(),
            self.version.options.append_mode,
        );

        // Remove field filters for LastNonNull mode after logging the request.
        self.maybe_remove_field_filters();

        let inverted_index_applier = self.build_invereted_index_applier();
        let fulltext_index_applier = self.build_fulltext_index_applier();
        let predicate = Predicate::new(self.request.filters.clone());
        // The mapper always computes projected column ids as the schema of SSTs may change.
        let mapper = match &self.request.projection {
            Some(p) => ProjectionMapper::new(&self.version.metadata, p.iter().copied())?,
            None => ProjectionMapper::all(&self.version.metadata)?,
        };

        let input = ScanInput::new(self.access_layer, mapper)
            .with_time_range(Some(time_range))
            .with_predicate(Some(predicate))
            .with_memtables(memtables)
            .with_files(files)
            .with_cache(self.cache_manager)
            .with_inverted_index_applier(inverted_index_applier)
            .with_fulltext_index_applier(fulltext_index_applier)
            .with_parallelism(self.parallelism)
            .with_start_time(self.start_time)
            .with_append_mode(self.version.options.append_mode)
            .with_filter_deleted(filter_deleted)
            .with_merge_mode(self.version.options.merge_mode())
            .with_series_row_selector(self.request.series_row_selector);
        Ok(input)
    }

    /// Build time range predicate from filters, also remove time filters from request.
    fn build_time_range_predicate(&mut self) -> TimestampRange {
        let time_index = self.version.metadata.time_index_column();
        let unit = time_index
            .column_schema
            .data_type
            .as_timestamp()
            .expect("Time index must have timestamp-compatible type")
            .unit();
        build_time_range_predicate(
            &time_index.column_schema.name,
            unit,
            &mut self.request.filters,
        )
    }

    /// Remove field filters if the merge mode is [MergeMode::LastNonNull].
    fn maybe_remove_field_filters(&mut self) {
        if self.version.options.merge_mode() != MergeMode::LastNonNull {
            return;
        }

        // TODO(yingwen): We can ignore field filters only when there are multiple sources in the same time window.
        let field_columns = self
            .version
            .metadata
            .field_columns()
            .map(|col| &col.column_schema.name)
            .collect::<HashSet<_>>();
        // Columns in the expr.
        let mut columns = HashSet::new();

        self.request.filters.retain(|expr| {
            columns.clear();
            // `expr_to_columns` won't return error.
            if expr_to_columns(expr, &mut columns).is_err() {
                return false;
            }
            for column in &columns {
                if field_columns.contains(&column.name) {
                    // This expr uses the field column.
                    return false;
                }
            }
            true
        });
    }

    /// Use the latest schema to build the inveretd index applier.
    fn build_invereted_index_applier(&self) -> Option<InvertedIndexApplierRef> {
        if self.ignore_inverted_index {
            return None;
        }

        let file_cache = || -> Option<FileCacheRef> {
            let cache_manager = self.cache_manager.as_ref()?;
            let write_cache = cache_manager.write_cache()?;
            let file_cache = write_cache.file_cache();
            Some(file_cache)
        }();

        let index_cache = self
            .cache_manager
            .as_ref()
            .and_then(|c| c.index_cache())
            .cloned();

        InvertedIndexApplierBuilder::new(
            self.access_layer.region_dir().to_string(),
            self.access_layer.object_store().clone(),
            file_cache,
            index_cache,
            self.version.metadata.as_ref(),
            self.version.metadata.inverted_indexed_column_ids(
                self.version
                    .options
                    .index_options
                    .inverted_index
                    .ignore_column_ids
                    .iter(),
            ),
            self.access_layer.puffin_manager_factory().clone(),
        )
        .build(&self.request.filters)
        .inspect_err(|err| warn!(err; "Failed to build invereted index applier"))
        .ok()
        .flatten()
        .map(Arc::new)
    }

    /// Use the latest schema to build the fulltext index applier.
    fn build_fulltext_index_applier(&self) -> Option<FulltextIndexApplierRef> {
        if self.ignore_fulltext_index {
            return None;
        }

        FulltextIndexApplierBuilder::new(
            self.access_layer.region_dir().to_string(),
            self.access_layer.object_store().clone(),
            self.access_layer.puffin_manager_factory().clone(),
            self.version.metadata.as_ref(),
        )
        .build(&self.request.filters)
        .inspect_err(|err| warn!(err; "Failed to build fulltext index applier"))
        .ok()
        .flatten()
        .map(Arc::new)
    }
}

/// Config for parallel scan.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct ScanParallelism {
    /// Number of tasks expect to spawn to read data.
    pub(crate) parallelism: usize,
    /// Channel size to send batches. Only takes effect when the parallelism > 1.
    pub(crate) channel_size: usize,
}

/// Returns true if the time range of a SST `file` matches the `predicate`.
fn file_in_range(file: &FileHandle, predicate: &TimestampRange) -> bool {
    if predicate == &TimestampRange::min_to_max() {
        return true;
    }
    // end timestamp of a SST is inclusive.
    let (start, end) = file.time_range();
    let file_ts_range = TimestampRange::new_inclusive(Some(start), Some(end));
    file_ts_range.intersects(predicate)
}

/// Common input for different scanners.
pub(crate) struct ScanInput {
    /// Region SST access layer.
    access_layer: AccessLayerRef,
    /// Maps projected Batches to RecordBatches.
    pub(crate) mapper: Arc<ProjectionMapper>,
    /// Time range filter for time index.
    time_range: Option<TimestampRange>,
    /// Predicate to push down.
    pub(crate) predicate: Option<Predicate>,
    /// Memtables to scan.
    pub(crate) memtables: Vec<MemtableRef>,
    /// Handles to SST files to scan.
    pub(crate) files: Vec<FileHandle>,
    /// Cache.
    pub(crate) cache_manager: Option<CacheManagerRef>,
    /// Ignores file not found error.
    ignore_file_not_found: bool,
    /// Parallelism to scan data.
    pub(crate) parallelism: ScanParallelism,
    /// Index appliers.
    inverted_index_applier: Option<InvertedIndexApplierRef>,
    fulltext_index_applier: Option<FulltextIndexApplierRef>,
    /// Start time of the query.
    pub(crate) query_start: Option<Instant>,
    /// The region is using append mode.
    pub(crate) append_mode: bool,
    /// Whether to remove deletion markers.
    pub(crate) filter_deleted: bool,
    /// Mode to merge duplicate rows.
    pub(crate) merge_mode: MergeMode,
    /// Hint to select rows from time series.
    pub(crate) series_row_selector: Option<TimeSeriesRowSelector>,
}

impl ScanInput {
    /// Creates a new [ScanInput].
    #[must_use]
    pub(crate) fn new(access_layer: AccessLayerRef, mapper: ProjectionMapper) -> ScanInput {
        ScanInput {
            access_layer,
            mapper: Arc::new(mapper),
            time_range: None,
            predicate: None,
            memtables: Vec::new(),
            files: Vec::new(),
            cache_manager: None,
            ignore_file_not_found: false,
            parallelism: ScanParallelism::default(),
            inverted_index_applier: None,
            fulltext_index_applier: None,
            query_start: None,
            append_mode: false,
            filter_deleted: true,
            merge_mode: MergeMode::default(),
            series_row_selector: None,
        }
    }

    /// Sets time range filter for time index.
    #[must_use]
    pub(crate) fn with_time_range(mut self, time_range: Option<TimestampRange>) -> Self {
        self.time_range = time_range;
        self
    }

    /// Sets predicate to push down.
    #[must_use]
    pub(crate) fn with_predicate(mut self, predicate: Option<Predicate>) -> Self {
        self.predicate = predicate;
        self
    }

    /// Sets memtables to read.
    #[must_use]
    pub(crate) fn with_memtables(mut self, memtables: Vec<MemtableRef>) -> Self {
        self.memtables = memtables;
        self
    }

    /// Sets files to read.
    #[must_use]
    pub(crate) fn with_files(mut self, files: Vec<FileHandle>) -> Self {
        self.files = files;
        self
    }

    /// Sets cache for this query.
    #[must_use]
    pub(crate) fn with_cache(mut self, cache: Option<CacheManagerRef>) -> Self {
        self.cache_manager = cache;
        self
    }

    /// Ignores file not found error.
    #[must_use]
    pub(crate) fn with_ignore_file_not_found(mut self, ignore: bool) -> Self {
        self.ignore_file_not_found = ignore;
        self
    }

    /// Sets scan parallelism.
    #[must_use]
    pub(crate) fn with_parallelism(mut self, parallelism: ScanParallelism) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Sets invereted index applier.
    #[must_use]
    pub(crate) fn with_inverted_index_applier(
        mut self,
        applier: Option<InvertedIndexApplierRef>,
    ) -> Self {
        self.inverted_index_applier = applier;
        self
    }

    /// Sets fulltext index applier.
    #[must_use]
    pub(crate) fn with_fulltext_index_applier(
        mut self,
        applier: Option<FulltextIndexApplierRef>,
    ) -> Self {
        self.fulltext_index_applier = applier;
        self
    }

    /// Sets start time of the query.
    #[must_use]
    pub(crate) fn with_start_time(mut self, now: Option<Instant>) -> Self {
        self.query_start = now;
        self
    }

    #[must_use]
    pub(crate) fn with_append_mode(mut self, is_append_mode: bool) -> Self {
        self.append_mode = is_append_mode;
        self
    }

    /// Sets whether to remove deletion markers during scan.
    #[must_use]
    pub(crate) fn with_filter_deleted(mut self, filter_deleted: bool) -> Self {
        self.filter_deleted = filter_deleted;
        self
    }

    /// Sets the merge mode.
    #[must_use]
    pub(crate) fn with_merge_mode(mut self, merge_mode: MergeMode) -> Self {
        self.merge_mode = merge_mode;
        self
    }

    /// Sets the time series row selector.
    #[must_use]
    pub(crate) fn with_series_row_selector(
        mut self,
        series_row_selector: Option<TimeSeriesRowSelector>,
    ) -> Self {
        self.series_row_selector = series_row_selector;
        self
    }

    /// Scans sources in parallel.
    ///
    /// # Panics if the input doesn't allow parallel scan.
    pub(crate) fn create_parallel_sources(
        &self,
        sources: Vec<Source>,
        semaphore: Arc<Semaphore>,
    ) -> Result<Vec<Source>> {
        debug_assert!(self.parallelism.parallelism > 1);
        // Spawn a task for each source.
        let sources = sources
            .into_iter()
            .map(|source| {
                let (sender, receiver) = mpsc::channel(self.parallelism.channel_size);
                self.spawn_scan_task(source, semaphore.clone(), sender);
                let stream = Box::pin(ReceiverStream::new(receiver));
                Source::Stream(stream)
            })
            .collect();
        Ok(sources)
    }

    /// Prunes a memtable to scan and returns the builder to build readers.
    fn prune_memtable(&self, mem_index: usize) -> MemRangeBuilder {
        let memtable = &self.memtables[mem_index];
        let row_groups = memtable.ranges(Some(self.mapper.column_ids()), self.predicate.clone());
        MemRangeBuilder { row_groups }
    }

    /// Prunes a file to scan and returns the builder to build readers.
    async fn prune_file(
        &self,
        file_index: usize,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<FileRangeBuilder> {
        let file = &self.files[file_index];
        let res = self
            .access_layer
            .read_sst(file.clone())
            .predicate(self.predicate.clone())
            .time_range(self.time_range)
            .projection(Some(self.mapper.column_ids().to_vec()))
            .cache(self.cache_manager.clone())
            .inverted_index_applier(self.inverted_index_applier.clone())
            .fulltext_index_applier(self.fulltext_index_applier.clone())
            .expected_metadata(Some(self.mapper.metadata().clone()))
            .build_reader_input(reader_metrics)
            .await;
        let (mut file_range_ctx, row_groups) = match res {
            Ok(x) => x,
            Err(e) => {
                if e.is_object_not_found() && self.ignore_file_not_found {
                    error!(e; "File to scan does not exist, region_id: {}, file: {}", file.region_id(), file.file_id());
                    return Ok(FileRangeBuilder::default());
                } else {
                    return Err(e);
                }
            }
        };
        if !compat::has_same_columns(
            self.mapper.metadata(),
            file_range_ctx.read_format().metadata(),
        ) {
            // They have different schema. We need to adapt the batch first so the
            // mapper can convert it.
            let compat = CompatBatch::new(
                &self.mapper,
                file_range_ctx.read_format().metadata().clone(),
            )?;
            file_range_ctx.set_compat_batch(Some(compat));
        }
        Ok(FileRangeBuilder {
            context: Some(Arc::new(file_range_ctx)),
            row_groups,
        })
    }

    /// Scans the input source in another task and sends batches to the sender.
    pub(crate) fn spawn_scan_task(
        &self,
        mut input: Source,
        semaphore: Arc<Semaphore>,
        sender: mpsc::Sender<Result<Batch>>,
    ) {
        common_runtime::spawn_global(async move {
            loop {
                // We release the permit before sending result to avoid the task waiting on
                // the channel with the permit held.
                let maybe_batch = {
                    // Safety: We never close the semaphore.
                    let _permit = semaphore.acquire().await.unwrap();
                    input.next_batch().await
                };
                match maybe_batch {
                    Ok(Some(batch)) => {
                        let _ = sender.send(Ok(batch)).await;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        let _ = sender.send(Err(e)).await;
                        break;
                    }
                }
            }
        });
    }

    pub(crate) fn total_rows(&self) -> usize {
        let rows_in_files: usize = self.files.iter().map(|f| f.num_rows()).sum();
        let rows_in_memtables: usize = self.memtables.iter().map(|m| m.stats().num_rows()).sum();
        rows_in_files + rows_in_memtables
    }

    pub(crate) fn predicate(&self) -> Option<Predicate> {
        self.predicate.clone()
    }

    /// Returns number of memtables to scan.
    pub(crate) fn num_memtables(&self) -> usize {
        self.memtables.len()
    }

    /// Returns number of SST files to scan.
    pub(crate) fn num_files(&self) -> usize {
        self.files.len()
    }
}

#[cfg(test)]
impl ScanInput {
    /// Returns SST file ids to scan.
    pub(crate) fn file_ids(&self) -> Vec<crate::sst::file::FileId> {
        self.files.iter().map(|file| file.file_id()).collect()
    }
}

/// Context shared by different streams from a scanner.
/// It contains the input and ranges to scan.
pub(crate) struct StreamContext {
    /// Input memtables and files.
    pub(crate) input: ScanInput,
    /// Metadata for partition ranges.
    pub(crate) ranges: Vec<RangeMeta>,
    /// Lists of range builders.
    range_builders: RangeBuilderList,

    // Metrics:
    /// The start time of the query.
    pub(crate) query_start: Instant,
}

impl StreamContext {
    /// Creates a new [StreamContext] for [SeqScan].
    pub(crate) fn seq_scan_ctx(input: ScanInput) -> Self {
        let query_start = input.query_start.unwrap_or_else(Instant::now);
        let ranges = RangeMeta::seq_scan_ranges(&input);
        READ_SST_COUNT.observe(input.num_files() as f64);
        let range_builders = RangeBuilderList::new(input.num_memtables(), input.num_files());

        Self {
            input,
            ranges,
            range_builders,
            query_start,
        }
    }

    /// Creates a new [StreamContext] for [UnorderedScan].
    pub(crate) fn unordered_scan_ctx(input: ScanInput) -> Self {
        let query_start = input.query_start.unwrap_or_else(Instant::now);
        let ranges = RangeMeta::unordered_scan_ranges(&input);
        READ_SST_COUNT.observe(input.num_files() as f64);
        let range_builders = RangeBuilderList::new(input.num_memtables(), input.num_files());

        Self {
            input,
            ranges,
            range_builders,
            query_start,
        }
    }

    /// Returns true if the index refers to a memtable.
    pub(crate) fn is_mem_range_index(&self, index: RowGroupIndex) -> bool {
        self.input.num_memtables() > index.index
    }

    /// Creates file ranges to scan.
    pub(crate) async fn build_file_ranges(
        &self,
        index: RowGroupIndex,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<SmallVec<[FileRange; 2]>> {
        let mut ranges = SmallVec::new();
        self.range_builders
            .build_file_ranges(&self.input, index, &mut ranges, reader_metrics)
            .await?;
        Ok(ranges)
    }

    /// Creates memtable ranges to scan.
    pub(crate) fn build_mem_ranges(&self, index: RowGroupIndex) -> SmallVec<[MemtableRange; 2]> {
        let mut ranges = SmallVec::new();
        self.range_builders
            .build_mem_ranges(&self.input, index, &mut ranges);
        ranges
    }

    /// Retrieves the partition ranges.
    pub(crate) fn partition_ranges(&self) -> Vec<PartitionRange> {
        self.ranges
            .iter()
            .enumerate()
            .map(|(idx, range_meta)| range_meta.new_partition_range(idx))
            .collect()
    }

    /// Format the context for explain.
    pub(crate) fn format_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (mut num_mem_ranges, mut num_file_ranges) = (0, 0);
        for range_meta in &self.ranges {
            for idx in &range_meta.row_group_indices {
                if self.is_mem_range_index(*idx) {
                    num_mem_ranges += 1;
                } else {
                    num_file_ranges += 1;
                }
            }
        }
        write!(
            f,
            "partition_count={} ({} memtable ranges, {} file {} ranges)",
            self.ranges.len(),
            num_mem_ranges,
            self.input.num_files(),
            num_file_ranges,
        )?;
        if let Some(selector) = &self.input.series_row_selector {
            write!(f, ", selector={}", selector)?;
        }
        Ok(())
    }
}

/// List to manages the builders to create file ranges.
struct RangeBuilderList {
    mem_builders: Vec<StdMutex<Option<MemRangeBuilder>>>,
    file_builders: Vec<Mutex<Option<FileRangeBuilder>>>,
}

impl RangeBuilderList {
    /// Creates a new [ReaderBuilderList] with the given number of memtables and files.
    fn new(num_memtables: usize, num_files: usize) -> Self {
        let mem_builders = (0..num_memtables).map(|_| StdMutex::new(None)).collect();
        let file_builders = (0..num_files).map(|_| Mutex::new(None)).collect();
        Self {
            mem_builders,
            file_builders,
        }
    }

    /// Builds file ranges to read the row group at `index`.
    async fn build_file_ranges(
        &self,
        input: &ScanInput,
        index: RowGroupIndex,
        ranges: &mut SmallVec<[FileRange; 2]>,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<()> {
        let file_index = index.index - self.mem_builders.len();
        let mut builder_opt = self.file_builders[file_index].lock().await;
        match &mut *builder_opt {
            Some(builder) => builder.build_ranges(index.row_group_index, ranges),
            None => {
                let builder = input.prune_file(file_index, reader_metrics).await?;
                builder.build_ranges(index.row_group_index, ranges);
                *builder_opt = Some(builder);
            }
        }
        Ok(())
    }

    /// Builds mem ranges to read the row group at `index`.
    fn build_mem_ranges(
        &self,
        input: &ScanInput,
        index: RowGroupIndex,
        ranges: &mut SmallVec<[MemtableRange; 2]>,
    ) {
        let mut builder_opt = self.mem_builders[index.index].lock().unwrap();
        match &mut *builder_opt {
            Some(builder) => builder.build_ranges(index.row_group_index, ranges),
            None => {
                let builder = input.prune_memtable(index.index);
                builder.build_ranges(index.row_group_index, ranges);
                *builder_opt = Some(builder);
            }
        }
    }
}

/// Builder to create file ranges.
#[derive(Default)]
struct FileRangeBuilder {
    /// Context for the file.
    /// None indicates nothing to read.
    context: Option<FileRangeContextRef>,
    /// Row selections for each row group to read.
    /// It skips the row group if it is not in the map.
    row_groups: BTreeMap<usize, Option<RowSelection>>,
}

impl FileRangeBuilder {
    /// Builds file ranges to read.
    /// Negative `row_group_index` indicates all row groups.
    fn build_ranges(&self, row_group_index: i64, ranges: &mut SmallVec<[FileRange; 2]>) {
        let Some(context) = self.context.clone() else {
            return;
        };
        if row_group_index >= 0 {
            let row_group_index = row_group_index as usize;
            // Scans one row group.
            let Some(row_selection) = self.row_groups.get(&row_group_index) else {
                return;
            };
            ranges.push(FileRange::new(
                context,
                row_group_index,
                row_selection.clone(),
            ));
        } else {
            // Scans all row groups.
            ranges.extend(
                self.row_groups
                    .iter()
                    .map(|(row_group_index, row_selection)| {
                        FileRange::new(context.clone(), *row_group_index, row_selection.clone())
                    }),
            );
        }
    }
}

/// Builder to create mem ranges.
struct MemRangeBuilder {
    /// Ranges of a memtable.
    row_groups: BTreeMap<usize, MemtableRange>,
}

impl MemRangeBuilder {
    /// Builds mem ranges to read in the memtable.
    /// Negative `row_group_index` indicates all row groups.
    fn build_ranges(&self, row_group_index: i64, ranges: &mut SmallVec<[MemtableRange; 2]>) {
        if row_group_index >= 0 {
            let row_group_index = row_group_index as usize;
            // Scans one row group.
            let Some(range) = self.row_groups.get(&row_group_index) else {
                return;
            };
            ranges.push(range.clone());
        } else {
            ranges.extend(self.row_groups.values().cloned());
        }
    }
}
