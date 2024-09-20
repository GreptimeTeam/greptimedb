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

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common_error::ext::BoxedError;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{debug, error, tracing, warn};
use common_time::range::TimestampRange;
use common_time::Timestamp;
use datafusion::physical_plan::DisplayFormatType;
use datafusion_expr::utils::expr_to_columns;
use smallvec::SmallVec;
use store_api::region_engine::{PartitionRange, RegionScannerRef};
use store_api::storage::{ScanRequest, TimeSeriesRowSelector};
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
use crate::read::seq_scan::SeqScan;
use crate::read::unordered_scan::UnorderedScan;
use crate::read::{Batch, Source};
use crate::region::options::MergeMode;
use crate::region::version::VersionRef;
use crate::sst::file::{overlaps, FileHandle, FileMeta};
use crate::sst::index::fulltext_index::applier::builder::FulltextIndexApplierBuilder;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplierRef;
use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;
use crate::sst::index::inverted_index::applier::InvertedIndexApplierRef;
use crate::sst::parquet::file_range::FileRange;
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
    pub(crate) async fn region_scanner(self) -> Result<RegionScannerRef> {
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
    parallelism: ScanParallism,
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
            parallelism: ScanParallism::default(),
            ignore_inverted_index: false,
            ignore_fulltext_index: false,
            start_time: None,
        }
    }

    /// Sets parallelism.
    #[must_use]
    pub(crate) fn with_parallelism(mut self, parallelism: ScanParallism) -> Self {
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
            self.version
                .options
                .index_options
                .inverted_index
                .ignore_column_ids
                .iter()
                .copied()
                .collect(),
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
pub(crate) struct ScanParallism {
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
    pub(crate) parallelism: ScanParallism,
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
            parallelism: ScanParallism::default(),
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
    pub(crate) fn with_parallelism(mut self, parallelism: ScanParallism) -> Self {
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

    /// Prunes file ranges to scan and adds them to the `collector`.
    pub(crate) async fn prune_file_ranges(
        &self,
        collector: &mut impl FileRangeCollector,
    ) -> Result<ReaderMetrics> {
        let mut file_prune_cost = Duration::ZERO;
        let mut reader_metrics = ReaderMetrics::default();
        for file in &self.files {
            let prune_start = Instant::now();
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
                .build_reader_input(&mut reader_metrics)
                .await;
            file_prune_cost += prune_start.elapsed();
            let (mut file_range_ctx, row_groups) = match res {
                Ok(x) => x,
                Err(e) => {
                    if e.is_object_not_found() && self.ignore_file_not_found {
                        error!(e; "File to scan does not exist, region_id: {}, file: {}", file.region_id(), file.file_id());
                        continue;
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
            // Build ranges from row groups.
            let file_range_ctx = Arc::new(file_range_ctx);
            let file_ranges = row_groups
                .into_iter()
                .map(|(row_group_idx, row_selection)| {
                    FileRange::new(file_range_ctx.clone(), row_group_idx, row_selection)
                });
            collector.append_file_ranges(file.meta_ref(), file_ranges);
        }

        READ_SST_COUNT.observe(self.files.len() as f64);

        common_telemetry::debug!(
            "Region {} prune {} files, cost is {:?}",
            self.mapper.metadata().region_id,
            self.files.len(),
            file_prune_cost
        );

        Ok(reader_metrics)
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

    /// Retrieves [`PartitionRange`] from memtable and files
    pub(crate) fn partition_ranges(&self) -> Vec<PartitionRange> {
        let mut id = 0;
        let mut container = Vec::with_capacity(self.memtables.len() + self.files.len());

        for memtable in &self.memtables {
            let range = PartitionRange {
                // TODO(ruihang): filter out empty memtables in the future.
                start: memtable.stats().time_range().unwrap().0,
                end: memtable.stats().time_range().unwrap().1,
                num_rows: memtable.stats().num_rows(),
                identifier: id,
            };
            id += 1;
            container.push(range);
        }

        for file in &self.files {
            if self.append_mode {
                // For append mode, we can parallelize reading row groups.
                for _ in 0..file.meta_ref().num_row_groups {
                    let range = PartitionRange {
                        start: file.time_range().0,
                        end: file.time_range().1,
                        num_rows: file.num_rows(),
                        identifier: id,
                    };
                    id += 1;
                    container.push(range);
                }
            } else {
                let range = PartitionRange {
                    start: file.meta_ref().time_range.0,
                    end: file.meta_ref().time_range.1,
                    num_rows: file.meta_ref().num_rows as usize,
                    identifier: id,
                };
                id += 1;
                container.push(range);
            }
        }

        container
    }
}

#[cfg(test)]
impl ScanInput {
    /// Returns number of memtables to scan.
    pub(crate) fn num_memtables(&self) -> usize {
        self.memtables.len()
    }

    /// Returns number of SST files to scan.
    pub(crate) fn num_files(&self) -> usize {
        self.files.len()
    }

    /// Returns SST file ids to scan.
    pub(crate) fn file_ids(&self) -> Vec<crate::sst::file::FileId> {
        self.files.iter().map(|file| file.file_id()).collect()
    }
}

/// Groups of file ranges. Each group in the list contains multiple file
/// ranges to scan. File ranges in the same group may come from different files.
pub(crate) type FileRangesGroup = SmallVec<[Vec<FileRange>; 4]>;

/// A partition of a scanner to read.
/// It contains memtables and file ranges to scan.
#[derive(Clone, Default)]
pub(crate) struct ScanPart {
    /// Memtable ranges to scan.
    pub(crate) memtable_ranges: Vec<MemtableRange>,
    /// File ranges to scan.
    pub(crate) file_ranges: FileRangesGroup,
    /// Optional time range of the part (inclusive).
    pub(crate) time_range: Option<(Timestamp, Timestamp)>,
}

impl fmt::Debug for ScanPart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ScanPart({} memtable ranges, {} file ranges",
            self.memtable_ranges.len(),
            self.file_ranges
                .iter()
                .map(|ranges| ranges.len())
                .sum::<usize>(),
        )?;
        if let Some(time_range) = &self.time_range {
            write!(f, ", time range: {:?})", time_range)
        } else {
            write!(f, ")")
        }
    }
}

impl ScanPart {
    /// Returns true if the time range given `part` overlaps with this part.
    pub(crate) fn overlaps(&self, part: &ScanPart) -> bool {
        let (Some(current_range), Some(part_range)) = (self.time_range, part.time_range) else {
            return true;
        };

        overlaps(&current_range, &part_range)
    }

    /// Merges given `part` to this part.
    pub(crate) fn merge(&mut self, mut part: ScanPart) {
        self.memtable_ranges.append(&mut part.memtable_ranges);
        self.file_ranges.append(&mut part.file_ranges);
        let Some(part_range) = part.time_range else {
            return;
        };
        let Some(current_range) = self.time_range else {
            self.time_range = part.time_range;
            return;
        };
        let start = current_range.0.min(part_range.0);
        let end = current_range.1.max(part_range.1);
        self.time_range = Some((start, end));
    }

    /// Returns true if the we can split the part into multiple parts
    /// and preserving order.
    pub(crate) fn can_split_preserve_order(&self) -> bool {
        self.memtable_ranges.is_empty()
            && self.file_ranges.len() == 1
            && self.file_ranges[0].len() > 1
    }
}

/// A trait to collect file ranges to scan.
pub(crate) trait FileRangeCollector {
    /// Appends file ranges from the **same file** to the collector.
    fn append_file_ranges(
        &mut self,
        file_meta: &FileMeta,
        file_ranges: impl Iterator<Item = FileRange>,
    );
}

/// Optional list of [ScanPart]s.
#[derive(Default)]
pub(crate) struct ScanPartList(pub(crate) Option<Vec<ScanPart>>);

impl fmt::Debug for ScanPartList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            Some(parts) => write!(f, "{:?}", parts),
            None => write!(f, "[]"),
        }
    }
}

impl ScanPartList {
    /// Returns true if the list is None.
    pub(crate) fn is_none(&self) -> bool {
        self.0.is_none()
    }

    /// Sets parts to the list.
    pub(crate) fn set_parts(&mut self, parts: Vec<ScanPart>) {
        self.0 = Some(parts);
    }

    /// Gets the part by index, returns None if the index is out of bound.
    /// # Panics
    /// Panics if parts are not initialized.
    pub(crate) fn get_part(&mut self, index: usize) -> Option<&ScanPart> {
        let parts = self.0.as_ref().unwrap();
        parts.get(index)
    }

    /// Returns the number of parts.
    pub(crate) fn len(&self) -> usize {
        self.0.as_ref().map_or(0, |parts| parts.len())
    }

    /// Returns the number of memtable ranges.
    pub(crate) fn num_mem_ranges(&self) -> usize {
        self.0.as_ref().map_or(0, |parts| {
            parts.iter().map(|part| part.memtable_ranges.len()).sum()
        })
    }

    /// Returns the number of files.
    pub(crate) fn num_files(&self) -> usize {
        self.0.as_ref().map_or(0, |parts| {
            parts.iter().map(|part| part.file_ranges.len()).sum()
        })
    }

    /// Returns the number of file ranges.
    pub(crate) fn num_file_ranges(&self) -> usize {
        self.0.as_ref().map_or(0, |parts| {
            parts
                .iter()
                .flat_map(|part| part.file_ranges.iter())
                .map(|ranges| ranges.len())
                .sum()
        })
    }
}

/// Context shared by different streams from a scanner.
/// It contains the input and distributes input to multiple parts
/// to scan.
pub(crate) struct StreamContext {
    /// Input memtables and files.
    pub(crate) input: ScanInput,
    /// Parts to scan and the cost to build parts.
    /// The scanner builds parts to scan from the input lazily.
    /// The mutex is used to ensure the parts are only built once.
    pub(crate) parts: Mutex<(ScanPartList, Duration)>,

    // Metrics:
    /// The start time of the query.
    pub(crate) query_start: Instant,
}

impl StreamContext {
    /// Creates a new [StreamContext].
    pub(crate) fn new(input: ScanInput) -> Self {
        let query_start = input.query_start.unwrap_or_else(Instant::now);

        Self {
            input,
            parts: Mutex::new((ScanPartList::default(), Duration::default())),
            query_start,
        }
    }

    /// Format the context for explain.
    pub(crate) fn format_for_explain(
        &self,
        t: DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        match self.parts.try_lock() {
            Ok(inner) => match t {
                DisplayFormatType::Default => write!(
                    f,
                    "partition_count={} ({} memtable ranges, {} file {} ranges)",
                    inner.0.len(),
                    inner.0.num_mem_ranges(),
                    inner.0.num_files(),
                    inner.0.num_file_ranges()
                )?,
                DisplayFormatType::Verbose => write!(f, "{:?}", inner.0)?,
            },
            Err(_) => write!(f, "<locked>")?,
        }
        if let Some(selector) = &self.input.series_row_selector {
            write!(f, ", selector={}", selector)?;
        }
        Ok(())
    }
}
