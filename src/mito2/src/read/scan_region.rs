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
use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use api::v1::SemanticType;
use common_error::ext::BoxedError;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::{debug, error, tracing, warn};
use common_time::range::TimestampRange;
use datafusion_common::Column;
use datafusion_expr::utils::expr_to_columns;
use datafusion_expr::Expr;
use smallvec::SmallVec;
use store_api::metadata::RegionMetadata;
use store_api::region_engine::{PartitionRange, RegionScannerRef};
use store_api::storage::{ScanRequest, TimeSeriesDistribution, TimeSeriesRowSelector};
use table::predicate::{build_time_range_predicate, Predicate};
use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::ReceiverStream;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheStrategy;
use crate::config::DEFAULT_SCAN_CHANNEL_SIZE;
use crate::error::Result;
use crate::memtable::MemtableRange;
use crate::metrics::READ_SST_COUNT;
use crate::read::compat::{self, CompatBatch};
use crate::read::projection::ProjectionMapper;
use crate::read::range::{FileRangeBuilder, MemRangeBuilder, RangeMeta, RowGroupIndex};
use crate::read::seq_scan::SeqScan;
use crate::read::series_scan::SeriesScan;
use crate::read::unordered_scan::UnorderedScan;
use crate::read::{Batch, Source};
use crate::region::options::MergeMode;
use crate::region::version::VersionRef;
use crate::sst::file::FileHandle;
use crate::sst::index::bloom_filter::applier::{
    BloomFilterIndexApplierBuilder, BloomFilterIndexApplierRef,
};
use crate::sst::index::fulltext_index::applier::builder::FulltextIndexApplierBuilder;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplierRef;
use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;
use crate::sst::index::inverted_index::applier::InvertedIndexApplierRef;
use crate::sst::parquet::reader::ReaderMetrics;

/// A scanner scans a region and returns a [SendableRecordBatchStream].
pub(crate) enum Scanner {
    /// Sequential scan.
    Seq(SeqScan),
    /// Unordered scan.
    Unordered(UnorderedScan),
    /// Per-series scan.
    Series(SeriesScan),
}

impl Scanner {
    /// Returns a [SendableRecordBatchStream] to retrieve scan results from all partitions.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub(crate) async fn scan(&self) -> Result<SendableRecordBatchStream, BoxedError> {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.build_stream(),
            Scanner::Unordered(unordered_scan) => unordered_scan.build_stream().await,
            Scanner::Series(series_scan) => series_scan.build_stream().await,
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
            Scanner::Series(series_scan) => series_scan.input().num_files(),
        }
    }

    /// Returns number of memtables to scan.
    pub(crate) fn num_memtables(&self) -> usize {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.input().num_memtables(),
            Scanner::Unordered(unordered_scan) => unordered_scan.input().num_memtables(),
            Scanner::Series(series_scan) => series_scan.input().num_memtables(),
        }
    }

    /// Returns SST file ids to scan.
    pub(crate) fn file_ids(&self) -> Vec<crate::sst::file::FileId> {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.input().file_ids(),
            Scanner::Unordered(unordered_scan) => unordered_scan.input().file_ids(),
            Scanner::Series(series_scan) => series_scan.input().file_ids(),
        }
    }

    /// Sets the target partitions for the scanner. It can controls the parallelism of the scanner.
    pub(crate) fn set_target_partitions(&mut self, target_partitions: usize) {
        use store_api::region_engine::{PrepareRequest, RegionScanner};

        let request = PrepareRequest::default().with_target_partitions(target_partitions);
        match self {
            Scanner::Seq(seq_scan) => seq_scan.prepare(request).unwrap(),
            Scanner::Unordered(unordered_scan) => unordered_scan.prepare(request).unwrap(),
            Scanner::Series(series_scan) => series_scan.prepare(request).unwrap(),
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
    cache_strategy: CacheStrategy,
    /// Capacity of the channel to send data from parallel scan tasks to the main task.
    parallel_scan_channel_size: usize,
    /// Whether to ignore inverted index.
    ignore_inverted_index: bool,
    /// Whether to ignore fulltext index.
    ignore_fulltext_index: bool,
    /// Whether to ignore bloom filter.
    ignore_bloom_filter: bool,
    /// Start time of the scan task.
    start_time: Option<Instant>,
}

impl ScanRegion {
    /// Creates a [ScanRegion].
    pub(crate) fn new(
        version: VersionRef,
        access_layer: AccessLayerRef,
        request: ScanRequest,
        cache_strategy: CacheStrategy,
    ) -> ScanRegion {
        ScanRegion {
            version,
            access_layer,
            request,
            cache_strategy,
            parallel_scan_channel_size: DEFAULT_SCAN_CHANNEL_SIZE,
            ignore_inverted_index: false,
            ignore_fulltext_index: false,
            ignore_bloom_filter: false,
            start_time: None,
        }
    }

    /// Sets parallel scan task channel size.
    #[must_use]
    pub(crate) fn with_parallel_scan_channel_size(
        mut self,
        parallel_scan_channel_size: usize,
    ) -> Self {
        self.parallel_scan_channel_size = parallel_scan_channel_size;
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

    /// Sets whether to ignore bloom filter.
    #[must_use]
    pub(crate) fn with_ignore_bloom_filter(mut self, ignore: bool) -> Self {
        self.ignore_bloom_filter = ignore;
        self
    }

    #[must_use]
    pub(crate) fn with_start_time(mut self, now: Instant) -> Self {
        self.start_time = Some(now);
        self
    }

    /// Returns a [Scanner] to scan the region.
    pub(crate) fn scanner(self) -> Result<Scanner> {
        if self.use_series_scan() {
            self.series_scan().map(Scanner::Series)
        } else if self.use_unordered_scan() {
            // If table is append only and there is no series row selector, we use unordered scan in query.
            // We still use seq scan in compaction.
            self.unordered_scan().map(Scanner::Unordered)
        } else {
            self.seq_scan().map(Scanner::Seq)
        }
    }

    /// Returns a [RegionScanner] to scan the region.
    #[tracing::instrument(level = tracing::Level::DEBUG, skip_all)]
    pub(crate) fn region_scanner(self) -> Result<RegionScannerRef> {
        if self.use_series_scan() {
            self.series_scan().map(|scanner| Box::new(scanner) as _)
        } else if self.use_unordered_scan() {
            self.unordered_scan().map(|scanner| Box::new(scanner) as _)
        } else {
            self.seq_scan().map(|scanner| Box::new(scanner) as _)
        }
    }

    /// Scan sequentially.
    pub(crate) fn seq_scan(self) -> Result<SeqScan> {
        let input = self.scan_input(true)?;
        Ok(SeqScan::new(input, false))
    }

    /// Unordered scan.
    pub(crate) fn unordered_scan(self) -> Result<UnorderedScan> {
        let input = self.scan_input(true)?;
        Ok(UnorderedScan::new(input))
    }

    /// Scans by series.
    pub(crate) fn series_scan(self) -> Result<SeriesScan> {
        let input = self.scan_input(true)?;
        Ok(SeriesScan::new(input))
    }

    #[cfg(test)]
    pub(crate) fn scan_without_filter_deleted(self) -> Result<SeqScan> {
        let input = self.scan_input(false)?;
        Ok(SeqScan::new(input, false))
    }

    /// Returns true if the region can use unordered scan for current request.
    fn use_unordered_scan(&self) -> bool {
        // We use unordered scan when:
        // 1. The region is in append mode.
        // 2. There is no series row selector.
        // 3. The required distribution is None or TimeSeriesDistribution::TimeWindowed.
        //
        // We still use seq scan in compaction.
        self.version.options.append_mode
            && self.request.series_row_selector.is_none()
            && (self.request.distribution.is_none()
                || self.request.distribution == Some(TimeSeriesDistribution::TimeWindowed))
    }

    /// Returns true if the region can use series scan for current request.
    fn use_series_scan(&self) -> bool {
        self.request.distribution == Some(TimeSeriesDistribution::PerSeries)
    }

    /// Creates a scan input.
    fn scan_input(mut self, filter_deleted: bool) -> Result<ScanInput> {
        let sst_min_sequence = self.request.sst_min_sequence.and_then(NonZeroU64::new);
        let time_range = self.build_time_range_predicate();

        let ssts = &self.version.ssts;
        let mut files = Vec::new();
        for level in ssts.levels() {
            for file in level.files.values() {
                let exceed_min_sequence = match (sst_min_sequence, file.meta_ref().sequence) {
                    (Some(min_sequence), Some(file_sequence)) => file_sequence > min_sequence,
                    // If the file's sequence is None (or actually is zero), it could mean the file
                    // is generated and added to the region "directly". In this case, its data should
                    // be considered as fresh as the memtable. So its sequence is treated greater than
                    // the min_sequence, whatever the value of min_sequence is. Hence the default
                    // "true" in this arm.
                    (Some(_), None) => true,
                    (None, _) => true,
                };

                // Finds SST files in range.
                if exceed_min_sequence && file_in_range(file, &time_range) {
                    files.push(file.clone());
                }
                // There is no need to check and prune for file's sequence here as the sequence number is usually very new,
                // unless the timing is too good, or the sequence number wouldn't be in file.
                // and the batch will be filtered out by tree reader anyway.
            }
        }

        let memtables = self.version.memtables.list_memtables();
        // Skip empty memtables and memtables out of time range.
        let memtables: Vec<_> = memtables
            .into_iter()
            .filter(|mem| {
                // check if memtable is empty by reading stats.
                let Some((start, end)) = mem.stats().time_range() else {
                    return false;
                };
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
        let bloom_filter_applier = self.build_bloom_filter_applier();
        let fulltext_index_applier = self.build_fulltext_index_applier();
        let predicate = PredicateGroup::new(&self.version.metadata, &self.request.filters);
        // The mapper always computes projected column ids as the schema of SSTs may change.
        let mapper = match &self.request.projection {
            Some(p) => ProjectionMapper::new(&self.version.metadata, p.iter().copied())?,
            None => ProjectionMapper::all(&self.version.metadata)?,
        };
        // Get memtable ranges to scan.
        let memtables = memtables
            .into_iter()
            .map(|mem| {
                mem.ranges(
                    Some(mapper.column_ids()),
                    predicate.clone(),
                    self.request.sequence,
                )
                .map(MemRangeBuilder::new)
            })
            .collect::<Result<Vec<_>>>()?;

        let input = ScanInput::new(self.access_layer, mapper)
            .with_time_range(Some(time_range))
            .with_predicate(predicate)
            .with_memtables(memtables)
            .with_files(files)
            .with_cache(self.cache_strategy)
            .with_inverted_index_applier(inverted_index_applier)
            .with_bloom_filter_index_applier(bloom_filter_applier)
            .with_fulltext_index_applier(fulltext_index_applier)
            .with_parallel_scan_channel_size(self.parallel_scan_channel_size)
            .with_start_time(self.start_time)
            .with_append_mode(self.version.options.append_mode)
            .with_filter_deleted(filter_deleted)
            .with_merge_mode(self.version.options.merge_mode())
            .with_series_row_selector(self.request.series_row_selector)
            .with_distribution(self.request.distribution);
        Ok(input)
    }

    /// Build time range predicate from filters.
    fn build_time_range_predicate(&self) -> TimestampRange {
        let time_index = self.version.metadata.time_index_column();
        let unit = time_index
            .column_schema
            .data_type
            .as_timestamp()
            .expect("Time index must have timestamp-compatible type")
            .unit();
        build_time_range_predicate(&time_index.column_schema.name, unit, &self.request.filters)
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

    /// Use the latest schema to build the inverted index applier.
    fn build_invereted_index_applier(&self) -> Option<InvertedIndexApplierRef> {
        if self.ignore_inverted_index {
            return None;
        }

        let file_cache = self.cache_strategy.write_cache().map(|w| w.file_cache());
        let inverted_index_cache = self.cache_strategy.inverted_index_cache().cloned();

        let puffin_metadata_cache = self.cache_strategy.puffin_metadata_cache().cloned();

        InvertedIndexApplierBuilder::new(
            self.access_layer.region_dir().to_string(),
            self.access_layer.object_store().clone(),
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
        .with_file_cache(file_cache)
        .with_inverted_index_cache(inverted_index_cache)
        .with_puffin_metadata_cache(puffin_metadata_cache)
        .build(&self.request.filters)
        .inspect_err(|err| warn!(err; "Failed to build invereted index applier"))
        .ok()
        .flatten()
        .map(Arc::new)
    }

    /// Use the latest schema to build the bloom filter index applier.
    fn build_bloom_filter_applier(&self) -> Option<BloomFilterIndexApplierRef> {
        if self.ignore_bloom_filter {
            return None;
        }

        let file_cache = self.cache_strategy.write_cache().map(|w| w.file_cache());
        let bloom_filter_index_cache = self.cache_strategy.bloom_filter_index_cache().cloned();
        let puffin_metadata_cache = self.cache_strategy.puffin_metadata_cache().cloned();

        BloomFilterIndexApplierBuilder::new(
            self.access_layer.region_dir().to_string(),
            self.access_layer.object_store().clone(),
            self.version.metadata.as_ref(),
            self.access_layer.puffin_manager_factory().clone(),
        )
        .with_file_cache(file_cache)
        .with_bloom_filter_index_cache(bloom_filter_index_cache)
        .with_puffin_metadata_cache(puffin_metadata_cache)
        .build(&self.request.filters)
        .inspect_err(|err| warn!(err; "Failed to build bloom filter index applier"))
        .ok()
        .flatten()
        .map(Arc::new)
    }

    /// Use the latest schema to build the fulltext index applier.
    fn build_fulltext_index_applier(&self) -> Option<FulltextIndexApplierRef> {
        if self.ignore_fulltext_index {
            return None;
        }

        let file_cache = self.cache_strategy.write_cache().map(|w| w.file_cache());
        let puffin_metadata_cache = self.cache_strategy.puffin_metadata_cache().cloned();
        let bloom_filter_index_cache = self.cache_strategy.bloom_filter_index_cache().cloned();
        FulltextIndexApplierBuilder::new(
            self.access_layer.region_dir().to_string(),
            self.version.metadata.region_id,
            self.access_layer.object_store().clone(),
            self.access_layer.puffin_manager_factory().clone(),
            self.version.metadata.as_ref(),
        )
        .with_file_cache(file_cache)
        .with_puffin_metadata_cache(puffin_metadata_cache)
        .with_bloom_filter_cache(bloom_filter_index_cache)
        .build(&self.request.filters)
        .inspect_err(|err| warn!(err; "Failed to build fulltext index applier"))
        .ok()
        .flatten()
        .map(Arc::new)
    }
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
    pub(crate) predicate: PredicateGroup,
    /// Memtable range builders for memtables in the time range..
    pub(crate) memtables: Vec<MemRangeBuilder>,
    /// Handles to SST files to scan.
    pub(crate) files: Vec<FileHandle>,
    /// Cache.
    pub(crate) cache_strategy: CacheStrategy,
    /// Ignores file not found error.
    ignore_file_not_found: bool,
    /// Capacity of the channel to send data from parallel scan tasks to the main task.
    pub(crate) parallel_scan_channel_size: usize,
    /// Index appliers.
    inverted_index_applier: Option<InvertedIndexApplierRef>,
    bloom_filter_index_applier: Option<BloomFilterIndexApplierRef>,
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
    /// Hint for the required distribution of the scanner.
    pub(crate) distribution: Option<TimeSeriesDistribution>,
}

impl ScanInput {
    /// Creates a new [ScanInput].
    #[must_use]
    pub(crate) fn new(access_layer: AccessLayerRef, mapper: ProjectionMapper) -> ScanInput {
        ScanInput {
            access_layer,
            mapper: Arc::new(mapper),
            time_range: None,
            predicate: PredicateGroup::default(),
            memtables: Vec::new(),
            files: Vec::new(),
            cache_strategy: CacheStrategy::Disabled,
            ignore_file_not_found: false,
            parallel_scan_channel_size: DEFAULT_SCAN_CHANNEL_SIZE,
            inverted_index_applier: None,
            bloom_filter_index_applier: None,
            fulltext_index_applier: None,
            query_start: None,
            append_mode: false,
            filter_deleted: true,
            merge_mode: MergeMode::default(),
            series_row_selector: None,
            distribution: None,
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
    pub(crate) fn with_predicate(mut self, predicate: PredicateGroup) -> Self {
        self.predicate = predicate;
        self
    }

    /// Sets memtable range builders.
    #[must_use]
    pub(crate) fn with_memtables(mut self, memtables: Vec<MemRangeBuilder>) -> Self {
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
    pub(crate) fn with_cache(mut self, cache: CacheStrategy) -> Self {
        self.cache_strategy = cache;
        self
    }

    /// Ignores file not found error.
    #[must_use]
    pub(crate) fn with_ignore_file_not_found(mut self, ignore: bool) -> Self {
        self.ignore_file_not_found = ignore;
        self
    }

    /// Sets scan task channel size.
    #[must_use]
    pub(crate) fn with_parallel_scan_channel_size(
        mut self,
        parallel_scan_channel_size: usize,
    ) -> Self {
        self.parallel_scan_channel_size = parallel_scan_channel_size;
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

    /// Sets bloom filter applier.
    #[must_use]
    pub(crate) fn with_bloom_filter_index_applier(
        mut self,
        applier: Option<BloomFilterIndexApplierRef>,
    ) -> Self {
        self.bloom_filter_index_applier = applier;
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

    /// Sets the distribution hint.
    #[must_use]
    pub(crate) fn with_distribution(
        mut self,
        distribution: Option<TimeSeriesDistribution>,
    ) -> Self {
        self.distribution = distribution;
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
        if sources.len() <= 1 {
            return Ok(sources);
        }

        // Spawn a task for each source.
        let sources = sources
            .into_iter()
            .map(|source| {
                let (sender, receiver) = mpsc::channel(self.parallel_scan_channel_size);
                self.spawn_scan_task(source, semaphore.clone(), sender);
                let stream = Box::pin(ReceiverStream::new(receiver));
                Source::Stream(stream)
            })
            .collect();
        Ok(sources)
    }

    /// Builds memtable ranges to scan by `index`.
    pub(crate) fn build_mem_ranges(&self, index: RowGroupIndex) -> SmallVec<[MemtableRange; 2]> {
        let memtable = &self.memtables[index.index];
        let mut ranges = SmallVec::new();
        memtable.build_ranges(index.row_group_index, &mut ranges);
        ranges
    }

    /// Prunes a file to scan and returns the builder to build readers.
    pub(crate) async fn prune_file(
        &self,
        file_index: usize,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<FileRangeBuilder> {
        let file = &self.files[file_index];
        let res = self
            .access_layer
            .read_sst(file.clone())
            .predicate(self.predicate.predicate().cloned())
            .projection(Some(self.mapper.column_ids().to_vec()))
            .cache(self.cache_strategy.clone())
            .inverted_index_applier(self.inverted_index_applier.clone())
            .bloom_filter_index_applier(self.bloom_filter_index_applier.clone())
            .fulltext_index_applier(self.fulltext_index_applier.clone())
            .expected_metadata(Some(self.mapper.metadata().clone()))
            .build_reader_input(reader_metrics)
            .await;
        let (mut file_range_ctx, selection) = match res {
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
        if !compat::has_same_columns_and_pk_encoding(
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
        Ok(FileRangeBuilder::new(Arc::new(file_range_ctx), selection))
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

    /// Returns table predicate of all exprs.
    pub(crate) fn predicate(&self) -> Option<&Predicate> {
        self.predicate.predicate()
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

    // Metrics:
    /// The start time of the query.
    pub(crate) query_start: Instant,
}

impl StreamContext {
    /// Creates a new [StreamContext] for [SeqScan].
    pub(crate) fn seq_scan_ctx(input: ScanInput, compaction: bool) -> Self {
        let query_start = input.query_start.unwrap_or_else(Instant::now);
        let ranges = RangeMeta::seq_scan_ranges(&input, compaction);
        READ_SST_COUNT.observe(input.num_files() as f64);

        Self {
            input,
            ranges,
            query_start,
        }
    }

    /// Creates a new [StreamContext] for [UnorderedScan].
    pub(crate) fn unordered_scan_ctx(input: ScanInput) -> Self {
        let query_start = input.query_start.unwrap_or_else(Instant::now);
        let ranges = RangeMeta::unordered_scan_ranges(&input);
        READ_SST_COUNT.observe(input.num_files() as f64);

        Self {
            input,
            ranges,
            query_start,
        }
    }

    /// Returns true if the index refers to a memtable.
    pub(crate) fn is_mem_range_index(&self, index: RowGroupIndex) -> bool {
        self.input.num_memtables() > index.index
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
    pub(crate) fn format_for_explain(&self, verbose: bool, f: &mut fmt::Formatter) -> fmt::Result {
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
        if let Some(distribution) = &self.input.distribution {
            write!(f, ", distribution={}", distribution)?;
        }

        if verbose {
            self.format_verbose_content(f)?;
        }

        Ok(())
    }

    fn format_verbose_content(&self, f: &mut fmt::Formatter) -> fmt::Result {
        struct FileWrapper<'a> {
            file: &'a FileHandle,
        }

        impl fmt::Debug for FileWrapper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    f,
                    "[file={}, time_range=({}::{}, {}::{}), rows={}, size={}, index_size={}]",
                    self.file.file_id(),
                    self.file.time_range().0.value(),
                    self.file.time_range().0.unit(),
                    self.file.time_range().1.value(),
                    self.file.time_range().1.unit(),
                    self.file.num_rows(),
                    self.file.size(),
                    self.file.index_size()
                )
            }
        }

        struct InputWrapper<'a> {
            input: &'a ScanInput,
        }

        impl fmt::Debug for InputWrapper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let output_schema = self.input.mapper.output_schema();
                if !output_schema.is_empty() {
                    write!(f, ", projection=")?;
                    f.debug_list()
                        .entries(output_schema.column_schemas().iter().map(|col| &col.name))
                        .finish()?;
                }
                if let Some(predicate) = &self.input.predicate.predicate() {
                    if !predicate.exprs().is_empty() {
                        write!(f, ", filters=[")?;
                        for (i, expr) in predicate.exprs().iter().enumerate() {
                            if i == predicate.exprs().len() - 1 {
                                write!(f, "{}]", expr)?;
                            } else {
                                write!(f, "{}, ", expr)?;
                            }
                        }
                    }
                }
                if !self.input.files.is_empty() {
                    write!(f, ", files=")?;
                    f.debug_list()
                        .entries(self.input.files.iter().map(|file| FileWrapper { file }))
                        .finish()?;
                }

                Ok(())
            }
        }

        write!(f, "{:?}", InputWrapper { input: &self.input })
    }
}

/// Predicates to evaluate.
/// It only keeps filters that [SimpleFilterEvaluator] supports.
#[derive(Clone, Default)]
pub struct PredicateGroup {
    time_filters: Option<Arc<Vec<SimpleFilterEvaluator>>>,

    /// Table predicate for all logical exprs to evaluate.
    /// Parquet reader uses it to prune row groups.
    predicate: Option<Predicate>,
}

impl PredicateGroup {
    /// Creates a new `PredicateGroup` from exprs according to the metadata.
    pub fn new(metadata: &RegionMetadata, exprs: &[Expr]) -> Self {
        let mut time_filters = Vec::with_capacity(exprs.len());
        // Columns in the expr.
        let mut columns = HashSet::new();
        for expr in exprs {
            columns.clear();
            let Some(filter) = Self::expr_to_filter(expr, metadata, &mut columns) else {
                continue;
            };
            time_filters.push(filter);
        }
        let time_filters = if time_filters.is_empty() {
            None
        } else {
            Some(Arc::new(time_filters))
        };
        let predicate = Predicate::new(exprs.to_vec());

        Self {
            time_filters,
            predicate: Some(predicate),
        }
    }

    /// Returns time filters.
    pub(crate) fn time_filters(&self) -> Option<Arc<Vec<SimpleFilterEvaluator>>> {
        self.time_filters.clone()
    }

    /// Returns predicate of all exprs.
    pub(crate) fn predicate(&self) -> Option<&Predicate> {
        self.predicate.as_ref()
    }

    fn expr_to_filter(
        expr: &Expr,
        metadata: &RegionMetadata,
        columns: &mut HashSet<Column>,
    ) -> Option<SimpleFilterEvaluator> {
        columns.clear();
        // `expr_to_columns` won't return error.
        // We still ignore these expressions for safety.
        expr_to_columns(expr, columns).ok()?;
        if columns.len() > 1 {
            // Simple filter doesn't support multiple columns.
            return None;
        }
        let column = columns.iter().next()?;
        let column_meta = metadata.column_by_name(&column.name)?;
        if column_meta.semantic_type == SemanticType::Timestamp {
            SimpleFilterEvaluator::try_new(expr)
        } else {
            None
        }
    }
}
