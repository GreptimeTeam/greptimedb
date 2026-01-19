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
use common_recordbatch::SendableRecordBatchStream;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_telemetry::tracing::Instrument;
use common_telemetry::{debug, error, tracing, warn};
use common_time::range::TimestampRange;
use datafusion_common::Column;
use datafusion_expr::Expr;
use datafusion_expr::utils::expr_to_columns;
use futures::StreamExt;
use partition::expr::PartitionExpr;
use smallvec::SmallVec;
use snafu::{OptionExt as _, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::region_engine::{PartitionRange, RegionScannerRef};
use store_api::storage::{
    ColumnId, RegionId, ScanRequest, SequenceRange, TimeSeriesDistribution, TimeSeriesRowSelector,
};
use table::predicate::{Predicate, build_time_range_predicate};
use tokio::sync::{Semaphore, mpsc};
use tokio_stream::wrappers::ReceiverStream;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheStrategy;
use crate::config::{DEFAULT_MAX_CONCURRENT_SCAN_FILES, DEFAULT_SCAN_CHANNEL_SIZE};
use crate::error::{InvalidPartitionExprSnafu, InvalidRequestSnafu, Result};
#[cfg(feature = "enterprise")]
use crate::extension::{BoxedExtensionRange, BoxedExtensionRangeProvider};
use crate::memtable::{MemtableRange, RangesOptions};
use crate::metrics::READ_SST_COUNT;
use crate::read::compat::{self, CompatBatch, FlatCompatBatch, PrimaryKeyCompatBatch};
use crate::read::projection::ProjectionMapper;
use crate::read::range::{FileRangeBuilder, MemRangeBuilder, RangeMeta, RowGroupIndex};
use crate::read::seq_scan::SeqScan;
use crate::read::series_scan::SeriesScan;
use crate::read::stream::ScanBatchStream;
use crate::read::unordered_scan::UnorderedScan;
use crate::read::{Batch, BoxedRecordBatchStream, RecordBatch, Source};
use crate::region::options::MergeMode;
use crate::region::version::VersionRef;
use crate::sst::FormatType;
use crate::sst::file::FileHandle;
use crate::sst::index::bloom_filter::applier::{
    BloomFilterIndexApplierBuilder, BloomFilterIndexApplierRef,
};
use crate::sst::index::fulltext_index::applier::FulltextIndexApplierRef;
use crate::sst::index::fulltext_index::applier::builder::FulltextIndexApplierBuilder;
use crate::sst::index::inverted_index::applier::InvertedIndexApplierRef;
use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;
#[cfg(feature = "vector_index")]
use crate::sst::index::vector_index::applier::{VectorIndexApplier, VectorIndexApplierRef};
use crate::sst::parquet::file_range::PreFilterMode;
use crate::sst::parquet::reader::ReaderMetrics;

/// Parallel scan channel size for flat format.
const FLAT_SCAN_CHANNEL_SIZE: usize = 2;
#[cfg(feature = "vector_index")]
const VECTOR_INDEX_OVERFETCH_MULTIPLIER: usize = 2;

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

    /// Create a stream of [`Batch`] by this scanner.
    pub(crate) fn scan_batch(&self) -> Result<ScanBatchStream> {
        match self {
            Scanner::Seq(x) => x.scan_all_partitions(),
            Scanner::Unordered(x) => x.scan_all_partitions(),
            Scanner::Series(x) => x.scan_all_partitions(),
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
    pub(crate) fn file_ids(&self) -> Vec<crate::sst::file::RegionFileId> {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.input().file_ids(),
            Scanner::Unordered(unordered_scan) => unordered_scan.input().file_ids(),
            Scanner::Series(series_scan) => series_scan.input().file_ids(),
        }
    }

    pub(crate) fn index_ids(&self) -> Vec<crate::sst::file::RegionIndexId> {
        match self {
            Scanner::Seq(seq_scan) => seq_scan.input().index_ids(),
            Scanner::Unordered(unordered_scan) => unordered_scan.input().index_ids(),
            Scanner::Series(series_scan) => series_scan.input().index_ids(),
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
    /// Maximum number of SST files to scan concurrently.
    max_concurrent_scan_files: usize,
    /// Whether to ignore inverted index.
    ignore_inverted_index: bool,
    /// Whether to ignore fulltext index.
    ignore_fulltext_index: bool,
    /// Whether to ignore bloom filter.
    ignore_bloom_filter: bool,
    /// Start time of the scan task.
    start_time: Option<Instant>,
    /// Whether to filter out the deleted rows.
    /// Usually true for normal read, and false for scan for compaction.
    filter_deleted: bool,
    #[cfg(feature = "enterprise")]
    extension_range_provider: Option<BoxedExtensionRangeProvider>,
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
            max_concurrent_scan_files: DEFAULT_MAX_CONCURRENT_SCAN_FILES,
            ignore_inverted_index: false,
            ignore_fulltext_index: false,
            ignore_bloom_filter: false,
            start_time: None,
            filter_deleted: true,
            #[cfg(feature = "enterprise")]
            extension_range_provider: None,
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

    /// Sets maximum number of SST files to scan concurrently.
    #[must_use]
    pub(crate) fn with_max_concurrent_scan_files(
        mut self,
        max_concurrent_scan_files: usize,
    ) -> Self {
        self.max_concurrent_scan_files = max_concurrent_scan_files;
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

    pub(crate) fn set_filter_deleted(&mut self, filter_deleted: bool) {
        self.filter_deleted = filter_deleted;
    }

    #[cfg(feature = "enterprise")]
    pub(crate) fn set_extension_range_provider(
        &mut self,
        extension_range_provider: BoxedExtensionRangeProvider,
    ) {
        self.extension_range_provider = Some(extension_range_provider);
    }

    /// Returns a [Scanner] to scan the region.
    #[tracing::instrument(skip_all, fields(region_id = %self.region_id()))]
    pub(crate) async fn scanner(self) -> Result<Scanner> {
        if self.use_series_scan() {
            self.series_scan().await.map(Scanner::Series)
        } else if self.use_unordered_scan() {
            // If table is append only and there is no series row selector, we use unordered scan in query.
            // We still use seq scan in compaction.
            self.unordered_scan().await.map(Scanner::Unordered)
        } else {
            self.seq_scan().await.map(Scanner::Seq)
        }
    }

    /// Returns a [RegionScanner] to scan the region.
    #[tracing::instrument(
        level = tracing::Level::DEBUG,
        skip_all,
        fields(region_id = %self.region_id())
    )]
    pub(crate) async fn region_scanner(self) -> Result<RegionScannerRef> {
        if self.use_series_scan() {
            self.series_scan()
                .await
                .map(|scanner| Box::new(scanner) as _)
        } else if self.use_unordered_scan() {
            self.unordered_scan()
                .await
                .map(|scanner| Box::new(scanner) as _)
        } else {
            self.seq_scan().await.map(|scanner| Box::new(scanner) as _)
        }
    }

    /// Scan sequentially.
    #[tracing::instrument(skip_all, fields(region_id = %self.region_id()))]
    pub(crate) async fn seq_scan(self) -> Result<SeqScan> {
        let input = self.scan_input().await?.with_compaction(false);
        Ok(SeqScan::new(input))
    }

    /// Unordered scan.
    #[tracing::instrument(skip_all, fields(region_id = %self.region_id()))]
    pub(crate) async fn unordered_scan(self) -> Result<UnorderedScan> {
        let input = self.scan_input().await?;
        Ok(UnorderedScan::new(input))
    }

    /// Scans by series.
    #[tracing::instrument(skip_all, fields(region_id = %self.region_id()))]
    pub(crate) async fn series_scan(self) -> Result<SeriesScan> {
        let input = self.scan_input().await?;
        Ok(SeriesScan::new(input))
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

    /// Returns true if the region use flat format.
    fn use_flat_format(&self) -> bool {
        self.version.options.sst_format.unwrap_or_default() == FormatType::Flat
    }

    /// Creates a scan input.
    #[tracing::instrument(skip_all, fields(region_id = %self.region_id()))]
    async fn scan_input(mut self) -> Result<ScanInput> {
        let sst_min_sequence = self.request.sst_min_sequence.and_then(NonZeroU64::new);
        let time_range = self.build_time_range_predicate();
        let predicate = PredicateGroup::new(&self.version.metadata, &self.request.filters)?;
        let flat_format = self.use_flat_format();

        let read_column_ids = match &self.request.projection {
            Some(p) => self.build_read_column_ids(p, &predicate)?,
            None => self
                .version
                .metadata
                .column_metadatas
                .iter()
                .map(|col| col.column_id)
                .collect(),
        };

        // The mapper always computes projected column ids as the schema of SSTs may change.
        let mapper = match &self.request.projection {
            Some(p) => ProjectionMapper::new_with_read_columns(
                &self.version.metadata,
                p.iter().copied(),
                flat_format,
                read_column_ids.clone(),
            )?,
            None => ProjectionMapper::all(&self.version.metadata, flat_format)?,
        };

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
        let mut mem_range_builders = Vec::new();
        let filter_mode = pre_filter_mode(
            self.version.options.append_mode,
            self.version.options.merge_mode(),
        );

        for m in memtables {
            // check if memtable is empty by reading stats.
            let Some((start, end)) = m.stats().time_range() else {
                continue;
            };
            // The time range of the memtable is inclusive.
            let memtable_range = TimestampRange::new_inclusive(Some(start), Some(end));
            if !memtable_range.intersects(&time_range) {
                continue;
            }
            let ranges_in_memtable = m.ranges(
                Some(read_column_ids.as_slice()),
                RangesOptions::default()
                    .with_predicate(predicate.clone())
                    .with_sequence(SequenceRange::new(
                        self.request.memtable_min_sequence,
                        self.request.memtable_max_sequence,
                    ))
                    .with_pre_filter_mode(filter_mode),
            )?;
            mem_range_builders.extend(ranges_in_memtable.ranges.into_values().map(|v| {
                let stats = v.stats().clone();
                MemRangeBuilder::new(v, stats)
            }));
        }

        let region_id = self.region_id();
        debug!(
            "Scan region {}, request: {:?}, time range: {:?}, memtables: {}, ssts_to_read: {}, append_mode: {}, flat_format: {}",
            region_id,
            self.request,
            time_range,
            mem_range_builders.len(),
            files.len(),
            self.version.options.append_mode,
            flat_format,
        );

        let (non_field_filters, field_filters) = self.partition_by_field_filters();
        let inverted_index_appliers = [
            self.build_invereted_index_applier(&non_field_filters),
            self.build_invereted_index_applier(&field_filters),
        ];
        let bloom_filter_appliers = [
            self.build_bloom_filter_applier(&non_field_filters),
            self.build_bloom_filter_applier(&field_filters),
        ];
        let fulltext_index_appliers = [
            self.build_fulltext_index_applier(&non_field_filters),
            self.build_fulltext_index_applier(&field_filters),
        ];
        #[cfg(feature = "vector_index")]
        let vector_index_applier = self.build_vector_index_applier();
        #[cfg(feature = "vector_index")]
        let vector_index_k = self.request.vector_search.as_ref().map(|search| {
            if self.request.filters.is_empty() {
                search.k
            } else {
                search.k.saturating_mul(VECTOR_INDEX_OVERFETCH_MULTIPLIER)
            }
        });

        if flat_format {
            // The batch is already large enough so we use a small channel size here.
            self.parallel_scan_channel_size = FLAT_SCAN_CHANNEL_SIZE;
        }

        let input = ScanInput::new(self.access_layer, mapper)
            .with_read_column_ids(read_column_ids)
            .with_time_range(Some(time_range))
            .with_predicate(predicate)
            .with_memtables(mem_range_builders)
            .with_files(files)
            .with_cache(self.cache_strategy)
            .with_inverted_index_appliers(inverted_index_appliers)
            .with_bloom_filter_index_appliers(bloom_filter_appliers)
            .with_fulltext_index_appliers(fulltext_index_appliers)
            .with_parallel_scan_channel_size(self.parallel_scan_channel_size)
            .with_max_concurrent_scan_files(self.max_concurrent_scan_files)
            .with_start_time(self.start_time)
            .with_append_mode(self.version.options.append_mode)
            .with_filter_deleted(self.filter_deleted)
            .with_merge_mode(self.version.options.merge_mode())
            .with_series_row_selector(self.request.series_row_selector)
            .with_distribution(self.request.distribution)
            .with_flat_format(flat_format);
        #[cfg(feature = "vector_index")]
        let input = input
            .with_vector_index_applier(vector_index_applier)
            .with_vector_index_k(vector_index_k);

        #[cfg(feature = "enterprise")]
        let input = if let Some(provider) = self.extension_range_provider {
            let ranges = provider
                .find_extension_ranges(self.version.flushed_sequence, time_range, &self.request)
                .await?;
            debug!("Find extension ranges: {ranges:?}");
            input.with_extension_ranges(ranges)
        } else {
            input
        };
        Ok(input)
    }

    fn region_id(&self) -> RegionId {
        self.version.metadata.region_id
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

    /// Return all columns id to read according to the projection and filters.
    fn build_read_column_ids(
        &self,
        projection: &[usize],
        predicate: &PredicateGroup,
    ) -> Result<Vec<ColumnId>> {
        let metadata = &self.version.metadata;
        // use Vec for read_column_ids to keep the order of columns.
        let mut read_column_ids = Vec::new();
        let mut seen = HashSet::new();

        for idx in projection {
            let column = metadata
                .column_metadatas
                .get(*idx)
                .context(InvalidRequestSnafu {
                    region_id: metadata.region_id,
                    reason: format!("projection index {} is out of bound", idx),
                })?;
            // keep the projection order
            seen.insert(column.column_id);
            read_column_ids.push(column.column_id);
        }

        if projection.is_empty() {
            let time_index = metadata.time_index_column().column_id;
            if seen.insert(time_index) {
                read_column_ids.push(time_index);
            }
        }

        let mut extra_names = HashSet::new();
        let mut columns = HashSet::new();

        for expr in &self.request.filters {
            columns.clear();
            if expr_to_columns(expr, &mut columns).is_err() {
                continue;
            }
            extra_names.extend(columns.iter().map(|column| column.name.clone()));
        }

        if let Some(expr) = predicate.region_partition_expr() {
            expr.collect_column_names(&mut extra_names);
        }

        if !extra_names.is_empty() {
            for column in &metadata.column_metadatas {
                if extra_names.contains(column.column_schema.name.as_str())
                    && !seen.contains(&column.column_id)
                {
                    read_column_ids.push(column.column_id);
                }
                extra_names.remove(column.column_schema.name.as_str());
            }
            if !extra_names.is_empty() {
                warn!(
                    "Some columns in filters are not found in region {}: {:?}",
                    metadata.region_id, extra_names
                );
            }
        }
        Ok(read_column_ids)
    }

    /// Partitions filters into two groups: non-field filters and field filters.
    /// Returns `(non_field_filters, field_filters)`.
    fn partition_by_field_filters(&self) -> (Vec<Expr>, Vec<Expr>) {
        let field_columns = self
            .version
            .metadata
            .field_columns()
            .map(|col| &col.column_schema.name)
            .collect::<HashSet<_>>();

        let mut columns = HashSet::new();

        self.request.filters.iter().cloned().partition(|expr| {
            columns.clear();
            // `expr_to_columns` won't return error.
            if expr_to_columns(expr, &mut columns).is_err() {
                // If we can't extract columns, treat it as non-field filter
                return true;
            }
            // Return true for non-field filters (partition puts true cases in first vec)
            !columns
                .iter()
                .any(|column| field_columns.contains(&column.name))
        })
    }

    /// Use the latest schema to build the inverted index applier.
    fn build_invereted_index_applier(&self, filters: &[Expr]) -> Option<InvertedIndexApplierRef> {
        if self.ignore_inverted_index {
            return None;
        }

        let file_cache = self.cache_strategy.write_cache().map(|w| w.file_cache());
        let inverted_index_cache = self.cache_strategy.inverted_index_cache().cloned();

        let puffin_metadata_cache = self.cache_strategy.puffin_metadata_cache().cloned();

        InvertedIndexApplierBuilder::new(
            self.access_layer.table_dir().to_string(),
            self.access_layer.path_type(),
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
        .build(filters)
        .inspect_err(|err| warn!(err; "Failed to build invereted index applier"))
        .ok()
        .flatten()
        .map(Arc::new)
    }

    /// Use the latest schema to build the bloom filter index applier.
    fn build_bloom_filter_applier(&self, filters: &[Expr]) -> Option<BloomFilterIndexApplierRef> {
        if self.ignore_bloom_filter {
            return None;
        }

        let file_cache = self.cache_strategy.write_cache().map(|w| w.file_cache());
        let bloom_filter_index_cache = self.cache_strategy.bloom_filter_index_cache().cloned();
        let puffin_metadata_cache = self.cache_strategy.puffin_metadata_cache().cloned();

        BloomFilterIndexApplierBuilder::new(
            self.access_layer.table_dir().to_string(),
            self.access_layer.path_type(),
            self.access_layer.object_store().clone(),
            self.version.metadata.as_ref(),
            self.access_layer.puffin_manager_factory().clone(),
        )
        .with_file_cache(file_cache)
        .with_bloom_filter_index_cache(bloom_filter_index_cache)
        .with_puffin_metadata_cache(puffin_metadata_cache)
        .build(filters)
        .inspect_err(|err| warn!(err; "Failed to build bloom filter index applier"))
        .ok()
        .flatten()
        .map(Arc::new)
    }

    /// Use the latest schema to build the fulltext index applier.
    fn build_fulltext_index_applier(&self, filters: &[Expr]) -> Option<FulltextIndexApplierRef> {
        if self.ignore_fulltext_index {
            return None;
        }

        let file_cache = self.cache_strategy.write_cache().map(|w| w.file_cache());
        let puffin_metadata_cache = self.cache_strategy.puffin_metadata_cache().cloned();
        let bloom_filter_index_cache = self.cache_strategy.bloom_filter_index_cache().cloned();
        FulltextIndexApplierBuilder::new(
            self.access_layer.table_dir().to_string(),
            self.access_layer.path_type(),
            self.access_layer.object_store().clone(),
            self.access_layer.puffin_manager_factory().clone(),
            self.version.metadata.as_ref(),
        )
        .with_file_cache(file_cache)
        .with_puffin_metadata_cache(puffin_metadata_cache)
        .with_bloom_filter_cache(bloom_filter_index_cache)
        .build(filters)
        .inspect_err(|err| warn!(err; "Failed to build fulltext index applier"))
        .ok()
        .flatten()
        .map(Arc::new)
    }

    /// Build the vector index applier from vector search request.
    #[cfg(feature = "vector_index")]
    fn build_vector_index_applier(&self) -> Option<VectorIndexApplierRef> {
        let vector_search = self.request.vector_search.as_ref()?;

        let file_cache = self.cache_strategy.write_cache().map(|w| w.file_cache());
        let puffin_metadata_cache = self.cache_strategy.puffin_metadata_cache().cloned();
        let vector_index_cache = self.cache_strategy.vector_index_cache().cloned();

        let applier = VectorIndexApplier::new(
            self.access_layer.table_dir().to_string(),
            self.access_layer.path_type(),
            self.access_layer.object_store().clone(),
            self.access_layer.puffin_manager_factory().clone(),
            vector_search.column_id,
            vector_search.query_vector.clone(),
            vector_search.metric,
        )
        .with_file_cache(file_cache)
        .with_puffin_metadata_cache(puffin_metadata_cache)
        .with_vector_index_cache(vector_index_cache);

        Some(Arc::new(applier))
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
pub struct ScanInput {
    /// Region SST access layer.
    access_layer: AccessLayerRef,
    /// Maps projected Batches to RecordBatches.
    pub(crate) mapper: Arc<ProjectionMapper>,
    /// Column ids to read from memtables and SSTs.
    /// Notice this is different from the columns in `mapper` which are projected columns.
    /// But this read columns might also include non-projected columns needed for filtering.
    pub(crate) read_column_ids: Vec<ColumnId>,
    /// Time range filter for time index.
    time_range: Option<TimestampRange>,
    /// Predicate to push down.
    pub(crate) predicate: PredicateGroup,
    /// Region partition expr applied at read time.
    region_partition_expr: Option<PartitionExpr>,
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
    /// Maximum number of SST files to scan concurrently.
    pub(crate) max_concurrent_scan_files: usize,
    /// Index appliers.
    inverted_index_appliers: [Option<InvertedIndexApplierRef>; 2],
    bloom_filter_index_appliers: [Option<BloomFilterIndexApplierRef>; 2],
    fulltext_index_appliers: [Option<FulltextIndexApplierRef>; 2],
    /// Vector index applier for KNN search.
    #[cfg(feature = "vector_index")]
    pub(crate) vector_index_applier: Option<VectorIndexApplierRef>,
    /// Over-fetched k for vector index scan.
    #[cfg(feature = "vector_index")]
    pub(crate) vector_index_k: Option<usize>,
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
    /// Whether to use flat format.
    pub(crate) flat_format: bool,
    /// Whether this scan is for compaction.
    pub(crate) compaction: bool,
    #[cfg(feature = "enterprise")]
    extension_ranges: Vec<BoxedExtensionRange>,
}

impl ScanInput {
    /// Creates a new [ScanInput].
    #[must_use]
    pub(crate) fn new(access_layer: AccessLayerRef, mapper: ProjectionMapper) -> ScanInput {
        ScanInput {
            access_layer,
            mapper: Arc::new(mapper),
            read_column_ids: Vec::new(),
            time_range: None,
            predicate: PredicateGroup::default(),
            region_partition_expr: None,
            memtables: Vec::new(),
            files: Vec::new(),
            cache_strategy: CacheStrategy::Disabled,
            ignore_file_not_found: false,
            parallel_scan_channel_size: DEFAULT_SCAN_CHANNEL_SIZE,
            max_concurrent_scan_files: DEFAULT_MAX_CONCURRENT_SCAN_FILES,
            inverted_index_appliers: [None, None],
            bloom_filter_index_appliers: [None, None],
            fulltext_index_appliers: [None, None],
            #[cfg(feature = "vector_index")]
            vector_index_applier: None,
            #[cfg(feature = "vector_index")]
            vector_index_k: None,
            query_start: None,
            append_mode: false,
            filter_deleted: true,
            merge_mode: MergeMode::default(),
            series_row_selector: None,
            distribution: None,
            flat_format: false,
            compaction: false,
            #[cfg(feature = "enterprise")]
            extension_ranges: Vec::new(),
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
        self.region_partition_expr = predicate.region_partition_expr().cloned();
        self.predicate = predicate;
        self
    }

    /// Sets column ids to read from memtables and SSTs.
    #[must_use]
    pub(crate) fn with_read_column_ids(mut self, read_column_ids: Vec<ColumnId>) -> Self {
        self.read_column_ids = read_column_ids;
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

    /// Sets maximum number of SST files to scan concurrently.
    #[must_use]
    pub(crate) fn with_max_concurrent_scan_files(
        mut self,
        max_concurrent_scan_files: usize,
    ) -> Self {
        self.max_concurrent_scan_files = max_concurrent_scan_files;
        self
    }

    /// Sets inverted index appliers.
    #[must_use]
    pub(crate) fn with_inverted_index_appliers(
        mut self,
        appliers: [Option<InvertedIndexApplierRef>; 2],
    ) -> Self {
        self.inverted_index_appliers = appliers;
        self
    }

    /// Sets bloom filter appliers.
    #[must_use]
    pub(crate) fn with_bloom_filter_index_appliers(
        mut self,
        appliers: [Option<BloomFilterIndexApplierRef>; 2],
    ) -> Self {
        self.bloom_filter_index_appliers = appliers;
        self
    }

    /// Sets fulltext index appliers.
    #[must_use]
    pub(crate) fn with_fulltext_index_appliers(
        mut self,
        appliers: [Option<FulltextIndexApplierRef>; 2],
    ) -> Self {
        self.fulltext_index_appliers = appliers;
        self
    }

    /// Sets vector index applier for KNN search.
    #[cfg(feature = "vector_index")]
    #[must_use]
    pub(crate) fn with_vector_index_applier(
        mut self,
        applier: Option<VectorIndexApplierRef>,
    ) -> Self {
        self.vector_index_applier = applier;
        self
    }

    /// Sets over-fetched k for vector index scan.
    #[cfg(feature = "vector_index")]
    #[must_use]
    pub(crate) fn with_vector_index_k(mut self, k: Option<usize>) -> Self {
        self.vector_index_k = k;
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

    /// Sets whether to use flat format.
    #[must_use]
    pub(crate) fn with_flat_format(mut self, flat_format: bool) -> Self {
        self.flat_format = flat_format;
        self
    }

    /// Sets whether this scan is for compaction.
    #[must_use]
    pub(crate) fn with_compaction(mut self, compaction: bool) -> Self {
        self.compaction = compaction;
        self
    }

    /// Scans sources in parallel.
    ///
    /// # Panics if the input doesn't allow parallel scan.
    #[tracing::instrument(
        skip(self, sources, semaphore),
        fields(
            region_id = %self.region_metadata().region_id,
            source_count = sources.len()
        )
    )]
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

    fn predicate_for_file(&self, file: &FileHandle) -> Option<Predicate> {
        if self.should_skip_region_partition(file) {
            self.predicate.predicate_without_region().cloned()
        } else {
            self.predicate.predicate().cloned()
        }
    }

    fn should_skip_region_partition(&self, file: &FileHandle) -> bool {
        match (
            self.region_partition_expr.as_ref(),
            file.meta_ref().partition_expr.as_ref(),
        ) {
            (Some(region_expr), Some(file_expr)) => region_expr == file_expr,
            _ => false,
        }
    }

    /// Prunes a file to scan and returns the builder to build readers.
    #[tracing::instrument(
        skip_all,
        fields(
            region_id = %self.region_metadata().region_id,
            file_id = %file.file_id()
        )
    )]
    pub async fn prune_file(
        &self,
        file: &FileHandle,
        reader_metrics: &mut ReaderMetrics,
    ) -> Result<FileRangeBuilder> {
        let predicate = self.predicate_for_file(file);
        let filter_mode = pre_filter_mode(self.append_mode, self.merge_mode);
        let decode_pk_values = !self.compaction && self.mapper.has_tags();
        let reader = self
            .access_layer
            .read_sst(file.clone())
            .predicate(predicate)
            .projection(Some(self.read_column_ids.clone()))
            .cache(self.cache_strategy.clone())
            .inverted_index_appliers(self.inverted_index_appliers.clone())
            .bloom_filter_index_appliers(self.bloom_filter_index_appliers.clone())
            .fulltext_index_appliers(self.fulltext_index_appliers.clone());
        #[cfg(feature = "vector_index")]
        let reader = {
            let mut reader = reader;
            reader =
                reader.vector_index_applier(self.vector_index_applier.clone(), self.vector_index_k);
            reader
        };
        let res = reader
            .expected_metadata(Some(self.mapper.metadata().clone()))
            .flat_format(self.flat_format)
            .compaction(self.compaction)
            .pre_filter_mode(filter_mode)
            .decode_primary_key_values(decode_pk_values)
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

        let need_compat = !compat::has_same_columns_and_pk_encoding(
            self.mapper.metadata(),
            file_range_ctx.read_format().metadata(),
        );
        if need_compat {
            // They have different schema. We need to adapt the batch first so the
            // mapper can convert it.
            let compat = if let Some(flat_format) = file_range_ctx.read_format().as_flat() {
                let mapper = self.mapper.as_flat().unwrap();
                FlatCompatBatch::try_new(
                    mapper,
                    flat_format.metadata(),
                    flat_format.format_projection(),
                    self.compaction,
                )?
                .map(CompatBatch::Flat)
            } else {
                let compact_batch = PrimaryKeyCompatBatch::new(
                    &self.mapper,
                    file_range_ctx.read_format().metadata().clone(),
                )?;
                Some(CompatBatch::PrimaryKey(compact_batch))
            };
            file_range_ctx.set_compat_batch(compat);
        }
        Ok(FileRangeBuilder::new(Arc::new(file_range_ctx), selection))
    }

    /// Scans the input source in another task and sends batches to the sender.
    #[tracing::instrument(
        skip(self, input, semaphore, sender),
        fields(region_id = %self.region_metadata().region_id)
    )]
    pub(crate) fn spawn_scan_task(
        &self,
        mut input: Source,
        semaphore: Arc<Semaphore>,
        sender: mpsc::Sender<Result<Batch>>,
    ) {
        let region_id = self.region_metadata().region_id;
        let span = tracing::info_span!(
            "ScanInput::parallel_scan_task",
            region_id = %region_id,
            stream_kind = "batch"
        );
        common_runtime::spawn_global(
            async move {
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
            }
            .instrument(span),
        );
    }

    /// Scans flat sources (RecordBatch streams) in parallel.
    ///
    /// # Panics if the input doesn't allow parallel scan.
    #[tracing::instrument(
        skip(self, sources, semaphore),
        fields(
            region_id = %self.region_metadata().region_id,
            source_count = sources.len()
        )
    )]
    pub(crate) fn create_parallel_flat_sources(
        &self,
        sources: Vec<BoxedRecordBatchStream>,
        semaphore: Arc<Semaphore>,
    ) -> Result<Vec<BoxedRecordBatchStream>> {
        if sources.len() <= 1 {
            return Ok(sources);
        }

        // Spawn a task for each source.
        let sources = sources
            .into_iter()
            .map(|source| {
                let (sender, receiver) = mpsc::channel(self.parallel_scan_channel_size);
                self.spawn_flat_scan_task(source, semaphore.clone(), sender);
                let stream = Box::pin(ReceiverStream::new(receiver));
                Box::pin(stream) as _
            })
            .collect();
        Ok(sources)
    }

    /// Spawns a task to scan a flat source (RecordBatch stream) asynchronously.
    #[tracing::instrument(
        skip(self, input, semaphore, sender),
        fields(region_id = %self.region_metadata().region_id)
    )]
    pub(crate) fn spawn_flat_scan_task(
        &self,
        mut input: BoxedRecordBatchStream,
        semaphore: Arc<Semaphore>,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) {
        let region_id = self.region_metadata().region_id;
        let span = tracing::info_span!(
            "ScanInput::parallel_scan_task",
            region_id = %region_id,
            stream_kind = "flat"
        );
        common_runtime::spawn_global(
            async move {
                loop {
                    // We release the permit before sending result to avoid the task waiting on
                    // the channel with the permit held.
                    let maybe_batch = {
                        // Safety: We never close the semaphore.
                        let _permit = semaphore.acquire().await.unwrap();
                        input.next().await
                    };
                    match maybe_batch {
                        Some(Ok(batch)) => {
                            let _ = sender.send(Ok(batch)).await;
                        }
                        Some(Err(e)) => {
                            let _ = sender.send(Err(e)).await;
                            break;
                        }
                        None => break,
                    }
                }
            }
            .instrument(span),
        );
    }

    pub(crate) fn total_rows(&self) -> usize {
        let rows_in_files: usize = self.files.iter().map(|f| f.num_rows()).sum();
        let rows_in_memtables: usize = self.memtables.iter().map(|m| m.stats().num_rows()).sum();

        let rows = rows_in_files + rows_in_memtables;
        #[cfg(feature = "enterprise")]
        let rows = rows
            + self
                .extension_ranges
                .iter()
                .map(|x| x.num_rows())
                .sum::<u64>() as usize;
        rows
    }

    pub(crate) fn predicate_group(&self) -> &PredicateGroup {
        &self.predicate
    }

    /// Returns number of memtables to scan.
    pub(crate) fn num_memtables(&self) -> usize {
        self.memtables.len()
    }

    /// Returns number of SST files to scan.
    pub(crate) fn num_files(&self) -> usize {
        self.files.len()
    }

    /// Gets the file handle from a row group index.
    pub(crate) fn file_from_index(&self, index: RowGroupIndex) -> &FileHandle {
        let file_index = index.index - self.num_memtables();
        &self.files[file_index]
    }

    pub fn region_metadata(&self) -> &RegionMetadataRef {
        self.mapper.metadata()
    }
}

#[cfg(feature = "enterprise")]
impl ScanInput {
    #[must_use]
    pub(crate) fn with_extension_ranges(self, extension_ranges: Vec<BoxedExtensionRange>) -> Self {
        Self {
            extension_ranges,
            ..self
        }
    }

    #[cfg(feature = "enterprise")]
    pub(crate) fn extension_ranges(&self) -> &[BoxedExtensionRange] {
        &self.extension_ranges
    }

    /// Get a boxed [ExtensionRange] by the index in all ranges.
    #[cfg(feature = "enterprise")]
    pub(crate) fn extension_range(&self, i: usize) -> &BoxedExtensionRange {
        &self.extension_ranges[i - self.num_memtables() - self.num_files()]
    }
}

#[cfg(test)]
impl ScanInput {
    /// Returns SST file ids to scan.
    pub(crate) fn file_ids(&self) -> Vec<crate::sst::file::RegionFileId> {
        self.files.iter().map(|file| file.file_id()).collect()
    }

    pub(crate) fn index_ids(&self) -> Vec<crate::sst::file::RegionIndexId> {
        self.files.iter().map(|file| file.index_id()).collect()
    }
}

fn pre_filter_mode(append_mode: bool, merge_mode: MergeMode) -> PreFilterMode {
    if append_mode {
        return PreFilterMode::All;
    }

    match merge_mode {
        MergeMode::LastRow => PreFilterMode::SkipFieldsOnDelete,
        MergeMode::LastNonNull => PreFilterMode::SkipFields,
    }
}

/// Context shared by different streams from a scanner.
/// It contains the input and ranges to scan.
pub struct StreamContext {
    /// Input memtables and files.
    pub input: ScanInput,
    /// Metadata for partition ranges.
    pub(crate) ranges: Vec<RangeMeta>,

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

    pub(crate) fn is_file_range_index(&self, index: RowGroupIndex) -> bool {
        !self.is_mem_range_index(index)
            && index.index < self.input.num_files() + self.input.num_memtables()
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
        let (mut num_mem_ranges, mut num_file_ranges, mut num_other_ranges) = (0, 0, 0);
        for range_meta in &self.ranges {
            for idx in &range_meta.row_group_indices {
                if self.is_mem_range_index(*idx) {
                    num_mem_ranges += 1;
                } else if self.is_file_range_index(*idx) {
                    num_file_ranges += 1;
                } else {
                    num_other_ranges += 1;
                }
            }
        }
        if verbose {
            write!(f, "{{")?;
        }
        write!(
            f,
            r#""partition_count":{{"count":{}, "mem_ranges":{}, "files":{}, "file_ranges":{}"#,
            self.ranges.len(),
            num_mem_ranges,
            self.input.num_files(),
            num_file_ranges,
        )?;
        if num_other_ranges > 0 {
            write!(f, r#", "other_ranges":{}"#, num_other_ranges)?;
        }
        write!(f, "}}")?;

        if let Some(selector) = &self.input.series_row_selector {
            write!(f, ", \"selector\":\"{}\"", selector)?;
        }
        if let Some(distribution) = &self.input.distribution {
            write!(f, ", \"distribution\":\"{}\"", distribution)?;
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
                let (start, end) = self.file.time_range();
                write!(
                    f,
                    r#"{{"file_id":"{}","time_range_start":"{}::{}","time_range_end":"{}::{}","rows":{},"size":{},"index_size":{}}}"#,
                    self.file.file_id(),
                    start.value(),
                    start.unit(),
                    end.value(),
                    end.unit(),
                    self.file.num_rows(),
                    self.file.size(),
                    self.file.index_size()
                )
            }
        }

        struct InputWrapper<'a> {
            input: &'a ScanInput,
        }

        #[cfg(feature = "enterprise")]
        impl InputWrapper<'_> {
            fn format_extension_ranges(&self, f: &mut fmt::Formatter) -> fmt::Result {
                if self.input.extension_ranges.is_empty() {
                    return Ok(());
                }

                let mut delimiter = "";
                write!(f, ", extension_ranges: [")?;
                for range in self.input.extension_ranges() {
                    write!(f, "{}{:?}", delimiter, range)?;
                    delimiter = ", ";
                }
                write!(f, "]")?;
                Ok(())
            }
        }

        impl fmt::Debug for InputWrapper<'_> {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                let output_schema = self.input.mapper.output_schema();
                if !output_schema.is_empty() {
                    let names: Vec<_> = output_schema
                        .column_schemas()
                        .iter()
                        .map(|col| &col.name)
                        .collect();
                    write!(f, ", \"projection\": {:?}", names)?;
                }
                if let Some(predicate) = &self.input.predicate.predicate()
                    && !predicate.exprs().is_empty()
                {
                    let exprs: Vec<_> = predicate.exprs().iter().map(|e| e.to_string()).collect();
                    write!(f, ", \"filters\": {:?}", exprs)?;
                }
                if !self.input.files.is_empty() {
                    write!(f, ", \"files\": ")?;
                    f.debug_list()
                        .entries(self.input.files.iter().map(|file| FileWrapper { file }))
                        .finish()?;
                }
                write!(f, ", \"flat_format\": {}", self.input.flat_format)?;

                #[cfg(feature = "enterprise")]
                self.format_extension_ranges(f)?;

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
    /// Predicate that includes request filters and region partition expr (if any).
    predicate_all: Option<Predicate>,
    /// Predicate that only includes request filters.
    predicate_without_region: Option<Predicate>,
    /// Region partition expression restored from metadata.
    region_partition_expr: Option<PartitionExpr>,
}

impl PredicateGroup {
    /// Creates a new `PredicateGroup` from exprs according to the metadata.
    pub fn new(metadata: &RegionMetadata, exprs: &[Expr]) -> Result<Self> {
        let mut combined_exprs = exprs.to_vec();
        let mut region_partition_expr = None;

        if let Some(expr_json) = metadata.partition_expr.as_ref()
            && !expr_json.is_empty()
            && let Some(expr) = PartitionExpr::from_json_str(expr_json)
                .context(InvalidPartitionExprSnafu { expr: expr_json })?
        {
            let logical_expr = expr
                .try_as_logical_expr()
                .context(InvalidPartitionExprSnafu {
                    expr: expr_json.clone(),
                })?;

            combined_exprs.push(logical_expr);
            region_partition_expr = Some(expr);
        }

        let mut time_filters = Vec::with_capacity(combined_exprs.len());
        // Columns in the expr.
        let mut columns = HashSet::new();
        for expr in &combined_exprs {
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

        let predicate_all = if combined_exprs.is_empty() {
            None
        } else {
            Some(Predicate::new(combined_exprs))
        };
        let predicate_without_region = if exprs.is_empty() {
            None
        } else {
            Some(Predicate::new(exprs.to_vec()))
        };

        Ok(Self {
            time_filters,
            predicate_all,
            predicate_without_region,
            region_partition_expr,
        })
    }

    /// Returns time filters.
    pub(crate) fn time_filters(&self) -> Option<Arc<Vec<SimpleFilterEvaluator>>> {
        self.time_filters.clone()
    }

    /// Returns predicate of all exprs (including region partition expr if present).
    pub(crate) fn predicate(&self) -> Option<&Predicate> {
        self.predicate_all.as_ref()
    }

    /// Returns predicate that excludes region partition expr.
    pub(crate) fn predicate_without_region(&self) -> Option<&Predicate> {
        self.predicate_without_region.as_ref()
    }

    /// Returns the region partition expr from metadata, if any.
    pub(crate) fn region_partition_expr(&self) -> Option<&PartitionExpr> {
        self.region_partition_expr.as_ref()
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion_expr::{col, lit};
    use store_api::storage::ScanRequest;

    use super::*;
    use crate::memtable::time_partition::TimePartitions;
    use crate::region::version::VersionBuilder;
    use crate::test_util::memtable_util::{EmptyMemtableBuilder, metadata_with_primary_key};
    use crate::test_util::scheduler_util::SchedulerEnv;

    fn new_version(metadata: RegionMetadataRef) -> VersionRef {
        let mutable = Arc::new(TimePartitions::new(
            metadata.clone(),
            Arc::new(EmptyMemtableBuilder::default()),
            0,
            None,
        ));
        Arc::new(VersionBuilder::new(metadata, mutable).build())
    }

    #[tokio::test]
    async fn test_build_read_column_ids_includes_filters() {
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let version = new_version(metadata.clone());
        let env = SchedulerEnv::new().await;
        let request = ScanRequest {
            projection: Some(vec![4]),
            filters: vec![
                col("v0").gt(lit(1)),
                col("ts").gt(lit(0)),
                col("k0").eq(lit("foo")),
            ],
            ..Default::default()
        };
        let scan_region = ScanRegion::new(
            version,
            env.access_layer.clone(),
            request,
            CacheStrategy::Disabled,
        );
        let predicate =
            PredicateGroup::new(metadata.as_ref(), &scan_region.request.filters).unwrap();
        let projection = scan_region.request.projection.as_ref().unwrap();
        let read_ids = scan_region
            .build_read_column_ids(projection, &predicate)
            .unwrap();
        assert_eq!(vec![4, 0, 2, 3], read_ids);
    }

    #[tokio::test]
    async fn test_build_read_column_ids_empty_projection() {
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let version = new_version(metadata.clone());
        let env = SchedulerEnv::new().await;
        let request = ScanRequest {
            projection: Some(vec![]),
            ..Default::default()
        };
        let scan_region = ScanRegion::new(
            version,
            env.access_layer.clone(),
            request,
            CacheStrategy::Disabled,
        );
        let predicate =
            PredicateGroup::new(metadata.as_ref(), &scan_region.request.filters).unwrap();
        let projection = scan_region.request.projection.as_ref().unwrap();
        let read_ids = scan_region
            .build_read_column_ids(projection, &predicate)
            .unwrap();
        // Empty projection should still read the time index column (id 2 in this test schema).
        assert_eq!(vec![2], read_ids);
    }

    #[tokio::test]
    async fn test_build_read_column_ids_keeps_projection_order() {
        let metadata = Arc::new(metadata_with_primary_key(vec![0, 1], false));
        let version = new_version(metadata.clone());
        let env = SchedulerEnv::new().await;
        let request = ScanRequest {
            projection: Some(vec![4, 1]),
            filters: vec![col("v0").gt(lit(1))],
            ..Default::default()
        };
        let scan_region = ScanRegion::new(
            version,
            env.access_layer.clone(),
            request,
            CacheStrategy::Disabled,
        );
        let predicate =
            PredicateGroup::new(metadata.as_ref(), &scan_region.request.filters).unwrap();
        let projection = scan_region.request.projection.as_ref().unwrap();
        let read_ids = scan_region
            .build_read_column_ids(projection, &predicate)
            .unwrap();
        // Projection order preserved, extra columns appended in schema order.
        assert_eq!(vec![4, 1, 3], read_ids);
    }
}
