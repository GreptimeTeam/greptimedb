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

//! Utilities for scanners.

use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fmt;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use async_stream::try_stream;
use common_telemetry::tracing;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, Time};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::timestamp::timestamp_array_to_primitive;
use futures::Stream;
use prometheus::IntGauge;
use smallvec::SmallVec;
use snafu::OptionExt;
use store_api::storage::RegionId;

use crate::error::{Result, UnexpectedSnafu};
use crate::memtable::MemScanMetrics;
use crate::metrics::{
    IN_PROGRESS_SCAN, PRECISE_FILTER_ROWS_TOTAL, READ_BATCHES_RETURN, READ_ROW_GROUPS_TOTAL,
    READ_ROWS_IN_ROW_GROUP_TOTAL, READ_ROWS_RETURN, READ_STAGE_ELAPSED,
};
use crate::read::dedup::{DedupMetrics, DedupMetricsReport};
use crate::read::merge::{MergeMetrics, MergeMetricsReport};
use crate::read::range::{RangeBuilderList, RangeMeta, RowGroupIndex};
use crate::read::scan_region::StreamContext;
use crate::read::{Batch, BoxedBatchStream, BoxedRecordBatchStream, ScannerMetrics, Source};
use crate::sst::file::{FileTimeRange, RegionFileId};
use crate::sst::index::bloom_filter::applier::BloomFilterIndexApplyMetrics;
use crate::sst::index::fulltext_index::applier::FulltextIndexApplyMetrics;
use crate::sst::index::inverted_index::applier::InvertedIndexApplyMetrics;
use crate::sst::parquet::DEFAULT_ROW_GROUP_SIZE;
use crate::sst::parquet::file_range::FileRange;
use crate::sst::parquet::flat_format::time_index_column_index;
use crate::sst::parquet::reader::{MetadataCacheMetrics, ReaderFilterMetrics, ReaderMetrics};
use crate::sst::parquet::row_group::ParquetFetchMetrics;

/// Per-file scan metrics.
#[derive(Default, Clone)]
pub struct FileScanMetrics {
    /// Number of ranges (row groups) read from this file.
    pub num_ranges: usize,
    /// Number of rows read from this file.
    pub num_rows: usize,
    /// Time spent building file ranges/parts (file-level preparation).
    pub build_part_cost: Duration,
    /// Time spent building readers for this file (accumulated across all ranges).
    pub build_reader_cost: Duration,
    /// Time spent scanning this file (accumulated across all ranges).
    pub scan_cost: Duration,
}

impl fmt::Debug for FileScanMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{{\"build_part_cost\":\"{:?}\"", self.build_part_cost)?;

        if self.num_ranges > 0 {
            write!(f, ", \"num_ranges\":{}", self.num_ranges)?;
        }
        if self.num_rows > 0 {
            write!(f, ", \"num_rows\":{}", self.num_rows)?;
        }
        if !self.build_reader_cost.is_zero() {
            write!(
                f,
                ", \"build_reader_cost\":\"{:?}\"",
                self.build_reader_cost
            )?;
        }
        if !self.scan_cost.is_zero() {
            write!(f, ", \"scan_cost\":\"{:?}\"", self.scan_cost)?;
        }

        write!(f, "}}")
    }
}

impl FileScanMetrics {
    /// Merges another FileMetrics into this one.
    pub(crate) fn merge_from(&mut self, other: &FileScanMetrics) {
        self.num_ranges += other.num_ranges;
        self.num_rows += other.num_rows;
        self.build_part_cost += other.build_part_cost;
        self.build_reader_cost += other.build_reader_cost;
        self.scan_cost += other.scan_cost;
    }
}

/// Verbose scan metrics for a partition.
#[derive(Default)]
pub(crate) struct ScanMetricsSet {
    /// Duration to prepare the scan task.
    prepare_scan_cost: Duration,
    /// Duration to build the (merge) reader.
    build_reader_cost: Duration,
    /// Duration to scan data.
    scan_cost: Duration,
    /// Duration while waiting for `yield`.
    yield_cost: Duration,
    /// Duration of the scan.
    total_cost: Duration,
    /// Number of rows returned.
    num_rows: usize,
    /// Number of batches returned.
    num_batches: usize,
    /// Number of mem ranges scanned.
    num_mem_ranges: usize,
    /// Number of file ranges scanned.
    num_file_ranges: usize,

    // Memtable related metrics:
    /// Duration to scan memtables.
    mem_scan_cost: Duration,
    /// Number of rows read from memtables.
    mem_rows: usize,
    /// Number of batches read from memtables.
    mem_batches: usize,
    /// Number of series read from memtables.
    mem_series: usize,

    // SST related metrics:
    /// Duration to build file ranges.
    build_parts_cost: Duration,
    /// Duration to scan SST files.
    sst_scan_cost: Duration,
    /// Number of row groups before filtering.
    rg_total: usize,
    /// Number of row groups filtered by fulltext index.
    rg_fulltext_filtered: usize,
    /// Number of row groups filtered by inverted index.
    rg_inverted_filtered: usize,
    /// Number of row groups filtered by min-max index.
    rg_minmax_filtered: usize,
    /// Number of row groups filtered by bloom filter index.
    rg_bloom_filtered: usize,
    /// Number of rows in row group before filtering.
    rows_before_filter: usize,
    /// Number of rows in row group filtered by fulltext index.
    rows_fulltext_filtered: usize,
    /// Number of rows in row group filtered by inverted index.
    rows_inverted_filtered: usize,
    /// Number of rows in row group filtered by bloom filter index.
    rows_bloom_filtered: usize,
    /// Number of rows filtered by precise filter.
    rows_precise_filtered: usize,
    /// Number of record batches read from SST.
    num_sst_record_batches: usize,
    /// Number of batches decoded from SST.
    num_sst_batches: usize,
    /// Number of rows read from SST.
    num_sst_rows: usize,

    /// Elapsed time before the first poll operation.
    first_poll: Duration,

    /// Number of send timeout in SeriesScan.
    num_series_send_timeout: usize,
    /// Number of send full in SeriesScan.
    num_series_send_full: usize,
    /// Number of rows the series distributor scanned.
    num_distributor_rows: usize,
    /// Number of batches the series distributor scanned.
    num_distributor_batches: usize,
    /// Duration of the series distributor to scan.
    distributor_scan_cost: Duration,
    /// Duration of the series distributor to yield.
    distributor_yield_cost: Duration,

    /// Merge metrics.
    merge_metrics: MergeMetrics,
    /// Dedup metrics.
    dedup_metrics: DedupMetrics,

    /// The stream reached EOF
    stream_eof: bool,

    // Optional verbose metrics:
    /// Inverted index apply metrics.
    inverted_index_apply_metrics: Option<InvertedIndexApplyMetrics>,
    /// Bloom filter index apply metrics.
    bloom_filter_apply_metrics: Option<BloomFilterIndexApplyMetrics>,
    /// Fulltext index apply metrics.
    fulltext_index_apply_metrics: Option<FulltextIndexApplyMetrics>,
    /// Parquet fetch metrics.
    fetch_metrics: Option<ParquetFetchMetrics>,
    /// Metadata cache metrics.
    metadata_cache_metrics: Option<MetadataCacheMetrics>,
    /// Per-file scan metrics, only populated when explain_verbose is true.
    per_file_metrics: Option<HashMap<RegionFileId, FileScanMetrics>>,
}

/// Wrapper for file metrics that compares by total cost in reverse order.
/// This allows using BinaryHeap as a min-heap for efficient top-K selection.
struct CompareCostReverse<'a> {
    total_cost: Duration,
    file_id: RegionFileId,
    metrics: &'a FileScanMetrics,
}

impl Ord for CompareCostReverse<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse comparison: smaller costs are "greater"
        other.total_cost.cmp(&self.total_cost)
    }
}

impl PartialOrd for CompareCostReverse<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for CompareCostReverse<'_> {}

impl PartialEq for CompareCostReverse<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.total_cost == other.total_cost
    }
}

impl fmt::Debug for ScanMetricsSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ScanMetricsSet {
            prepare_scan_cost,
            build_reader_cost,
            scan_cost,
            yield_cost,
            total_cost,
            num_rows,
            num_batches,
            num_mem_ranges,
            num_file_ranges,
            build_parts_cost,
            sst_scan_cost,
            rg_total,
            rg_fulltext_filtered,
            rg_inverted_filtered,
            rg_minmax_filtered,
            rg_bloom_filtered,
            rows_before_filter,
            rows_fulltext_filtered,
            rows_inverted_filtered,
            rows_bloom_filtered,
            rows_precise_filtered,
            num_sst_record_batches,
            num_sst_batches,
            num_sst_rows,
            first_poll,
            num_series_send_timeout,
            num_series_send_full,
            num_distributor_rows,
            num_distributor_batches,
            distributor_scan_cost,
            distributor_yield_cost,
            merge_metrics,
            dedup_metrics,
            stream_eof,
            mem_scan_cost,
            mem_rows,
            mem_batches,
            mem_series,
            inverted_index_apply_metrics,
            bloom_filter_apply_metrics,
            fulltext_index_apply_metrics,
            fetch_metrics,
            metadata_cache_metrics,
            per_file_metrics,
        } = self;

        // Write core metrics
        write!(
            f,
            "{{\"prepare_scan_cost\":\"{prepare_scan_cost:?}\", \
            \"build_reader_cost\":\"{build_reader_cost:?}\", \
            \"scan_cost\":\"{scan_cost:?}\", \
            \"yield_cost\":\"{yield_cost:?}\", \
            \"total_cost\":\"{total_cost:?}\", \
            \"num_rows\":{num_rows}, \
            \"num_batches\":{num_batches}, \
            \"num_mem_ranges\":{num_mem_ranges}, \
            \"num_file_ranges\":{num_file_ranges}, \
            \"build_parts_cost\":\"{build_parts_cost:?}\", \
            \"sst_scan_cost\":\"{sst_scan_cost:?}\", \
            \"rg_total\":{rg_total}, \
            \"rows_before_filter\":{rows_before_filter}, \
            \"num_sst_record_batches\":{num_sst_record_batches}, \
            \"num_sst_batches\":{num_sst_batches}, \
            \"num_sst_rows\":{num_sst_rows}, \
            \"first_poll\":\"{first_poll:?}\""
        )?;

        // Write non-zero filter counters
        if *rg_fulltext_filtered > 0 {
            write!(f, ", \"rg_fulltext_filtered\":{rg_fulltext_filtered}")?;
        }
        if *rg_inverted_filtered > 0 {
            write!(f, ", \"rg_inverted_filtered\":{rg_inverted_filtered}")?;
        }
        if *rg_minmax_filtered > 0 {
            write!(f, ", \"rg_minmax_filtered\":{rg_minmax_filtered}")?;
        }
        if *rg_bloom_filtered > 0 {
            write!(f, ", \"rg_bloom_filtered\":{rg_bloom_filtered}")?;
        }
        if *rows_fulltext_filtered > 0 {
            write!(f, ", \"rows_fulltext_filtered\":{rows_fulltext_filtered}")?;
        }
        if *rows_inverted_filtered > 0 {
            write!(f, ", \"rows_inverted_filtered\":{rows_inverted_filtered}")?;
        }
        if *rows_bloom_filtered > 0 {
            write!(f, ", \"rows_bloom_filtered\":{rows_bloom_filtered}")?;
        }
        if *rows_precise_filtered > 0 {
            write!(f, ", \"rows_precise_filtered\":{rows_precise_filtered}")?;
        }

        // Write non-zero distributor metrics
        if *num_series_send_timeout > 0 {
            write!(f, ", \"num_series_send_timeout\":{num_series_send_timeout}")?;
        }
        if *num_series_send_full > 0 {
            write!(f, ", \"num_series_send_full\":{num_series_send_full}")?;
        }
        if *num_distributor_rows > 0 {
            write!(f, ", \"num_distributor_rows\":{num_distributor_rows}")?;
        }
        if *num_distributor_batches > 0 {
            write!(f, ", \"num_distributor_batches\":{num_distributor_batches}")?;
        }
        if !distributor_scan_cost.is_zero() {
            write!(
                f,
                ", \"distributor_scan_cost\":\"{distributor_scan_cost:?}\""
            )?;
        }
        if !distributor_yield_cost.is_zero() {
            write!(
                f,
                ", \"distributor_yield_cost\":\"{distributor_yield_cost:?}\""
            )?;
        }

        // Write non-zero memtable metrics
        if *mem_rows > 0 {
            write!(f, ", \"mem_rows\":{mem_rows}")?;
        }
        if *mem_batches > 0 {
            write!(f, ", \"mem_batches\":{mem_batches}")?;
        }
        if *mem_series > 0 {
            write!(f, ", \"mem_series\":{mem_series}")?;
        }
        if !mem_scan_cost.is_zero() {
            write!(f, ", \"mem_scan_cost\":\"{mem_scan_cost:?}\"")?;
        }

        // Write optional verbose metrics if they are not empty
        if let Some(metrics) = inverted_index_apply_metrics
            && !metrics.is_empty()
        {
            write!(f, ", \"inverted_index_apply_metrics\":{:?}", metrics)?;
        }
        if let Some(metrics) = bloom_filter_apply_metrics
            && !metrics.is_empty()
        {
            write!(f, ", \"bloom_filter_apply_metrics\":{:?}", metrics)?;
        }
        if let Some(metrics) = fulltext_index_apply_metrics
            && !metrics.is_empty()
        {
            write!(f, ", \"fulltext_index_apply_metrics\":{:?}", metrics)?;
        }
        if let Some(metrics) = fetch_metrics
            && !metrics.is_empty()
        {
            write!(f, ", \"fetch_metrics\":{:?}", metrics)?;
        }
        if let Some(metrics) = metadata_cache_metrics
            && !metrics.is_empty()
        {
            write!(f, ", \"metadata_cache_metrics\":{:?}", metrics)?;
        }

        // Write merge metrics if not empty
        if !merge_metrics.scan_cost.is_zero() {
            write!(f, ", \"merge_metrics\":{:?}", merge_metrics)?;
        }

        // Write dedup metrics if not empty
        if !dedup_metrics.dedup_cost.is_zero() {
            write!(f, ", \"dedup_metrics\":{:?}", dedup_metrics)?;
        }

        // Write top file metrics if present and non-empty
        if let Some(file_metrics) = per_file_metrics
            && !file_metrics.is_empty()
        {
            // Use min-heap (BinaryHeap with reverse comparison) to keep only top 10
            let mut heap = BinaryHeap::new();
            for (file_id, metrics) in file_metrics.iter() {
                let total_cost =
                    metrics.build_part_cost + metrics.build_reader_cost + metrics.scan_cost;

                if heap.len() < 10 {
                    // Haven't reached 10 yet, just push
                    heap.push(CompareCostReverse {
                        total_cost,
                        file_id: *file_id,
                        metrics,
                    });
                } else if let Some(min_entry) = heap.peek() {
                    // If current cost is higher than the minimum in our top-10, replace it
                    if total_cost > min_entry.total_cost {
                        heap.pop();
                        heap.push(CompareCostReverse {
                            total_cost,
                            file_id: *file_id,
                            metrics,
                        });
                    }
                }
            }

            let top_files = heap.into_sorted_vec();
            write!(f, ", \"top_file_metrics\": {{")?;
            for (i, item) in top_files.iter().enumerate() {
                let CompareCostReverse {
                    total_cost: _,
                    file_id,
                    metrics,
                } = item;
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "\"{}\": {:?}", file_id, metrics)?;
            }
            write!(f, "}}")?;
        }

        write!(f, ", \"stream_eof\":{stream_eof}}}")
    }
}
impl ScanMetricsSet {
    /// Attaches the `prepare_scan_cost` to the metrics set.
    fn with_prepare_scan_cost(mut self, cost: Duration) -> Self {
        self.prepare_scan_cost += cost;
        self
    }

    /// Merges the local scanner metrics.
    fn merge_scanner_metrics(&mut self, other: &ScannerMetrics) {
        let ScannerMetrics {
            prepare_scan_cost,
            build_reader_cost,
            scan_cost,
            yield_cost,
            num_batches,
            num_rows,
            num_mem_ranges,
            num_file_ranges,
        } = other;

        self.prepare_scan_cost += *prepare_scan_cost;
        self.build_reader_cost += *build_reader_cost;
        self.scan_cost += *scan_cost;
        self.yield_cost += *yield_cost;
        self.num_rows += *num_rows;
        self.num_batches += *num_batches;
        self.num_mem_ranges += *num_mem_ranges;
        self.num_file_ranges += *num_file_ranges;
    }

    /// Merges the local reader metrics.
    fn merge_reader_metrics(&mut self, other: &ReaderMetrics) {
        let ReaderMetrics {
            build_cost,
            filter_metrics:
                ReaderFilterMetrics {
                    rg_total,
                    rg_fulltext_filtered,
                    rg_inverted_filtered,
                    rg_minmax_filtered,
                    rg_bloom_filtered,
                    rows_total,
                    rows_fulltext_filtered,
                    rows_inverted_filtered,
                    rows_bloom_filtered,
                    rows_precise_filtered,
                    inverted_index_apply_metrics,
                    bloom_filter_apply_metrics,
                    fulltext_index_apply_metrics,
                },
            num_record_batches,
            num_batches,
            num_rows,
            scan_cost,
            metadata_cache_metrics,
            fetch_metrics,
        } = other;

        self.build_parts_cost += *build_cost;
        self.sst_scan_cost += *scan_cost;

        self.rg_total += *rg_total;
        self.rg_fulltext_filtered += *rg_fulltext_filtered;
        self.rg_inverted_filtered += *rg_inverted_filtered;
        self.rg_minmax_filtered += *rg_minmax_filtered;
        self.rg_bloom_filtered += *rg_bloom_filtered;

        self.rows_before_filter += *rows_total;
        self.rows_fulltext_filtered += *rows_fulltext_filtered;
        self.rows_inverted_filtered += *rows_inverted_filtered;
        self.rows_bloom_filtered += *rows_bloom_filtered;
        self.rows_precise_filtered += *rows_precise_filtered;

        self.num_sst_record_batches += *num_record_batches;
        self.num_sst_batches += *num_batches;
        self.num_sst_rows += *num_rows;

        // Merge optional verbose metrics
        if let Some(metrics) = inverted_index_apply_metrics {
            self.inverted_index_apply_metrics
                .get_or_insert_with(InvertedIndexApplyMetrics::default)
                .merge_from(metrics);
        }
        if let Some(metrics) = bloom_filter_apply_metrics {
            self.bloom_filter_apply_metrics
                .get_or_insert_with(BloomFilterIndexApplyMetrics::default)
                .merge_from(metrics);
        }
        if let Some(metrics) = fulltext_index_apply_metrics {
            self.fulltext_index_apply_metrics
                .get_or_insert_with(FulltextIndexApplyMetrics::default)
                .merge_from(metrics);
        }
        if let Some(metrics) = fetch_metrics {
            self.fetch_metrics
                .get_or_insert_with(ParquetFetchMetrics::default)
                .merge_from(metrics);
        }
        self.metadata_cache_metrics
            .get_or_insert_with(MetadataCacheMetrics::default)
            .merge_from(metadata_cache_metrics);
    }

    /// Merges per-file metrics.
    fn merge_per_file_metrics(&mut self, other: &HashMap<RegionFileId, FileScanMetrics>) {
        let self_file_metrics = self.per_file_metrics.get_or_insert_with(HashMap::new);
        for (file_id, metrics) in other {
            self_file_metrics
                .entry(*file_id)
                .or_default()
                .merge_from(metrics);
        }
    }

    /// Sets distributor metrics.
    fn set_distributor_metrics(&mut self, distributor_metrics: &SeriesDistributorMetrics) {
        let SeriesDistributorMetrics {
            num_series_send_timeout,
            num_series_send_full,
            num_rows,
            num_batches,
            scan_cost,
            yield_cost,
        } = distributor_metrics;

        self.num_series_send_timeout += *num_series_send_timeout;
        self.num_series_send_full += *num_series_send_full;
        self.num_distributor_rows += *num_rows;
        self.num_distributor_batches += *num_batches;
        self.distributor_scan_cost += *scan_cost;
        self.distributor_yield_cost += *yield_cost;
    }

    /// Observes metrics.
    fn observe_metrics(&self) {
        READ_STAGE_ELAPSED
            .with_label_values(&["prepare_scan"])
            .observe(self.prepare_scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["build_reader"])
            .observe(self.build_reader_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["scan"])
            .observe(self.scan_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["yield"])
            .observe(self.yield_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["total"])
            .observe(self.total_cost.as_secs_f64());
        READ_ROWS_RETURN.observe(self.num_rows as f64);
        READ_BATCHES_RETURN.observe(self.num_batches as f64);

        READ_STAGE_ELAPSED
            .with_label_values(&["build_parts"])
            .observe(self.build_parts_cost.as_secs_f64());

        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(self.rg_total as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["fulltext_index_filtered"])
            .inc_by(self.rg_fulltext_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(self.rg_inverted_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["minmax_index_filtered"])
            .inc_by(self.rg_minmax_filtered as u64);
        READ_ROW_GROUPS_TOTAL
            .with_label_values(&["bloom_filter_index_filtered"])
            .inc_by(self.rg_bloom_filtered as u64);

        PRECISE_FILTER_ROWS_TOTAL
            .with_label_values(&["parquet"])
            .inc_by(self.rows_precise_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["before_filtering"])
            .inc_by(self.rows_before_filter as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["fulltext_index_filtered"])
            .inc_by(self.rows_fulltext_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["inverted_index_filtered"])
            .inc_by(self.rows_inverted_filtered as u64);
        READ_ROWS_IN_ROW_GROUP_TOTAL
            .with_label_values(&["bloom_filter_index_filtered"])
            .inc_by(self.rows_bloom_filtered as u64);
    }
}

struct PartitionMetricsInner {
    region_id: RegionId,
    /// Index of the partition to scan.
    partition: usize,
    /// Label to distinguish different scan operation.
    scanner_type: &'static str,
    /// Query start time.
    query_start: Instant,
    /// Whether to use verbose logging.
    explain_verbose: bool,
    /// Verbose scan metrics that only log to debug logs by default.
    metrics: Mutex<ScanMetricsSet>,
    in_progress_scan: IntGauge,

    // Normal metrics that always report to the [ExecutionPlanMetricsSet]:
    /// Duration to build file ranges.
    build_parts_cost: Time,
    /// Duration to build the (merge) reader.
    build_reader_cost: Time,
    /// Duration to scan data.
    scan_cost: Time,
    /// Duration while waiting for `yield`.
    yield_cost: Time,
    /// Duration to convert [`Batch`]es.
    convert_cost: Time,
    /// Aggregated compute time reported to DataFusion.
    elapsed_compute: Time,
}

impl PartitionMetricsInner {
    fn on_finish(&self, stream_eof: bool) {
        let mut metrics = self.metrics.lock().unwrap();
        if metrics.total_cost.is_zero() {
            metrics.total_cost = self.query_start.elapsed();
        }
        if !metrics.stream_eof {
            metrics.stream_eof = stream_eof;
        }
    }
}

impl MergeMetricsReport for PartitionMetricsInner {
    fn report(&self, metrics: &mut MergeMetrics) {
        let mut scan_metrics = self.metrics.lock().unwrap();
        // Merge the metrics into scan_metrics
        scan_metrics.merge_metrics.merge(metrics);

        // Reset the input metrics
        *metrics = MergeMetrics::default();
    }
}

impl DedupMetricsReport for PartitionMetricsInner {
    fn report(&self, metrics: &mut DedupMetrics) {
        let mut scan_metrics = self.metrics.lock().unwrap();
        // Merge the metrics into scan_metrics
        scan_metrics.dedup_metrics.merge(metrics);

        // Reset the input metrics
        *metrics = DedupMetrics::default();
    }
}

impl Drop for PartitionMetricsInner {
    fn drop(&mut self) {
        self.on_finish(false);
        let metrics = self.metrics.lock().unwrap();
        metrics.observe_metrics();
        self.in_progress_scan.dec();

        if self.explain_verbose {
            common_telemetry::info!(
                "{} finished, region_id: {}, partition: {}, scan_metrics: {:?}, convert_batch_costs: {}",
                self.scanner_type,
                self.region_id,
                self.partition,
                metrics,
                self.convert_cost,
            );
        } else {
            common_telemetry::debug!(
                "{} finished, region_id: {}, partition: {}, scan_metrics: {:?}, convert_batch_costs: {}",
                self.scanner_type,
                self.region_id,
                self.partition,
                metrics,
                self.convert_cost,
            );
        }
    }
}

/// List of PartitionMetrics.
#[derive(Default)]
pub(crate) struct PartitionMetricsList(Mutex<Vec<Option<PartitionMetrics>>>);

impl PartitionMetricsList {
    /// Sets a new [PartitionMetrics] at the specified partition.
    pub(crate) fn set(&self, partition: usize, metrics: PartitionMetrics) {
        let mut list = self.0.lock().unwrap();
        if list.len() <= partition {
            list.resize(partition + 1, None);
        }
        list[partition] = Some(metrics);
    }

    /// Format verbose metrics for each partition for explain.
    pub(crate) fn format_verbose_metrics(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let list = self.0.lock().unwrap();
        write!(f, ", \"metrics_per_partition\": ")?;
        f.debug_list()
            .entries(list.iter().filter_map(|p| p.as_ref()))
            .finish()?;
        write!(f, "}}")
    }
}

/// Metrics while reading a partition.
#[derive(Clone)]
pub struct PartitionMetrics(Arc<PartitionMetricsInner>);

impl PartitionMetrics {
    pub(crate) fn new(
        region_id: RegionId,
        partition: usize,
        scanner_type: &'static str,
        query_start: Instant,
        explain_verbose: bool,
        metrics_set: &ExecutionPlanMetricsSet,
    ) -> Self {
        let partition_str = partition.to_string();
        let in_progress_scan = IN_PROGRESS_SCAN.with_label_values(&[scanner_type, &partition_str]);
        in_progress_scan.inc();
        let metrics = ScanMetricsSet::default().with_prepare_scan_cost(query_start.elapsed());
        let inner = PartitionMetricsInner {
            region_id,
            partition,
            scanner_type,
            query_start,
            explain_verbose,
            metrics: Mutex::new(metrics),
            in_progress_scan,
            build_parts_cost: MetricBuilder::new(metrics_set)
                .subset_time("build_parts_cost", partition),
            build_reader_cost: MetricBuilder::new(metrics_set)
                .subset_time("build_reader_cost", partition),
            scan_cost: MetricBuilder::new(metrics_set).subset_time("scan_cost", partition),
            yield_cost: MetricBuilder::new(metrics_set).subset_time("yield_cost", partition),
            convert_cost: MetricBuilder::new(metrics_set).subset_time("convert_cost", partition),
            elapsed_compute: MetricBuilder::new(metrics_set).elapsed_compute(partition),
        };
        Self(Arc::new(inner))
    }

    pub(crate) fn on_first_poll(&self) {
        let mut metrics = self.0.metrics.lock().unwrap();
        metrics.first_poll = self.0.query_start.elapsed();
    }

    pub(crate) fn inc_num_mem_ranges(&self, num: usize) {
        let mut metrics = self.0.metrics.lock().unwrap();
        metrics.num_mem_ranges += num;
    }

    pub fn inc_num_file_ranges(&self, num: usize) {
        let mut metrics = self.0.metrics.lock().unwrap();
        metrics.num_file_ranges += num;
    }

    fn record_elapsed_compute(&self, duration: Duration) {
        if duration.is_zero() {
            return;
        }
        self.0.elapsed_compute.add_duration(duration);
    }

    /// Merges `build_reader_cost`.
    pub(crate) fn inc_build_reader_cost(&self, cost: Duration) {
        self.0.build_reader_cost.add_duration(cost);

        let mut metrics = self.0.metrics.lock().unwrap();
        metrics.build_reader_cost += cost;
    }

    pub(crate) fn inc_convert_batch_cost(&self, cost: Duration) {
        self.0.convert_cost.add_duration(cost);
        self.record_elapsed_compute(cost);
    }

    /// Reports memtable scan metrics.
    pub(crate) fn report_mem_scan_metrics(&self, data: &crate::memtable::MemScanMetricsData) {
        let mut metrics = self.0.metrics.lock().unwrap();
        metrics.mem_scan_cost += data.scan_cost;
        metrics.mem_rows += data.num_rows;
        metrics.mem_batches += data.num_batches;
        metrics.mem_series += data.total_series;
    }

    /// Merges [ScannerMetrics], `build_reader_cost`, `scan_cost` and `yield_cost`.
    pub(crate) fn merge_metrics(&self, metrics: &ScannerMetrics) {
        self.0
            .build_reader_cost
            .add_duration(metrics.build_reader_cost);
        self.0.scan_cost.add_duration(metrics.scan_cost);
        self.record_elapsed_compute(metrics.scan_cost);
        self.0.yield_cost.add_duration(metrics.yield_cost);
        self.record_elapsed_compute(metrics.yield_cost);

        let mut metrics_set = self.0.metrics.lock().unwrap();
        metrics_set.merge_scanner_metrics(metrics);
    }

    /// Merges [ReaderMetrics] and `build_reader_cost`.
    pub fn merge_reader_metrics(
        &self,
        metrics: &ReaderMetrics,
        per_file_metrics: Option<&HashMap<RegionFileId, FileScanMetrics>>,
    ) {
        self.0.build_parts_cost.add_duration(metrics.build_cost);

        let mut metrics_set = self.0.metrics.lock().unwrap();
        metrics_set.merge_reader_metrics(metrics);

        // Merge per-file metrics if provided
        if let Some(file_metrics) = per_file_metrics {
            metrics_set.merge_per_file_metrics(file_metrics);
        }
    }

    /// Finishes the query.
    pub(crate) fn on_finish(&self) {
        self.0.on_finish(true);
    }

    /// Sets the distributor metrics.
    pub(crate) fn set_distributor_metrics(&self, metrics: &SeriesDistributorMetrics) {
        let mut metrics_set = self.0.metrics.lock().unwrap();
        metrics_set.set_distributor_metrics(metrics);
    }

    /// Returns whether verbose explain is enabled.
    pub(crate) fn explain_verbose(&self) -> bool {
        self.0.explain_verbose
    }

    /// Returns a MergeMetricsReport trait object for reporting merge metrics.
    pub(crate) fn merge_metrics_reporter(&self) -> Arc<dyn MergeMetricsReport> {
        self.0.clone()
    }

    /// Returns a DedupMetricsReport trait object for reporting dedup metrics.
    pub(crate) fn dedup_metrics_reporter(&self) -> Arc<dyn DedupMetricsReport> {
        self.0.clone()
    }
}

impl fmt::Debug for PartitionMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let metrics = self.0.metrics.lock().unwrap();
        write!(
            f,
            r#"{{"partition":{}, "metrics":{:?}}}"#,
            self.0.partition, metrics
        )
    }
}

/// Metrics for the series distributor.
#[derive(Default)]
pub(crate) struct SeriesDistributorMetrics {
    /// Number of send timeout in SeriesScan.
    pub(crate) num_series_send_timeout: usize,
    /// Number of send full in SeriesScan.
    pub(crate) num_series_send_full: usize,
    /// Number of rows the series distributor scanned.
    pub(crate) num_rows: usize,
    /// Number of batches the series distributor scanned.
    pub(crate) num_batches: usize,
    /// Duration of the series distributor to scan.
    pub(crate) scan_cost: Duration,
    /// Duration of the series distributor to yield.
    pub(crate) yield_cost: Duration,
}

/// Scans memtable ranges at `index`.
#[tracing::instrument(
    skip_all,
    fields(
        region_id = %stream_ctx.input.region_metadata().region_id,
        file_or_mem_index = %index.index,
        row_group_index = %index.row_group_index,
        source = "mem"
    )
)]
pub(crate) fn scan_mem_ranges(
    stream_ctx: Arc<StreamContext>,
    part_metrics: PartitionMetrics,
    index: RowGroupIndex,
    time_range: FileTimeRange,
) -> impl Stream<Item = Result<Batch>> {
    try_stream! {
        let ranges = stream_ctx.input.build_mem_ranges(index);
        part_metrics.inc_num_mem_ranges(ranges.len());
        for range in ranges {
            let build_reader_start = Instant::now();
            let mem_scan_metrics = Some(MemScanMetrics::default());
            let iter = range.build_prune_iter(time_range, mem_scan_metrics.clone())?;
            part_metrics.inc_build_reader_cost(build_reader_start.elapsed());

            let mut source = Source::Iter(iter);
            while let Some(batch) = source.next_batch().await? {
                yield batch;
            }

            // Report the memtable scan metrics to partition metrics
            if let Some(ref metrics) = mem_scan_metrics {
                let data = metrics.data();
                part_metrics.report_mem_scan_metrics(&data);
            }
        }
    }
}

/// Scans memtable ranges at `index` using flat format that returns RecordBatch.
#[tracing::instrument(
    skip_all,
    fields(
        region_id = %stream_ctx.input.region_metadata().region_id,
        row_group_index = %index.index,
        source = "mem_flat"
    )
)]
pub(crate) fn scan_flat_mem_ranges(
    stream_ctx: Arc<StreamContext>,
    part_metrics: PartitionMetrics,
    index: RowGroupIndex,
) -> impl Stream<Item = Result<RecordBatch>> {
    try_stream! {
        let ranges = stream_ctx.input.build_mem_ranges(index);
        part_metrics.inc_num_mem_ranges(ranges.len());
        for range in ranges {
            let build_reader_start = Instant::now();
            let mem_scan_metrics = Some(MemScanMetrics::default());
            let mut iter = range.build_record_batch_iter(mem_scan_metrics.clone())?;
            part_metrics.inc_build_reader_cost(build_reader_start.elapsed());

            while let Some(record_batch) = iter.next().transpose()? {
                yield record_batch;
            }

            // Report the memtable scan metrics to partition metrics
            if let Some(ref metrics) = mem_scan_metrics {
                let data = metrics.data();
                part_metrics.report_mem_scan_metrics(&data);
            }
        }
    }
}

/// Files with row count greater than this threshold can contribute to the estimation.
const SPLIT_ROW_THRESHOLD: u64 = DEFAULT_ROW_GROUP_SIZE as u64;
/// Number of series threshold for splitting batches.
const NUM_SERIES_THRESHOLD: u64 = 10240;
/// Minimum batch size after splitting. The batch size is less than 60 because a series may only have
/// 60 samples per hour.
const BATCH_SIZE_THRESHOLD: u64 = 50;

/// Returns true if splitting flat record batches may improve merge performance.
pub(crate) fn should_split_flat_batches_for_merge(
    stream_ctx: &Arc<StreamContext>,
    range_meta: &RangeMeta,
) -> bool {
    // Number of files to split and scan.
    let mut num_files_to_split = 0;
    let mut num_mem_rows = 0;
    let mut num_mem_series = 0;
    // Checks each file range, returns early if any range is not splittable.
    // For mem ranges, we collect the total number of rows and series because the number of rows in a
    // mem range may be too small.
    for index in &range_meta.row_group_indices {
        if stream_ctx.is_mem_range_index(*index) {
            let memtable = &stream_ctx.input.memtables[index.index];
            // Is mem range
            let stats = memtable.stats();
            num_mem_rows += stats.num_rows();
            num_mem_series += stats.series_count();
        } else if stream_ctx.is_file_range_index(*index) {
            // This is a file range.
            let file_index = index.index - stream_ctx.input.num_memtables();
            let file = &stream_ctx.input.files[file_index];
            if file.meta_ref().num_rows < SPLIT_ROW_THRESHOLD || file.meta_ref().num_series == 0 {
                // If the file doesn't have enough rows, or the number of series is unavailable, skips it.
                continue;
            }
            debug_assert!(file.meta_ref().num_rows > 0);
            if !can_split_series(file.meta_ref().num_rows, file.meta_ref().num_series) {
                // We can't split batches in a file.
                return false;
            } else {
                num_files_to_split += 1;
            }
        }
        // Skips non-file and non-mem ranges.
    }

    if num_files_to_split > 0 {
        // We mainly consider file ranges because they have enough data for sampling.
        true
    } else if num_mem_series > 0 && num_mem_rows > 0 {
        // If we don't have files to scan, we check whether to split by the memtable.
        can_split_series(num_mem_rows as u64, num_mem_series as u64)
    } else {
        false
    }
}

fn can_split_series(num_rows: u64, num_series: u64) -> bool {
    assert!(num_series > 0);
    assert!(num_rows > 0);

    // It doesn't have too many series or it will have enough rows for each batch.
    num_series < NUM_SERIES_THRESHOLD || num_rows / num_series >= BATCH_SIZE_THRESHOLD
}

/// Creates a new [ReaderFilterMetrics] with optional apply metrics initialized
/// based on the `explain_verbose` flag.
fn new_filter_metrics(explain_verbose: bool) -> ReaderFilterMetrics {
    if explain_verbose {
        ReaderFilterMetrics {
            inverted_index_apply_metrics: Some(InvertedIndexApplyMetrics::default()),
            bloom_filter_apply_metrics: Some(BloomFilterIndexApplyMetrics::default()),
            fulltext_index_apply_metrics: Some(FulltextIndexApplyMetrics::default()),
            ..Default::default()
        }
    } else {
        ReaderFilterMetrics::default()
    }
}

/// Scans file ranges at `index`.
#[tracing::instrument(
    skip_all,
    fields(
        region_id = %stream_ctx.input.region_metadata().region_id,
        row_group_index = %index.index,
        source = read_type
    )
)]
pub(crate) async fn scan_file_ranges(
    stream_ctx: Arc<StreamContext>,
    part_metrics: PartitionMetrics,
    index: RowGroupIndex,
    read_type: &'static str,
    range_builder: Arc<RangeBuilderList>,
) -> Result<impl Stream<Item = Result<Batch>>> {
    let mut reader_metrics = ReaderMetrics {
        filter_metrics: new_filter_metrics(part_metrics.explain_verbose()),
        ..Default::default()
    };
    let ranges = range_builder
        .build_file_ranges(&stream_ctx.input, index, &mut reader_metrics)
        .await?;
    part_metrics.inc_num_file_ranges(ranges.len());
    part_metrics.merge_reader_metrics(&reader_metrics, None);

    // Creates initial per-file metrics with build_part_cost.
    let init_per_file_metrics = if part_metrics.explain_verbose() {
        let file = stream_ctx.input.file_from_index(index);
        let file_id = file.file_id();

        let mut map = HashMap::new();
        map.insert(
            file_id,
            FileScanMetrics {
                build_part_cost: reader_metrics.build_cost,
                ..Default::default()
            },
        );
        Some(map)
    } else {
        None
    };

    Ok(build_file_range_scan_stream(
        stream_ctx,
        part_metrics,
        read_type,
        ranges,
        init_per_file_metrics,
    ))
}

/// Scans file ranges at `index` using flat reader that returns RecordBatch.
#[tracing::instrument(
    skip_all,
    fields(
        region_id = %stream_ctx.input.region_metadata().region_id,
        row_group_index = %index.index,
        source = read_type
    )
)]
pub(crate) async fn scan_flat_file_ranges(
    stream_ctx: Arc<StreamContext>,
    part_metrics: PartitionMetrics,
    index: RowGroupIndex,
    read_type: &'static str,
    range_builder: Arc<RangeBuilderList>,
) -> Result<impl Stream<Item = Result<RecordBatch>>> {
    let mut reader_metrics = ReaderMetrics {
        filter_metrics: new_filter_metrics(part_metrics.explain_verbose()),
        ..Default::default()
    };
    let ranges = range_builder
        .build_file_ranges(&stream_ctx.input, index, &mut reader_metrics)
        .await?;
    part_metrics.inc_num_file_ranges(ranges.len());
    part_metrics.merge_reader_metrics(&reader_metrics, None);

    // Creates initial per-file metrics with build_part_cost.
    let init_per_file_metrics = if part_metrics.explain_verbose() {
        let file = stream_ctx.input.file_from_index(index);
        let file_id = file.file_id();

        let mut map = HashMap::new();
        map.insert(
            file_id,
            FileScanMetrics {
                build_part_cost: reader_metrics.build_cost,
                ..Default::default()
            },
        );
        Some(map)
    } else {
        None
    };

    Ok(build_flat_file_range_scan_stream(
        stream_ctx,
        part_metrics,
        read_type,
        ranges,
        init_per_file_metrics,
    ))
}

/// Build the stream of scanning the input [`FileRange`]s.
#[tracing::instrument(
    skip_all,
    fields(read_type = read_type, range_count = ranges.len())
)]
pub fn build_file_range_scan_stream(
    stream_ctx: Arc<StreamContext>,
    part_metrics: PartitionMetrics,
    read_type: &'static str,
    ranges: SmallVec<[FileRange; 2]>,
    mut per_file_metrics: Option<HashMap<RegionFileId, FileScanMetrics>>,
) -> impl Stream<Item = Result<Batch>> {
    try_stream! {
        let fetch_metrics = if part_metrics.explain_verbose() {
            Some(Arc::new(ParquetFetchMetrics::default()))
        } else {
            None
        };
        let reader_metrics = &mut ReaderMetrics {
            fetch_metrics: fetch_metrics.clone(),
            ..Default::default()
        };
        for range in ranges {
            let build_reader_start = Instant::now();
            let Some(reader) = range.reader(stream_ctx.input.series_row_selector, fetch_metrics.as_deref()).await? else {
                continue;
            };
            let build_cost = build_reader_start.elapsed();
            part_metrics.inc_build_reader_cost(build_cost);
            let compat_batch = range.compat_batch();
            let mut source = Source::PruneReader(reader);
            while let Some(mut batch) = source.next_batch().await? {
                if let Some(compact_batch) = compat_batch {
                    batch = compact_batch.as_primary_key().unwrap().compat_batch(batch)?;
                }
                yield batch;
            }
            if let Source::PruneReader(reader) = source {
                let prune_metrics = reader.metrics();

                // Update per-file metrics if tracking is enabled
                if let Some(file_metrics_map) = per_file_metrics.as_mut() {
                    let file_id = range.file_handle().file_id();
                    let file_metrics = file_metrics_map
                        .entry(file_id)
                        .or_insert_with(FileScanMetrics::default);

                    file_metrics.num_ranges += 1;
                    file_metrics.num_rows += prune_metrics.num_rows;
                    file_metrics.build_reader_cost += build_cost;
                    file_metrics.scan_cost += prune_metrics.scan_cost;
                }

                reader_metrics.merge_from(&prune_metrics);
            }
        }

        // Reports metrics.
        reader_metrics.observe_rows(read_type);
        reader_metrics.filter_metrics.observe();
        part_metrics.merge_reader_metrics(reader_metrics, per_file_metrics.as_ref());
    }
}

/// Build the stream of scanning the input [`FileRange`]s using flat reader that returns RecordBatch.
#[tracing::instrument(
    skip_all,
    fields(read_type = read_type, range_count = ranges.len())
)]
pub fn build_flat_file_range_scan_stream(
    _stream_ctx: Arc<StreamContext>,
    part_metrics: PartitionMetrics,
    read_type: &'static str,
    ranges: SmallVec<[FileRange; 2]>,
    mut per_file_metrics: Option<HashMap<RegionFileId, FileScanMetrics>>,
) -> impl Stream<Item = Result<RecordBatch>> {
    try_stream! {
        let fetch_metrics = if part_metrics.explain_verbose() {
            Some(Arc::new(ParquetFetchMetrics::default()))
        } else {
            None
        };
        let reader_metrics = &mut ReaderMetrics {
            fetch_metrics: fetch_metrics.clone(),
            ..Default::default()
        };
        for range in ranges {
            let build_reader_start = Instant::now();
            let Some(mut reader) = range.flat_reader(fetch_metrics.as_deref()).await? else{continue};
            let build_cost = build_reader_start.elapsed();
            part_metrics.inc_build_reader_cost(build_cost);

            let may_compat = range
                .compat_batch()
                .map(|compat| {
                    compat.as_flat().context(UnexpectedSnafu {
                        reason: "Invalid compat for flat format",
                    })
                })
                .transpose()?;
            while let Some(record_batch) = reader.next_batch()? {
                if let Some(flat_compat) = may_compat {
                    let batch = flat_compat.compat(record_batch)?;
                    yield batch;
                } else {
                    yield record_batch;
                }
            }

            let prune_metrics = reader.metrics();

            // Update per-file metrics if tracking is enabled
            if let Some(file_metrics_map) = per_file_metrics.as_mut() {
                let file_id = range.file_handle().file_id();
                let file_metrics = file_metrics_map
                    .entry(file_id)
                    .or_insert_with(FileScanMetrics::default);

                file_metrics.num_ranges += 1;
                file_metrics.num_rows += prune_metrics.num_rows;
                file_metrics.build_reader_cost += build_cost;
                file_metrics.scan_cost += prune_metrics.scan_cost;
            }

            reader_metrics.merge_from(&prune_metrics);
        }

        // Reports metrics.
        reader_metrics.observe_rows(read_type);
        reader_metrics.filter_metrics.observe();
        part_metrics.merge_reader_metrics(reader_metrics, per_file_metrics.as_ref());
    }
}

/// Build the stream of scanning the extension range denoted by the [`RowGroupIndex`].
#[cfg(feature = "enterprise")]
pub(crate) async fn scan_extension_range(
    context: Arc<StreamContext>,
    index: RowGroupIndex,
    partition_metrics: PartitionMetrics,
) -> Result<BoxedBatchStream> {
    use snafu::ResultExt;

    let range = context.input.extension_range(index.index);
    let reader = range.reader(context.as_ref());
    let stream = reader
        .read(context, partition_metrics, index)
        .await
        .context(crate::error::ScanExternalRangeSnafu)?;
    Ok(stream)
}

pub(crate) async fn maybe_scan_other_ranges(
    context: &Arc<StreamContext>,
    index: RowGroupIndex,
    metrics: &PartitionMetrics,
) -> Result<BoxedBatchStream> {
    #[cfg(feature = "enterprise")]
    {
        scan_extension_range(context.clone(), index, metrics.clone()).await
    }

    #[cfg(not(feature = "enterprise"))]
    {
        let _ = context;
        let _ = index;
        let _ = metrics;

        crate::error::UnexpectedSnafu {
            reason: "no other ranges scannable",
        }
        .fail()
    }
}

pub(crate) async fn maybe_scan_flat_other_ranges(
    context: &Arc<StreamContext>,
    index: RowGroupIndex,
    metrics: &PartitionMetrics,
) -> Result<BoxedRecordBatchStream> {
    let _ = context;
    let _ = index;
    let _ = metrics;

    crate::error::UnexpectedSnafu {
        reason: "no other ranges scannable in flat format",
    }
    .fail()
}

/// A stream wrapper that splits record batches from an inner stream.
pub(crate) struct SplitRecordBatchStream<S> {
    /// The inner stream that yields record batches.
    inner: S,
    /// Buffer for split batches.
    batches: VecDeque<RecordBatch>,
}

impl<S> SplitRecordBatchStream<S> {
    /// Creates a new splitting stream wrapper.
    pub(crate) fn new(inner: S) -> Self {
        Self {
            inner,
            batches: VecDeque::new(),
        }
    }
}

impl<S> Stream for SplitRecordBatchStream<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            // First, check if we have buffered split batches
            if let Some(batch) = self.batches.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }

            // Poll the inner stream for the next batch
            let record_batch = match futures::ready!(Pin::new(&mut self.inner).poll_next(cx)) {
                Some(Ok(batch)) => batch,
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => return Poll::Ready(None),
            };

            // Split the batch and buffer the results
            split_record_batch(record_batch, &mut self.batches);
            // Continue the loop to return the first split batch
        }
    }
}

/// Splits the batch by timestamps.
///
/// # Panics
/// Panics if the timestamp array is invalid.
pub(crate) fn split_record_batch(record_batch: RecordBatch, batches: &mut VecDeque<RecordBatch>) {
    let batch_rows = record_batch.num_rows();
    if batch_rows == 0 {
        return;
    }
    if batch_rows < 2 {
        batches.push_back(record_batch);
        return;
    }

    let time_index_pos = time_index_column_index(record_batch.num_columns());
    let timestamps = record_batch.column(time_index_pos);
    let (ts_values, _unit) = timestamp_array_to_primitive(timestamps).unwrap();
    let mut offsets = Vec::with_capacity(16);
    offsets.push(0);
    let values = ts_values.values();
    for (i, &value) in values.iter().take(batch_rows - 1).enumerate() {
        if value > values[i + 1] {
            offsets.push(i + 1);
        }
    }
    offsets.push(values.len());

    // Splits the batch by offsets.
    for (i, &start) in offsets[..offsets.len() - 1].iter().enumerate() {
        let end = offsets[i + 1];
        let rows_in_batch = end - start;
        batches.push_back(record_batch.slice(start, rows_in_batch));
    }
}
